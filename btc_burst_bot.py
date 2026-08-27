#!/usr/bin/env python3
"""
BTC BURST / volatility-capture bot (v5) — LIVE execution + rich logging.

Reuses strategy_bot.py's feeds/CLOB/helpers. Different strategy: at open+120s into a
BTC 15m window, predict the volatility-burst direction (XGBoost, 6 chainlink features),
buy that side at market ($6 flat), then EXIT with TP +30% / SL -15% (sell at the bid,
marketable) — do NOT hold to settlement. BTC only (tight ~1.1pt spread).

Every entry + every exit attempt + actual fill + slippage + timing is logged to the
burst_trades table AND the log, so live execution quality can be studied.

Safety: LIVE only when STRAT_ENABLED=1 AND STRAT_DRY_RUN=0 (else DRY = logs only, no orders).
Rails: one open position at a time, kill-switch on cumulative realized loss, $6 stake
(≥5 shares so the exit sells are placeable), defensive skip if any feature/feed is stale.
"""
import os, time, json, sqlite3, collections
import numpy as np
import strategy_bot as sb   # feeds, CLOB, discovery, helpers

# --- enlarge the chainlink buffer BEFORE the RTDS thread starts (ret60 needs 60min back) ---
sb.BUF = collections.defaultdict(lambda: collections.deque(maxlen=7000))   # ~115min @1Hz

ASSET     = "BTC"
STAKE     = float(os.environ.get("STRAT_BURST_STAKE", "6"))
TP        = float(os.environ.get("STRAT_BURST_TP", "0.30"))
SL        = float(os.environ.get("STRAT_BURST_SL", "0.15"))
ENTRY_SEC = int(os.environ.get("STRAT_BURST_ENTRY", "120"))
BAND      = (ENTRY_SEC - 5, ENTRY_SEC + 45)     # act within this many secs into the window
KILL      = float(os.environ.get("STRAT_BURST_KILL", "30"))
CONF_MIN  = float(os.environ.get("STRAT_BURST_CONF", "0.55"))   # skip near-random bets (conf<this)
SL_GRACE  = float(os.environ.get("STRAT_BURST_SL_GRACE", "240"))  # secs after entry before SL activates (avoid early whipsaw). TP always active.
MAX_ENTRY = float(os.environ.get("STRAT_BURST_MAX_ENTRY", "0.70"))  # skip if ask>this: TP+30% unreachable near 1.0 + expensive-favorite low-ROI
MAX_SPREAD= float(os.environ.get("STRAT_BURST_MAX_SPREAD", "0.04")) # skip if ask-bid wider than this (bad liquidity/whipsaw)
HOLD      = os.environ.get("STRAT_BURST_HOLD", "0") == "1"  # hold-to-settlement mode: TP only, NO SL, ride non-TP positions to settlement (auto-redeem)
SETTLE_WAIT = int(os.environ.get("STRAT_BURST_SETTLE_WAIT", "60"))  # secs after window close before polling gamma for the resolution
FEATURES  = ["clm30", "clm60", "clm120", "ret15", "ret30", "ret60"]
MODEL     = os.path.join(os.path.dirname(__file__), "models", "btc_burst6.json")
DB_PATH   = os.environ.get("STRAT_BURST_DB", os.path.join(os.path.dirname(__file__), "burst_bot.sqlite3"))
POLL_IDLE = float(os.environ.get("STRAT_BURST_POLL_IDLE", "4"))   # flat: entry detection (Gamma)
POLL_HOLD = float(os.environ.get("STRAT_BURST_POLL_HOLD", "1"))   # holding: tight TP/SL monitor (orderbook only)
STALE_TOL = 12   # a chainlink sample must be within this many secs of the target epoch

_booster = None
_settle_next = {}   # hold-mode: per-market throttle for gamma resolution polling
def model():
    global _booster
    if _booster is None:
        import xgboost as xgb
        _booster = xgb.Booster(); _booster.load_model(MODEL)
    return _booster

def buf_at(epoch):
    """chainlink price closest to `epoch` (within STALE_TOL) from sb.BUF[BTC], else None."""
    best = None; bd = 1e18
    for ts, pr in sb.BUF[ASSET]:
        d = abs(ts - epoch)
        if d < bd: bd = d; best = pr
    return best if (best is not None and bd <= STALE_TOL) else None

def features(start):
    strike = buf_at(start)
    if not strike or strike <= 0: return None
    out = {}
    for s, k in [(30, "clm30"), (60, "clm60"), (120, "clm120")]:
        p = buf_at(start + s)
        if not p: return None
        out[k] = (p - strike) / strike * 100.0
    for dt, k in [(900, "ret15"), (1800, "ret30"), (3600, "ret60")]:
        p = buf_at(start - dt)
        if not p or p <= 0: return None
        out[k] = (strike - p) / p * 100.0
    return [out[f] for f in FEATURES], strike

def predict(vec):
    import xgboost as xgb
    p = float(model().predict(xgb.DMatrix(np.array([vec], float), feature_names=FEATURES))[0])
    return p

def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS burst_trades(
        market_id TEXT PRIMARY KEY, asset TEXT, start INT, close INT,
        direction TEXT, token_id TEXT, prob_up REAL, feats TEXT,
        entry_ts INT, entry_ask REAL, shares REAL, buy_order TEXT, buy_status TEXT,
        tp_level REAL, sl_level REAL, exit_ts INT, exit_reason TEXT, exit_price REAL,
        exit_order TEXT, ret REAL, pnl REAL, dry INT, settled INT DEFAULT 0)""")
    con.commit(); return con

def realized(con):
    r = con.execute("SELECT COALESCE(SUM(pnl),0) FROM burst_trades WHERE pnl IS NOT NULL AND dry=0").fetchone()
    return float(r[0] or 0)

def has_open(con):
    return con.execute("SELECT 1 FROM burst_trades WHERE exit_ts IS NULL AND buy_status='filled' AND dry=0 LIMIT 1").fetchone() is not None

# ---------- order primitives (mirror strategy_bot patterns) ----------
def market_buy(token, cap):
    from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions as OPT
    try: from py_clob_client_v2 import Side; buy = Side.BUY
    except Exception: from py_clob_client_v2.order_builder.constants import BUY as buy
    resp = sb.clob().create_and_post_market_order(
        order_args=MarketOrderArgs(token_id=token, amount=float(STAKE), side=buy, price=float(cap)),
        options=OPT(tick_size=sb.tick_size(token)), order_type=OrderType.FAK)
    return resp or {}

def marketable_sell(token, shares, limit_price):
    """Sell `shares` at a marketable limit (fills at/above the bid). FAK."""
    from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions as OPT
    try: from py_clob_client_v2 import Side; sell = Side.SELL
    except Exception: from py_clob_client_v2.order_builder.constants import SELL as sell
    resp = sb.clob().create_and_post_order(
        order_args=OrderArgs(token_id=token, price=float(limit_price), size=float(int(shares)), side=sell),
        options=OPT(tick_size=sb.tick_size(token)), order_type=OrderType.FAK)
    return resp or {}

# ---------- entry ----------
def try_enter(con, w):
    mid = w["market_id"]
    if con.execute("SELECT 1 FROM burst_trades WHERE market_id=?", (mid,)).fetchone(): return
    if sb.LIVE and realized(con) <= -KILL:
        sb.log("BURST KILL-SWITCH: realized <= -%.0f — halting entries" % KILL); return
    if sb.LIVE and has_open(con):
        return  # one position at a time
    fr = features(w["start"])
    if fr is None:
        sb.log("burst skip %s: features/feed not ready" % mid[:28]); return
    vec, strike = fr
    p = predict(vec); direction = "UP" if p >= 0.5 else "DN"
    conf = max(p, 1 - p)
    if conf < CONF_MIN:                                    # skip near-random bets
        sb.log("burst skip %s: conf %.3f < %.2f" % (mid[:26], conf, CONF_MIN)); return
    token = w["ty"] if direction == "UP" else w["tn"]
    ask, bid = sb.token_book(token)
    if ask is None or not (0 < ask < 0.985):
        sb.log("burst skip %s: bad ask %s" % (mid[:26], ask)); return
    if ask > MAX_ENTRY:                                    # too expensive: TP unreachable + low-ROI favorite
        sb.log("burst skip %s: ask %.3f > max_entry %.2f" % (mid[:26], ask, MAX_ENTRY)); return
    if bid is not None and (ask - bid) > MAX_SPREAD:       # bad liquidity → whipsaw/slippage risk
        sb.log("burst skip %s: spread %.3f > %.2f" % (mid[:26], ask - bid, MAX_SPREAD)); return
    entry = ask; shares = round(STAKE / entry, 2)
    tp_lvl = round(entry * (1 + TP), 4); sl_lvl = round(entry * (1 - SL), 4)
    dry = 0 if sb.LIVE else 1
    buy_status = "dry"; buy_order = None
    if sb.LIVE:
        try:
            resp = market_buy(token, min(0.985, entry + 0.03))
            buy_order = resp.get("orderID") or resp.get("orderId")
            buy_status = "filled" if resp.get("success") else "failed"
            sb.log("BURST BUY %s %s $%.0f ~%.3f (%.1f sh) tp=%.3f sl=%.3f -> %s %s"
                   % (ASSET, direction, STAKE, entry, shares, tp_lvl, sl_lvl, buy_status, buy_order or resp))
            if buy_status == "failed": return  # retry next cycle within band
        except Exception as e:
            sb.log("BURST BUY ERROR %s: %r" % (mid[:20], e)); return
    else:
        sb.log("BURST DRY %s %s $%.0f entry~%.3f tp=%.3f sl=%.3f prob_up=%.3f" % (ASSET, direction, STAKE, entry, tp_lvl, sl_lvl, p))
    con.execute("""INSERT OR IGNORE INTO burst_trades(market_id,asset,start,close,direction,token_id,
        prob_up,feats,entry_ts,entry_ask,shares,buy_order,buy_status,tp_level,sl_level,dry)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (mid, ASSET, w["start"], w["close"], direction, token, round(p,4), json.dumps({f:round(v,4) for f,v in zip(FEATURES,vec)}),
         int(time.time()), entry, shares, buy_order, buy_status, tp_lvl, sl_lvl, dry))
    con.commit()
    if (not dry) or sb.TG_DRY:
        arrow = "🟢 UP" if direction == "UP" else "🔴 DOWN"
        sb.tg_send(("[DRY] " if dry else "") + "🎯 <b>BURST bet placed</b>\n"
                   f"{ASSET} {arrow}  ${STAKE:.0f} @ {entry*100:.1f}¢  (prob_up {p:.2f})\n"
                   f"TP {tp_lvl*100:.1f}¢ / SL {sl_lvl*100:.1f}¢ · entry@{ENTRY_SEC}s")

# ---------- monitor / exit ----------
def manage(con):
    now = int(time.time())
    rows = con.execute("""SELECT market_id,token_id,direction,entry_ask,shares,tp_level,sl_level,close,dry,entry_ts
        FROM burst_trades WHERE exit_ts IS NULL AND buy_status IN ('filled','dry')""").fetchall()
    for mid, token, direction, entry, shares, tp, sl, close, dry, entry_ts in rows:
        ask, bid = sb.token_book(token)
        reason = None; px = None
        if HOLD:
            # HOLD-to-settlement: take the TP+X spike if it comes; otherwise ride to settlement
            # (no SL, no timeout market-sell) and resolve via gamma — shares auto-redeem.
            if bid is not None and bid >= tp:
                reason, px = "TP", bid
            elif now >= close + SETTLE_WAIT:
                if _settle_next.get(mid, 0) > now: continue      # throttle gamma polling to ~15s
                _settle_next[mid] = now + 15
                oc = sb.gamma_outcome(mid)                        # 'YES'=Up won, 'NO'=Down won, None=not resolved
                if oc is None: continue                           # retry next cycle
                won = (oc == "YES" and direction == "UP") or (oc == "NO" and direction == "DN")
                ret = (1 - entry) / entry if won else -1.0        # shares settle to 1 (win) or 0 (loss)
                pnl = round(ret * shares * entry, 4)
                con.execute("UPDATE burst_trades SET exit_ts=?,exit_reason=?,exit_price=?,exit_order=?,ret=?,pnl=?,settled=1 WHERE market_id=?",
                            (now, "settle", 1.0 if won else 0.0, "redeem", round(ret, 4), pnl, mid))
                con.commit()
                sb.log("BURST SETTLE %s %s %s ret %+.1f%% ($%+.2f)" % (mid[:24], direction, "WON" if won else "LOST", 100*ret, pnl))
                if (not dry) or sb.TG_DRY:
                    sb.tg_send(f"{'✅' if won else '❌'} <b>BURST SETTLE</b> {ASSET} {direction} {'WON' if won else 'LOST'} · ret {100*ret:+.1f}% (${pnl:+.2f})")
                continue
            else:
                continue                                          # still holding — do nothing
        else:
            if bid is None: continue
            if bid >= tp: reason, px = "TP", bid                              # TP always active
            elif bid <= sl and (now - entry_ts) >= SL_GRACE: reason, px = "SL", bid  # SL only after grace (avoid early whipsaw)
            elif now >= close - 8: reason, px = "timeout", bid   # window ending → exit at market
        if reason is None: continue
        if dry:
            ret = (px - entry) / entry
            con.execute("UPDATE burst_trades SET exit_ts=?,exit_reason=?,exit_price=?,ret=?,pnl=? WHERE market_id=?",
                        (now, reason, px, round(ret,4), round(ret*shares*entry,4), mid))
            con.commit(); sb.log("BURST DRY EXIT %s %s @%.3f ret=%+.1f%%" % (mid[:24], reason, px, 100*ret)); continue
        try:
            limit = round(max(0.01, bid - 0.02), 2)   # marketable sell → fills at/above ~bid
            resp = marketable_sell(token, shares, limit)
            ok = resp.get("success"); eo = resp.get("orderID") or resp.get("orderId")
            if ok:
                # realized at ~bid (record bid; true fill studied from account/trade tape)
                ret = (px - entry) / entry
                con.execute("""UPDATE burst_trades SET exit_ts=?,exit_reason=?,exit_price=?,exit_order=?,ret=?,pnl=? WHERE market_id=?""",
                            (now, reason, px, eo, round(ret,4), round(ret*shares*entry,4), mid))
                con.commit()
                sb.log("BURST SELL %s %s %.0fsh @~%.3f (ret %+.1f%%) %s" % (mid[:20], reason, shares, px, 100*ret, eo))
                sb.tg_send(f"{'✅' if ret>0 else '❌'} <b>BURST {reason}</b> {ASSET} {direction} @{px*100:.1f}¢ ret {100*ret:+.1f}% (${ret*shares*entry:+.2f})")
            else:
                msg = str(resp).lower()
                if "balance" in msg or "not enough" in msg:
                    sb.log("burst sell wait (shares settling) %s" % mid[:20])   # retry next cycle
                else:
                    sb.log("burst sell rejected %s: %s" % (mid[:20], resp))
        except Exception as e:
            if "balance" in str(e).lower(): sb.log("burst sell wait %s" % mid[:20])
            else: sb.log("burst sell err %s: %r" % (mid[:20], e))

def main():
    sb.start_rtds(); time.sleep(1); sb.backfill_prices(minutes=110)
    exitdesc = ("HOLD-to-settle TP+%.0f/no-SL" % (TP*100)) if HOLD else ("TP+%.0f/SL-%.0f SLgrace=%ds" % (TP*100, SL*100, SL_GRACE))
    sb.log("BURST bot start | mode=%s%s asset=%s stake=$%.0f %s entry@%ds conf>=%.2f max_entry=%.2f kill-$%.0f"
           % ("LIVE" if sb.LIVE else "DRY", " HOLD" if HOLD else "", ASSET, STAKE, exitdesc, ENTRY_SEC, CONF_MIN, MAX_ENTRY, KILL))
    con = db()
    while True:
        try:
            holding = has_open(con)
            if not holding:                      # only look for entries when flat (Gamma call)
                now = time.time()
                for w in [w for w in sb.open_windows() if w["asset"] == ASSET]:
                    if BAND[0] <= (now - w["start"]) <= BAND[1]:
                        try_enter(con, w)
            manage(con)                          # monitor/exit open positions (orderbook)
        except Exception as e:
            sb.log("burst loop err: %r" % e)
        time.sleep(POLL_HOLD if has_open(con) else POLL_IDLE)

if __name__ == "__main__":
    main()
