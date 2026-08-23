#!/usr/bin/env python3
"""LinkHash strategy bot — Polymarket crypto 15m up/down edge trader.

STRATEGY v2 (per-coin, tuned + fee-adjusted on all stored 15m windows):
  In the final minutes of a 15m window, only when the Chainlink price has led its
  window-open "strike" by a STRONG per-coin margin (LEAD%), buy the MARKET
  FAVORITE (the higher-priced side) — provided the lead agrees with the favorite
  and the favorite is priced in [FAV_FLOOR, FAV_CEIL]. At a strong lead the win
  rate exceeds the price paid (win > price ⇒ +EV); weak leads / cheap "underdog"
  sides are skipped (they were -EV live). XRP excluded (its moves mean-revert).
  Fee-adjusted net ROI/bet: ETH +5% · HYPE +4% · SOL +3% · DOGE +3% · BNB +2% · BTC +5%.

DATA SOURCES — Polymarket-direct (same feeds any Polymarket trader has, lowest
latency, fair):
  * Chainlink price   : Polymarket RTDS websocket (crypto_prices_chainlink). A
                        background thread keeps a per-coin price buffer; the
                        strike = the buffered price at the window's open epoch.
  * Market discovery  : Polymarket Gamma API (/events?tag_slug=crypto).
  * Order book        : public Polymarket CLOB (/book) — the real ask we'd pay.
  * Order placement   : Polymarket CLOB V2 (py-clob-client-v2, POLY_1271 deposit
                        wallet: signature_type=3 + funder). FAK marketable orders.
  * Settlement (P&L)  : LinkHash Data API /settlements — post-hoc accounting only
                        (observation, not a live-edge input).

SAFETY — does NOTHING live unless STRAT_ENABLED=1 AND STRAT_DRY_RUN=0 AND
PM_PRIVATE_KEY present. Otherwise DRY-RUN: logs the exact orders it *would* place
and tracks hypothetical P&L. Rails: per-bet cap, max exposure, realized-loss
kill-switch, one bet per market (idempotent), stale-feed guards.

    python strategy_bot.py --status | --once | --loop | --test-telegram
"""
import argparse
import collections
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CYCLE_SEC = 900   # 15m window length

RTDS_URL  = "wss://ws-live-data.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com/events"
CLOB_REST = "https://clob.polymarket.com"

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(HERE, ".env"))
except Exception:
    pass


def envf(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def envi(name, default):
    try:
        return int(float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return int(default)


# Per-coin rules (v2 — "strong lead + market-favorite" strategy):
#   n    = seconds-left at which to act (within the final 5 minutes)
#   lead = MIN |chainlink move %| vs the window-open strike (per-coin, tuned)
# We BUY the market FAVORITE (the higher-priced side, must be in [FAV_FLOOR,
# FAV_CEIL]) only when the chainlink lead is strong enough AND points the same
# way. High win rate at these strong leads makes the favorite +EV (win>price).
# XRP is EXCLUDED — its short-term moves mean-revert, so leads don't persist.
RULES = {
    "BTC":  dict(n=120, lead=0.15),
    "ETH":  dict(n=120, lead=0.10),
    "SOL":  dict(n=120, lead=0.15),
    "BNB":  dict(n=240, lead=0.15),
    "DOGE": dict(n=180, lead=0.25),
    "HYPE": dict(n=180, lead=0.30),
}
FAV_FLOOR = envf("STRAT_FAV_FLOOR", 0.80)   # only back a clear market favorite
FAV_CEIL  = envf("STRAT_FAV_CEIL", 0.97)    # but not so pricey there's no profit

ENABLED      = envi("STRAT_ENABLED", 0) == 1
DRY_RUN      = envi("STRAT_DRY_RUN", 1) == 1
BET_USDC     = envf("STRAT_BET_USDC", 5)
MAX_EXPOSURE = envf("STRAT_MAX_EXPOSURE", 50)
MAX_LOSS     = envf("STRAT_MAX_LOSS", 30)
COINS        = [c.strip().upper() for c in
                os.environ.get("STRAT_COINS", ",".join(RULES)).split(",") if c.strip()]
FIRE_BAND    = envi("STRAT_FIRE_BAND", 45)   # act within (n-BAND, n] seconds left
MIN_TLEFT    = envi("STRAT_MIN_TLEFT", 15)   # need this many secs to fill
STALE_SEC    = envi("STRAT_STALE_SEC", 20)   # chainlink price must be fresher
SETTLE_GRACE = envi("STRAT_SETTLE_GRACE", 180)
LOOP_MIN     = envf("STRAT_INTERVAL", 5)      # fast poll near a window close
LOOP_MAX     = envf("STRAT_INTERVAL_IDLE", 30)
SELL_AT      = envf("STRAT_SELL_AT", 0.99)    # after a buy, rest a limit SELL here
                                              # (0 = off; hold to $1 redeem instead)
DB_PATH      = os.environ.get("STRAT_DB", os.path.join(HERE, "strategy_bot.sqlite3"))

# LinkHash Data API — settlement accounting only (observation).
LHX_BASE = os.environ.get("LHX_API_BASE", "https://link-hash.com").rstrip("/")
LHX_KEY  = os.environ.get("LHX_API_KEY", "").strip()
VENUE    = os.environ.get("STRAT_VENUE", "polymarket")

PM_HOST      = os.environ.get("PM_HOST", "https://clob.polymarket.com")
PM_KEY       = os.environ.get("PM_PRIVATE_KEY", "").strip()
PM_FUNDER    = os.environ.get("PM_FUNDER", "").strip()
PM_SIG_TYPE  = envi("PM_SIG_TYPE", 1)
PM_CHAIN     = envi("PM_CHAIN_ID", 137)

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TG_DRY   = envi("TELEGRAM_NOTIFY_DRY", 0) == 1

LIVE = ENABLED and not DRY_RUN and bool(PM_KEY)

_session = requests.Session()
_session.headers["User-Agent"] = "linkhash-tactical/1.0"


def log(*a):
    print(datetime.now(timezone.utc).strftime("%H:%M:%S"), *a, flush=True)


def slug_epoch(market_id):
    m = re.search(r"-(\d{9,11})(?:$|\D)", market_id or "")
    return int(m.group(1)) if m else None


def tg_send(text):
    """Post to the LinkHash Telegram group. No-op if unconfigured; never raises."""
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        _session.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=10)
    except Exception as e:
        log("telegram error: %r" % e)


# ---- Chainlink RTDS price feed (background thread) --------------------------
_plock = threading.Lock()
PRICE = {}                                             # asset -> (price, ts)
BUF = collections.defaultdict(lambda: collections.deque(maxlen=2400))  # asset -> [(ts,price)]


def _on_rtds(raw):
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return
    now = time.time()
    for m in (data if isinstance(data, list) else [data]):
        if not isinstance(m, dict) or m.get("topic") != "crypto_prices_chainlink":
            continue
        p = m.get("payload") or {}
        sym = str(p.get("symbol") or "").lower()
        if not sym.endswith("/usd"):
            continue
        asset = sym.split("/")[0].upper()
        if asset not in RULES:
            continue
        val = p.get("value")
        if val in (None, ""):
            continue
        try:
            val = float(val)
        except (TypeError, ValueError):
            continue
        mts = p.get("timestamp")
        ts = float(mts) / 1000.0 if mts else now
        with _plock:
            PRICE[asset] = (val, ts)
            BUF[asset].append((ts, val))


def _rtds_thread():
    import websocket  # websocket-client
    sub = json.dumps({"action": "subscribe", "subscriptions": [
        {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]})
    while True:
        try:
            ws = websocket.create_connection(RTDS_URL, timeout=20, max_size=4_000_000)
            ws.send(sub)
            log("rtds connected")
            while True:
                try:
                    _on_rtds(ws.recv())
                except Exception:
                    break
            try:
                ws.close()
            except Exception:
                pass
        except Exception as e:
            log("rtds reconnect: %r" % e)
        time.sleep(2)


def start_rtds():
    threading.Thread(target=_rtds_thread, daemon=True).start()


def strike_at(asset, epoch):
    """Buffered Chainlink price at/just-before the window-open epoch = the strike.
    None until the buffer covers that time (bot needs ~1 window of warmup)."""
    with _plock:
        best = None
        for ts, pr in BUF[asset]:
            if ts <= epoch:
                best = pr
            else:
                break
        return best


def price_now(asset):
    with _plock:
        return PRICE.get(asset)


def rtds_coverage():
    with _plock:
        return {a: (len(BUF[a]), PRICE.get(a, (None, None))[1]) for a in RULES}


# ---- Polymarket market discovery (Gamma) + order book (public CLOB) ---------
def open_windows():
    """Current open 15m window per coin from Gamma. Window start/close derived
    from the slug epoch (canonical); tokens from clobTokenIds."""
    now = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    params = {"active": "true", "closed": "false", "tag_slug": "crypto",
              "limit": 500, "end_date_min": now_iso, "order": "endDate",
              "ascending": "true"}
    try:
        events = _session.get(GAMMA_URL, params=params, timeout=15).json()
    except Exception as e:
        log("gamma error: %r" % e)
        return []
    best = {}
    for e in events or []:
        slug = e.get("slug") or ""
        parts = slug.split("-updown-")
        if len(parts) != 2 or not parts[1].startswith("15m"):
            continue
        asset = parts[0].upper()
        if asset not in RULES or asset not in COINS:
            continue
        epoch = slug_epoch(slug)
        if not epoch:
            continue
        close = epoch + CYCLE_SEC
        if close <= now:
            continue
        mk = (e.get("markets") or [{}])[0]
        toks = mk.get("clobTokenIds")
        if isinstance(toks, str):
            toks = json.loads(toks or "[]")
        if not toks or len(toks) < 2:
            continue
        if asset not in best or close < best[asset]["close"]:
            best[asset] = dict(asset=asset, market_id=slug, start=epoch, close=close,
                               ty=str(toks[0]), tn=str(toks[1]), t_left=int(close - now))
    return list(best.values())


def token_book(token_id):
    """Public CLOB order book → (best_ask, best_bid). No auth."""
    try:
        b = _session.get(CLOB_REST + "/book", params={"token_id": token_id}, timeout=10).json()
    except Exception:
        return None, None
    asks = [float(x["price"]) for x in (b.get("asks") or []) if x.get("price")]
    bids = [float(x["price"]) for x in (b.get("bids") or []) if x.get("price")]
    return (min(asks) if asks else None), (max(bids) if bids else None)


# ---- LinkHash Data API (settlement accounting only) ------------------------
def lhx_get(path, **params):
    headers = {"Authorization": f"Bearer {LHX_KEY}"} if LHX_KEY else {}
    r = _session.get(LHX_BASE + path, params={k: v for k, v in params.items() if v is not None},
                     headers=headers, timeout=15)
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError("LHX %s: %s" % (path, j.get("message") or j.get("error") or j))
    return j.get("data")


# ---- persistence ------------------------------------------------------------
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS trades(
        market_id TEXT PRIMARY KEY, asset TEXT, direction TEXT, token_id TEXT,
        cap REAL, entry_price REAL, size REAL, notional REAL, placed_at INT,
        order_id TEXT, status TEXT, t_left INT, lead REAL, deadline INT,
        outcome TEXT, pnl REAL, dry INT)""")
    try:
        con.execute("ALTER TABLE trades ADD COLUMN sell_status TEXT")  # NULL until a 99c sell rests
    except Exception:
        pass  # column already exists
    return con


def open_exposure(con):
    r = con.execute("SELECT COALESCE(SUM(notional),0) FROM trades "
                    "WHERE status IN ('placed','filled') AND dry=0").fetchone()
    return float(r[0] or 0)


def realized_pnl(con, dry):
    r = con.execute("SELECT COALESCE(SUM(pnl),0) FROM trades "
                    "WHERE outcome IS NOT NULL AND dry=?", (1 if dry else 0,)).fetchone()
    return float(r[0] or 0)


def session_stats(con, dry):
    """Aggregate settled trades for the Telegram summary. cumulative % = the sum
    of each settled bet's own return % (money-out − money-in ÷ money-in)."""
    rs = con.execute("SELECT notional, pnl FROM trades WHERE outcome IS NOT NULL AND dry=?",
                     (1 if dry else 0,)).fetchall()
    n = len(rs)
    wins = sum(1 for _nt, p in rs if (p or 0) > 0)
    pnl = sum((p or 0) for _nt, p in rs)
    cum = sum(((p or 0) / nt * 100) for nt, p in rs if nt)
    return dict(n=n, wins=wins, losses=n - wins, pnl=pnl, cum=cum,
                wr=(wins / n * 100 if n else 0))


# ---- CLOB client (lazy; only to PLACE orders when LIVE) ---------------------
_clob = None


def clob():
    """Polymarket V2 CLOB client (py-clob-client-v2) — supports the deposit-wallet
    POLY_1271 flow (signature_type=3 + funder). Only used to PLACE orders."""
    global _clob
    if _clob is None:
        from py_clob_client_v2 import ClobClient
        c = ClobClient(host=PM_HOST, chain_id=PM_CHAIN, key=PM_KEY,
                       signature_type=PM_SIG_TYPE, funder=PM_FUNDER or None)
        c.set_api_creds(c.create_or_derive_api_key())
        _clob = c
    return _clob


_tick_cache = {}


def tick_size(token_id):
    if token_id not in _tick_cache:
        try:
            _tick_cache[token_id] = str(clob().get_tick_size(token_id))
        except Exception:
            _tick_cache[token_id] = "0.01"
    return _tick_cache[token_id]


# ---- signal scan ------------------------------------------------------------
def signals_from(windows):
    """v2: buy the market FAVORITE only when the chainlink lead is strong (per-coin)
    AND agrees with the favorite AND the favorite is priced in [FAV_FLOOR, FAV_CEIL]."""
    now = time.time()
    sigs = []
    for w in windows:
        a = w["asset"]
        rule = RULES.get(a)
        if not rule:
            continue  # coin not in the strategy (e.g. XRP)
        t_left = int(w["close"] - now)
        if not (rule["n"] - FIRE_BAND < t_left <= rule["n"]) or t_left < MIN_TLEFT:
            continue
        # 1) chainlink lead must be strong enough
        strike = strike_at(a, w["start"])
        pn = price_now(a)
        if not strike or strike <= 0 or not pn:
            continue
        price, pts = pn
        if now - pts > STALE_SEC:
            continue  # stale RTDS feed — do not trust
        lead = (price - strike) / strike * 100.0
        if abs(lead) < rule["lead"]:
            continue
        lead_dir = "UP" if lead > 0 else "DN"
        # 2) find the market favorite (higher-priced side) from the live books
        up_ask, up_bid = token_book(w["ty"])
        dn_ask, dn_bid = token_book(w["tn"])
        if up_ask is None or dn_ask is None:
            continue
        up_mid = (up_ask + up_bid) / 2 if up_bid else up_ask
        if up_mid >= 0.5:
            fav_dir, fav_tok, fav_ask = "UP", w["ty"], up_ask
        else:
            fav_dir, fav_tok, fav_ask = "DN", w["tn"], dn_ask
        # 3) lead must agree with the favorite, and the favorite must be priced right
        if fav_dir != lead_dir:
            continue
        if not (FAV_FLOOR <= fav_ask <= FAV_CEIL):
            continue
        sigs.append(dict(asset=a, market_id=w["market_id"], direction=fav_dir,
                         token_id=fav_tok, cap=FAV_CEIL, entry_est=fav_ask,
                         lead=round(lead, 3), t_left=t_left, deadline=int(w["close"])))
    return sigs


def next_sleep(windows):
    now = time.time()
    soonest = None
    for w in windows:
        rule = RULES.get(w["asset"])
        if not rule:
            continue
        t_left = w["close"] - now
        if rule["n"] - FIRE_BAND < t_left <= rule["n"]:
            return LOOP_MIN
        d = t_left - rule["n"]
        if d > 0:
            soonest = d if soonest is None else min(soonest, d)
    if soonest is None:
        return LOOP_MIN
    return max(LOOP_MIN, min(soonest, LOOP_MAX))


# ---- execution --------------------------------------------------------------
def execute(con, sig):
    mid = sig["market_id"]
    if con.execute("SELECT 1 FROM trades WHERE market_id=?", (mid,)).fetchone():
        return  # already acted on this window
    if LIVE and realized_pnl(con, dry=False) <= -MAX_LOSS:
        log("KILL-SWITCH: realized loss <= -%.0f USDC — real trading disabled" % MAX_LOSS)
        return
    if LIVE and open_exposure(con) + BET_USDC > MAX_EXPOSURE:
        log("skip %s: open exposure cap (%.0f) reached" % (sig["asset"], MAX_EXPOSURE))
        return

    entry = sig["entry_est"]
    size = round(BET_USDC / max(entry, 0.01), 2)
    order_id = None
    dry = 0 if LIVE else 1
    status = "dry"

    if LIVE:
        try:
            ask, _bid = token_book(sig["token_id"])     # re-check live ask
            if ask is None or ask > sig["cap"]:
                log("skip %s: live ask %s > cap %.2f" % (sig["asset"], ask, sig["cap"]))
                return
            entry = ask
            amount = round(max(BET_USDC, 1.0), 2)              # USDC to spend (>= $1 min)
            size = round(amount / max(entry, 0.01), 2)         # shares, for the record
            from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions as _OPT
            try:
                from py_clob_client_v2 import Side as _Side; _buy = _Side.BUY
            except Exception:
                from py_clob_client_v2.order_builder.constants import BUY as _buy
            # Market BUY of `amount` USDC, capped at `cap` (worst price), FAK. Passing a
            # USDC amount lets the client round maker/taker amounts to valid decimals;
            # neg_risk (options=None) is auto-detected per token.
            resp = clob().create_and_post_market_order(
                order_args=MarketOrderArgs(token_id=sig["token_id"], amount=float(amount),
                                           side=_buy, price=sig["cap"]),
                options=_OPT(tick_size=tick_size(sig["token_id"])),
                order_type=OrderType.FAK)
            order_id = (resp or {}).get("orderID") or (resp or {}).get("orderId")
            status = "filled" if (resp or {}).get("success") else "failed"
            log("LIVE %s %s size=%.2f @<=%.2f -> %s %s"
                % (sig["asset"], sig["direction"], size, sig["cap"], status, order_id or resp))
        except Exception as e:
            status = "failed"
            log("ORDER ERROR %s: %r" % (sig["asset"], e))
    else:
        log("DRY  %s %s size=%.2f entry~%.3f cap<=%.2f lead=%+.3f%% t_left=%ds"
            % (sig["asset"], sig["direction"], size, entry, sig["cap"], sig["lead"], sig["t_left"]))

    if status == "failed":
        return   # don't record — let the next cycle retry this window while in band

    con.execute("""INSERT OR IGNORE INTO trades(market_id,asset,direction,token_id,cap,
        entry_price,size,notional,placed_at,order_id,status,t_left,lead,deadline,dry)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (mid, sig["asset"], sig["direction"], sig["token_id"], sig["cap"], entry, size,
         round(size * entry, 2), int(time.time()), order_id, status,
         sig["t_left"], sig["lead"], sig["deadline"], dry))
    con.commit()
    # The 99c limit SELL is placed by place_pending_sells() on a later cycle —
    # a fresh fill's shares aren't sellable for a few seconds (on-chain settle).

    if status in ("filled", "dry") and ((not dry) or TG_DRY):
        arrow = "🟢 UP" if sig["direction"] == "UP" else "🔴 DOWN"
        tg_send(("[DRY] " if dry else "") +
                "🎯 <b>LinkHash strategy — bet placed</b>\n"
                f"{sig['asset']} {arrow}  ${round(size * entry, 2)} @ {entry * 100:.1f}¢\n"
                f"lead {sig['lead']:+.3f}% · {sig['t_left']}s left")


def place_pending_sells(con):
    """Rest a limit SELL at SELL_AT on filled positions, once their shares are
    sellable. A fresh fill's balance lands a few seconds later (on-chain settle),
    so we retry across cycles. Integer share count → shares×0.99 stays ≤2 dp."""
    if not (LIVE and SELL_AT > 0):
        return
    now = int(time.time())
    rows = con.execute(
        "SELECT market_id, token_id, size, deadline FROM trades "
        "WHERE status='filled' AND dry=0 AND sell_status IS NULL AND deadline > ?",
        (now + 20,)).fetchall()
    if not rows:
        return
    from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions as _OPT2
    try:
        from py_clob_client_v2 import Side as _S2; _sell = _S2.SELL
    except Exception:
        from py_clob_client_v2.order_builder.constants import SELL as _sell
    for mid, token, size, _deadline in rows:
        sell_sz = int(size or 0)
        if sell_sz < 1:
            con.execute("UPDATE trades SET sell_status='skip' WHERE market_id=?", (mid,))
            continue
        try:
            resp = clob().create_and_post_order(
                order_args=OrderArgs(token_id=token, price=SELL_AT, size=float(sell_sz), side=_sell),
                options=_OPT2(tick_size=tick_size(token)), order_type=OrderType.GTC)
            if (resp or {}).get("success"):
                con.execute("UPDATE trades SET sell_status='resting' WHERE market_id=?", (mid,))
                log("SELL rest %d @ %.2f  %s" % (sell_sz, SELL_AT, mid[:26]))
            else:
                log("sell rejected %s: %s" % (mid[:16], resp))
        except Exception as e:
            msg = str(e).lower()
            if "balance" in msg and "enough" in msg:
                pass  # shares not settled into a sellable balance yet — retry next cycle
            else:
                con.execute("UPDATE trades SET sell_status='err' WHERE market_id=?", (mid,))
                log("sell err %s: %r" % (mid[:16], e))
    con.commit()


# ---- settlement (P&L accounting via LinkHash /settlements = observation) -----
def settle(con):
    now = int(time.time())
    todo = con.execute(
        "SELECT market_id,asset,direction,entry_price,size,notional,dry FROM trades "
        "WHERE outcome IS NULL AND status IN ('placed','filled','dry') AND deadline < ?",
        (now - SETTLE_GRACE,)).fetchall()
    if not todo:
        return
    settled_now = []
    by_asset = {}
    for row in todo:
        by_asset.setdefault(row[1], []).append(row)
    for asset, items in by_asset.items():
        try:
            setts = lhx_get("/api/v1/settlements", asset=asset, venue=VENUE,
                            cycle="15m", limit=500) or []
        except Exception as e:
            log("settle fetch error %s: %r" % (asset, e))
            continue
        omap = {s.get("market_id"): (s.get("outcome") or "").upper() for s in setts}
        for mid, _a, direction, entry, size, notional, dry in items:
            outcome = omap.get(mid)
            if not outcome:
                continue
            won = (outcome == "YES" and direction == "UP") or (outcome == "NO" and direction == "DN")
            if dry:
                pnl = round((1.0 - entry) * size if won else -entry * size, 4)
            else:
                pnl = round((size - notional) if won else -notional, 4)
            con.execute("UPDATE trades SET outcome=?, pnl=?, status=? WHERE market_id=?",
                        (outcome, pnl, "settled_win" if won else "settled_loss", mid))
            log("SETTLE %s %s %s pnl=%+.3f" % (asset, direction, outcome, pnl))
            settled_now.append(dict(asset=asset, direction=direction, outcome=outcome,
                                    pnl=pnl, won=won, dry=dry))
    con.commit()

    notify = [s for s in settled_now if (not s["dry"]) or TG_DRY]
    if notify:
        lines = [("[DRY] " if s["dry"] else "") +
                 f"{'✅' if s['won'] else '❌'} {s['asset']} {s['direction']} → "
                 f"{s['outcome']}  {s['pnl']:+.2f}$" for s in notify]
        st = session_stats(con, dry=notify[0]["dry"])
        lines.append(f"📊 <b>Session</b>: {st['n']} bets · {st['wins']}W/{st['losses']}L "
                     f"({st['wr']:.0f}%) · cumulative {st['cum']:+.1f}% · PnL {st['pnl']:+.2f}$")
        tg_send("\n".join(lines))


# ---- status -----------------------------------------------------------------
def status(con):
    mode = "LIVE (real orders)" if LIVE else ("DRY-RUN" if ENABLED else "DISABLED->dry")
    print("=== strategy bot status ===")
    print("mode:", mode, "| coins:", ",".join(COINS))
    print("data: RTDS(price) + Gamma(markets) + CLOB(book) | settle via LinkHash",
          "(key set)" if LHX_KEY else "(key MISSING)")
    print("bet=%.0f max_exposure=%.0f max_loss=%.0f | db=%s" %
          (BET_USDC, MAX_EXPOSURE, MAX_LOSS, DB_PATH))
    print("telegram:", ("on" if (TG_TOKEN and TG_CHAT) else "off"),
          "(notify_dry=%d)" % (1 if TG_DRY else 0))
    print("rtds buffer:", rtds_coverage())
    for dry in (0, 1):
        tag = "DRY" if dry else "REAL"
        n = con.execute("SELECT COUNT(*) FROM trades WHERE dry=?", (dry,)).fetchone()[0]
        s = con.execute("SELECT COUNT(*),SUM(CASE WHEN status='settled_win' THEN 1 ELSE 0 END),"
                        "COALESCE(SUM(pnl),0) FROM trades WHERE outcome IS NOT NULL AND dry=?",
                        (dry,)).fetchone()
        settled, wins, pnl = s[0], s[1] or 0, s[2] or 0
        wr = (wins / settled * 100) if settled else 0
        print(f"[{tag}] trades={n} settled={settled} wins={wins} winrate={wr:.1f}% pnl={pnl:+.3f}")
    print("--- recent ---")
    for row in con.execute("SELECT placed_at,asset,direction,entry_price,size,status,lead,pnl "
                           "FROM trades ORDER BY placed_at DESC LIMIT 12").fetchall():
        ts = datetime.fromtimestamp(row[0], timezone.utc).strftime("%m-%d %H:%M")
        print(f"  {ts} {row[1]:4} {row[2]} entry={row[3]:.3f} size={row[4]:.1f} "
              f"{row[5]:12} lead={row[6]:+.2f}% pnl={row[7] if row[7] is not None else '-'}")


# ---- main -------------------------------------------------------------------
def cycle(con, settle_only=False):
    settle(con)
    if settle_only:
        return []
    windows = open_windows()
    for sig in signals_from(windows):
        execute(con, sig)
    place_pending_sells(con)   # rest 99c sells on filled positions (retries until sellable)
    return windows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--settle-only", action="store_true")
    ap.add_argument("--test-telegram", action="store_true")
    ap.add_argument("--interval", type=float, default=None)
    args = ap.parse_args()

    con = db()
    if args.status:
        status(con)
        return
    if args.test_telegram:
        if not (TG_TOKEN and TG_CHAT):
            print("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set in .env")
        else:
            tg_send("✅ <b>LinkHash Tactical</b> connected — announcements are live.")
            print("test message sent — check the group")
        return

    log("strategy bot start | mode=%s coins=%s bet=%.0f" %
        ("LIVE" if LIVE else ("DRY" if ENABLED else "DISABLED"), ",".join(COINS), BET_USDC))
    start_rtds()
    if args.loop:
        while True:
            try:
                windows = cycle(con, args.settle_only)
            except Exception as e:
                log("cycle error: %r" % e)
                windows = []
            time.sleep(args.interval or next_sleep(windows))
    else:
        time.sleep(3)   # let RTDS connect for a one-shot
        cycle(con, args.settle_only)


if __name__ == "__main__":
    main()
