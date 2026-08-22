#!/usr/bin/env python3
"""LinkHash strategy bot — Polymarket crypto 15m up/down edge trader.

STRATEGY (per-coin, tuned from a backtest of every stored BTC + all-coin 15m
up/down window, priced at the real ask/bid):
  In the final minutes of a 15m window, when the Chainlink price has *led* its
  window-open price ("strike") by >= LEAD% in one direction AND that direction's
  token is still buyable at <= CAP cents, BUY that direction. The edge is that
  the market under-prices an already-established lead in the closing minutes
  (settlement is a TWAP that has mostly locked in). Backtest ROI (ask-based):
      BTC +22% · ETH +30% · SOL +15% · XRP +9% · BNB +20% · DOGE +15% · HYPE +20%

FAIRNESS: signals come ONLY from the *public* LinkHash Data API (/api/v1/*) —
the exact same interface every competitor can subscribe to. The bot has no
privileged data access; the edge is the strategy, not the plumbing. It touches
Polymarket directly only to PLACE orders (which anyone does).

SAFETY — this bot does NOTHING live unless ALL are true:
    STRAT_ENABLED=1   AND   STRAT_DRY_RUN=0   AND   PM_PRIVATE_KEY present.
Otherwise it runs in DRY-RUN: it detects + logs the exact orders it *would*
place and tracks hypothetical P&L, touching no money. Rails (all env-tunable):
  * per-bet notional cap                  STRAT_BET_USDC
  * max simultaneous open exposure        STRAT_MAX_EXPOSURE
  * cumulative realized-loss kill-switch  STRAT_MAX_LOSS  (bot self-disables)
  * exactly one bet per market (idempotent via sqlite)
  * data-freshness guards (stale chainlink/orderbook -> skip)

Standalone (NO Django). Reads all config from a `.env` beside this file.

    python strategy_bot.py --status          # positions + P&L, exits
    python strategy_bot.py --once            # one scan/execute/settle cycle
    python strategy_bot.py --loop            # daemon (systemd; adaptive polling)
    python strategy_bot.py --once --settle-only
"""
import argparse
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

import requests

CYCLE_SEC = 900   # 15m window length


def slug_epoch(market_id):
    """Window START (unix) from a slug like 'btc-updown-15m-1787394600'. The
    trailing number IS the open time; close = start + CYCLE_SEC. This is the
    canonical reference — do NOT use market_meta.opened_at (that's first-seen)."""
    m = re.search(r"-(\d{9,11})(?:$|\D)", market_id or "")
    return int(m.group(1)) if m else None

HERE = os.path.dirname(os.path.abspath(__file__))

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


# Per-coin rules: n = seconds-left at which to act, lead = min |move%| vs strike,
# cap = max price (cents/100) we'll pay for the leading side.
RULES = {
    "BTC":  dict(n=120, lead=0.05, cap=0.80),
    "ETH":  dict(n=180, lead=0.10, cap=0.85),
    "SOL":  dict(n=180, lead=0.10, cap=0.85),
    "XRP":  dict(n=300, lead=0.15, cap=0.80),
    "BNB":  dict(n=120, lead=0.05, cap=0.85),
    "DOGE": dict(n=120, lead=0.05, cap=0.80),
    "HYPE": dict(n=120, lead=0.10, cap=0.85),
}

# ---- public LinkHash Data API (same interface competitors use) ---------------
API_BASE = os.environ.get("LHX_API_BASE", "https://link-hash.com").rstrip("/")
API_KEY  = os.environ.get("LHX_API_KEY", "").strip()
VENUE    = os.environ.get("STRAT_VENUE", "polymarket")

ENABLED      = envi("STRAT_ENABLED", 0) == 1
DRY_RUN      = envi("STRAT_DRY_RUN", 1) == 1
BET_USDC     = envf("STRAT_BET_USDC", 5)
MAX_EXPOSURE = envf("STRAT_MAX_EXPOSURE", 50)
MAX_LOSS     = envf("STRAT_MAX_LOSS", 30)
COINS        = [c.strip().upper() for c in
                os.environ.get("STRAT_COINS", ",".join(RULES)).split(",") if c.strip()]
FIRE_BAND    = envi("STRAT_FIRE_BAND", 45)   # act within (n-BAND, n] seconds left
MIN_TLEFT    = envi("STRAT_MIN_TLEFT", 15)   # need this many secs to fill
STALE_SEC    = envi("STRAT_STALE_SEC", 20)   # chainlink/orderbook must be fresher
SETTLE_GRACE = envi("STRAT_SETTLE_GRACE", 180)
LOOP_MIN     = envf("STRAT_INTERVAL", 6)     # fast poll near a window close
LOOP_MAX     = envf("STRAT_INTERVAL_IDLE", 30)  # slow poll when all windows far
DB_PATH      = os.environ.get("STRAT_DB", os.path.join(HERE, "strategy_bot.sqlite3"))

PM_HOST      = os.environ.get("PM_HOST", "https://clob.polymarket.com")
PM_KEY       = os.environ.get("PM_PRIVATE_KEY", "").strip()
PM_FUNDER    = os.environ.get("PM_FUNDER", "").strip()
PM_SIG_TYPE  = envi("PM_SIG_TYPE", 2)
PM_CHAIN     = envi("PM_CHAIN_ID", 137)

LIVE = ENABLED and not DRY_RUN and bool(PM_KEY)


def log(*a):
    print(datetime.now(timezone.utc).strftime("%H:%M:%S"), *a, flush=True)


def _epoch(s):
    """ISO string (from the API) -> unix seconds (UTC)."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


# ---- API client -------------------------------------------------------------
_session = requests.Session()


def api_get(path, **params):
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    r = _session.get(API_BASE + path, params={k: v for k, v in params.items() if v is not None},
                     headers=headers, timeout=15)
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError("API %s: %s" % (path, j.get("message") or j.get("error") or j))
    return j.get("data")


_strike_cache = {}   # market_id -> strike (window-open price is fixed once open)


def chainlink_at(asset, at_epoch, market_id=None):
    """Chainlink price at/just-before a timestamp (the window-open strike).
    Cached per market — a window's open price never changes once set."""
    if market_id and market_id in _strike_cache:
        return _strike_cache[market_id]
    d = api_get("/api/v1/prices/chainlink", asset=asset,
                start=_iso(at_epoch - 120), end=_iso(at_epoch), limit=1)
    val = float(d[0]["price"]) if d else None
    if market_id and val:
        _strike_cache[market_id] = val
        if len(_strike_cache) > 512:      # bound it
            _strike_cache.pop(next(iter(_strike_cache)))
    return val


def chainlink_now(asset):
    d = api_get("/api/v1/prices/chainlink", asset=asset, limit=1)
    if not d:
        return None, None
    return float(d[0]["price"]), _epoch(d[0]["ts"])


def orderbook_now(market_id):
    d = api_get(f"/api/v1/markets/{market_id}/snapshots", limit=1)
    if not d:
        return None
    return d[0]  # {ts, best_bid, best_ask, mid_price, ...}


# ---- persistence ------------------------------------------------------------
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS trades(
        market_id TEXT PRIMARY KEY, asset TEXT, direction TEXT, token_id TEXT,
        cap REAL, entry_price REAL, size REAL, notional REAL, placed_at INT,
        order_id TEXT, status TEXT, t_left INT, lead REAL, deadline INT,
        outcome TEXT, pnl REAL, dry INT)""")
    return con


def open_exposure(con):
    r = con.execute("SELECT COALESCE(SUM(notional),0) FROM trades "
                    "WHERE status IN ('placed','filled') AND dry=0").fetchone()
    return float(r[0] or 0)


def realized_pnl(con, dry):
    r = con.execute("SELECT COALESCE(SUM(pnl),0) FROM trades "
                    "WHERE outcome IS NOT NULL AND dry=?", (1 if dry else 0,)).fetchone()
    return float(r[0] or 0)


# ---- CLOB client (lazy; only if LIVE) ---------------------------------------
_clob = None


def clob():
    global _clob
    if _clob is None:
        from py_clob_client.client import ClobClient
        c = ClobClient(host=PM_HOST, key=PM_KEY, chain_id=PM_CHAIN,
                       signature_type=PM_SIG_TYPE, funder=PM_FUNDER or None)
        c.set_api_creds(c.create_or_derive_api_creds())
        _clob = c
    return _clob


def token_best_ask(token_id):
    """Live best ask (price to BUY 1 share) for a token, from the CLOB book."""
    book = clob().get_order_book(token_id)
    asks = getattr(book, "asks", None) or []
    prices = []
    for lvl in asks:
        try:
            prices.append(float(lvl.price))
        except Exception:
            pass
    return min(prices) if prices else None


# ---- signal scan ------------------------------------------------------------
def open_windows():
    """Current open 15m window per coin, via the public markets endpoint.
    Window start/close are derived from the slug epoch (canonical), not from
    market_meta timestamps."""
    data = api_get("/api/v1/markets", cycle="15m", venue=VENUE) or []
    now = time.time()
    out = []
    for m in data:
        a = (m.get("asset") or "").upper()
        if a not in RULES:
            continue
        mid = m.get("market_id")
        start = slug_epoch(mid)
        if not start:
            continue
        close = start + CYCLE_SEC
        out.append(dict(asset=a, market_id=mid, start=start, close=close,
                        ty=m.get("token_yes"), tn=m.get("token_no"),
                        t_left=int(close - now)))
    return out


def signals_from(windows):
    now = time.time()
    sigs = []
    for w in windows:
        a = w["asset"]
        rule = RULES[a]
        t_left = int(w["close"] - now)
        if not (rule["n"] - FIRE_BAND < t_left <= rule["n"]) or t_left < MIN_TLEFT:
            continue
        strike = chainlink_at(a, w["start"], market_id=w["market_id"])
        price, cts = chainlink_now(a)
        ob = orderbook_now(w["market_id"])
        if not (strike and price and ob):
            continue
        if (cts and now - cts > STALE_SEC) or \
           (ob.get("ts") and now - _epoch(ob["ts"]) > STALE_SEC):
            continue  # stale feed — do not trust
        if strike <= 0:
            continue
        lead = (price - strike) / strike * 100.0
        if abs(lead) < rule["lead"]:
            continue
        direction = "UP" if lead > 0 else "DN"
        bid = float(ob.get("best_bid") or 0)
        ask = float(ob.get("best_ask") or 0)
        entry_est = ask if direction == "UP" else (1.0 - bid)   # snapshot estimate
        token = w["ty"] if direction == "UP" else w["tn"]
        if not token or not (0 < entry_est <= rule["cap"]):
            continue
        sigs.append(dict(asset=a, market_id=w["market_id"], direction=direction,
                         token_id=token, cap=rule["cap"], entry_est=entry_est,
                         lead=round(lead, 3), t_left=t_left, deadline=int(w["close"])))
    return sigs


def next_sleep(windows):
    """Adaptive: poll fast when a window is in (or near) its firing band, slow
    otherwise. Keeps API usage modest (windows are on a fixed 15m grid)."""
    now = time.time()
    soonest = None
    for w in windows:
        rule = RULES[w["asset"]]
        t_left = w["close"] - now
        if rule["n"] - FIRE_BAND < t_left <= rule["n"]:
            return LOOP_MIN
        d = t_left - rule["n"]            # time until this coin enters its band
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
            ask = token_best_ask(sig["token_id"])
            if ask is None or ask > sig["cap"]:
                log("skip %s: live ask %.3f > cap %.2f" % (sig["asset"], ask or -1, sig["cap"]))
                return
            entry = ask
            size = round(BET_USDC / max(entry, 0.01), 2)
            if size * entry < 1.0:      # Polymarket min order ~ $1
                size = round(1.0 / entry, 2)
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
            order = clob().create_order(OrderArgs(
                token_id=sig["token_id"], price=sig["cap"], size=size, side=BUY))
            resp = clob().post_order(order, OrderType.FAK)  # marketable, no resting
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

    con.execute("""INSERT OR IGNORE INTO trades(market_id,asset,direction,token_id,cap,
        entry_price,size,notional,placed_at,order_id,status,t_left,lead,deadline,dry)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (mid, sig["asset"], sig["direction"], sig["token_id"], sig["cap"], entry, size,
         round(size * entry, 2), int(time.time()), order_id, status,
         sig["t_left"], sig["lead"], sig["deadline"], dry))
    con.commit()


# ---- settlement -------------------------------------------------------------
def settle(con):
    now = int(time.time())
    todo = con.execute(
        "SELECT market_id,asset,direction,entry_price,size,notional,dry FROM trades "
        "WHERE outcome IS NULL AND status IN ('placed','filled','dry') AND deadline < ?",
        (now - SETTLE_GRACE,)).fetchall()
    if not todo:
        return
    by_asset = {}
    for row in todo:
        by_asset.setdefault(row[1], []).append(row)
    for asset, items in by_asset.items():
        try:
            setts = api_get("/api/v1/settlements", asset=asset, venue=VENUE,
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
    con.commit()


# ---- status -----------------------------------------------------------------
def status(con):
    mode = "LIVE (real orders)" if LIVE else ("DRY-RUN" if ENABLED else "DISABLED->dry")
    print("=== strategy bot status ===")
    print("mode:", mode, "| api:", API_BASE, "| key:", "set" if API_KEY else "MISSING")
    print("coins:", ",".join(COINS))
    print("bet=%.0f max_exposure=%.0f max_loss=%.0f | db=%s" %
          (BET_USDC, MAX_EXPOSURE, MAX_LOSS, DB_PATH))
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
    return windows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--settle-only", action="store_true")
    ap.add_argument("--interval", type=float, default=None)
    args = ap.parse_args()

    con = db()
    if args.status:
        status(con)
        return
    log("strategy bot start | mode=%s coins=%s bet=%.0f api=%s" %
        ("LIVE" if LIVE else ("DRY" if ENABLED else "DISABLED"), ",".join(COINS), BET_USDC, API_BASE))
    if args.loop:
        while True:
            try:
                windows = cycle(con, args.settle_only)
            except Exception as e:
                log("cycle error: %r" % e)
                windows = []
            time.sleep(args.interval or next_sleep(windows))
    else:
        cycle(con, args.settle_only)


if __name__ == "__main__":
    main()
