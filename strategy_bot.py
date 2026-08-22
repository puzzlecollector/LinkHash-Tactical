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
    python strategy_bot.py --loop            # daemon (systemd)
    python strategy_bot.py --once --settle-only
"""
import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone

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
DB_PATH      = os.environ.get("STRAT_DB", os.path.join(HERE, "strategy_bot.sqlite3"))

PM_HOST      = os.environ.get("PM_HOST", "https://clob.polymarket.com")
PM_KEY       = os.environ.get("PM_PRIVATE_KEY", "").strip()
PM_FUNDER    = os.environ.get("PM_FUNDER", "").strip()
PM_SIG_TYPE  = envi("PM_SIG_TYPE", 2)
PM_CHAIN     = envi("PM_CHAIN_ID", 137)

LIVE = ENABLED and not DRY_RUN and bool(PM_KEY)


def log(*a):
    print(datetime.now(timezone.utc).strftime("%H:%M:%S"), *a, flush=True)


# ---- ClickHouse (direct; proxy-cleared for ClickHouse Cloud) -----------------
_PROXY = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy")
_ch = None


def ch():
    global _ch
    if _ch is None:
        import clickhouse_connect
        saved = {k: os.environ.pop(k, None) for k in _PROXY}
        try:
            _ch = clickhouse_connect.get_client(
                host=os.environ["CLICKHOUSE_HOST"],
                port=int(os.environ.get("CLICKHOUSE_PORT", "8443")),
                username=os.environ.get("CLICKHOUSE_USER", "default"),
                password=os.environ.get("CLICKHOUSE_PASSWORD"),
                database=os.environ.get("CLICKHOUSE_DB", "linkhash"),
                secure=True, connect_timeout=10, send_receive_timeout=30,
            )
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v
    return _ch


def rows(sql):
    res = ch().query(sql)
    cols = res.column_names
    return [dict(zip(cols, r)) for r in res.result_rows]


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
def scan():
    """Return live signals meeting each coin's rule, ready to (maybe) execute."""
    now = int(time.time())
    coin_filter = "','".join(COINS)
    meta = rows(f"""
        SELECT asset,
               argMin(market_id, deadline_at)  mid,
               toUnixTimestamp(min(deadline_at)) dl,
               toUnixTimestamp(argMin(opened_at, deadline_at)) op,
               argMin(token_yes, deadline_at)  ty,
               argMin(token_no, deadline_at)   tn
        FROM linkhash.market_meta
        WHERE cycle='15m' AND asset IN ('{coin_filter}')
          AND deadline_at > now() AND opened_at <= now()
        GROUP BY asset""")
    sigs = []
    for m in meta:
        a = m["asset"]
        if a not in RULES:
            continue
        rule = RULES[a]
        t_left = int(m["dl"]) - now
        if not (rule["n"] - FIRE_BAND < t_left <= rule["n"]) or t_left < MIN_TLEFT:
            continue
        sr = rows(f"SELECT price p, toUnixTimestamp(ts) t FROM linkhash.chainlink_price "
                  f"WHERE asset='{a}' AND ts<=fromUnixTimestamp({int(m['op'])}) ORDER BY ts DESC LIMIT 1")
        cur = rows(f"SELECT price p, toUnixTimestamp(ts) t FROM linkhash.chainlink_price "
                   f"WHERE asset='{a}' ORDER BY ts DESC LIMIT 1")
        ob = rows(f"SELECT best_bid bid, best_ask ask, toUnixTimestamp(ts) t "
                  f"FROM linkhash.orderbook_snapshot WHERE market_id='{m['mid']}' ORDER BY ts DESC LIMIT 1")
        if not (sr and cur and ob):
            continue
        if now - int(cur[0]["t"]) > STALE_SEC or now - int(ob[0]["t"]) > STALE_SEC:
            continue  # stale feed — do not trust
        strike = float(sr[0]["p"] or 0)
        price = float(cur[0]["p"] or 0)
        if strike <= 0:
            continue
        lead = (price - strike) / strike * 100.0
        if abs(lead) < rule["lead"]:
            continue
        direction = "UP" if lead > 0 else "DN"
        bid = float(ob[0]["bid"] or 0)
        ask = float(ob[0]["ask"] or 0)
        entry_est = ask if direction == "UP" else (1.0 - bid)   # snapshot estimate
        token = m["ty"] if direction == "UP" else m["tn"]
        if not token or not (0 < entry_est <= rule["cap"]):
            continue
        sigs.append(dict(asset=a, market_id=m["mid"], direction=direction,
                         token_id=token, cap=rule["cap"], entry_est=entry_est,
                         lead=round(lead, 3), t_left=t_left, deadline=int(m["dl"])))
    return sigs


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
    for mid, asset, direction, entry, size, notional, dry in todo:
        r = rows(f"SELECT outcome FROM linkhash.market_settlement FINAL "
                 f"WHERE market_id='{mid}' LIMIT 1")
        if not r:
            continue
        outcome = (r[0]["outcome"] or "").upper()
        won = (outcome == "YES" and direction == "UP") or (outcome == "NO" and direction == "DN")
        if dry:
            pnl = round((1.0 - entry) * size if won else -entry * size, 4)
        else:
            pnl = round((size - notional) if won else -notional, 4)  # ~ shares*1 - cost
        con.execute("UPDATE trades SET outcome=?, pnl=?, status=? WHERE market_id=?",
                    (outcome, pnl, "settled_win" if won else "settled_loss", mid))
        log("SETTLE %s %s %s pnl=%+.3f" % (asset, direction, outcome, pnl))
    con.commit()


# ---- status -----------------------------------------------------------------
def status(con):
    mode = "LIVE (real orders)" if LIVE else ("DRY-RUN" if ENABLED else "DISABLED->dry")
    print("=== strategy bot status ===")
    print("mode:", mode, "| coins:", ",".join(COINS))
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
        return
    for sig in scan():
        execute(con, sig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--settle-only", action="store_true")
    ap.add_argument("--interval", type=float, default=6.0)
    args = ap.parse_args()

    con = db()
    if args.status:
        status(con)
        return
    log("strategy bot start | mode=%s coins=%s bet=%.0f" %
        ("LIVE" if LIVE else ("DRY" if ENABLED else "DISABLED"), ",".join(COINS), BET_USDC))
    if args.loop:
        while True:
            try:
                cycle(con, args.settle_only)
            except Exception as e:
                log("cycle error: %r" % e)
            time.sleep(args.interval)
    else:
        cycle(con, args.settle_only)


if __name__ == "__main__":
    main()
