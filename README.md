# LinkHash Tactical

Automated trading bot for Polymarket crypto **15-minute up/down** markets, run on
a dedicated server, isolated from the LinkHash web app.

## The edge (backtested)

Over **every stored 15m up/down window** (7 coins, ~33 days), priced at the real
ask/bid:

> In the closing minutes of a window, when the Chainlink price has **led** its
> window-open price ("strike") by a small margin, and the leading side's token is
> still **cheap** (≤80–85¢), buy that side. Settlement is a TWAP that has largely
> locked in, so the market under-prices an already-decided outcome.

Per-coin tuned ROI (real ask fills): ETH +30% · BTC +22% · BNB/HYPE +20% ·
SOL +15% · DOGE +15% · XRP +9%. Rules live in `RULES` atop `strategy_bot.py`.

`lead% = (current_chainlink − strike) / strike × 100`. Profit needs
`win_rate > price_paid` — which is why cheap leads win, not "near-certain 97¢" bets.

⚠️ Backtest ≠ future. Top-of-book fills only. No fees modeled.

## Data sources — Polymarket-direct

Live signals use the same feeds any Polymarket trader has (lowest latency, fair):

| Input | Source |
|-------|--------|
| Chainlink price / strike | Polymarket **RTDS** websocket (`crypto_prices_chainlink`) — a background thread keeps a per-coin price buffer; strike = buffered price at the window-open epoch |
| Market discovery | Polymarket **Gamma** API (`/events?tag_slug=crypto`) → tokens + window |
| Order book | public Polymarket **CLOB** (`/book`) — the real ask we'd pay |
| Order placement | Polymarket **CLOB** (`py-clob-client`, signed) |
| Settlement / P&L | LinkHash `/api/v1/settlements` — post-hoc accounting only (a free key is plenty) |

The bot must be running ~1 window (15 min) before it can trade — it needs the RTDS
buffer to cover a window's open to know the strike.

## Telegram

Announces to the LinkHash group when it places a bet and, per settle batch, a
combined result + running session totals (bets, W/L, win rate, cumulative %,
realized PnL). Set `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`; `--test-telegram`
checks connectivity. Real trades always post; dry-run only if `TELEGRAM_NOTIFY_DRY=1`.

## Safety

Does **nothing live** unless `STRAT_ENABLED=1` **and** `STRAT_DRY_RUN=0` **and**
`PM_PRIVATE_KEY` is set. Otherwise dry-run: logs would-be orders + hypothetical
P&L. Rails: per-bet cap, max exposure, realized-loss kill-switch, one bet per
market (idempotent), stale-feed guard, FAK orders (never pay above cap).

## Setup (fresh Ubuntu server)

```bash
sudo apt-get install -y python3-venv
git clone https://<PAT>@github.com/puzzlecollector/LinkHash-Tactical.git linkhash-strategies
cd linkhash-strategies
cp .env.example .env && nano .env      # set LHX_API_KEY (+ Telegram); leave STRAT_ENABLED=0
bash setup.sh                          # venv + install + start service (dry-run)
tail -f bot.log
```

## Go live

1. In `.env` set `PM_PRIVATE_KEY`, confirm `PM_FUNDER` + `PM_SIG_TYPE` (1 MetaMask).
   Fund the wallet with USDC via the Polymarket UI (sets the CLOB allowance).
2. Start small: `STRAT_BET_USDC=1`, `STRAT_MAX_EXPOSURE=8`.
3. Flip `STRAT_ENABLED=1`, keep `STRAT_DRY_RUN=1` a while to confirm signals live.
4. Set `STRAT_DRY_RUN=0`, `sudo systemctl restart linkhash-strategy-bot`.

## Commands

```bash
./venv/bin/python strategy_bot.py --status         # positions + P&L + RTDS buffer
./venv/bin/python strategy_bot.py --once           # single cycle
./venv/bin/python strategy_bot.py --test-telegram  # send a test message
sudo systemctl restart linkhash-strategy-bot       # after editing .env
```
