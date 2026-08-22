# linkhash-strategies

Automated trading strategies for Polymarket crypto **15-minute up/down** markets,
run on a dedicated server, isolated from the LinkHash web app.

## The edge (backtested)

Backtest over **every stored 15m up/down window** (all 7 coins, ~Jul 19–Aug 21,
3,197 BTC windows + all-coin), priced at the **real ask/bid** (not mid):

> In the closing minutes of a window, when the Chainlink price has **led** its
> window-open price ("strike") by a small margin in one direction, and that
> direction's token is **still cheap** (not yet fully priced in), buy that
> direction. Settlement is a TWAP that has largely locked in, so the market is
> under-pricing an already-decided outcome.

Counter-intuitively, the profit is **not** in the "≤97¢ near-certain" bets (those
are roughly break-even to −EV once you pay the ask) — it's in the moderately
priced (≤80–85¢) leads the market hasn't caught up to. Per-coin tuned ROI:

| Coin | when | lead ≥ | pay ≤ | win% | ROI/bet |
|------|------|--------|-------|------|---------|
| BTC  | 2 min left | 0.05% | 80¢ | 86% | **+22%** |
| ETH  | 3 min left | 0.10% | 85¢ | 100% | **+30%** |
| SOL  | 3 min left | 0.10% | 85¢ | 91% | +15% |
| XRP  | 5 min left | 0.15% | 80¢ | 77% | +9% |
| BNB  | 2 min left | 0.05% | 85¢ | 90% | +20% |
| DOGE | 2 min left | 0.05% | 80¢ | 80% | +15% |
| HYPE | 2 min left | 0.10% | 85¢ | 89% | +20% |

"lead" = `(current_chainlink − strike) / strike × 100`, where `strike` is the
Chainlink price at the window's open (the "price to beat").

Rules live in `RULES` at the top of `strategy_bot.py`.

⚠️ Backtest ≠ future. Top-of-book fills only (large size slips). No fees modeled.

## Data source — fairness

Signals come **only from the public LinkHash Data API** (`/api/v1/*`) — the exact
same interface any competitor can subscribe to (create a key at `/developers/`).
The bot has **no privileged data access** (no direct ClickHouse), so the edge is
the *strategy*, not the plumbing, and it runs on a level field with every entrant.
It touches Polymarket directly only to **place orders** via the CLOB (which any
trader does). Bonus: this also removes the ClickHouse IP-allowlist problem — the
bot just needs an API key + internet, like anyone else.

Endpoints used: `/api/v1/markets` (open windows + tokens + open/close times),
`/api/v1/prices/chainlink` (strike @ open + current price → the "lead"),
`/api/v1/markets/{id}/snapshots?limit=1` (live order book),
`/api/v1/settlements` (outcomes → P&L). Polling is adaptive (fast near a window
close, slow otherwise) to keep API usage modest — windows are on a fixed 15m grid.

## Safety

Does **nothing live** unless `STRAT_ENABLED=1` **and** `STRAT_DRY_RUN=0` **and**
`PM_PRIVATE_KEY` is set. Otherwise it runs in **dry-run**: logs the exact orders
it *would* place and tracks hypothetical P&L. Rails: per-bet cap, max open
exposure, cumulative realized-loss kill-switch, one bet per market (idempotent),
stale-feed guards.

## Setup (fresh Ubuntu server)

```bash
git clone https://<PAT>@github.com/<you>/linkhash-strategies.git
cd linkhash-strategies
cp .env.example .env && nano .env      # set LHX_API_KEY; leave STRAT_ENABLED=0 for now
bash setup.sh                          # venv + install + start service (dry-run)
tail -f bot.log
```

## Go live (only when ready)

1. In `.env` set `PM_PRIVATE_KEY`, `PM_FUNDER`, `PM_SIG_TYPE` (1 browser-wallet /
   2 email-wallet). Fund the wallet with USDC and set the CLOB USDC allowance.
2. Start small: `STRAT_BET_USDC=1`, `STRAT_MAX_EXPOSURE=5`.
3. Flip `STRAT_ENABLED=1`, keep `STRAT_DRY_RUN=1` one cycle to confirm signals.
4. Set `STRAT_DRY_RUN=0` and `sudo systemctl restart linkhash-strategy-bot`.

## Commands

```bash
./venv/bin/python strategy_bot.py --status         # positions + P&L
./venv/bin/python strategy_bot.py --once           # single cycle
./venv/bin/python strategy_bot.py --once --settle-only
sudo systemctl restart linkhash-strategy-bot       # after editing .env
```
