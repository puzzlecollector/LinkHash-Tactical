"""
BTC burst / volatility-capture strategy (v5 candidate) — self-contained CORE logic.

DIFFERENT from the settlement-betting bot (strategy_bot.py). Instead of predicting
the 15m settlement direction (which is efficient / unpredictable), this predicts the
early-window VOLATILITY-BURST direction and captures the move with a TP/SL exit —
it does NOT hold to settlement.

Flow (per BTC 15m window):
  1. Wait until open + ENTRY_SEC (120s) into the window.
  2. Compute 8 chainlink-only features (all available from the bot's RTDS buffer + CLOB book):
       clm30/clm60/clm120  = chainlink %move since window open, at 30/60/120s
       tok_mid60           = token mid at 60s minus 0.5
       tok_spread60        = token ask-bid at 60s
       ret15/ret30/ret60   = pre-open chainlink %return over prior 15/30/60 min
  3. XGBoost model -> P(burst UP). Buy UP token if P>=0.5 else DOWN token, at market (FAK), flat $2.
  4. Actively monitor the position: SELL at TP (+30% on entry) or SL (-15%); otherwise hold to settle.

Backtest (full-res orderbook, spread-in, OOS test n~1000, pre-fee):
  BTC only, entry@120, TP+30%/SL-15%, model AUC~0.62  ->  ~+1.5%/trade.
  KEY UNVALIDATED RISK: live SELL execution (fills/slippage). Backtest assumes selling
  at the bid at the TP level; live must be measured. START TINY ($2), kill at -$15.

WHY BTC only: token spread ~1.1pt (vs 5-12pt on alts) — the vol-capture only survives
the round-trip cost on the tightest-spread coin. ETH negative on the realistic model.

Sizing: FLAT for the validation phase. Confidence-proportional sizing adds only ~+0.2pp
(edge-prop 2c-1 -> +2.05% vs flat +1.81%); add it AFTER live confirms the base edge.

Integration TODO (do in a SUPERVISED session, not unattended with real money):
  - wire compute_features() to the bot's PRICE buffer (chainlink) + token_book()
  - add the entry@120 trigger to the main loop (one shot per window)
  - build the TP/SL monitor+sell loop (the risky, execution-critical part)
  - deploy DRY_RUN first to confirm timing/direction, then real $2 with supervision
"""
import os, json
import numpy as np

MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
FEATURES = ["clm30", "clm60", "clm120", "tok_mid60", "tok_spread60", "ret15", "ret30", "ret60"]
ENTRY_SEC = 120
TP = 0.30
SL = 0.15
BET_USDC = 2.0
KILL_LOSS = 15.0   # halt if cumulative realized loss reaches this

_booster = None
def _load():
    global _booster
    if _booster is None:
        import xgboost as xgb
        _booster = xgb.Booster()
        _booster.load_model(os.path.join(MODEL_DIR, "btc_burst.json"))
    return _booster

def compute_features(chainlink_at, token_book_at, strike, open_ep):
    """chainlink_at(sec)->price at open+sec ; token_book_at(sec)->(bid,ask) ; strike=chainlink@open.
    Returns an ordered feature vector (or None if data missing). Pre-open returns need
    chainlink_at(-900/-1800/-3600)."""
    def mv(sec):
        p = chainlink_at(sec)
        return None if (not p or strike <= 0) else (p - strike) / strike * 100.0
    clm30, clm60, clm120 = mv(30), mv(60), mv(120)
    bk = token_book_at(60)
    if None in (clm30, clm60, clm120) or bk is None:
        return None
    bb, ba = bk
    if not (0 < bb < 1 and 0 < ba < 1 and ba >= bb):
        return None
    tok_mid60 = (bb + ba) / 2 - 0.5
    tok_spread60 = ba - bb
    def ret(dt):
        p = chainlink_at(-dt)
        return None if (not p or p <= 0) else (strike - p) / p * 100.0
    r15, r30, r60 = ret(900), ret(1800), ret(3600)
    if None in (r15, r30, r60):
        return None
    return [clm30, clm60, clm120, tok_mid60, tok_spread60, r15, r30, r60]

def predict_direction(feat_vec):
    """Returns dict(direction='UP'|'DN', prob_up=float). feat_vec ordered per FEATURES."""
    import xgboost as xgb
    p = float(_load().predict(xgb.DMatrix(np.array([feat_vec], dtype=float), feature_names=FEATURES))[0])
    return {"direction": "UP" if p >= 0.5 else "DN", "prob_up": p, "conf": max(p, 1 - p)}

def exit_targets(entry_price):
    """TP/SL sell levels for a long at entry_price."""
    return {"tp": round(entry_price * (1 + TP), 4), "sl": round(entry_price * (1 - SL), 4)}

if __name__ == "__main__":
    meta = json.load(open(os.path.join(MODEL_DIR, "btc_burst_meta.json")))
    print("BTC burst model loaded. meta:", json.dumps(meta))
    # smoke test
    demo = [0.05, 0.08, 0.10, 0.02, 0.02, 0.01, 0.0, -0.01]
    print("demo predict:", predict_direction(demo), "targets@0.52:", exit_targets(0.52))
