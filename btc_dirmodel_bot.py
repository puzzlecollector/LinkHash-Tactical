#!/usr/bin/env python3
"""
BTC 15m DIRECTIONAL-MODEL bot (Tactical 3).

Different edge from the burst bots: predict the DIRECTION of the upcoming 15m window
from BINANCE 15m OHLCV (a 3-model gradient-boosting ensemble: XGBoost + LightGBM +
CatBoost, trained on 2024-2026, 57 causal features). Honest OOS test AUC ~0.54; on real
Polymarket entry prices this backtested to ~+2-5% EV/trade at confidence>=0.55 with entry
price <= 0.60 (held to settlement). NOT alpha-rich — a thin, real-ish edge; deployed small.

Flow per window: at window open (+a few s), fetch latest Binance bars, build features,
predict p_up. If confidence>=CONF and the chosen side's ask<=MAX_ENTRY, market-buy $STAKE.
Exit: HARD stop at -STOP (default -50%; data shows an early position that halves rarely
recovers), else hold to settlement (gamma outcome -> auto-redeem).

Safety: LIVE only when STRAT_ENABLED=1 AND STRAT_DRY_RUN=0 AND PM_PRIVATE_KEY set.
Rails: one position at a time, cumulative-loss kill-switch, flat stake, entry-price cap.
"""
import os, time, json, sqlite3, urllib.request
import numpy as np, pandas as pd
import strategy_bot as sb

ASSET     = "BTC"
STAKE     = float(os.environ.get("STRAT_DIR_STAKE", "6"))
CONF_MIN  = float(os.environ.get("STRAT_DIR_CONF", "0.55"))
MAX_ENTRY = float(os.environ.get("STRAT_DIR_MAX_ENTRY", "0.60"))
STOP      = float(os.environ.get("STRAT_DIR_STOP", "0.50"))     # hard stop-loss (fraction)
KILL      = float(os.environ.get("STRAT_DIR_KILL", "40"))
ENTRY_SEC = int(os.environ.get("STRAT_DIR_ENTRY", "20"))         # act this many secs into the window
BAND      = (ENTRY_SEC - 10, ENTRY_SEC + 120)
SETTLE_WAIT = int(os.environ.get("STRAT_DIR_SETTLE_WAIT", "60"))
MODEL_DIR = os.path.join(os.path.dirname(__file__), "deploy_models")
DB_PATH   = os.environ.get("STRAT_DIR_DB", os.path.join(os.path.dirname(__file__), "dirmodel_bot.sqlite3"))
POLL_IDLE = float(os.environ.get("STRAT_DIR_POLL_IDLE", "4"))
POLL_HOLD = float(os.environ.get("STRAT_DIR_POLL_HOLD", "2"))

_models=None; _feats=None; _settle_next={}
def load_models():
    global _models,_feats
    if _models is None:
        import xgboost as xgb, lightgbm as lgb
        from catboost import CatBoostClassifier
        mx=xgb.XGBClassifier(); mx.load_model(os.path.join(MODEL_DIR,"dir_xgb.json"))
        ml=lgb.Booster(model_file=os.path.join(MODEL_DIR,"dir_lgb.txt"))
        mc=CatBoostClassifier(); mc.load_model(os.path.join(MODEL_DIR,"dir_cat.cbm"))
        _models=(mx,ml,mc); _feats=json.load(open(os.path.join(MODEL_DIR,"dir_feats.json")))["feats"]
    return _models,_feats

def binance_bars(limit=400):
    url=f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit={limit}"
    req=urllib.request.Request(url,headers={"User-Agent":"lh"})
    d=json.load(urllib.request.urlopen(req,timeout=15))
    df=pd.DataFrame(d,columns=["open_ms","open","high","low","close","vol","close_ms","qvol","ntrades","tbbav","tbqav","ignore"])
    for c in ["open","high","low","close","vol","qvol","ntrades","tbbav"]: df[c]=df[c].astype(float)
    return df

def build_feats(df):
    o=df.open.values;h=df.high.values;l=df.low.values;c=df.close.values;v=df.vol.values;nt=df.ntrades.values
    qv=df.qvol.values.astype(float);tbb=df.tbbav.values;n=len(df);lc=np.log(c);S=lambda a:pd.Series(a);F={}
    r1=np.zeros(n);r1[1:]=np.diff(lc)
    for w in [1,2,3,4,6,8,12,16,24,32,48,64,96,192]:
        r=np.zeros(n);r[w:]=lc[w:]-lc[:-w];F[f"ret{w}"]=r
    for w in [4,8,16,32,48,96,192]: F[f"vol{w}"]=S(r1).rolling(w).std().values
    F["volvol"]=S(F["vol16"]).rolling(48).std().values
    for w in [16,48]: F[f"skew{w}"]=S(r1).rolling(w).skew().values; F[f"kurt{w}"]=S(r1).rolling(w).kurt().values
    def rsi(a,w):
        d=np.diff(a,prepend=a[0]);up=np.where(d>0,d,0.);dn=np.where(d<0,-d,0.)
        return 100-100/(1+S(up).rolling(w).mean().values/(S(dn).rolling(w).mean().values+1e-9))
    for w in [7,14,32]: F[f"rsi{w}"]=rsi(c,w)
    ema=lambda a,s:S(a).ewm(span=s,adjust=False).mean().values
    macd=ema(c,12)-ema(c,26);F["macd"]=macd/c;F["macd_hist"]=(macd-ema(macd,9))/c
    for w in [20,48,96]:
        ma=S(c).rolling(w).mean().values;sd=S(c).rolling(w).std().values
        F[f"boll{w}"]=(c-ma)/(2*sd+1e-9);F[f"madist{w}"]=(c-ma)/ma
        sl=np.zeros(n);sl[w:]=(ma[w:]-ma[:-w])/ma[:-w];F[f"maslope{w}"]=sl
    tr=np.maximum(h-l,np.maximum(np.abs(h-np.roll(c,1)),np.abs(l-np.roll(c,1))));tr[0]=h[0]-l[0]
    F["atr14"]=S(tr).rolling(14).mean().values/c;F["hl_range"]=(h-l)/c
    rng=(h-l)+1e-9;F["body"]=(c-o)/rng;F["uwick"]=(h-np.maximum(o,c))/rng;F["lwick"]=(np.minimum(o,c)-l)/rng;F["clpos"]=(c-l)/rng
    F["vz"]=(v-S(v).rolling(96).mean().values)/(S(v).rolling(96).std().values+1e-9)
    F["ntz"]=(nt-S(nt).rolling(96).mean().values)/(S(nt).rolling(96).std().values+1e-9)
    tf=tbb/(v+1e-9);F["takerbuy_frac"]=tf
    F["takerbuy_z"]=(tf-S(tf).rolling(96).mean().values)/(S(tf).rolling(96).std().values+1e-9)
    F["dollar_vol"]=np.log1p(qv)
    dirn=np.sign(np.diff(c,prepend=c[0]));streak=np.zeros(n)
    for i in range(1,n): streak[i]=streak[i-1]+1 if dirn[i]==dirn[i-1] else 0
    F["streak"]=streak*dirn;F["mom_agree"]=np.sign(F["ret4"])+np.sign(F["ret16"])+np.sign(F["ret48"])
    sec=(df.open_ms.values//1000);tod=(sec%86400)/86400.0;dow=((sec//86400+4)%7)/7.0
    F["tod_sin"]=np.sin(2*np.pi*tod);F["tod_cos"]=np.cos(2*np.pi*tod);F["dow_sin"]=np.sin(2*np.pi*dow);F["dow_cos"]=np.cos(2*np.pi*dow)
    return pd.DataFrame(F)

def predict_up():
    """p_up for the UPCOMING window, using the just-CLOSED bar as the latest feature row."""
    (mx,ml,mc),feats=load_models()
    df=binance_bars(400)
    F=build_feats(df)
    row=F[feats].iloc[[-1]].replace([np.inf,-np.inf],np.nan)
    if row.isna().any(axis=1).iloc[0]: return None
    X=row.values
    p=(mx.predict_proba(X)[:,1][0]+ml.predict(X)[0]+mc.predict_proba(X)[:,1][0])/3.0
    return float(p)

def db():
    con=sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS dir_trades(
        market_id TEXT PRIMARY KEY, asset TEXT, start INT, close INT, direction TEXT, token_id TEXT,
        p_up REAL, conf REAL, entry_ts INT, entry_ask REAL, shares REAL, buy_order TEXT, buy_status TEXT,
        stop_level REAL, exit_ts INT, exit_reason TEXT, exit_price REAL, exit_order TEXT, ret REAL, pnl REAL,
        dry INT, settled INT DEFAULT 0)""")
    con.commit(); return con
def realized(con): return float(con.execute("SELECT COALESCE(SUM(pnl),0) FROM dir_trades WHERE pnl IS NOT NULL AND dry=0").fetchone()[0] or 0)
def has_open(con): return con.execute("SELECT 1 FROM dir_trades WHERE exit_ts IS NULL AND buy_status='filled' AND dry=0 LIMIT 1").fetchone() is not None

def market_buy(token,cap):
    from py_clob_client_v2 import MarketOrderArgs, OrderType, PartialCreateOrderOptions as OPT
    try: from py_clob_client_v2 import Side; buy=Side.BUY
    except Exception: from py_clob_client_v2.order_builder.constants import BUY as buy
    return sb.clob().create_and_post_market_order(order_args=MarketOrderArgs(token_id=token,amount=float(STAKE),side=buy,price=float(cap)),options=OPT(tick_size=sb.tick_size(token)),order_type=OrderType.FAK) or {}
def marketable_sell(token,shares,limit):
    from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions as OPT
    try: from py_clob_client_v2 import Side; sell=Side.SELL
    except Exception: from py_clob_client_v2.order_builder.constants import SELL as sell
    last={}
    for sz in range(int(shares),max(0,int(shares)-3),-1):
        if sz<1: break
        last=sb.clob().create_and_post_order(order_args=OrderArgs(token_id=token,price=float(limit),size=float(sz),side=sell),options=OPT(tick_size=sb.tick_size(token)),order_type=OrderType.FAK) or {}
        if last.get("success"): return last
        if not ("balance" in str(last).lower() or "not enough" in str(last).lower()): return last
    return last

def resolve_settled(con,mid,direction,entry,shares,now,dry):
    oc=sb.gamma_outcome(mid)
    if oc is None: return False
    won=(oc=="YES" and direction=="UP") or (oc=="NO" and direction=="DN")
    ret=(1-entry)/entry if won else -1.0; pnl=round(ret*shares*entry,4)
    con.execute("UPDATE dir_trades SET exit_ts=?,exit_reason=?,exit_price=?,exit_order=?,ret=?,pnl=?,settled=1 WHERE market_id=?",(now,"settle",1.0 if won else 0.0,"redeem",round(ret,4),pnl,mid)); con.commit()
    sb.log("DIR SETTLE %s %s %s ret %+.1f%% ($%+.2f)"%(mid[:24],direction,"WON" if won else "LOST",100*ret,pnl))
    if (not dry) or sb.TG_DRY: sb.tg_send(f"{'✅' if won else '❌'} <b>DIR SETTLE</b> {ASSET} {direction} {'WON' if won else 'LOST'} · {100*ret:+.1f}% (${pnl:+.2f})")
    return True

def try_enter(con,w):
    mid=w["market_id"]
    if con.execute("SELECT 1 FROM dir_trades WHERE market_id=?",(mid,)).fetchone(): return
    if sb.LIVE and realized(con)<=-KILL: sb.log("DIR KILL-SWITCH: realized<=-%.0f"%KILL); return
    if sb.LIVE and has_open(con): return
    try: p=predict_up()
    except Exception as e: sb.log("dir predict err %s: %r"%(mid[:20],e)); return
    if p is None: sb.log("dir skip %s: features not ready"%mid[:24]); return
    direction="UP" if p>=0.5 else "DN"; conf=max(p,1-p)
    if conf<CONF_MIN: sb.log("dir skip %s: conf %.3f < %.2f (p_up=%.3f)"%(mid[:22],conf,CONF_MIN,p)); return
    token=w["ty"] if direction=="UP" else w["tn"]
    ask,bid=sb.token_book(token)
    if ask is None or not (0<ask<0.985): sb.log("dir skip %s: bad ask %s"%(mid[:22],ask)); return
    if ask>MAX_ENTRY: sb.log("dir skip %s: ask %.3f > cap %.2f"%(mid[:22],ask,MAX_ENTRY)); return
    entry=ask; shares=round(STAKE/entry,2); stop=round(entry*(1-STOP),4); dry=0 if sb.LIVE else 1
    buy_status="dry"; buy_order=None
    if sb.LIVE:
        try:
            resp=market_buy(token,min(0.985,entry+0.03))
            buy_order=resp.get("orderID") or resp.get("orderId"); buy_status="filled" if resp.get("success") else "failed"
            sb.log("DIR BUY %s %s $%.0f ~%.3f (%.1f sh) p_up=%.3f conf=%.3f stop=%.3f -> %s %s"%(ASSET,direction,STAKE,entry,shares,p,conf,stop,buy_status,buy_order or resp))
            if buy_status=="failed": return
        except Exception as e: sb.log("DIR BUY ERROR %s: %r"%(mid[:20],e)); return
    else:
        sb.log("DIR DRY %s %s $%.0f entry~%.3f p_up=%.3f conf=%.3f stop=%.3f"%(ASSET,direction,STAKE,entry,p,conf,stop))
    con.execute("""INSERT OR IGNORE INTO dir_trades(market_id,asset,start,close,direction,token_id,p_up,conf,entry_ts,entry_ask,shares,buy_order,buy_status,stop_level,dry) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (mid,ASSET,w["start"],w["close"],direction,token,round(p,4),round(conf,4),int(time.time()),entry,shares,buy_order,buy_status,stop,dry)); con.commit()
    if (not dry) or sb.TG_DRY:
        arrow="🟢 UP" if direction=="UP" else "🔴 DOWN"
        sb.tg_send(("[DRY] " if dry else "")+f"🎯 <b>DIR bet</b> {ASSET} {arrow} ${STAKE:.0f} @ {entry*100:.1f}¢ (p_up {p:.2f}, conf {conf:.2f}) · stop -{STOP*100:.0f}%")

def manage(con):
    now=int(time.time())
    for mid,token,direction,entry,shares,stop,close,dry in con.execute("SELECT market_id,token_id,direction,entry_ask,shares,stop_level,close,dry FROM dir_trades WHERE exit_ts IS NULL AND buy_status IN ('filled','dry')").fetchall():
        ask,bid=sb.token_book(token)
        if bid is None:
            if now>=close and _settle_next.get(mid,0)<=now:
                _settle_next[mid]=now+15; resolve_settled(con,mid,direction,entry,shares,now,dry)
            continue
        if bid<=stop:                                    # HARD -50% stop
            px=bid
            if dry:
                ret=(px-entry)/entry; con.execute("UPDATE dir_trades SET exit_ts=?,exit_reason=?,exit_price=?,ret=?,pnl=? WHERE market_id=?",(now,"STOP",px,round(ret,4),round(ret*shares*entry,4),mid)); con.commit()
                sb.log("DIR DRY STOP %s @%.3f ret=%+.1f%%"%(mid[:22],px,100*ret)); continue
            try:
                limit=round(max(0.01,bid-0.02),2); resp=marketable_sell(token,shares,limit)
                if resp.get("success"):
                    ret=(px-entry)/entry; con.execute("UPDATE dir_trades SET exit_ts=?,exit_reason=?,exit_price=?,exit_order=?,ret=?,pnl=? WHERE market_id=?",(now,"STOP",px,resp.get("orderID") or resp.get("orderId"),round(ret,4),round(ret*shares*entry,4),mid)); con.commit()
                    sb.log("DIR STOP %s %.0fsh @~%.3f (ret %+.1f%%)"%(mid[:20],shares,px,100*ret))
                    sb.tg_send(f"🛑 <b>DIR STOP</b> {ASSET} {direction} @{px*100:.1f}¢ {100*ret:+.1f}% (${ret*shares*entry:+.2f})")
                else:
                    m=str(resp).lower()
                    if ("balance" in m or "orderbook" in m or "not enough" in m) and now>=close and _settle_next.get(mid,0)<=now:
                        _settle_next[mid]=now+15; resolve_settled(con,mid,direction,entry,shares,now,dry)
                    else: sb.log("dir stop-sell wait %s"%mid[:20])
            except Exception as e: sb.log("dir stop err %s: %r"%(mid[:20],e))
            continue
        if now>=close+SETTLE_WAIT and _settle_next.get(mid,0)<=now:   # else hold to settlement
            _settle_next[mid]=now+15; resolve_settled(con,mid,direction,entry,shares,now,dry)

def main():
    sb.start_rtds(); time.sleep(1)
    load_models()
    sb.log("DIR bot start | mode=%s asset=%s stake=$%.0f conf>=%.2f max_entry=%.2f stop-%.0f%% kill-$%.0f (binance 3-model ens)"%(
        "LIVE" if sb.LIVE else "DRY",ASSET,STAKE,CONF_MIN,MAX_ENTRY,STOP*100,KILL))
    con=db()
    while True:
        try:
            if not has_open(con):
                now=time.time()
                for w in [w for w in sb.open_windows() if w["asset"]==ASSET]:
                    if BAND[0]<=(now-w["start"])<=BAND[1]: try_enter(con,w)
            manage(con)
        except Exception as e: sb.log("dir loop err: %r"%e)
        time.sleep(POLL_HOLD if has_open(con) else POLL_IDLE)

if __name__=="__main__": main()
