#!/usr/bin/env python3
"""
BTC 15m TECHNICAL-ANALYSIS voting bot (Tactical 1 slot, redeployed).

A TA-based counterpart to Tactical 3's ML ensemble: predict the upcoming 15m window
direction from BINANCE 15m OHLCV using a WEIGHTED technical-indicator vote — the same 22
signals investing.com/TradingView aggregate (SMA/EMA 5-200, RSI, Stoch, MACD, CCI,
Williams %R, ROC, StochRSI, Bull/Bear Power, DI), but instead of the naive "price>MA=Buy"
sum (which is -EV at 15m: backtest AUC 0.464, ~47% acc — trend-following fails intraday),
a LOGISTIC model learned each indicator's direction+weight on 2024-2026 (most MAs+RSI+MACD
come out CONTRARIAN — overbought -> next-bar down, i.e. mean-reversion). Honest temporal-OOS
test AUC ~0.541 (≈ the ML ensemble's 0.540); high-conf acc ~56%. Interpretable, low-overfit.

Flow: at window open, fetch Binance bars -> 22 TA signals -> logistic p_up. Bet if conf>=CONF
and chosen side ask<=MAX_ENTRY; exit HARD -STOP else hold to settlement. Head-to-head vs
Tactical 3 (ML) = TA-voting vs gradient-boosting, same entry/exit.

Safety: LIVE only when STRAT_ENABLED=1 AND STRAT_DRY_RUN=0 AND PM_PRIVATE_KEY set.
"""
import os, time, json, sqlite3, urllib.request
import numpy as np, pandas as pd
import strategy_bot as sb

ASSET     = "BTC"
STAKE     = float(os.environ.get("STRAT_TA_STAKE", "6"))
CONF_MIN  = float(os.environ.get("STRAT_TA_CONF", "0.55"))
MAX_ENTRY = float(os.environ.get("STRAT_TA_MAX_ENTRY", "0.60"))
STOP      = float(os.environ.get("STRAT_TA_STOP", "0.50"))     # hard stop-loss (fraction)
KILL      = float(os.environ.get("STRAT_TA_KILL", "40"))
ENTRY_SEC = int(os.environ.get("STRAT_TA_ENTRY", "20"))
BAND      = (ENTRY_SEC - 10, ENTRY_SEC + 120)
SETTLE_WAIT = int(os.environ.get("STRAT_TA_SETTLE_WAIT", "60"))
MODEL_DIR = os.path.join(os.path.dirname(__file__), "deploy_models")
DB_PATH   = os.environ.get("STRAT_TA_DB", os.path.join(os.path.dirname(__file__), "ta_bot.sqlite3"))
POLL_IDLE = float(os.environ.get("STRAT_TA_POLL_IDLE", "4"))
POLL_HOLD = float(os.environ.get("STRAT_TA_POLL_HOLD", "2"))

_ta=None; _settle_next={}
def load_models():
    global _ta
    if _ta is None:
        _ta=json.load(open(os.path.join(MODEL_DIR,"ta_logistic.json")))
    return _ta

def binance_bars(limit=400):
    url=f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit={limit}"
    req=urllib.request.Request(url,headers={"User-Agent":"lh"})
    d=json.load(urllib.request.urlopen(req,timeout=15))
    df=pd.DataFrame(d,columns=["open_ms","open","high","low","close","vol","close_ms","qvol","ntrades","tbbav","tbqav","ignore"])
    for c in ["open","high","low","close","vol","qvol","ntrades","tbbav"]: df[c]=df[c].astype(float)
    return df

def build_feats(df):
    """The 22 TA signals (must match ta_logistic.json feature order)."""
    h=df.high.values;l=df.low.values;c=df.close.values;n=len(df);S=lambda a:pd.Series(a);F={}
    for w in [5,10,20,50,100,200]:
        F[f"sma{w}"]=np.sign(c-S(c).rolling(w).mean().values); F[f"ema{w}"]=np.sign(c-S(c).ewm(span=w,adjust=False).mean().values)
    def rsi(w=14):
        d=np.diff(c,prepend=c[0]);up=np.where(d>0,d,0.);dn=np.where(d<0,-d,0.)
        return 100-100/(1+S(up).rolling(w).mean().values/(S(dn).rolling(w).mean().values+1e-9))
    R=rsi(14); F["rsi"]=(R-50)/50
    ll=S(l).rolling(14).min().values;hh=S(h).rolling(14).max().values;k=100*(c-ll)/(hh-ll+1e-9); F["stoch"]=(S(k).rolling(3).mean().values-50)/50
    ema=lambda s:S(c).ewm(span=s,adjust=False).mean().values; macd=ema(12)-ema(26)
    F["macd"]=np.sign(macd-S(macd).ewm(span=9,adjust=False).mean().values); F["macd_lvl"]=np.tanh(macd/c*200)
    tp=(h+l+c)/3;ma=S(tp).rolling(20).mean().values;md=S(tp).rolling(20).apply(lambda x:np.abs(x-x.mean()).mean(),raw=True).values; F["cci"]=np.tanh(((tp-ma)/(0.015*md+1e-9))/100)
    hh14=S(h).rolling(14).max().values;ll14=S(l).rolling(14).min().values; F["wr"]=(-100*(hh14-c)/(hh14-ll14+1e-9)+50)/50
    r10=np.zeros(n);r10[10:]=100*(c[10:]-c[:-10])/c[:-10]; F["roc"]=np.tanh(r10/2)
    mn=S(R).rolling(14).min().values;mx=S(R).rolling(14).max().values; F["srsi"]=((R-mn)/(mx-mn+1e-9)*100-50)/50
    F["bbp"]=np.tanh(((h-ema(13))+(l-ema(13)))/c*100)
    up_=h-np.roll(h,1);dn_=np.roll(l,1)-l;pdm=np.where((up_>dn_)&(up_>0),up_,0.);mdm=np.where((dn_>up_)&(dn_>0),dn_,0.)
    tr=np.maximum(h-l,np.maximum(np.abs(h-np.roll(c,1)),np.abs(l-np.roll(c,1))));tr[0]=h[0]-l[0];atr=S(tr).rolling(14).mean().values
    pdi=100*S(pdm).rolling(14).mean().values/(atr+1e-9);mdi=100*S(mdm).rolling(14).mean().values/(atr+1e-9); F["di"]=np.tanh((pdi-mdi)/20)
    return pd.DataFrame(F)

def predict_up():
    """Weighted-TA logistic p_up for the UPCOMING window (latest closed bar)."""
    ta=load_models(); feats=ta["feats"]
    df=binance_bars(400)
    F=build_feats(df)
    # iloc[-1] is the CURRENTLY-FORMING (incomplete) Binance bar -> corrupts features & biases UP.
    # Use iloc[-2] = the last COMPLETED bar, matching the training label alignment.
    row=F[feats].iloc[[-2]].replace([np.inf,-np.inf],np.nan)
    if row.isna().any(axis=1).iloc[0]: return None
    x=(row.values[0]-np.array(ta["mean"]))/np.array(ta["scale"])
    z=float(np.dot(x,np.array(ta["coef"]))+ta["intercept"])
    p=1.0/(1.0+np.exp(-z))
    return float(p)

def db():
    con=sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS ta_trades(
        market_id TEXT PRIMARY KEY, asset TEXT, start INT, close INT, direction TEXT, token_id TEXT,
        p_up REAL, conf REAL, entry_ts INT, entry_ask REAL, shares REAL, buy_order TEXT, buy_status TEXT,
        stop_level REAL, exit_ts INT, exit_reason TEXT, exit_price REAL, exit_order TEXT, ret REAL, pnl REAL,
        dry INT, settled INT DEFAULT 0)""")
    con.commit(); return con
def realized(con): return float(con.execute("SELECT COALESCE(SUM(pnl),0) FROM ta_trades WHERE pnl IS NOT NULL AND dry=0").fetchone()[0] or 0)
def has_open(con): return con.execute("SELECT 1 FROM ta_trades WHERE exit_ts IS NULL AND buy_status='filled' AND dry=0 LIMIT 1").fetchone() is not None

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
    con.execute("UPDATE ta_trades SET exit_ts=?,exit_reason=?,exit_price=?,exit_order=?,ret=?,pnl=?,settled=1 WHERE market_id=?",(now,"settle",1.0 if won else 0.0,"redeem",round(ret,4),pnl,mid)); con.commit()
    sb.log("TA SETTLE %s %s %s ret %+.1f%% ($%+.2f)"%(mid[:24],direction,"WON" if won else "LOST",100*ret,pnl))
    if (not dry) or sb.TG_DRY: sb.tg_send(f"{'✅' if won else '❌'} <b>TA SETTLE</b> {ASSET} {direction} {'WON' if won else 'LOST'} · {100*ret:+.1f}% (${pnl:+.2f})")
    return True

def try_enter(con,w):
    mid=w["market_id"]
    if con.execute("SELECT 1 FROM ta_trades WHERE market_id=?",(mid,)).fetchone(): return
    if sb.LIVE and realized(con)<=-KILL: sb.log("TA KILL-SWITCH: realized<=-%.0f"%KILL); return
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
            sb.log("TA BUY %s %s $%.0f ~%.3f (%.1f sh) p_up=%.3f conf=%.3f stop=%.3f -> %s %s"%(ASSET,direction,STAKE,entry,shares,p,conf,stop,buy_status,buy_order or resp))
            if buy_status=="failed": return
        except Exception as e: sb.log("TA BUY ERROR %s: %r"%(mid[:20],e)); return
    else:
        sb.log("TA DRY %s %s $%.0f entry~%.3f p_up=%.3f conf=%.3f stop=%.3f"%(ASSET,direction,STAKE,entry,p,conf,stop))
    con.execute("""INSERT OR IGNORE INTO ta_trades(market_id,asset,start,close,direction,token_id,p_up,conf,entry_ts,entry_ask,shares,buy_order,buy_status,stop_level,dry) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (mid,ASSET,w["start"],w["close"],direction,token,round(p,4),round(conf,4),int(time.time()),entry,shares,buy_order,buy_status,stop,dry)); con.commit()
    if (not dry) or sb.TG_DRY:
        arrow="🟢 UP" if direction=="UP" else "🔴 DOWN"
        sb.tg_send(("[DRY] " if dry else "")+f"🎯 <b>TA bet</b> {ASSET} {arrow} ${STAKE:.0f} @ {entry*100:.1f}¢ (p_up {p:.2f}, conf {conf:.2f}) · stop -{STOP*100:.0f}%")

def manage(con):
    now=int(time.time())
    for mid,token,direction,entry,shares,stop,close,dry in con.execute("SELECT market_id,token_id,direction,entry_ask,shares,stop_level,close,dry FROM ta_trades WHERE exit_ts IS NULL AND buy_status IN ('filled','dry')").fetchall():
        ask,bid=sb.token_book(token)
        if bid is None:
            if now>=close and _settle_next.get(mid,0)<=now:
                _settle_next[mid]=now+15; resolve_settled(con,mid,direction,entry,shares,now,dry)
            continue
        if bid<=stop:                                    # HARD -50% stop
            px=bid
            if dry:
                ret=(px-entry)/entry; con.execute("UPDATE ta_trades SET exit_ts=?,exit_reason=?,exit_price=?,ret=?,pnl=? WHERE market_id=?",(now,"STOP",px,round(ret,4),round(ret*shares*entry,4),mid)); con.commit()
                sb.log("TA DRY STOP %s @%.3f ret=%+.1f%%"%(mid[:22],px,100*ret)); continue
            try:
                limit=round(max(0.01,bid-0.02),2); resp=marketable_sell(token,shares,limit)
                if resp.get("success"):
                    ret=(px-entry)/entry; con.execute("UPDATE ta_trades SET exit_ts=?,exit_reason=?,exit_price=?,exit_order=?,ret=?,pnl=? WHERE market_id=?",(now,"STOP",px,resp.get("orderID") or resp.get("orderId"),round(ret,4),round(ret*shares*entry,4),mid)); con.commit()
                    sb.log("TA STOP %s %.0fsh @~%.3f (ret %+.1f%%)"%(mid[:20],shares,px,100*ret))
                    sb.tg_send(f"🛑 <b>TA STOP</b> {ASSET} {direction} @{px*100:.1f}¢ {100*ret:+.1f}% (${ret*shares*entry:+.2f})")
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
    sb.log("TA bot start | mode=%s asset=%s stake=$%.0f conf>=%.2f max_entry=%.2f stop-%.0f%% kill-$%.0f (binance weighted-TA voting)"%(
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
