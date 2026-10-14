#!/usr/bin/env python3
"""Bounded-memory read-only period-table key/latest-date/current-factor audit."""
import argparse, json, sqlite3, time
from pathlib import Path
PAIRS={'week':('tbl_cn_week','tbl_cn_week_origin'),'month':('tbl_cn_month','tbl_cn_month_origin')}

def row_stream(cur, batch=20000):
    while True:
        rows=cur.fetchmany(batch)
        if not rows: return
        yield from rows

def key_diff(conn,a,b):
    ca=conn.execute(f'SELECT ts_code,trade_date FROM "{a}" ORDER BY ts_code,trade_date')
    cb=conn.execute(f'SELECT ts_code,trade_date FROM "{b}" ORDER BY ts_code,trade_date')
    ia,ib=iter(row_stream(ca)),iter(row_stream(cb)); x=next(ia,None); y=next(ib,None); only_a=only_b=0; sa=[]; sb=[]
    while x is not None and y is not None:
        if x==y: x=next(ia,None);y=next(ib,None)
        elif x<y:
            only_a+=1
            if len(sa)<5:sa.append(x)
            x=next(ia,None)
        else:
            only_b+=1
            if len(sb)<5:sb.append(y)
            y=next(ib,None)
    while x is not None:
        only_a+=1
        if len(sa)<5:sa.append(x)
        x=next(ia,None)
    while y is not None:
        only_b+=1
        if len(sb)<5:sb.append(y)
        y=next(ib,None)
    return {'left_only':only_a,'right_only':only_b,'left_sample':sa,'right_sample':sb}

def latest_map(conn,table):
    return dict(conn.execute(f'SELECT ts_code,MAX(trade_date) FROM "{table}" GROUP BY ts_code'))
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--db',required=True); ap.add_argument('--output',required=True); args=ap.parse_args()
 db=Path(args.db).resolve(); output=Path(args.output).resolve()
 t0=time.monotonic(); conn=sqlite3.connect(f'file:{db}?mode=ro&immutable=1',uri=True,timeout=.5);conn.execute('PRAGMA query_only=ON')
 out={'db':str(db),'read_mode':'immutable; no WAL/SHM/open writer found before scan','table_meta':{},'key_set_diffs':{},'daily_latest':{},'latest_period_vs_daily':{},'current_qfq_factor':{},'elapsed_seconds':None}
 try:
  for pair in PAIRS.values():
   for t in pair:
    count,codes,mn,mx=conn.execute(f'SELECT COUNT(*),COUNT(DISTINCT ts_code),MIN(trade_date),MAX(trade_date) FROM "{t}"').fetchone()
    out['table_meta'][t]={'rows':count,'codes':codes,'min_date':mn,'max_date':mx,'max_snap_ts':conn.execute(f'SELECT MAX(snap_ts) FROM "{t}"').fetchone()[0]}
  for freq,(q,o) in PAIRS.items():
   out['key_set_diffs'][freq]=key_diff(conn,q,o)
  daily=latest_map(conn,'tbl_cn_day'); out['daily_latest']={'codes':len(daily),'min_latest_date':min(daily.values()),'max_latest_date':max(daily.values())}
  for _,pair in PAIRS.items():
   for t in pair:
    pl=latest_map(conn,t); missing=sorted(set(daily)-set(pl));extra=sorted(set(pl)-set(daily)); mismatch=[(c,daily[c],pl[c]) for c in sorted(set(daily)&set(pl)) if daily[c]!=pl[c]]
    out['latest_period_vs_daily'][t]={'daily_codes':len(daily),'period_codes':len(pl),'missing_period_codes':missing[:20],'missing_period_count':len(missing),'period_only_codes':extra[:20],'period_only_count':len(extra),'latest_date_mismatch_count':len(mismatch),'latest_date_mismatch_sample':mismatch[:20]}
  # Resolve current latest non-null adjustment-factor baseline with indexed per-code seeks.
  codes=sorted(daily)
  latest_factor={}
  for code in codes:
   r=conn.execute('SELECT adj_factor,trade_date,snap_ts FROM tbl_cn_adj_factor WHERE ts_code=? AND adj_factor IS NOT NULL ORDER BY trade_date DESC LIMIT 1',(code,)).fetchone()
   if r:latest_factor[code]=r
  # For every monthly qfq historical row, verify its close is priced from the current
  # daily close/factor at that period end and the current latest factor baseline.
  sql='''SELECT q.ts_code,q.trade_date,q.close,d.close,df.adj_factor
    FROM tbl_cn_month q LEFT JOIN tbl_cn_day d USING(ts_code,trade_date)
    LEFT JOIN tbl_cn_adj_factor df USING(ts_code,trade_date)
    ORDER BY q.ts_code,q.trade_date'''
  checked=missing_daily=missing_factor=bad=0; max_abs=max_rel=0.0; examples=[]
  for code,td,qclose,rawclose,day_factor in row_stream(conn.execute(sql)):
   checked+=1
   if rawclose is None: missing_daily+=1;continue
   if day_factor is None:missing_factor+=1;day_factor=1.0
   current=latest_factor.get(code,(1.0,None,None))[0]
   if current is None or current==0:current=1.0
   expected=rawclose*day_factor/current; diff=abs(qclose-expected); rel=diff/max(1.0,abs(expected)); max_abs=max(max_abs,diff);max_rel=max(max_rel,rel)
   if diff>max(1e-6,abs(expected)*1e-9):
    bad+=1
    if len(examples)<20:examples.append({'ts_code':code,'trade_date':td,'qfq_close':qclose,'expected_current_factor_close':expected,'daily_close':rawclose,'factor_at_period_end':day_factor,'current_latest_factor':current,'abs_diff':diff})
  out['current_qfq_factor']['tbl_cn_month']={'rows_checked':checked,'missing_daily_row':missing_daily,'missing_factor_row_defaulted_to_one':missing_factor,'mismatch_count':bad,'max_abs_diff':max_abs,'max_relative_diff':max_rel,'mismatch_examples':examples,'meaning':'all historical monthly qfq closes checked against current daily close at period end and current latest non-null factor'}
  # Check latest weekly qfq rows for every code against the same current factor baseline.
  sql='''WITH latest AS (SELECT ts_code,MAX(trade_date) td FROM tbl_cn_week GROUP BY ts_code)
    SELECT q.ts_code,q.trade_date,q.close,d.close,df.adj_factor FROM tbl_cn_week q JOIN latest l
    ON l.ts_code=q.ts_code AND l.td=q.trade_date LEFT JOIN tbl_cn_day d USING(ts_code,trade_date)
    LEFT JOIN tbl_cn_adj_factor df USING(ts_code,trade_date) ORDER BY q.ts_code'''
  checked=missing_daily=missing_factor=bad=0;max_abs=max_rel=0.0;examples=[]
  for code,td,qclose,rawclose,day_factor in row_stream(conn.execute(sql)):
   checked+=1
   if rawclose is None:missing_daily+=1;continue
   if day_factor is None:missing_factor+=1;day_factor=1.0
   current=latest_factor.get(code,(1.0,None,None))[0]
   if current is None or current==0:current=1.0
   expected=rawclose*day_factor/current;diff=abs(qclose-expected);rel=diff/max(1.0,abs(expected));max_abs=max(max_abs,diff);max_rel=max(max_rel,rel)
   if diff>max(1e-6,abs(expected)*1e-9):
    bad+=1
    if len(examples)<20:examples.append({'ts_code':code,'trade_date':td,'qfq_close':qclose,'expected_current_factor_close':expected,'daily_close':rawclose,'factor_at_period_end':day_factor,'current_latest_factor':current,'abs_diff':diff})
  out['current_qfq_factor']['tbl_cn_week_latest']={'rows_checked':checked,'missing_daily_row':missing_daily,'missing_factor_row_defaulted_to_one':missing_factor,'mismatch_count':bad,'max_abs_diff':max_abs,'max_relative_diff':max_rel,'mismatch_examples':examples,'meaning':'latest weekly qfq closes checked for every code against current daily close/factors'}
  out['current_qfq_factor']['latest_factor_snapshot']={'codes':len(latest_factor),'min_factor_snap_ts':min((x[2] for x in latest_factor.values() if x[2]),default=None),'max_factor_snap_ts':max((x[2] for x in latest_factor.values() if x[2]),default=None),'min_factor_date':min((x[1] for x in latest_factor.values() if x[1]),default=None),'max_factor_date':max((x[1] for x in latest_factor.values() if x[1]),default=None)}
 finally:
  out['elapsed_seconds']=round(time.monotonic()-t0,3);conn.close()
 output.write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps(out,ensure_ascii=False,indent=2),flush=True)
if __name__=='__main__':main()
