#!/usr/bin/env python3
"""Read-only weekly/monthly sample checks against local daily bars and factor API."""
import argparse,json,sys,hashlib,sqlite3,urllib.parse
from pathlib import Path
import numpy as np
import pandas as pd

CODES=['000001.SZ','600000.SH','300750.SZ']
START='20250101'; END='20261014'

def agg_daily(df,freq):
    x=df.sort_values(['ts_code','trade_date']).copy()
    td=pd.to_datetime(x.trade_date.astype(str),format='%Y%m%d')
    if freq=='week':
        iso=td.dt.isocalendar();x['_period']=iso.year.astype(str)+'-W'+iso.week.astype(str).str.zfill(2)
    else: x['_period']=td.dt.strftime('%Y-%m')
    y=x.groupby(['ts_code','_period'],as_index=False).agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),vol=('vol','sum'),amount=('amount','sum'),trade_date=('trade_date','last'))
    y=y.sort_values(['ts_code','trade_date']).reset_index(drop=True)
    y['pre_close']=y.groupby('ts_code').close.shift(1)
    y['change']=y.close-y.pre_close
    y['pct_chg']=(y.change/y.pre_close*100).round(4)
    return y

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--repo-root',default='/Users/nickzhang/TradingDataManager');ap.add_argument('--database-root',required=True);ap.add_argument('--output',required=True);a=ap.parse_args()
    sys.path.insert(0,str(Path(a.repo_root).resolve()))
    import os;os.environ['TRADING_DATABASE_ROOT']=str(Path(a.database_root).resolve())
    from coreClient.data_provider import get_day,get_week,get_month
    daily_raw=get_day(ts_codes=CODES,start_date=START,end_date=END,qfq=False,source='database_only')
    daily_qfq=get_day(ts_codes=CODES,start_date=START,end_date=END,qfq=True,source='database_only')
    result={'codes':CODES,'daily_range':[START,END],'daily_raw_rows':len(daily_raw),'daily_qfq_rows':len(daily_qfq),'periods':{}}
    dates=pd.to_datetime(daily_raw.trade_date.astype(str),format='%Y%m%d')
    latest=dates.max()
    # Exclude the still-open latest week/month; period rows end on the last trading day.
    last_full_week=latest.to_period('W-FRI')-1
    week_cutoff=daily_raw.loc[dates.dt.to_period('W-FRI')==last_full_week,'trade_date'].max()
    last_full_month=(latest.to_period('M')-1)
    month_cutoff=daily_raw.loc[dates.dt.to_period('M')==last_full_month,'trade_date'].max()
    for freq,df0,df1,reader,cutoff in [('week',daily_raw,daily_qfq,get_week,week_cutoff),('month',daily_raw,daily_qfq,get_month,month_cutoff)]:
        raw_agg=agg_daily(df0,freq);qfq_agg=agg_daily(df1,freq)
        raw_period=reader(ts_codes=CODES,start_date=START,end_date=END,qfq=False,source='database_only')
        qfq_period=reader(ts_codes=CODES,start_date=START,end_date=END,qfq=True,source='database_only')
        key=f'week_{{}}' if freq=='week' else f'month_{{}}'
        by={}
        for mode,period,aggregate in [('origin',raw_period,raw_agg),('qfq',qfq_period,qfq_agg)]:
            name=key.format(mode)
            sel=period[pd.to_numeric(period.trade_date,errors='coerce')<=int(cutoff)].sort_values(['ts_code','trade_date']).groupby('ts_code').tail(3)
            expected=aggregate[aggregate.trade_date.isin(sel.trade_date)].copy()
            joins=sel.merge(expected,on=['ts_code','trade_date'],suffixes=('_table','_daily'),how='outer',indicator=True)
            cols=['open','high','low','close','vol','amount']
            mism=[]
            for _,r in joins.iterrows():
                if r['_merge']!='both':mism.append({'ts_code':r.ts_code,'trade_date':int(r.trade_date),'reason':'row_presence'});continue
                bad=[col for col in cols if not np.isclose(float(r[f'{col}_table']),float(r[f'{col}_daily']),rtol=1e-9,atol=1e-6,equal_nan=True)]
                if bad:mism.append({'ts_code':r.ts_code,'trade_date':int(r.trade_date),'fields':bad})
            by[name]={'max_table_date':str(period.trade_date.max()) if not period.empty else None,'cutoff':str(cutoff),'sampled_rows':len(sel),'sample_codes':sorted(sel.ts_code.unique().tolist()),'mismatches':mism,'checked_fields':cols,'volume_rule':'compared to project get_day(qfq=mode) then OHLC first/max/min/last, vol/amount sum; not judged against a generic adjustment formula'}
        result['periods'][freq]=by
    result['complete_cutoffs']={'week':str(week_cutoff),'month':str(month_cutoff),'latest_daily':str(daily_raw.trade_date.max())}
    basic_path=Path(a.database_root)/'offline'/'db_cn_basic.db'
    uri='file:'+urllib.parse.quote(str(basic_path),safe='/')+'?mode=ro'
    conn=sqlite3.connect(uri,uri=True,timeout=.5)
    try:
        result['period_table_counts']={}
        for table in ('tbl_cn_week','tbl_cn_week_origin','tbl_cn_month','tbl_cn_month_origin'):
            result['period_table_counts'][table]=dict(zip(('rows','codes','min_trade_date','max_trade_date'),conn.execute(f'SELECT count(*),count(distinct ts_code),min(trade_date),max(trade_date) FROM "{table}"').fetchone()))
        result['control_rows']=[dict(zip(('key','max_date','updated_at'),row)) for row in conn.execute("SELECT key,max_date,updated_at FROM tbl_basic_ctrl WHERE key IN ('cn_daily','cn_adj_factor','cn_week','cn_month') ORDER BY key")]
    finally: conn.close()
    Path(a.output).write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str)+'\n')
if __name__=='__main__':main()
