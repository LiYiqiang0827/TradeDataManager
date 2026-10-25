from __future__ import annotations
import argparse, json
from pathlib import Path
import duckdb

p=argparse.ArgumentParser()
p.add_argument('--db', required=True)
p.add_argument('--out', required=True)
p.add_argument('--start', required=True)
p.add_argument('--end', required=True)
a=p.parse_args()
out=Path(a.out); out.mkdir(parents=True, exist_ok=True)
c=duckdb.connect(a.db, read_only=True)
dates=[r[0] for r in c.execute('''select distinct trade_date from fact_limit_event where tag=? and trade_date between ? and ? order by trade_date''',['涨停',a.start,a.end]).fetchall()]
for d in dates:
    rows=c.execute('''
      select e.ts_code, coalesce(e.name,s.name,'未知'), e.status_raw, e.continuous_board,
             e.lu_time,e.ld_time,e.open_time,e.last_time,e.lu_desc,e.theme_raw,
             e.amount,e.turnover_rate,e.bid_amount,
             coalesce((select string_agg(x.source_label, ' | ' order by x.source_label)
                       from (select distinct r.source_label from rel_limit_theme r where r.event_id=e.event_id) x),'') normalized_themes
      from fact_limit_event e left join dim_stock s using(ts_code)
      where e.trade_date=? and e.tag=?
      order by e.lu_time nulls last, e.ts_code
    ''',[d,'涨停']).fetchall()
    fields=['ts_code','name','status_raw','continuous_board','lu_time','ld_time','open_time','last_time','lu_desc','theme_raw','amount','turnover_rate','bid_amount','normalized_themes']
    limitups=[dict(zip(fields,row)) for row in rows]
    metrics=c.execute('''select t.canonical_name,d.limit_up_count,d.break_count,d.max_board_height,d.first_board_count,
      d.multi_board_count,d.ladder_points,d.seal_rate,d.heat_score,d.lifecycle_state
      from fact_theme_daily d join dim_theme t using(theme_id)
      where d.trade_date=? order by d.limit_up_count desc,t.canonical_name''',[d]).fetchall()
    mfields=['canonical_name','limit_up_count','break_count','max_board_height','first_board_count','multi_board_count','ladder_points','seal_rate','heat_score','lifecycle_state']
    packet={'trade_date':d,'cutoff':'收盘 (Asia/Shanghai)','source':'TradingDataBase/offline/db_theme_graph.duckdb fact_limit_event + fact_theme_daily; extracted from Mac mini, read-only','limitup_stock_count':len(limitups),'limitups':limitups,'theme_metrics':[dict(zip(mfields,row)) for row in metrics]}
    (out/f'{d}.json').write_text(json.dumps(packet,ensure_ascii=False,indent=2,allow_nan=True)+'\n',encoding='utf-8')
    print(d,len(limitups),len(metrics))
