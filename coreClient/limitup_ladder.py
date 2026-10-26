"""Read-only API for the materialized limit-up ladder (stdlib SQLite only)."""
from __future__ import annotations

import datetime
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from coreClient._paths import database_root

SCHEMA_VERSION = 1


def ladder_database_path():
    return Path(os.environ.get('TRADING_LADDER_DATABASE', database_root()/'offline'/'limitup_ladder.db')).expanduser()


class LadderNotReady(LookupError):
    pass


class LimitupLadderStore:
    def __init__(self, database=None):
        self.database = Path(database or ladder_database_path())

    def connect(self):
        # Read-only mode must never create an empty database on a clean clone.
        c = sqlite3.connect(self.database.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)
        c.row_factory = sqlite3.Row
        return c

    def metadata(self):
        if not self.database.is_file(): return None
        try:
            with closing(self.connect()) as c:
                m = {r[0]:json.loads(r[1]) for r in c.execute('SELECT key,value FROM metadata')}
                if m.get('schema_version')!=SCHEMA_VERSION or not m.get('revision'): return None
                return m
        except sqlite3.Error:
            return None

    def dates(self):
        with closing(self.connect()) as c:
            return [r[0] for r in c.execute("SELECT day FROM trade_dates ORDER BY day")]

    def contains(self, day):
        if not self.database.is_file(): return False
        with closing(self.connect()) as c:
            return c.execute('SELECT 1 FROM trade_dates WHERE day=? AND ready=1',(day,)).fetchone() is not None

    def broken(self, start, end):
        with closing(self.connect()) as c:
            c.execute('BEGIN')
            day_rows=list(c.execute('SELECT day,ready FROM trade_dates WHERE day BETWEEN ? AND ? ORDER BY day',(start,end)))
            if any(not r['ready'] for r in day_rows):raise LadderNotReady('Incomplete historical source range')
            dates=[r['day'] for r in day_rows]
            rows = [json.loads(r[0]) for r in c.execute("SELECT payload FROM nodes WHERE kind='broken' AND day BETWEEN ? AND ? ORDER BY day,id",(start,end))]
            m=c.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()
            return {'dates':dates,'brokenStocks':rows,'storeRevision':json.loads(m[0]) if m else None}

    def read(self, end_date, sessions=20):
        if not 1<=sessions<=30: raise ValueError('sessions must be 1..30')
        with closing(self.connect()) as c:
            c.execute('BEGIN')  # All tables and the published revision share one SQLite snapshot.
            meta = {r[0]:json.loads(r[1]) for r in c.execute('SELECT key,value FROM metadata')}
            if meta.get('schema_version')!=SCHEMA_VERSION or not meta.get('revision'): raise LadderNotReady('No published ladder generation')
            rows = list(c.execute('SELECT day,previous_day,ready FROM trade_dates WHERE day<=? ORDER BY day DESC LIMIT ?',(end_date,sessions)))
            if not rows or rows[0]['day']!=end_date or any(not r['ready'] for r in rows): raise LadderNotReady(end_date)
            rows.reverse();dates=[r['day'] for r in rows];date_set=set(dates)
            previous_dates={r['day']:r['previous_day'] for r in rows}
            pool_days=sorted(date_set|{d for d in previous_dates.values() if d})
            marks=','.join('?' for _ in pool_days)
            records={r['id']:r for r in c.execute(f"SELECT * FROM nodes WHERE kind='sealed' AND day IN ({marks})",pool_days)}
            promoted=[json.loads(r[0]) for r in c.execute(f"SELECT payload FROM edges WHERE status='promoted' AND target_day IN ({marks})",pool_days)]
            relimits=[json.loads(r[0]) for r in c.execute("SELECT payload FROM edges WHERE status='relimit' AND target_day BETWEEN ? AND ? ORDER BY target_day,source_id",(dates[0],end_date))]
            needed={key for edge in promoted+relimits for key in (edge['source'],edge['target'])}-records.keys()
            for offset in range(0,len(needed),400):
                chunk=sorted(needed)[offset:offset+400];q=','.join('?' for _ in chunk)
                records.update((r['id'],r) for r in c.execute(f'SELECT * FROM nodes WHERE id IN ({q})',chunk))
            # Effective date is the queried cutoff, never the latest built database day.
            chain_ids=sorted({r['chain_id'] for r in records.values() if r['chain_id']})
            peaks={}
            for offset in range(0,len(chain_ids),400):
                chunk=chain_ids[offset:offset+400];q=','.join('?' for _ in chunk)
                for r in c.execute(f'''SELECT h.* FROM theme_history h
                    WHERE h.chain_id IN ({q}) AND h.effective_date=(SELECT MAX(h2.effective_date)
                    FROM theme_history h2 WHERE h2.chain_id=h.chain_id AND h2.effective_date<=?)''',[*chunk,end_date]): peaks[r['chain_id']]=r
            def node(record):
                item=json.loads(record['payload']);item['originalTheme']=record['original_theme']
                p=peaks.get(record['chain_id'])
                if p is not None:
                    item.update(theme=p['theme'],themePeakDate=p['effective_date'],themePeakBoard=p['peak_board'],themeAsOfDate=end_date)
                return item
            items={key:node(r) for key,r in records.items()}
            sealed=sorted([items[key] for key,r in records.items() if r['kind']=='sealed' and r['day'] in date_set],key=lambda x:(x['date'],x['code']))
            # Context is the previous cohort plus exact ancestors needed by promotion edges.
            context_ids={key for key,r in records.items() if r['kind']=='sealed' and r['day'] in pool_days and r['day'] not in date_set}
            context_ids|={e['source'] for e in promoted if e['source'] in items and items[e['source']]['date'] not in date_set}
            context=sorted([items[key] for key in context_ids],key=lambda x:(x['date'],x['code']))
            active_sources={e['source'] for e in promoted}
            links=list(promoted);outcomes=[]
            candidates={s['id']:s for s in sealed+context if s['board']>1 and s['id'] not in active_sources}
            for offset in range(0,len(candidates),400):
                chunk=sorted(candidates)[offset:offset+400];q=','.join('?' for _ in chunk)
                for r in c.execute(f'SELECT * FROM followups WHERE source_id IN ({q})',chunk):
                    source=candidates[r['source_id']]
                    if r['next_active_day'] and r['next_active_day']<=end_date:
                        day=r['next_active_day'];status='failed';pct=r['pct']
                    elif r['next_session'] and r['next_session']<=end_date:
                        day=end_date;status='paused';pct=None
                    else: continue
                    target={'id':day+':'+source['code']+':'+status,'date':day,'code':source['code'],'name':source['name'],'theme':source['theme'],'originalTheme':source['originalTheme'],'board':0,'previousBoard':source['board'],'status':status,'pct':pct,'time':'','representative':False,'firstCount':0,'members':[]}
                    outcomes.append(target);links.append({'source':source['id'],'target':target['id'],'status':status,'value':1})
            unpromoted=[json.loads(r[0]) for r in c.execute("SELECT payload FROM nodes WHERE kind='unpromoted' AND day BETWEEN ? AND ? ORDER BY day,id",(dates[0],end_date))]
            for stock in unpromoted:
                original=stock.get('originalTheme',stock['theme']);stock['originalTheme']=original
                if stock.get('sourceId') in items:stock['theme']=items[stock['sourceId']]['theme']
            carry=[]
            for edge in promoted:
                if edge['kind']!='suspension':continue
                day,code=edge['target'].split(':',1);source=items.get(edge['source']);prior=previous_dates.get(day)
                if day in date_set and source and prior and source['date']<prior:
                    carry.append({'date':prior,'targetDate':day,'code':code,'board':source['board'],'theme':source['theme'],'sourceId':edge['source'],'targetId':edge['target']})
            first={day:{} for day in dates}
            for stock in sealed:
                if stock['board']==1:first[stock['date']][stock['theme']]=first[stock['date']].get(stock['theme'],0)+1
            relimit_context=[items[e['source']] for e in relimits if e['source'] in items and items[e['source']].get('status')=='sealed']
            relimit_broken=[items[e['source']] for e in relimits if e['source'] in items and items[e['source']].get('status')=='broken']
            broken=[json.loads(r[0]) for r in c.execute("SELECT payload FROM nodes WHERE kind='broken' AND day BETWEEN ? AND ? ORDER BY day,id",(dates[0],end_date))]
            return {'date':end_date,'dates':dates,'previousDates':previous_dates,'nodes':sealed+outcomes,'sealedStocks':sealed,'contextStocks':context,'links':links,'relimitLinks':relimits,'relimitContextStocks':relimit_context,'relimitBrokenStocks':relimit_broken,'unpromotedStocks':unpromoted,'suspensionCarry':carry,'suspendedStocks':[],'brokenStocks':broken,'brokenDates':dates,'firstCounts':first,'themes':[{'name':t} for t in sorted({s['theme'] for s in sealed})],'live':None,'source':'offline_limitup_ladder','precomputed':True,'storeRevision':meta['revision'],'themeAsOfDate':end_date}
