"""Incremental, atomic publisher for the dedicated ladder read database."""
from __future__ import annotations
import argparse
import datetime
import fcntl
import hashlib
import json
import sqlite3
import time
from collections import defaultdict
from contextlib import closing
from pathlib import Path

from coreClient.limitup_ladder import LimitupLadderStore, SCHEMA_VERSION, ladder_database_path
from limitup_ladder.calculator import LadderCalculator

ALGORITHM_VERSION = 'deployed-paths-v1'
SCHEMA = '''
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS trade_dates(day TEXT PRIMARY KEY,previous_day TEXT,ordinal INTEGER,
 ready INTEGER NOT NULL,kpl_hash TEXT,raw_hash TEXT,updated_at TEXT);
CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY,day TEXT NOT NULL,code TEXT NOT NULL,
 kind TEXT NOT NULL,original_theme TEXT NOT NULL,board INTEGER NOT NULL,chain_id TEXT,payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS nodes_day_kind ON nodes(day,kind);
CREATE INDEX IF NOT EXISTS nodes_code_day ON nodes(code,day);
CREATE INDEX IF NOT EXISTS nodes_chain ON nodes(chain_id);
CREATE TABLE IF NOT EXISTS edges(source_id TEXT NOT NULL,target_id TEXT NOT NULL,source_day TEXT NOT NULL,
 target_day TEXT NOT NULL,status TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,
 PRIMARY KEY(source_id,target_id,status));
CREATE INDEX IF NOT EXISTS edges_target ON edges(target_day,status);
CREATE INDEX IF NOT EXISTS edges_source ON edges(source_id,status);
CREATE TABLE IF NOT EXISTS followups(source_id TEXT PRIMARY KEY,next_session TEXT,next_active_day TEXT,pct REAL);
CREATE TABLE IF NOT EXISTS theme_history(chain_id TEXT NOT NULL,effective_date TEXT NOT NULL,
 peak_node_id TEXT NOT NULL,peak_board INTEGER NOT NULL,theme TEXT NOT NULL,
 PRIMARY KEY(chain_id,effective_date));
CREATE TABLE IF NOT EXISTS daily_stats(day TEXT PRIMARY KEY,sealed_count INTEGER,broken_count INTEGER,
 unpromoted_count INTEGER,highest_board INTEGER,theme_counts TEXT);
'''

def encode(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)

def digest_rows(rows):
    h=hashlib.sha256()
    for row in rows:h.update(encode(list(row)).encode());h.update(b'\n')
    return h.hexdigest()

class Builder:
    def __init__(self,database=None,basic_db=None,kpl_db=None):
        self.database=Path(database or ladder_database_path())
        self.calculator=LadderCalculator(basic_db,kpl_db)

    def fingerprints(self,dates,old,audit=False,explicit_start=None):
        """All historical KPL changes are detected; raw daily facts audit recent seven sessions.

        Explicit historical ingestion passes --start-date; --audit verifies all raw days.
        """
        hashes={day:hashlib.sha256() for day in dates}
        with closing(sqlite3.connect(self.calculator.kpl_db)) as c:
            for table,cols,condition in [
                ('tbl_cn_kpl_limit_performance','trade_date,ts_code,name,theme,lu_time,close_price,board_period,pct_chg',''),
                ('tbl_cn_kpl_list','trade_date,ts_code,name,tag,lu_time,last_time,theme,lu_desc,pct_chg',"WHERE tag IN ('涨停','炸板')"),
                ('tbl_cn_limit_list','trade_date,ts_code,name,"limit",first_time,last_time,open_times,close,pct_chg','WHERE "limit" IN (\'U\',\'Z\')'),
            ]:
                for row in c.execute(f'SELECT {cols} FROM {table} {condition} ORDER BY trade_date,ts_code'):
                    if row[0] in hashes:hashes[row[0]].update((table+encode(list(row))+'\n').encode())
        recent=set(dates[-7:])
        selected=[day for day in dates if audit or day in recent or day not in old or (explicit_start and day>=explicit_start)]
        raw={day:old[day]['raw_hash'] for day in dates if day in old}
        with closing(sqlite3.connect(self.calculator.basic_db)) as c:
            for day in selected:
                h=hashlib.sha256()
                for table,query in [
                    ('daily','SELECT ts_code,open,high,low,close,pre_close,pct_chg,vol FROM tbl_cn_day WHERE trade_date=? ORDER BY ts_code'),
                    ('limits','SELECT ts_code,up_limit,down_limit FROM tbl_cn_stk_limit WHERE trade_date=? ORDER BY ts_code'),
                    ('suspends','SELECT ts_code,suspend_type,suspend_timing FROM tbl_cn_suspend WHERE trade_date=? ORDER BY ts_code'),
                    ('calendar',"SELECT exchange,is_open FROM tbl_cn_tradecal WHERE cal_date=? ORDER BY exchange"),
                ]:
                    h.update(table.encode());h.update(digest_rows(c.execute(query,(day,))).encode())
                raw[day]=h.hexdigest()
        return {day:(hashes[day].hexdigest(),raw.get(day,'')) for day in dates}

    def ready(self,day):
        """Same coverage gate as the active dashboard; never publish half of a trading day."""
        with closing(sqlite3.connect(self.calculator.kpl_db)) as k, closing(sqlite3.connect(self.calculator.basic_db)) as b:
            expected=set()
            for table,condition in [('tbl_cn_kpl_limit_performance',''),('tbl_cn_kpl_list'," AND tag='涨停'"),('tbl_cn_limit_list'," AND \"limit\"='U'")]:
                expected.update(r[0] for r in k.execute(f'SELECT ts_code FROM {table} WHERE trade_date=?'+condition,(day,)))
            prior=b.execute('SELECT MAX(trade_date) FROM tbl_cn_day WHERE trade_date<?',(day,)).fetchone()[0]
            expected.update(r[0] for r in b.execute('''SELECT d.ts_code FROM tbl_cn_day d JOIN tbl_cn_stk_limit l
                ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date WHERE d.trade_date=?
                AND l.up_limit>0 AND abs(d.close-l.up_limit)<.005''',(prior,)))
            actual={r[0] for r in b.execute('SELECT d.ts_code FROM tbl_cn_day d JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date WHERE d.trade_date=? AND d.close IS NOT NULL',(day,))}
            halted={r[0] for r in b.execute("SELECT ts_code FROM tbl_cn_suspend WHERE trade_date=? AND suspend_type='S' AND coalesce(suspend_timing,'')=''",(day,))}
            missing=expected-actual-halted
            return bool(actual) and not missing,sorted(missing)

    def insert_node(self,c,node,kind,replace=False):
        # Context is supplemental; it must not replace a node's authoritative daily record.
        item=dict(node);original=item.get('originalTheme',item.get('theme') or '暂无题材')
        item['theme']=original;item['originalTheme']=original
        values=(item['id'],item['date'],item['code'],kind,original,int(item.get('board') or 0),None,encode(item))
        clause='DO UPDATE SET day=excluded.day,code=excluded.code,kind=excluded.kind,original_theme=excluded.original_theme,board=excluded.board,payload=excluded.payload' if replace else 'DO NOTHING'
        c.execute('INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(id) '+clause,values)

    def rebuild_followups_and_themes(self,c,calendar,changed_from):
        # Follow-ups are materialized from trading sessions, including full-day halts.
        with closing(sqlite3.connect(self.calculator.basic_db)) as b:
            halted=defaultdict(set)
            for day,code in b.execute("SELECT trade_date,ts_code FROM tbl_cn_suspend WHERE suspend_type='S' AND coalesce(suspend_timing,'')=''"):halted[code].add(day)
            index={d:i for i,d in enumerate(calendar)}
            follow=[]
            affected={r[0] for r in c.execute('SELECT DISTINCT code FROM nodes WHERE day>=?',(changed_from,))}
            affected.update(r[0] for r in c.execute('SELECT n.code FROM followups f JOIN nodes n ON n.id=f.source_id WHERE f.next_active_day>=? OR f.next_active_day IS NULL',(changed_from,)))
            records=[]
            for offset in range(0,len(affected),400):
                chunk=sorted(affected)[offset:offset+400];q=','.join('?' for _ in chunk)
                records.extend(c.execute(f"SELECT id,day,code,board,original_theme,chain_id FROM nodes WHERE kind='sealed' AND code IN ({q})",chunk))
            records.sort(key=lambda r:(r['day'],r['id']))
            for r in records:
                i=index.get(r['day'])
                if i is None:continue
                next_session=calendar[i+1] if i+1<len(calendar) else None
                j=i+1
                while j<len(calendar) and calendar[j] in halted[r['code']]:j+=1
                active=calendar[j] if j<len(calendar) else None
                price=b.execute('SELECT pct_chg FROM tbl_cn_day WHERE ts_code=? AND trade_date=?',(r['code'],active)).fetchone() if active else None
                follow.append((r['id'],next_session,active,price[0] if price else None))
        c.execute('DELETE FROM followups WHERE NOT EXISTS (SELECT 1 FROM nodes n WHERE n.id=followups.source_id)')
        c.executemany('INSERT OR REPLACE INTO followups VALUES (?,?,?,?)',follow)
        incoming={r['target_id']:r['source_id'] for r in c.execute("SELECT source_id,target_id FROM edges WHERE status='promoted' ORDER BY target_day,source_id")}
        chains={};peaks={};history=[];node_chains=[]
        for r in records:
            chain=chains.get(incoming.get(r['id']),r['id']);chains[r['id']]=chain
            peak=peaks.get(chain)
            if peak is None or r['board']>=peak['board']:
                peaks[chain]=r;history.append((chain,r['day'],r['id'],r['board'],r['original_theme']))
            node_chains.append((chain,r['id']))
        old_chains=sorted({r['chain_id'] for r in records if r['chain_id']}|set(chains.values()))
        for offset in range(0,len(old_chains),400):
            chunk=old_chains[offset:offset+400];q=','.join('?' for _ in chunk)
            c.execute(f'DELETE FROM theme_history WHERE chain_id IN ({q})',chunk)
        c.executemany('INSERT OR REPLACE INTO theme_history VALUES (?,?,?,?,?)',history)
        c.executemany('UPDATE nodes SET chain_id=? WHERE id=?',node_chains)

    def run(self,start_date=None,end_date=None,audit=False,force=False):
        began=time.perf_counter();self.database.parent.mkdir(parents=True,exist_ok=True)
        with self.database.with_suffix('.db.lock').open('a') as lock:
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                print(encode({'status':'waiting_for_publisher','database':str(self.database)}),flush=True)
                fcntl.flock(lock,fcntl.LOCK_EX)
            with closing(sqlite3.connect(self.database,timeout=30)) as c:
                c.row_factory=sqlite3.Row;c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=NORMAL');c.executescript(SCHEMA)
                old_meta={r[0]:json.loads(r[1]) for r in c.execute('SELECT key,value FROM metadata')}
                if old_meta.get('schema_version',SCHEMA_VERSION)!=SCHEMA_VERSION:raise RuntimeError('Unsupported ladder schema; use a separate rebuild path')
                self.calculator._dates=None
                effective_end=max(end_date,old_meta.get('latest_date') or end_date) if end_date else None
                dates=[d for d in self.calculator.market_data_dates() if not effective_end or d<=effective_end]
                if not dates:return {'status':'no_source_data'}
                old={r['day']:dict(r) for r in c.execute('SELECT * FROM trade_dates')}
                fingerprints=self.fingerprints(dates,old,audit,start_date)
                dirty=[d for d in dates if d not in old or fingerprints[d]!=(old[d]['kpl_hash'],old[d]['raw_hash']) or (force and (not start_date or d>=start_date))]
                if old_meta.get('algorithm_version')!=ALGORITHM_VERSION:dirty=dates
                if not dirty:return {'status':'unchanged','days':len(dates),'revision':old_meta.get('revision'),'seconds':round(time.perf_counter()-began,3)}
                earliest=min(dirty)
                # Any correction can alter later board totals, classification and resumed chains.
                # Rebuild the suffix, instead of arbitrarily assuming a fixed propagation horizon.
                rebuild=[d for d in dates if d>=earliest]
                with closing(sqlite3.connect(self.calculator.basic_db)) as b:
                    calendar=[r[0] for r in b.execute("SELECT cal_date FROM tbl_cn_tradecal WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date")]
                cal_index={d:i for i,d in enumerate(calendar)}
                now=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat()
                blocked=[];built=0
                c.execute('BEGIN IMMEDIATE')
                try:
                    for day in rebuild:
                        ready,missing=self.ready(day)
                        if not ready:
                            if day in old and old[day]['ready']:
                                raise RuntimeError(f'{day} upstream coverage regressed; retaining the published generation')
                            blocked.append({'date':day,'missingCodes':missing[:30],'missingCount':len(missing)})
                            # Keep a last-good published day through incomplete upstream replacement.
                            if day not in old or not old[day]['ready']:
                                i=cal_index.get(day,0);c.execute('INSERT OR REPLACE INTO trade_dates VALUES (?,?,?,?,?,?,?)',(day,calendar[i-1] if i else None,i,0,*fingerprints[day],now))
                            continue
                        payload=self.calculator.calculate(day,1)
                        broken=self.calculator.broken_for_day(day)['brokenStocks']
                        c.execute('DELETE FROM nodes WHERE day=?',(day,));c.execute('DELETE FROM edges WHERE target_day=?',(day,))
                        for item in payload['sealedStocks']:self.insert_node(c,item,'sealed',True)
                        for item in payload['contextStocks']+payload['relimitContextStocks']:self.insert_node(c,item,'sealed')
                        for item in payload['relimitBrokenStocks']:self.insert_node(c,item,'relimit_broken')
                        for item in payload['unpromotedStocks']:self.insert_node(c,item,'unpromoted',True)
                        for item in broken:self.insert_node(c,item,'broken',True)
                        for edge in payload['links']+payload['relimitLinks']:
                            if edge['status'] not in ('promoted','relimit'):continue
                            c.execute('INSERT OR REPLACE INTO edges VALUES (?,?,?,?,?,?,?)',(edge['source'],edge['target'],edge['source'].split(':')[0],edge['target'].split(':')[0],edge['status'],edge.get('kind',edge['status']),encode(edge)))
                        i=cal_index.get(day,0);c.execute('INSERT OR REPLACE INTO trade_dates VALUES (?,?,?,?,?,?,?)',(day,calendar[i-1] if i else None,i,1,*fingerprints[day],now))
                        themes=defaultdict(int)
                        for item in payload['sealedStocks']:themes[item['theme']]+=1
                        c.execute('INSERT OR REPLACE INTO daily_stats VALUES (?,?,?,?,?,?)',(day,len(payload['sealedStocks']),len(broken),len(payload['unpromotedStocks']),max([0]+[n['board'] for n in payload['sealedStocks']]),encode(themes)))
                        built+=1
                        if built%100==0:print(encode({'progressDays':built,'date':day,'elapsedSeconds':round(time.perf_counter()-began,1)}),flush=True)
                    # Publish only if relevant source facts still match the captured generation.
                    verified=self.fingerprints(dates,old,audit,start_date)
                    if verified!=fingerprints:raise RuntimeError('Source facts changed during build; previous generation retained; retry after upstream commit')
                    self.rebuild_followups_and_themes(c,calendar,earliest)
                    content=[tuple(r) for r in c.execute('SELECT day,kpl_hash,raw_hash,ready FROM trade_dates ORDER BY day')]
                    revision=hashlib.sha256((ALGORITHM_VERSION+encode(content)).encode()).hexdigest()[:20]
                    m={'schema_version':SCHEMA_VERSION,'algorithm_version':ALGORITHM_VERSION,'revision':revision,'source_snapshot_at':now,'updated_at':datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(),'latest_date':c.execute('SELECT MAX(day) FROM trade_dates WHERE ready=1').fetchone()[0],'day_count':c.execute('SELECT COUNT(*) FROM trade_dates WHERE ready=1').fetchone()[0],'blocked_days':blocked}
                    c.executemany('INSERT OR REPLACE INTO metadata VALUES (?,?)',[(k,encode(v)) for k,v in m.items()]);c.commit()
                except BaseException:c.rollback();raise
                return {'status':'updated','builtDays':built,'from':earliest,'through':dates[-1],'blockedDays':blocked,'revision':revision,'seconds':round(time.perf_counter()-began,3),'database':str(self.database)}

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',type=Path);p.add_argument('--start-date');p.add_argument('--end-date');p.add_argument('--trade-date');p.add_argument('--audit',action='store_true');p.add_argument('--force',action='store_true')
    a=p.parse_args()
    print(encode(Builder(a.database).run(a.start_date or a.trade_date,a.end_date,a.audit,a.force)),flush=True)

if __name__=='__main__':main()
