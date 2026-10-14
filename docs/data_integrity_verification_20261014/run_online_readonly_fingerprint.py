#!/usr/bin/env python3
"""Read-only structural and selected online-table fingerprint runner."""
import argparse, hashlib, json, sqlite3, time, urllib.parse
from pathlib import Path


def qid(s): return '"' + str(s).replace('"', '""') + '"'

def typed(v):
    if v is None: return ["null", None]
    if isinstance(v, bytes): return ["bytes", v.hex()]
    return [type(v).__name__, v]

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',required=True)
    ap.add_argument('--label',required=True)
    ap.add_argument('--output',required=True)
    ap.add_argument('--dates',default='20261012,20261013,20261014')
    ap.add_argument('--immutable',action='store_true',help='only after writers stopped and no WAL exists')
    a=ap.parse_args()
    suffix='mode=ro&immutable=1' if a.immutable else 'mode=ro'
    uri='file:'+urllib.parse.quote(str(Path(a.db).resolve()),safe='/')+'?'+suffix
    con=sqlite3.connect(uri,uri=True,timeout=.5)
    con.execute('PRAGMA query_only=ON')
    tables=con.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    schema={}; counts={}; rows={}
    for name,sql in tables:
        cols=con.execute(f'PRAGMA table_info({qid(name)})').fetchall()
        idx=con.execute(f'PRAGMA index_list({qid(name)})').fetchall()
        index_defs=[]
        for ix in idx:
            index_defs.append({'name':ix[1],'unique':ix[2],'origin':ix[3],'partial':ix[4], 'sql':con.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?",(ix[1],)).fetchone()[0]})
        schema[name]={'sql':sql,'columns':cols,'indexes':index_defs,'foreign_keys':con.execute(f'PRAGMA foreign_key_list({qid(name)})').fetchall()}
        counts[name]=con.execute(f'SELECT count(*) FROM {qid(name)}').fetchone()[0]
        day=name.rsplit('_',1)[-1]
        family=name.rsplit('_',1)[0]
        selected_family = family in {'minute','snapshot','snapshot_index','auction'} and day in set(a.dates.split(','))
        if not selected_family and name != 'online_stream_cursor':
            continue
        pk=[r[1] for r in sorted(cols,key=lambda r:r[5]) if r[5]]
        order=pk or [r[1] for r in cols]
        stmt=f'SELECT * FROM {qid(name)} ORDER BY '+','.join(qid(c) for c in order)
        h=hashlib.sha256(); n=0
        cur=con.execute(stmt)
        while True:
            batch=cur.fetchmany(2000)
            if not batch: break
            for row in batch:
                h.update(json.dumps([typed(x) for x in row],ensure_ascii=False,separators=(',',':')).encode())
                h.update(b'\n'); n+=1
        rows[name]={'row_count':n,'ordered_sha256':h.hexdigest(),'order_by':order}
    con.close()
    selected_dates=a.dates.split(',')
    candidates=[f'{family}_{day}' for day in selected_dates for family in ('minute','snapshot','snapshot_index','auction')]
    present=set(schema)
    out={'label':a.label,'db':str(Path(a.db).resolve()),'generated_epoch':time.time(),'immutable':a.immutable,'table_count':len(tables),'schema_sha256':hashlib.sha256(json.dumps(schema,ensure_ascii=False,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest(),'schema':schema,'table_counts':counts,'selected_recent_rows':rows,'selected_dates':selected_dates,'selected_table_candidates':candidates,'selected_tables_absent':[t for t in candidates if t not in present]}
    Path(a.output).write_text(json.dumps(out,ensure_ascii=False,indent=2,default=str)+'\n')
if __name__=='__main__': main()
