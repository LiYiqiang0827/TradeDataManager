#!/usr/bin/env python3
"""Compare full online SQLite structures/counts and selected recent table hashes."""
import argparse,json
from pathlib import Path

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--source',required=True);ap.add_argument('--target',required=True);ap.add_argument('--output',required=True);a=ap.parse_args()
    x=json.loads(Path(a.source).read_text());y=json.loads(Path(a.target).read_text())
    diffs=[]
    if x['schema_sha256']!=y['schema_sha256']: diffs.append({'kind':'schema','source':x['schema_sha256'],'target':y['schema_sha256']})
    for key in sorted(set(x['table_counts'])|set(y['table_counts'])):
        if x['table_counts'].get(key)!=y['table_counts'].get(key): diffs.append({'kind':'row_count','table':key,'source':x['table_counts'].get(key),'target':y['table_counts'].get(key)})
    for key in sorted(set(x['selected_recent_rows'])|set(y['selected_recent_rows'])):
        if x['selected_recent_rows'].get(key)!=y['selected_recent_rows'].get(key): diffs.append({'kind':'row_hash','table':key,'source':x['selected_recent_rows'].get(key),'target':y['selected_recent_rows'].get(key)})
    if x.get('selected_tables_absent')!=y.get('selected_tables_absent'): diffs.append({'kind':'selected_table_presence','source':x.get('selected_tables_absent'),'target':y.get('selected_tables_absent')})
    report={'source_db':x['db'],'target_db':y['db'],'source_tables':x['table_count'],'target_tables':y['table_count'],'source_days':x.get('selected_dates'),'target_days':y.get('selected_dates'),'status':'pass' if not diffs else 'mismatch','differences':diffs}
    Path(a.output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return int(bool(diffs))
if __name__=='__main__':raise SystemExit(main())
