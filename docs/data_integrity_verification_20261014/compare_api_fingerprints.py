#!/usr/bin/env python3
"""Compare pre-migration and post-migration outputs from run_api_fingerprints.py."""
import argparse,json
from pathlib import Path

def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--target',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    src=json.loads(Path(a.source).read_text());dst=json.loads(Path(a.target).read_text())
    s={x['id']:x for x in src['results']};d={x['id']:x for x in dst['results']};out=[]
    for key in sorted(set(s)|set(d)):
        left=s.get(key);right=d.get(key)
        equal=left is not None and right is not None and left.get('status')==right.get('status') and left.get('fingerprint')==right.get('fingerprint')
        out.append({'id':key,'status':'pass' if equal else 'mismatch','source':left,'target':right})
    report={'source_label':src.get('label'),'target_label':dst.get('label'),'source_case_count':len(s),'target_case_count':len(d),'pass_count':sum(x['status']=='pass' for x in out),'mismatch_count':sum(x['status']!='pass' for x in out),'cases':out}
    Path(a.output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return int(report['mismatch_count']>0)
if __name__=='__main__':raise SystemExit(main())
