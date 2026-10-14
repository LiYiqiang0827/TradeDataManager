#!/usr/bin/env python3
"""Compare Redis values while treating elapsed TTL decay and expected expiry separately."""
import argparse,json
from pathlib import Path

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--before',required=True);ap.add_argument('--after',required=True);ap.add_argument('--output',required=True);ap.add_argument('--elapsed-seconds',type=float,required=True);ap.add_argument('--ttl-tolerance-ms',type=int,default=2000);a=ap.parse_args()
    b=json.loads(Path(a.before).read_text());n=json.loads(Path(a.after).read_text());x={r['key_sha256']:r for r in b['keys']};y={r['key_sha256']:r for r in n['keys']};out=[]
    for k in sorted(set(x)|set(y)):
        old=x.get(k);new=y.get(k)
        if old is None: status='new_key'
        elif new is None:
            ttl=old['pttl_ms']; status='expected_expired' if 0 <= ttl <= a.elapsed_seconds*1000+a.ttl_tolerance_ms else 'missing_key'
        elif old['type']!=new['type'] or old['dump_sha256']!=new['dump_sha256']:status='value_changed'
        else:
            ot,nt=old['pttl_ms'],new['pttl_ms']; delta=nt-ot+a.elapsed_seconds*1000
            status='pass' if (ot<0 and nt==ot) or abs(delta)<=a.ttl_tolerance_ms else 'ttl_changed'
        out.append({'key_sha256':k,'status':status,'before':old,'after':new})
    report={'elapsed_seconds':a.elapsed_seconds,'before_keys':len(x),'after_keys':len(y),'status_counts':{s:sum(r['status']==s for r in out) for s in sorted(set(r['status'] for r in out))},'keys':out}
    Path(a.output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return int(any(r['status'] in {'missing_key','new_key','value_changed','ttl_changed'} for r in out))
if __name__=='__main__':raise SystemExit(main())
