#!/usr/bin/env python3
"""Read-only Redis key/type/TTL/value fingerprint; never calls write commands."""
import argparse, hashlib, json, sys, time
from pathlib import Path

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--repo-root',default='/Users/nickzhang/TradingDataManager');ap.add_argument('--label',required=True);ap.add_argument('--output',required=True);a=ap.parse_args()
    sys.path.insert(0,str(Path(a.repo_root).expanduser().resolve()))
    from onlineDataManager.scripts.core.redis_online import OnlineRedis
    store=OnlineRedis(); client=store.r
    try:
        keys=sorted(set(client.scan_iter(match='*',count=1000)))
        records=[]
        for start in range(0,len(keys),250):
            batch=keys[start:start+250]; pipe=client.pipeline(transaction=False)
            for key in batch: pipe.type(key);pipe.pttl(key);pipe.dump(key)
            vals=pipe.execute()
            for i,key in enumerate(batch):
                typ,ttl,payload=vals[3*i:3*i+3]
                key_bytes=key if isinstance(key,bytes) else str(key).encode('utf-8')
                records.append({'key_sha256':hashlib.sha256(key_bytes).hexdigest(),'type':typ.decode() if isinstance(typ,bytes) else str(typ),'pttl_ms':int(ttl),'dump_sha256':hashlib.sha256(payload).hexdigest() if payload is not None else None})
        records.sort(key=lambda x:x['key_sha256'])
        info=client.info('persistence')
        report={'label':a.label,'generated_epoch':time.time(),'dbsize':client.dbsize(),'key_count':len(keys),'key_type_counts':{t:sum(x['type']==t for x in records) for t in sorted(set(x['type'] for x in records))},'keys':records,'persistence':{k:info[k] for k in ('aof_enabled','aof_rewrite_in_progress','rdb_bgsave_in_progress','aof_last_write_status','aof_current_size','aof_base_size') if k in info}}
        Path(a.output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    finally:
        client.close()
if __name__=='__main__':main()
