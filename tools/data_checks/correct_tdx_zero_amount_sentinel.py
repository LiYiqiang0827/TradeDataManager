#!/usr/bin/env python3
"""Correct only pytdx's proven uint32-zero -> 2**-127 amount sentinel.

The input JSON must be the frozen full-partition audit receipt. Every touched
Parquet partition, its day manifest, and the DuckDB ingest catalog are backed
up before writes. Replacements are per-file atomic; any failure restores all
already-replaced files and metadata from the private backup.
"""
from __future__ import annotations
import argparse, hashlib, json, os, shutil, tempfile
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SENTINEL = 2.0 ** -127

def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''): h.update(b)
    return h.hexdigest()

def atomic_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix=path.name+'.',suffix='.tmp',dir=path.parent)
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as f:
            json.dump(obj,f,ensure_ascii=False,indent=2,sort_keys=True); f.write('\n'); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,path)
    except BaseException:
        try: os.unlink(tmp)
        except OSError: pass
        raise

def arrow_hash(table: pa.Table) -> str:
    sink=pa.BufferOutputStream()
    with pa.ipc.new_stream(sink,table.schema) as writer: writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()

def backup_file(src: Path, dst: Path, expected_hash: str|None=None) -> str:
    dst.parent.mkdir(parents=True,exist_ok=True)
    src_before=sha256(src)
    if expected_hash and src_before != expected_hash: raise RuntimeError(f'source hash mismatch: {src}')
    if dst.exists():
        got=sha256(dst)
        if got != src_before: raise RuntimeError(f'existing backup collision: {dst}')
    else:
        shutil.copy2(src,dst)
        got=sha256(dst)
    src_after=sha256(src)
    if src_after != src_before: raise RuntimeError(f'source changed while backing up: {src}')
    if got != src_before: raise RuntimeError(f'backup hash mismatch: {src}')
    if src.stat().st_size != dst.stat().st_size: raise RuntimeError(f'backup size mismatch: {src}')
    return got

def correct_amount_chunk(amount: pa.ChunkedArray | pa.Array) -> tuple[pa.Array, int]:
    """Replace only the exact decoder sentinel with zero; preserve all other values."""
    mask=pc.fill_null(pc.equal(amount,pa.scalar(SENTINEL,type=amount.type)),False)
    count=int(pc.sum(pc.cast(mask,pa.int64())).as_py() or 0)
    return pc.if_else(mask,pa.scalar(0.0,type=amount.type),amount),count

def main() -> int:
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root',type=Path,default=Path('/Users/nickzhang/TradingDatabase/offline/oneMinute'))
    ap.add_argument('--audit',type=Path,required=True,help='frozen parquet_numeric_supplement.json')
    ap.add_argument('--backup-root',type=Path,required=True,help='private rollback archive root (mode 0700)')
    ap.add_argument('--receipt',type=Path,required=True)
    ap.add_argument('--apply',action='store_true',help='perform backup and atomic corrections; without this flag only report scope')
    args=ap.parse_args()
    base=args.root/'parquet'/'schema_v2'; catalog=args.root/'catalog.duckdb'
    audit=json.loads(args.audit.read_text()); entries=audit['partitions']
    affected=[]
    for rel,rec in sorted(entries.items()):
        n=int(rec.get('pytdx_zero_amount_sentinel_rows') or 0)
        if n: affected.append((rel,rec,n))
    plan={'audit':str(args.audit),'root':str(args.root),'affected_files':len(affected),
          'sentinel_rows':sum(x[2] for x in affected),'source_bytes':sum(int(x[1]['input_before']['size_bytes']) for x in affected),
          'sentinel':SENTINEL,'dry_run':not args.apply}
    if not args.apply:
        print(json.dumps(plan,indent=2)); return 0
    if not args.backup_root.is_absolute(): raise RuntimeError('backup root must be absolute')
    args.backup_root.mkdir(parents=True,exist_ok=True)
    os.chmod(args.backup_root,0o700)
    # Revalidate every target against frozen file size/mtime before touching any file.
    targets=[]
    for rel,rec,n in affected:
        src=base/rel
        st=src.stat()
        if st.st_size != rec['input_before']['size_bytes'] or st.st_mtime_ns != rec['input_before']['mtime_ns']:
            raise RuntimeError(f'frozen input changed since scan: {src}')
        day=rec['trade_date']
        manifest=args.root/'parquet'/'schema_v2'/'_manifest'/f'trade_date={day}.json'
        if not manifest.is_file(): raise RuntimeError(f'missing manifest: {manifest}')
        targets.append((src,manifest,day,rec,n))
    if not catalog.is_file(): raise RuntimeError(f'missing catalog: {catalog}')
    backup_index={'created_at':datetime.now(timezone.utc).isoformat(),'items':[]}
    # Back up every original and metadata before any writes.
    for src,manifest,day,rec,n in targets:
        rel=src.relative_to(base)
        dst=args.backup_root/'parquet'/rel
        h=backup_file(src,dst)
        backup_index['items'].append({'kind':'parquet','path':str(src),'backup':str(dst),'sha256':h,'bytes':src.stat().st_size})
        m_dst=args.backup_root/'manifests'/manifest.name
        mh=backup_file(manifest,m_dst)
        backup_index['items'].append({'kind':'manifest','path':str(manifest),'backup':str(m_dst),'sha256':mh,'bytes':manifest.stat().st_size})
    c_dst=args.backup_root/'catalog.duckdb'
    ch=backup_file(catalog,c_dst)
    backup_index['items'].append({'kind':'catalog','path':str(catalog),'backup':str(c_dst),'sha256':ch,'bytes':catalog.stat().st_size})
    atomic_json(args.backup_root/'backup_index.json',backup_index)
    progress={'started_at':datetime.now(timezone.utc).isoformat(),'plan':plan,'files':[],'state':'processing'}
    atomic_json(args.receipt,progress)
    replaced=[]
    try:
        for src,manifest,day,rec,expected_count in targets:
            pf=pq.ParquetFile(src)
            schema=pf.schema_arrow
            if 'amount' not in schema.names: raise RuntimeError(f'no amount column: {src}')
            ai=schema.get_field_index('amount')
            codec=pf.metadata.row_group(0).column(ai).compression.lower()
            tmp=src.with_name(src.name+'.sentinel-correcting.tmp')
            if tmp.exists(): tmp.unlink()
            nfix=0; before_other=hashlib.sha256(); expected_amount_hash=hashlib.sha256()
            writer=pq.ParquetWriter(tmp,schema,compression=codec,version='2.6',data_page_version='1.0')
            try:
                for rg in range(pf.num_row_groups):
                    t=pf.read_row_group(rg)
                    amount=t.column(ai)
                    expected,count=correct_amount_chunk(amount); nfix+=count
                    other=t.select([n for n in schema.names if n!='amount'])
                    before_other.update(bytes.fromhex(arrow_hash(other)))
                    # Assert all fields except amount are preserved, and corrected amount equals the mask-only transform.
                    expected_amount_hash.update(bytes.fromhex(arrow_hash(pa.table({'amount':expected}))))
                    t=t.set_column(ai,schema.field(ai),expected)
                    writer.write_table(t,row_group_size=t.num_rows)
            finally: writer.close()
            if nfix != expected_count: raise RuntimeError(f'sentinel count changed {src}: expected {expected_count}, got {nfix}')
            out=pq.ParquetFile(tmp)
            if not out.schema_arrow.equals(schema,check_metadata=True): raise RuntimeError(f'physical Arrow schema changed: {src}')
            if out.metadata.num_rows != rec['row_count']: raise RuntimeError(f'row count changed: {src}')
            after_other=hashlib.sha256(); actual_amount_hash=hashlib.sha256(); remain=0
            for rg in range(out.num_row_groups):
                t=out.read_row_group(rg); amount=t.column(ai)
                remain += int(pc.sum(pc.cast(pc.fill_null(pc.equal(amount,pa.scalar(SENTINEL,type=amount.type)),False),pa.int64())).as_py() or 0)
                after_other.update(bytes.fromhex(arrow_hash(t.select([n for n in schema.names if n!='amount']))))
                actual_amount_hash.update(bytes.fromhex(arrow_hash(pa.table({'amount':amount}))))
            if remain: raise RuntimeError(f'sentinel remains after rewrite: {src} ({remain})')
            if before_other.digest()!=after_other.digest(): raise RuntimeError(f'non-amount column value hash changed: {src}')
            # The expected amount hash is validated by a per-file streaming equality check below using a temporary reference checksum.
            # Verify exact amount values by regenerating expected transformed values from the private backup.
            bsrc=args.backup_root/'parquet'/src.relative_to(base); bpf=pq.ParquetFile(bsrc)
            verify_amount=hashlib.sha256()
            for rg in range(bpf.num_row_groups):
                bt=bpf.read_row_group(rg); ba=bt.column(ai)
                ex,_=correct_amount_chunk(ba)
                verify_amount.update(bytes.fromhex(arrow_hash(pa.table({'amount':ex}))))
            if verify_amount.digest()!=actual_amount_hash.digest(): raise RuntimeError(f'amount values differ outside exact sentinel replacement: {src}')
            os.replace(tmp,src); replaced.append((src, bsrc))
            progress['files'].append({'path':str(src),'trade_date':day,'rows':rec['row_count'],'corrected_amount_sentinel_rows':nfix,'before_sha256':sha256(bsrc),'after_sha256':sha256(src),'other_columns_arrow_hash':before_other.hexdigest(),'bytes_before':bsrc.stat().st_size,'bytes_after':src.stat().st_size,'status':'replaced_verified'})
            atomic_json(args.receipt,progress)
        # Update only per-day audit metadata and the catalog's byte-size records.
        for src,manifest,day,rec,n in targets:
            doc=json.loads(manifest.read_text())
            warning=int(rec.get('amount_nonzero_when_vol_zero_rows',0))-n
            audit_detail={'status':'audited','scope':'all_rows','audit_receipt':str(args.receipt),
              'correction':'exact TDX decoded amount 2**-127 sentinel from raw uint32 zero to 0.0',
              'informational_decoder_correction_rows':n,'zero_reported_volume_positive_amount_rows_after_correction':max(0,warning),
              'sidecar_note':'Aggregate historical audit metadata; it does not create per-bar warning rows. Existing source quality sidecar remains unchanged.'}
            doc['historical_quality_audit']=audit_detail
            doc['target_size']=src.stat().st_size
            atomic_json(manifest,doc)
        with duckdb.connect(str(catalog)) as conn:
            conn.execute('BEGIN TRANSACTION')
            for src,manifest,day,rec,n in targets:
                conn.execute('UPDATE one_min_ingest_catalog SET parquet_size=? WHERE trade_date=?',[src.stat().st_size,day])
                got=conn.execute('SELECT parquet_size FROM one_min_ingest_catalog WHERE trade_date=?',[day]).fetchone()
                if not got or int(got[0])!=src.stat().st_size: raise RuntimeError(f'catalog update verification failed: {day}')
            conn.execute('COMMIT')
        progress['state']='complete'; progress['completed_at']=datetime.now(timezone.utc).isoformat()
        progress['catalog_backup_sha256']=ch
        atomic_json(args.receipt,progress)
    except BaseException as exc:
        progress['state']='rolling_back'; progress['error']=repr(exc); atomic_json(args.receipt,progress)
        for src,bsrc in reversed(replaced):
            tmp=src.with_name(src.name+'.rollback.tmp'); shutil.copy2(bsrc,tmp); os.replace(tmp,src)
        # Restore metadata files/catalog from their backups.
        for item in backup_index['items']:
            if item['kind'] in ('manifest','catalog'):
                src=Path(item['path']); bsrc=Path(item['backup']); tmp=src.with_name(src.name+'.rollback.tmp'); shutil.copy2(bsrc,tmp); os.replace(tmp,src)
        progress['state']='rolled_back'; atomic_json(args.receipt,progress)
        raise
    print(json.dumps({'state':progress['state'],'files':len(progress['files']),'corrected_rows':sum(x['corrected_amount_sentinel_rows'] for x in progress['files']),'receipt':str(args.receipt),'backup_root':str(args.backup_root)},indent=2))
    return 0
if __name__=='__main__': raise SystemExit(main())
