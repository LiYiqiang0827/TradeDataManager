"""Check daily report stock coverage and market fields against its DuckDB packet."""
from __future__ import annotations
import argparse, json, re
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))
from theme_graph.daily_limitup_graph.import_report import parse_report

def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument('--packet', required=True, type=Path)
    ap.add_argument('--report', required=True, type=Path)
    a=ap.parse_args()
    packet=json.loads(a.packet.read_text(encoding='utf-8'))
    date,sections,_=parse_report(a.report)
    actual=[]
    for section in sections:
        actual.extend(section['stocks'])
    expected={r['ts_code']:(r.get('name',''), r.get('status_raw') or '', r.get('continuous_board'), r.get('lu_time') or '') for r in packet['limitups']}
    counts={}
    for r in actual: counts[r['stock_code']]=counts.get(r['stock_code'],0)+1
    duplicates=sorted(k for k,v in counts.items() if v>1)
    missing=sorted(set(expected)-set(counts))
    extra=sorted(set(counts)-set(expected))
    field_errors=[]
    for row in actual:
        code=row['stock_code']; exp=expected.get(code)
        if not exp: continue
        name,status_raw,board,seal=exp
        if row['stock_name'] != name:
            field_errors.append({'stock':code,'field':'name','expected':name,'actual':row['stock_name']})
        state=row['limitup_status']
        if status_raw and status_raw not in state:
            field_errors.append({'stock':code,'field':'status_raw','expected_substring':status_raw,'actual':state})
        if board is not None and row['consecutive_board'] != board:
            field_errors.append({'stock':code,'field':'continuous_board','expected':board,'actual':row['consecutive_board']})
        if seal:
            actual_seal=row['seal_time']
            exp_seal=seal if len(seal.split(':'))==3 else seal+':00'
            act_seal=actual_seal if actual_seal and len(actual_seal.split(':'))==3 else (actual_seal+':00' if actual_seal else None)
            if act_seal != exp_seal:
                field_errors.append({'stock':code,'field':'lu_time','expected':seal,'actual':actual_seal})
    result={'trade_date':date,'packet_date':packet['trade_date'],'expected_rows':packet['limitup_stock_count'],'actual_rows':len(actual),'unique_codes':len(counts),'topic_count':len(sections),'missing_codes':missing,'extra_codes':extra,'duplicate_codes':duplicates,'field_errors':field_errors}
    ok=(date==packet['trade_date'] and len(actual)==packet['limitup_stock_count'] and not missing and not extra and not duplicates and not field_errors)
    result['status']='passed' if ok else 'failed'
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0 if ok else 1
if __name__=='__main__': raise SystemExit(main())
