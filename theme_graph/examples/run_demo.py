"""Exercise synthetic packet -> validated report -> SQLite -> real empty Neo4j.

Requires an explicitly configured, EMPTY Neo4j instance. Never clears a graph.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args()
    if not os.environ.get('TRADING_THEME_NEO4J_URI'):
        parser.error('Explicit TRADING_THEME_NEO4J_URI required; use an isolated demo instance')
    from coreClient.neo4j_client import ThemeGraphNeo4jStore
    with ThemeGraphNeo4jStore() as graph:
        if graph._run('MATCH (n) RETURN count(n) AS count')[0]['count']:
            parser.error('Demo requires an empty graph; no existing nodes are deleted')
    root=args.root.expanduser().resolve()
    if root.exists():parser.error('Use a new data root')
    env={**os.environ,'TRADING_DATABASE_ROOT':str(root),'TRADING_DATA_MANAGER_ROOT':str(ROOT)}
    def run(*args):subprocess.run([sys.executable,*map(str,args)],cwd=ROOT,env=env,check=True)
    run(ROOT/'theme_graph/examples/seed_demo.py','--root',root)
    reports=root/'theme_graph/每日涨停原因';db=root/'theme_graph/topic_limitup_graph.sqlite3'
    for day in ['20261015','20261016']:
        run(ROOT/'theme_graph/scripts/validate_daily_report.py','--packet',reports/'分析输入包'/f'{day}.json','--report',reports/f'{day}.md')
        run('-m','theme_graph.daily_limitup_graph.cli','--db',db,'import-report',reports/f'{day}.md')
    run('-m','theme_graph.daily_limitup_graph.cli','--db',db,'integrity')
    snapshot=root/'theme_graph/neo4j/import/demo.json'
    run(ROOT/'theme_graph/scripts/export_sqlite_snapshot.py','--db',db,'--out',snapshot)
    from theme_graph.neo4j.import_sqlite_snapshot import migrate
    first=migrate(snapshot);second=migrate(snapshot)
    assert first['expected_counts']==second['expected_counts']
    with ThemeGraphNeo4jStore() as graph:
        days=graph.list_topic_days('20261015','20261016')
        detail=graph.get_topic_day('20261016','示例题材A')
        history=graph.get_topic_history('示例题材A')
        attrs=graph.get_stock_attributions('20261016','600000.SH')
        assert len(days)==2 and len(history)==2
        assert detail and len(detail['attributions'])==2
        assert '演示催化摘要' in detail['summary']['catalyst_reason']
        assert len(attrs)==1 and '演示甲涨停原因' in attrs[0]['attribution']['limitup_reason']
        td=detail['topic_date']['topic_date_id']
        # Exercise the same public update API, then restore the snapshot value.
        graph.upsert_stock_attribution(td,'600000.SH','演示甲',{'limitup_reason':'演示更新API'})
        assert graph.get_stock_attributions('20261016','600000.SH')[0]['attribution']['limitup_reason']=='演示更新API'
    migrate(snapshot)
    run(ROOT/'theme_graph/scripts/generate_daily_report.py','--packet',reports/'分析输入包/20261016.json','--previous-report',reports/'20261015.md','--prompt-out',root/'theme_graph/demo-task.txt')
    print(json.dumps({'status':'passed','data_root':str(root),'graph_counts':first['expected_counts'],'model_called':False},ensure_ascii=False,indent=2))
    return 0


if __name__=='__main__':raise SystemExit(main())
