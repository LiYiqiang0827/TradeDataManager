#!/usr/bin/env python3
"""
data_provider 总测试入口

按顺序跑 2 个测试套件:
  1. test_data_provider_full.py  — 18 个核心接口(107 个断言)
  2. test_data_provider_v2.py    — 12 个扩展接口(92 个断言)
                                  ─────────────────
                            合计: 199 个断言

运行方式:
  cd ~/TradingAgent
  PYTHONPATH=. python3 coreClient/test/test_data_provider_all.py
"""
import subprocess
import sys
from pathlib import Path

TEST_DIR = Path(__file__).parent
SUITES = [
    'test_data_provider_full.py',
    'test_data_provider_v2.py',
]


def run_one(suite_name):
    """跑单个测试套件,返回 (returncode, stdout, stderr)"""
    print('=' * 80)
    print(f' 套件:{suite_name}')
    print('=' * 80)
    result = subprocess.run(
        ['python3', '-u', str(TEST_DIR / suite_name)],
        cwd=str(TEST_DIR.parent.parent),  # ~/TradingAgent
        env={**__import__('os').environ, 'PYTHONPATH': '.'},
        capture_output=True,
        text=True,
        timeout=180,
    )
    # 打印最后 15 行(摘要)
    out_lines = result.stdout.strip().split('\n')
    for line in out_lines[-15:]:
        print(line)
    if result.returncode != 0:
        print()
        print('STDERR:')
        print(result.stderr[-2000:])
    return result.returncode


def main():
    total = len(SUITES)
    passed = 0
    failed_suites = []

    for suite in SUITES:
        rc = run_one(suite)
        if rc == 0:
            passed += 1
        else:
            failed_suites.append(suite)
        print()

    print('=' * 80)
    print(f' 总计:{passed}/{total} 个套件通过')
    if failed_suites:
        print(f' ❌ 失败:{failed_suites}')
        sys.exit(1)
    else:
        print(' 🎉 全部套件通过')
        print('=' * 80)
        sys.exit(0)


if __name__ == '__main__':
    main()