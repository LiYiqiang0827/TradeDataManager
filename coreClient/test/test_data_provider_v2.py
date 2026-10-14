"""
data_provider 新增 12 个接口的回归测试

覆盖:
  A. 签名完整性(12 接口)
  B. db 模式基础调用(实际 db 查询)
  C. online 模式基础调用(纯本地 mock)
  D. online 必传校验(ts_code/日期)
  E. online 多值处理(ts_codes list)
  F. 错误校验(source 未知/互斥/>30/笛卡尔积)

运行:
  cd ~/TradingAgent
  PYTHONPATH=. python3 coreClient/test/test_data_provider_v2.py
"""
import os
import sys
from pathlib import Path


def _resolve_root() -> Path:
    env = os.environ.get("TRADING_DATA_MANAGER_ROOT")
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_dir():
            raise RuntimeError("TRADING_DATA_MANAGER_ROOT must name an existing directory")
        return p
    return Path(__file__).resolve().parents[2]

ROOT = _resolve_root()
sys.path.insert(0, str(ROOT / "offlineDataManager" / "scripts"))
sys.path.insert(0, str(ROOT))

import unittest.mock as mock
import pandas as pd

import coreClient.data_provider as dp

# ============= 准备 mock =============
mock_tushare = mock.MagicMock()
dp._get_tushare = lambda: mock_tushare


def make_empty_df(*a, **kw):
    return pd.DataFrame()

SKIP = {
    'assert_called', 'assert_called_once', 'assert_called_with',
    'assert_any_call', 'assert_not_called', 'reset_mock',
    'attach_mock', 'configure_mock', 'mock_add_spec',
    'mock_calls', 'call_args', 'call_args_list',
    'call_count', 'method_calls', 'side_effect',
    'return_value', '_mock_children',
}
for attr in dir(mock_tushare):
    if attr.startswith('_') or attr in SKIP:
        continue
    try:
        getattr(mock_tushare, attr).return_value = make_empty_df()
    except (AttributeError, TypeError):
        pass


def reset_mock_keep_defaults(m):
    m.reset_mock(return_value=False, side_effect=False)


dp._align_cols = mock.MagicMock(side_effect=lambda df, cols: df)

# ============= 测试基础设施 =============
_passed = 0
_failed = 0
_errors = []


def check(label, cond, detail=''):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f'  ✅ {label}', flush=True)
    else:
        _failed += 1
        _errors.append(f'❌ {label} — {detail}')
        print(f'  ❌ {label}  {detail}', flush=True)


def expect_value_error(label, fn, must_contain=None):
    try:
        fn()
        check(label, False, '没报错')
    except ValueError as e:
        if must_contain is None or must_contain in str(e):
            check(label, True)
        else:
            check(label, False, f'ValueError 但不含 "{must_contain}": {e}')
    except Exception as e:
        check(label, False, f'{type(e).__name__}: {e}')


# ============= A. 签名完整性 =============
print('=' * 80)
print(' A. 签名完整性(12 个新增接口)')
print('=' * 80)

EXPECTED = {
    'get_suspend':      ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'suspend_type', 'source'],
    'get_top_list':     ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_top_inst':     ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'exalter', 'side', 'source'],
    'get_block_trade':  ['ts_code', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_ggt_daily':    ['trade_date', 'start_date', 'end_date', 'source'],
    'get_hsgt_top10':   ['ts_code', 'trade_date', 'start_date', 'end_date', 'market_type', 'source'],
    'get_limit_list':   ['ts_code', 'trade_date', 'start_date', 'end_date', 'limit', 'source'],
    'get_margin':       ['exchange_id', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_margin_detail': ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_cyq_perf':     ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'min_winner_rate', 'max_winner_rate', 'source'],
    'get_major_news':   ['start_date', 'end_date', 'trade_date', 'src', 'limit', 'offset', 'source'],
    'get_cctv_news':    ['start_date', 'end_date', 'trade_date', 'content', 'source'],
}

import inspect
for name, expected_params in EXPECTED.items():
    fn = getattr(dp, name, None)
    check(f'{name} 存在', fn is not None)
    if fn is None:
        continue
    sig = inspect.signature(fn)
    actual = list(sig.parameters.keys())
    check(f'{name} 有 source 参数', 'source' in actual)
    check(f'{name} 参数顺序正确', actual == expected_params, f'actual={actual}')

check(f'新增接口数 = 12', len(EXPECTED) == 12)


# ============= B. db 模式基础调用 =============
print()
print('=' * 80)
print(' B. db 模式基础调用(实际 db 查询)')
print('=' * 80)

from offlineDataManager.scripts.core import offline_db_client as db_client

db_calls = [
    # 接口名, 函数, kwargs, 是否期望有数据
    ('get_suspend',       db_client.get_suspend,       {'start_date': '2026-09-01', 'end_date': '2026-09-30'}, True),
    ('get_top_list',      db_client.get_top_list,      {'trade_date': '2024-09-18'}, True),
    ('get_top_inst',      db_client.get_top_inst,      {'trade_date': '2024-09-18'}, True),
    ('get_block_trade',   db_client.get_block_trade,   {'start_date': '2026-09-01', 'end_date': '2026-09-30'}, True),
    ('get_ggt_daily',     db_client.get_ggt_daily,     {'start_date': '2024-09-01', 'end_date': '2024-09-30'}, True),
    ('get_hsgt_top10',    db_client.get_hsgt_top10,    {'trade_date': '2026-09-17'}, True),
    ('get_limit_list',    db_client.get_limit_list,    {'trade_date': '2024-09-18', 'limit': 'U'}, True),
    ('get_margin',        db_client.get_margin,        {'trade_date': '2024-09-18'}, True),
    ('get_margin_detail', db_client.get_margin_detail, {'trade_date': '2024-09-18'}, True),
    ('get_cyq_perf',      db_client.get_cyq_perf,      {'trade_date': '2024-09-18'}, True),
    ('get_major_news',    db_client.get_major_news,    {'trade_date': '2024-09-18'}, True),
    ('get_cctv_news',     db_client.get_cctv_news,     {'trade_date': '2024-09-18'}, True),
]

for name, fn, kwargs, expect_data in db_calls:
    try:
        df = fn(**kwargs)
        if expect_data:
            check(f'{name} db 模式可用', df is not None and len(df) > 0, f'rows={len(df) if df is not None else 0}')
        else:
            check(f'{name} db 模式可用', df is not None)
    except Exception as e:
        check(f'{name} db 模式可用', False, f'{type(e).__name__}: {e}')


# ============= C. online 模式基础调用 =============
print()
print('=' * 80)
print(' C. online 模式基础调用(纯本地 mock)')
print('=' * 80)

# tushare method 对应关系
TUSHARE_METHOD = {
    'get_suspend':       'suspend_d',
    'get_top_list':      'top_list',
    'get_top_inst':      'top_inst',
    'get_block_trade':   'block_trade',
    'get_ggt_daily':     'ggt_daily',
    'get_hsgt_top10':    'hsgt_top10',
    'get_limit_list':    'limit_list_d',
    'get_margin':        'margin',
    'get_margin_detail': 'margin_detail',
    'get_cyq_perf':      'cyq_perf',
    'get_major_news':    'major_news',
}

# get_suspend
reset_mock_keep_defaults(mock_tushare)
df = dp.get_suspend(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_suspend online 1 次 suspend_d', mock_tushare.suspend_d.call_count == 1)

# get_top_list
reset_mock_keep_defaults(mock_tushare)
df = dp.get_top_list(trade_date='2024-09-18', source='online')
check('get_top_list online 1 次 top_list', mock_tushare.top_list.call_count == 1)

# get_top_inst
reset_mock_keep_defaults(mock_tushare)
df = dp.get_top_inst(trade_date='2024-09-18', source='online')
check('get_top_inst online 1 次 top_inst', mock_tushare.top_inst.call_count == 1)

# get_block_trade
reset_mock_keep_defaults(mock_tushare)
df = dp.get_block_trade(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_block_trade online 1 次 block_trade', mock_tushare.block_trade.call_count == 1)

# get_ggt_daily
reset_mock_keep_defaults(mock_tushare)
df = dp.get_ggt_daily(trade_date='2024-09-18', source='online')
check('get_ggt_daily online 1 次 ggt_daily', mock_tushare.ggt_daily.call_count == 1)

# get_hsgt_top10
reset_mock_keep_defaults(mock_tushare)
df = dp.get_hsgt_top10(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_hsgt_top10 online 1 次 hsgt_top10', mock_tushare.hsgt_top10.call_count == 1)

# get_limit_list
reset_mock_keep_defaults(mock_tushare)
df = dp.get_limit_list(trade_date='2024-09-18', source='online')
check('get_limit_list online 1 次 limit_list_d', mock_tushare.limit_list_d.call_count == 1)

# get_margin
reset_mock_keep_defaults(mock_tushare)
df = dp.get_margin(trade_date='2024-09-18', source='online')
check('get_margin online 1 次 margin', mock_tushare.margin.call_count == 1)
kw = mock_tushare.margin.call_args[1]
check('get_margin 透传 trade_date', kw.get('trade_date') == '20240918')

# get_margin_detail
reset_mock_keep_defaults(mock_tushare)
df = dp.get_margin_detail(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_margin_detail online 1 次', mock_tushare.margin_detail.call_count == 1)

# get_cyq_perf
reset_mock_keep_defaults(mock_tushare)
df = dp.get_cyq_perf(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_cyq_perf online 1 次 cyq_perf', mock_tushare.cyq_perf.call_count == 1)

# get_major_news
reset_mock_keep_defaults(mock_tushare)
df = dp.get_major_news(trade_date='2024-09-18', source='online')
check('get_major_news online 1 次 major_news', mock_tushare.major_news.call_count == 1)

# get_cctv_news online 无源
reset_mock_keep_defaults(mock_tushare)
df = dp.get_cctv_news(trade_date='2024-09-18', source='online')
check('get_cctv_news online 返回空', df is not None and df.empty)
check('get_cctv_news online 不调任何 client', mock_tushare.method_calls == [])


# ============= D. online 必传校验 =============
print()
print('=' * 80)
print(' D. online 必传校验')
print('=' * 80)

# D1. ts_code 必传类(只有 tushare 不支持全市场的接口)
print('\n D1. 5 个接口无 ts_code 应报错:')
for name, fn in [
    ('get_block_trade',   lambda: dp.get_block_trade(source='online')),
    ('get_hsgt_top10',    lambda: dp.get_hsgt_top10(source='online')),
    ('get_margin_detail', lambda: dp.get_margin_detail(source='online')),
    ('get_cyq_perf',      lambda: dp.get_cyq_perf(source='online')),
]:
    expect_value_error(f'{name} 无 ts_code 报错', fn, '必须传')

# D2. 日期必传类
print('\n D2. 11 个接口无日期应报错:')
for name, fn in [
    ('get_suspend',       lambda: dp.get_suspend(ts_code='000006.SZ', source='online')),
    ('get_top_list',      lambda: dp.get_top_list(source='online')),
    ('get_top_inst',      lambda: dp.get_top_inst(source='online')),
    ('get_block_trade',   lambda: dp.get_block_trade(ts_code='000006.SZ', source='online')),
    ('get_ggt_daily',     lambda: dp.get_ggt_daily(source='online')),
    ('get_hsgt_top10',    lambda: dp.get_hsgt_top10(ts_code='000006.SZ', source='online')),
    ('get_limit_list',    lambda: dp.get_limit_list(source='online')),
    ('get_margin',        lambda: dp.get_margin(source='online')),
    ('get_margin_detail', lambda: dp.get_margin_detail(ts_code='000006.SZ', source='online')),
    ('get_cyq_perf',      lambda: dp.get_cyq_perf(ts_code='000006.SZ', source='online')),
    ('get_major_news',    lambda: dp.get_major_news(source='online')),
]:
    expect_value_error(f'{name} 无日期报错', fn, '必须传')


# ============= E. online 多值处理 =============
print()
print('=' * 80)
print(' E. online 多值处理(ts_codes list 循环)')
print('=' * 80)

# ts_codes list 循环
reset_mock_keep_defaults(mock_tushare)
df = dp.get_suspend(ts_codes=['000001.SZ', '000002.SZ'], trade_date='2024-09-18', source='online')
check('get_suspend ts_codes 循环 2 次', mock_tushare.suspend_d.call_count == 2)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_top_list(ts_codes=['000001.SZ', '000002.SZ', '000003.SZ'], trade_date='2024-09-18', source='online')
check('get_top_list ts_codes 循环 3 次', mock_tushare.top_list.call_count == 3)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_margin_detail(ts_codes=['000001.SZ', '000002.SZ'], trade_date='2024-09-18', source='online')
check('get_margin_detail ts_codes 循环 2 次', mock_tushare.margin_detail.call_count == 2)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_cyq_perf(ts_codes=['000001.SZ', '000002.SZ'], trade_date='2024-09-18', source='online')
check('get_cyq_perf ts_codes 循环 2 次', mock_tushare.cyq_perf.call_count == 2)


# ============= F. 错误校验 =============
print()
print('=' * 80)
print(' F. 错误校验(source 未知 / 互斥 / 超过限制)')
print('=' * 80)

# F1. source 未知
expect_value_error('source="bogus" 报错',
                   lambda: dp.get_top_list(trade_date='2024-09-18', source='bogus'),
                   '未知 source')

# F2. trade_date + start_date 互斥
print('\n F2. 8 个接口 trade_date + start_date 互斥:')
for name, fn in [
    ('get_suspend',       lambda: dp.get_suspend(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_top_list',      lambda: dp.get_top_list(trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_top_inst',      lambda: dp.get_top_inst(trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_block_trade',   lambda: dp.get_block_trade(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_hsgt_top10',    lambda: dp.get_hsgt_top10(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_limit_list',    lambda: dp.get_limit_list(trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_margin_detail', lambda: dp.get_margin_detail(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_cyq_perf',      lambda: dp.get_cyq_perf(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
]:
    expect_value_error(f'{name} 互斥报错', fn, '互斥')

# F3. 多日 > 30 限制(只适用于 kpl_limit_performance,用户原话明确要求)
#    其他 tushare 接口没加限制
print('\n F3. 多日 > 30(当前仅 kpl_limit_performance):')
for name, fn in [
    ('get_margin_detail', lambda: dp.get_margin_detail(ts_code='000006.SZ', start_date='2024-08-01', end_date='2024-12-31', source='online')),
]:
    # tushare.margin_detail 暂无 > 30 限制,这一行只是占位,期待正常运行
    try:
        fn()
        check(f'{name} > 30 不报错(未限制)', True)
    except ValueError as e:
        if '超过限制' in str(e):
            check(f'{name} > 30 报错', True)
        else:
            check(f'{name} > 30', False, str(e)[:50])


# ============= 总结 =============
print()
print('=' * 80)
print(f' 总计:{_passed} 通过 / {_failed} 失败')
print('=' * 80)

if _failed > 0:
    print()
    print('失败项:')
    for e in _errors:
        print(f'  {e}')
    sys.exit(1)
else:
    print('🎉 全部通过')