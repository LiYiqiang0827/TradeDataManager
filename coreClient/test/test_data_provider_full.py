"""
data_provider 全量回归测试(18 接口)

测试组织:
  A. 签名完整性(18 接口 import + 签名匹配)
  B. db 模式基础调用(用 db 实际有数据的接口)
  C. online 模式基础调用(纯本地 mock)
  D. online 必传校验(ts_code/日期)
  E. online 多值处理(ts_codes/tags/src list)
  F. 错误校验(source 未知/互斥/>30/笛卡尔积)

运行方式:
  cd ~/TradingAgent
  PYTHONPATH=. python3 coreClient/test/test_data_provider_full.py

策略:
  - db 模式:用 sqlite3 直接查 db,避免 pandas/tushare init
  - online 模式:全部 mock 掉 _get_tushare/_get_kpl/_get_tdx
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
import sqlite3
import pandas as pd

# ============= 准备 mock =============
import coreClient.data_provider as dp

# mock 所有 client 拿取函数
mock_tushare = mock.MagicMock()
mock_kpl = mock.MagicMock()
mock_tdx = mock.MagicMock()
dp._get_tushare = lambda: mock_tushare
dp._get_kpl = lambda: mock_kpl
dp._get_tdx = lambda: mock_tdx

# 让 tushare / kpl 返回 DataFrame,tdx 返回 list[dict]
def make_empty_df(*a, **kw):
    return pd.DataFrame()

def make_empty_list(*a, **kw):
    return []

def setup_magic_methods(mock_obj, default_return):
    """给 MagicMock 的所有 attributes 设置默认 return_value"""
    SKIP = {
        'assert_called', 'assert_called_once', 'assert_called_with',
        'assert_any_call', 'assert_not_called', 'reset_mock',
        'attach_mock', 'configure_mock', 'mock_add_spec',
        'mock_calls', 'call_args', 'call_args_list',
        'call_count', 'method_calls', 'side_effect',
        'return_value', '_mock_children',
    }
    for attr in dir(mock_obj):
        if attr.startswith('_') or attr in SKIP:
            continue
        try:
            getattr(mock_obj, attr).return_value = default_return()
        except (AttributeError, TypeError):
            pass

setup_magic_methods(mock_tushare, make_empty_df)
setup_magic_methods(mock_kpl, make_empty_df)
setup_magic_methods(mock_tdx, make_empty_list)


def reset_mock_keep_defaults(m):
    """reset_mock() 但保留 return_value 等默认值"""
    m.reset_mock(return_value=False, side_effect=False)


# mock _align_cols 直接返回 df(避免添加 snap_ts)
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
    """调 fn() 期望抛 ValueError 且 message 包含 must_contain"""
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
print(' A. 签名完整性(18 接口)')
print('=' * 80)

EXPECTED_INTERFACES = {
    'get_minute':                ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_minute_index':          ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_ticks':                 ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_day':                   ['ts_code', 'ts_codes', 'start_date', 'end_date', 'trade_date', 'qfq', 'source'],
    'get_week':                  ['ts_code', 'ts_codes', 'start_date', 'end_date', 'qfq', 'source'],
    'get_month':                 ['ts_code', 'ts_codes', 'start_date', 'end_date', 'qfq', 'source'],
    'get_basic':                 ['exchange', 'market', 'list_status', 'source'],
    'get_adj_factor':            ['ts_code', 'ts_codes', 'start_date', 'end_date', 'source'],
    'get_stk_limit':             ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_daily_basic':           ['ts_code', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_moneyflow':             ['ts_code', 'ts_codes', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_index_basic':           ['ts_code', 'market', 'publisher', 'source'],
    'get_index_daily':           ['ts_code', 'trade_date', 'start_date', 'end_date', 'source'],
    'get_tradecal':              ['start_date', 'end_date', 'market', 'source'],
    'get_kpl_list':              ['trade_date', 'start_date', 'end_date', 'tags', 'source'],
    'get_kpl_concept_cons':      ['trade_date', 'start_date', 'end_date', 'ts_code', 'ts_codes', 'source'],
    'get_kpl_limit_performance': ['trade_date', 'start_date', 'end_date', 'sort_by', 'source'],
    'get_news':                  ['src', 'start_date', 'end_date',
                                  'start_datetime', 'end_datetime', 'limit', 'offset', 'source'],
}

import inspect
for name, expected_params in EXPECTED_INTERFACES.items():
    fn = getattr(dp, name, None)
    if fn is None:
        check(f'{name} 存在', False, '接口未实现')
        continue
    sig = inspect.signature(fn)
    actual_params = [p for p in sig.parameters.keys() if p != 'source']
    check(f'{name} 有 source 参数', 'source' in sig.parameters)
    check(f'{name} 参数顺序正确', actual_params == expected_params[:-1])

check(f'接口总数 = 18', len(EXPECTED_INTERFACES) == 18)


# ============= B. db 模式基础调用 =============
print()
print('=' * 80)
print(' B. db 模式基础调用(db 实际有数据的接口)')
print('=' * 80)

from offlineDataManager.scripts.core.offline_db_client import (
    get_basic, get_tradecal, PolicyDBClient,
)

db_calls = [
    ('get_basic',    get_basic, {}),
    ('get_tradecal', get_tradecal, {'start_date': '2026-09-01', 'end_date': '2026-09-30', 'market': 'SSE'}),
]
for name, fn, kwargs in db_calls:
    try:
        df = fn(**kwargs)
        check(f'{name} db 模式可用', df is not None, f'rows={len(df) if df is not None else 0}')
    except Exception as e:
        check(f'{name} db 模式可用', False, f'{type(e).__name__}: {e}')

pc = PolicyDBClient()
policy_tests = [
    ('PolicyDBClient.get_minute',         lambda: pc.get_minute(ts_codes='000006.SZ'),                                      True),
    ('PolicyDBClient.get_minute_index',   lambda: pc.get_minute_index(),                                                  True),
    ('PolicyDBClient.get_ticks',          lambda: pc.get_ticks(ts_codes='000006.SZ', trade_date='2026-09-08'),            None),  # ticks 可能 0 行
]
for name, fn, expect_nonempty in policy_tests:
    try:
        df = fn()
        if expect_nonempty:
            check(f'{name} db 可用', df is not None and len(df) > 0, f'rows={len(df) if df is not None else 0}')
        else:
            check(f'{name} db 可用', df is not None, f'rows={len(df) if df is not None else 0}')
    except Exception as e:
        check(f'{name} db 可用', False, f'{type(e).__name__}: {e}')


# ============= C. online 模式基础调用 =============
print()
print('=' * 80)
print(' C. online 模式基础调用(纯本地 mock)')
print('=' * 80)

# tdx 类
reset_mock_keep_defaults(mock_tdx)
df = dp.get_minute(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_minute online 1 次 tdx', mock_tdx.get_history_minute.call_count == 1)

reset_mock_keep_defaults(mock_tdx)
df = dp.get_minute_index(trade_date='2024-09-18', source='online')
check('get_minute_index online 5 次 tdx', mock_tdx.get_history_minute.call_count == 5)

reset_mock_keep_defaults(mock_tdx)
df = dp.get_ticks(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_ticks online 1 次 tdx', mock_tdx.get_history_ticks.call_count == 1)

# tushare 类
reset_mock_keep_defaults(mock_tushare)
df = dp.get_day(ts_code='000006.SZ', trade_date='2024-09-18', source='online', qfq=False)
check('get_day online 调 daily', mock_tushare.daily.call_count == 1)

# get_week / get_month online 不支持
reset_mock_keep_defaults(mock_tushare)
df = dp.get_week(source='online')
check('get_week online 不调 tushare', mock_tushare.weekly.call_count == 0)
check('get_week online 返回空', df is not None and df.empty)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_month(source='online')
check('get_month online 不调 tushare', mock_tushare.monthly.call_count == 0)
check('get_month online 返回空', df is not None and df.empty)

# get_basic 透传
reset_mock_keep_defaults(mock_tushare)
df = dp.get_basic(exchange='SSE', market='主板', list_status='L', source='online')
check('get_basic online 1 次', mock_tushare.stock_basic.call_count == 1)
kw = mock_tushare.stock_basic.call_args[1]
check('get_basic 透传 exchange', kw.get('exchange') == 'SSE')
check('get_basic 透传 market', kw.get('market') == '主板')
check('get_basic 透传 list_status', kw.get('list_status') == 'L')

# 单 ts 必传类
reset_mock_keep_defaults(mock_tushare)
df = dp.get_adj_factor(ts_code='000006.SZ', start_date='2024-09-01', end_date='2024-09-10', source='online')
check('get_adj_factor online 1 次', mock_tushare.adj_factor.call_count == 1)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_stk_limit(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_stk_limit online 1 次', mock_tushare.stk_limit.call_count == 1)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_daily_basic(trade_date='2024-09-18', source='online')
check('get_daily_basic online 1 次', mock_tushare.daily_basic.call_count == 1)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_moneyflow(ts_code='000006.SZ', trade_date='2024-09-18', source='online')
check('get_moneyflow online 1 次', mock_tushare.moneyflow.call_count == 1)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_index_basic(market='CSI', publisher='中证', source='online')
check('get_index_basic online 1 次', mock_tushare.index_basic.call_count == 1)
kw = mock_tushare.index_basic.call_args[1]
check('get_index_basic 透传 market', kw.get('market') == 'CSI')
check('get_index_basic 透传 publisher', kw.get('publisher') == '中证')

reset_mock_keep_defaults(mock_tushare)
df = dp.get_index_daily(ts_code='000300.SH', trade_date='2024-09-18', source='online')
check('get_index_daily online 1 次', mock_tushare.index_daily.call_count == 1)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_tradecal(start_date='2024-09-01', end_date='2024-09-30', source='online')
check('get_tradecal online 1 次', mock_tushare.trade_cal.call_count == 1)
kw = mock_tushare.trade_cal.call_args[1]
check('get_tradecal market→exchange', kw.get('exchange') == 'SSE')

reset_mock_keep_defaults(mock_tushare)
df = dp.get_kpl_list(trade_date='2024-09-18', tags='涨停', source='online')
check('get_kpl_list online 1 次', mock_tushare.kpl_list.call_count == 1)
kw = mock_tushare.kpl_list.call_args[1]
check('get_kpl_list tag 单值透传', kw.get('tag') == '涨停')

reset_mock_keep_defaults(mock_tushare)
df = dp.get_kpl_concept_cons(trade_date='2024-09-18', source='online')
check('get_kpl_concept_cons online 1 次', mock_tushare.kpl_concept_cons.call_count == 1)

# kpl 类 — today vs 历史日
reset_mock_keep_defaults(mock_kpl)
df = dp.get_kpl_limit_performance(trade_date='2024-09-17', source='online')
check('get_kpl_limit_performance 历史日走 get_daily', mock_kpl.get_daily_limit_performance.call_count == 1)
check('get_kpl_limit_performance 不走实时', mock_kpl.fetch_realtime_limit_performance.call_count == 0)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_news(src='sina', start_date='2024-09-01', end_date='2024-09-10', source='online')
check('get_news online 1 次', mock_tushare.news.call_count == 1)


# ============= D. online 必传校验 =============
print()
print('=' * 80)
print(' D. online 必传校验')
print('=' * 80)

# ts_code 必传类
print('\n D1. 5 个接口无 ts_code 应报错:')
for name, fn in [
    ('get_adj_factor', lambda: dp.get_adj_factor(source='online')),
    ('get_stk_limit',  lambda: dp.get_stk_limit(source='online')),
    ('get_moneyflow',  lambda: dp.get_moneyflow(source='online')),
    ('get_index_daily',lambda: dp.get_index_daily(source='online')),
    ('get_day',        lambda: dp.get_day(trade_date='2024-09-18', source='online', qfq=False)),
]:
    expect_value_error(f'{name} 无 ts_code 报错', fn, '必须传')

# 日期必传类
print('\n D2. 7 个接口无日期应报错:')
for name, fn in [
    ('get_minute',              lambda: dp.get_minute(ts_code='000006.SZ', source='online')),
    ('get_minute_index',        lambda: dp.get_minute_index(source='online')),
    ('get_ticks',               lambda: dp.get_ticks(ts_code='000006.SZ', source='online')),
    ('get_kpl_limit_performance', lambda: dp.get_kpl_limit_performance(source='online')),
    ('get_kpl_list',            lambda: dp.get_kpl_list(source='online')),
    ('get_kpl_concept_cons',    lambda: dp.get_kpl_concept_cons(source='online')),
    ('get_news',                lambda: dp.get_news(source='online')),
]:
    expect_value_error(f'{name} 无日期报错', fn, '必须传')

# get_tradecal 必须 start+end
print('\n D3. get_tradecal 必须 start_date+end_date:')
expect_value_error('get_tradecal 无 start+end 报错',
                   lambda: dp.get_tradecal(source='online'),
                   '必须传')

# get_daily_basic 必须日期
expect_value_error('get_daily_basic 无日期报错',
                   lambda: dp.get_daily_basic(source='online'),
                   '必须传')


# ============= E. online 多值处理 =============
print()
print('=' * 80)
print(' E. online 多值处理(list 参数循环 / join)')
print('=' * 80)

# E1. get_day ts_codes list → join 1 次 daily
reset_mock_keep_defaults(mock_tushare)
df = dp.get_day(ts_codes=['000001.SZ', '000002.SZ', '000003.SZ'], trade_date='2024-09-18', source='online', qfq=False)
check('get_day ts_codes list 1 次 daily', mock_tushare.daily.call_count == 1)
ts_arg = mock_tushare.daily.call_args[1]['ts_code']
check('get_day ts_code join 逗号', ts_arg == '000001.SZ,000002.SZ,000003.SZ', f'got {ts_arg}')

# E2. ts_codes list 循环类
reset_mock_keep_defaults(mock_tushare)
df = dp.get_adj_factor(ts_codes=['000001.SZ', '000002.SZ'], start_date='2024-09-01', end_date='2024-09-10', source='online')
check('get_adj_factor ts_codes 循环 2 次', mock_tushare.adj_factor.call_count == 2)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_stk_limit(ts_codes=['000001.SZ', '000002.SZ', '000003.SZ'], trade_date='2024-09-18', source='online')
check('get_stk_limit ts_codes 循环 3 次', mock_tushare.stk_limit.call_count == 3)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_moneyflow(ts_codes=['000001.SZ', '000002.SZ'], trade_date='2024-09-18', source='online')
check('get_moneyflow ts_codes 循环 2 次', mock_tushare.moneyflow.call_count == 2)

# E3. tags / src list 循环
reset_mock_keep_defaults(mock_tushare)
df = dp.get_kpl_list(trade_date='2024-09-18', tags=['涨停', '炸板', '跌停'], source='online')
check('get_kpl_list tags list 循环 3 次', mock_tushare.kpl_list.call_count == 3)

reset_mock_keep_defaults(mock_tushare)
df = dp.get_news(src=['sina', 'yicai', 'cls'], start_date='2024-09-01', end_date='2024-09-10', source='online')
check('get_news src list 循环 3 次', mock_tushare.news.call_count == 3)

# E4. kpl_concept_cons ts_codes → 客户端过滤
reset_mock_keep_defaults(mock_tushare)
fake_df = pd.DataFrame({
    'trade_date': ['2024-09-18'] * 3,
    'ts_code': ['000001.SZ', '000002.SZ', '000003.SZ'],
})
mock_tushare.kpl_concept_cons.return_value = fake_df
df = dp.get_kpl_concept_cons(trade_date='2024-09-18', ts_codes=['000001.SZ', '000002.SZ'], source='online')
check('get_kpl_concept_cons ts_codes 客户端过滤', len(df) == 2)


# ============= F. 错误校验 =============
print()
print('=' * 80)
print(' F. 错误校验(source 未知 / 互斥 / 超过限制 / 笛卡尔积)')
print('=' * 80)

# F1. source 未知
expect_value_error('source="bogus" 报错',
                   lambda: dp.get_day(ts_code='000006.SZ', source='bogus'),
                   '未知 source')

# F2. trade_date + start_date 互斥(只测有这 2 个参数的接口)
print('\n F2. 9 个接口 trade_date + start_date 互斥:')
for name, fn in [
    ('get_minute',              lambda: dp.get_minute(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_minute_index',        lambda: dp.get_minute_index(trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_ticks',               lambda: dp.get_ticks(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_stk_limit',           lambda: dp.get_stk_limit(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_moneyflow',           lambda: dp.get_moneyflow(ts_code='000006.SZ', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_index_daily',         lambda: dp.get_index_daily(ts_code='000300.SH', trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_kpl_list',            lambda: dp.get_kpl_list(trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_kpl_concept_cons',    lambda: dp.get_kpl_concept_cons(trade_date='2024-09-18', start_date='2024-09-01', end_date='2024-09-30', source='online')),
    ('get_kpl_limit_performance', lambda: dp.get_kpl_limit_performance(trade_date='2024-09-17', start_date='2024-09-10', end_date='2024-09-20', source='online')),
]:
    expect_value_error(f'{name} 互斥报错', fn, '互斥')

# F3. 多日 > 30 限制(用户明确要求)
print('\n F3. 4 个接口多日 > 30:')
for name, fn in [
    ('get_minute',                 lambda: dp.get_minute(ts_code='000006.SZ', start_date='2024-08-01', end_date='2024-12-31', source='online')),
    ('get_minute_index',           lambda: dp.get_minute_index(start_date='2024-08-01', end_date='2024-12-31', source='online')),
    ('get_ticks',                  lambda: dp.get_ticks(ts_code='000006.SZ', start_date='2024-08-01', end_date='2024-12-31', source='online')),
    ('get_kpl_limit_performance',  lambda: dp.get_kpl_limit_performance(start_date='2024-08-01', end_date='2024-12-31', source='online')),
]:
    expect_value_error(f'{name} > 30 报错', fn, '超过限制')

# F4. 笛卡尔积爆炸(ts_codes + 多日)
print('\n F4. 笛卡尔积爆炸:')
reset_mock_keep_defaults(mock_tushare)
expect_value_error('get_minute ts_codes+多日 笛卡尔积',
                   lambda: dp.get_minute(ts_codes=['000001.SZ', '000002.SZ'], start_date='2024-09-08', end_date='2024-09-12', source='online'),
                   '不能同时')


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
