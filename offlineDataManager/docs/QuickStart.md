# QuickStart

> 5 分钟跑通 offlineDataManager。读完后应能:**启动 scheduler、跑一次完整更新、读数据、查状态**。

---

## 0. 前置条件(3 个)

### 0.1 Python 环境
**必须用 `/opt/anaconda3/bin/python3`**(3.12.7,带 tushare / pandas / loguru / schedule)。

Hermes 自带的 `~/.local/bin/python3.11` **没有这些包**,直接跑会 `ModuleNotFoundError`。

```bash
which /opt/anaconda3/bin/python3
/opt/anaconda3/bin/python3 --version  # 期望 3.12.7
/opt/anaconda3/bin/python3 -c "import tushare, pandas, loguru, schedule; print('ok')"
```

如果某个包缺,装上:
```bash
/opt/anaconda3/bin/python3 -m pip install tushare>=1.4 pandas>=2.0 schedule>=1.2 loguru>=0.7
```

### 0.2 Tushare token
Tushare token 必须在 `~/TradingAgent/coreClient/tushare_config.py` 存在并有效。

检查:
```bash
cat ~/TradingAgent/coreClient/tushare_config.py
# 期望看到 TUSHARE_TOKEN = "你的 token" 或类似变量
```

如果没设:
```bash
echo 'TUSHARE_TOKEN = "你的 token"' > ~/TradingAgent/coreClient/tushare_config.py
# 或编辑该文件,设置 TUSHARE_TOKEN 变量
```

### 0.3 launchd(可选,只想手动跑可跳过)
plist 文件已就位:`~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist`

```bash
# 加载(开机自启 + KeepAlive)
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist
launchctl list | grep offlineData  # 应看到 com.tradingagent.scheduler.offlineData
```

---

## 1. 5 分钟启动(手动跑一次)

### 1.1 看状态(不修改任何数据)
```bash
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --status
```

期望输出(2026-09-15 实测):
```
============================================================
DB 状态总览
============================================================

[basic] db_cn_basic.db
  tbl_basic_ctrl                          8 行  (日期: ?)
  tbl_cn_adj_factor              12,244,479 行  (日期: 20150105)
  tbl_cn_basic                        5,562 行  (日期: ?)
  tbl_cn_day                     11,725,879 行  (日期: 20150105)
  tbl_cn_month                      593,382 行  (日期: 20150105)
  tbl_cn_tradecal                     6,358 行  (日期: 20180101)
  tbl_cn_week                     2,489,381 行  (日期: 20150105)

[kpl] db_cn_kpl.db
  tbl_cn_kpl_concept_cons         4,099,392 行  (日期: 20240420)
  tbl_cn_kpl_limit_performance          289 行  (日期: 20260907)
  tbl_cn_kpl_list                   104,435 行  (日期: 20201009)

[news] db_cn_news.db
  tbl_cctv_news                      35,647 行  (日期: 20200101)
  tbl_major_news                  1,455,343 行  (日期: 2020-01-01)
  tbl_news                        9,454,159 行  (日期: 2020-01-01)
  tbl_news_ctrl                          11 行  (日期: ?)
```

如果 `data/` 下 DB 文件不存在,`--status` 不会报错,但表全 0 行(因为 `init_db` 只在 `CNDataDown()` 初始化时建表)。

### 1.2 跑单个 service(快速体验)
```bash
cd ~/TradingAgent/offlineDataManager/scripts

# 跑交易日历(单次拉今天,几秒)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_tradecal --trade-date 20260915

# 跑日 K(单日,~30 秒)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_daily --trade-date 20260915

# 跑复权因子
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_adj_factor --end-date 20260915

# 跑周 K(全量派生,~5 分钟,会校验 ctrl 一致性)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_week

# 跑月 K
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_month
```

每个 service 跑完会 print 类似:
```
[service_daily] 完成(单日 20260915): +5,623 行, 用时 23.4s
```

### 1.3 跑一次完整更新(慢,~75 分钟首次,~5 分钟增量)
```bash
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --full
```

会按 4 阶段依次跑:
1. 基础数据(basic / tradecal)
2. 日 K 链(daily / adj_factor)
3. 周月 K(week / month)
4. kpl + cctv(kpl_list / kpl_concept / kpl_limit_perf / cctv_news)

任何阶段失败会中止后续。日志写到 `logs/service_*.log`。

### 1.3b 一次性跑全部 service(`scheduler_once`,2026-09-16 新增)

跟 `--full` 类似,但**一次性跑完 27 个 service 即退出**(不进入循环):

```bash
cd ~/TradingAgent/offlineDataManager/scripts

# 默认:跑全部 27 个 service(按依赖顺序)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once

# 只跑某个阶段(例:阶段 0 = 元数据)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only stage0

# 跑多个 service(空格分隔)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only kpl_list daily_basic index_daily

# 跑多个 service(逗号分隔)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only kpl_list,daily_basic,index_daily

# 跳过某些 service(可多次 --skip)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --skip cctv_news --skip kpl_limit_performance

# 失败时继续跑(默认:失败即中止)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --continue

# 只看计划,不实际跑
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --dry-run
```

**跟 `scheduler_updateData --full` 的区别**:
- `--full` 只跑 4 阶段 13 个核心 service(按时间窗口逻辑,只补昨天+今天)
- `scheduler_once` 按依赖顺序跑**全部 27 个 service**(包含 KPL/沪深股通/融资融券/指数日线/新闻等)
- 用途:**首次安装回填 / 新增 service 后批量补跑 / 重置 ctrl 后重跑**

### 1.4 启动常驻调度(launchd 已在跑就不用)
```bash
# 方式 A:手动 foreground(用于调试)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --daemon

# 方式 B:launchd 拉起(推荐,生产用)
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist
```

启动后立即跑一次 `task_full_update`,然后进入循环:
- **09:05** `task_morning_update`(快速补:涨跌停价 + kpl 涨停榜 + 联播新闻)
- 16:30 / 20:00 补 kpl_limit_performance
- **18:00 / 20:00 / 22:00** 全量 `task_full_update`(收盘后兜底)
- 每 10 分钟(08:00-05:00)news 增量

---

## 2. 读数据(5 个常见场景)

所有读取通过 `core/offline_db_client` 的 28 个 `get_*` 函数(含 `get_stk_limit` 涨跌停 / `get_suspend` 停复牌 / `get_top_list` 龙虎榜每日 / `get_top_inst` 龙虎榜机构 / `get_block_trade` 大宗 / `get_ggt_daily` 港股通 / `get_hsgt_top10` 沪深股通 / `get_limit_list` 涨跌停列表 / `get_moneyflow` 资金流向 / `get_margin` 融资融券汇总 / `get_margin_detail` 融资融券明细 / `get_cyq_perf` 筹码及胜率 / `get_daily_basic` 每日指标 / `get_index_basic` 指数基本信息 / `get_index_daily` 12 只指数日线行情,2026-09-15 加)。

### 2.1 拉某只股票前复权日 K
```python
import sys
sys.path.insert(0, '/Users/nickzhang/TradingAgent/offlineDataManager/scripts')
from core.offline_db_client import get_day

df = get_day(
    ts_code='000001.SZ',
    start_date='20240101',
    end_date='20241231',
    qfq=True,  # 前复权
)
print(df.head())
print(f"rows={len(df)}")
```

### 2.2 拉当天所有涨停股(含封单/振幅/炸板)
```python
from core.offline_db_client import get_kpl_limit_performance

df = get_kpl_limit_performance(trade_date='20260911')
print(df[['ts_code', 'name', 'board_count', 'lu_time', 'theme', 'amplitude']].head(10))
```

### 2.3 拉当天连板股
```python
from core.offline_db_client import get_kpl_list

# status 支持关键字:非首板 / N板以上 / N天 / 天M板
df = get_kpl_list(trade_date='20260911', status='2板以上')
print(df[['ts_code', 'name', 'status']].head())

# 题材过滤
df = get_kpl_list(trade_date='20260911', themes='军工', status='3板以上')
```

### 2.4 查交易日历
```python
from core.offline_db_client import get_tradecal

# 从某天起 5 个交易日(自动跳过周末/节假日)
df = get_tradecal(trade_date='20240915', day_num=5, market='SSE')
# 周日 → 9/18 起 5 天 → 5 行(SSE 单交易所)
```

### 2.5 拉某只股票相关题材
```python
from core.offline_db_client import get_kpl_concept_cons

# ts_codes 参数实际查 con_code(成分股代码)
df = get_kpl_concept_cons(ts_codes='600104.SH', trade_date='20260911')
print(df[['name', 'con_name', 'hot_num']].head())
```

### 2.6 拉新闻快讯
```python
from core.offline_db_client import get_news

# 当天 sina + cls 源
df = get_news(
    trade_date='20260911',
    src=['sina', 'cls'],
    content='新能源',
    limit=20,
)
print(df[['datetime', 'src', 'title']])
```

更多接口见 [01_数据详细设计文档.md](01_数据详细设计文档.md) 和 `scripts/test/*_demo.py`。

---

## 3. 跑 demo(快速看全部接口)

```bash
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 test/basic_demo.py
/opt/anaconda3/bin/python3 test/kpl_demo.py
/opt/anaconda3/bin/python3 test/news_demo.py
```

每个 demo 会演示 7-10 个 `get_*` 接口,带打印输出。

---

## 4. 数据验证(对比 Tushare 实时)

```bash
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 test/validate_data.py
```

会抽样几个日期(20260908 / 20260115 / 20250115 / 20241015 / 20240615),对比 DB 里的行数和 Tushare 实际行数,输出 ✓/✗。**注意:会消耗 Tushare API 配额**,按需跑。

---

## 5. 故障排查

### 5.1 `ModuleNotFoundError: No module named 'tushare'`
用错 Python。必须用 `/opt/anaconda3/bin/python3`,不是 `python3` 或 `python`。

### 5.2 `sqlite3.OperationalError: no such table: tbl_basic_ctrl`
DB 文件不存在或表未建。先跑一次 `CNDataDown()`(会触发 `init_db` 自动建表),或手动:
```bash
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 -c "from core.offline_db_client import init_db; init_db('basic'); init_db('kpl'); init_db('news')"
```

### 5.3 `service_week` 跑出 rc=2 业务中止
ctrl 一致性校验失败,通常是 `cn_daily` 或 `cn_adj_factor` 没拉到今天。先:
```bash
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_daily --trade-date $(date +%Y%m%d)
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_adj_factor --end-date $(date +%Y%m%d)
```
然后再跑 week / month。

### 5.4 launchd 没起来
```bash
launchctl list | grep offlineData
# 空 → 没加载
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist
# 重新加载
launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist
launchctl load ~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist
# 看 daemon 日志
tail -f ~/TradingAgent/offlineDataManager/logs/scheduler.launchd.err.log
```

### 5.5 想看某个 service 实际在干啥
```bash
tail -f ~/TradingAgent/offlineDataManager/logs/service_daily.log
```

---

## 6. 卸载 / 重建

### 6.1 停调度
```bash
launchctl unload ~/Library/LaunchAgents/com.tradingagent.scheduler.offlineData.plist
```

### 6.2 重建(删所有 DB,下次跑会自动建)
```bash
rm -f ~/TradingAgent/offlineDataManager/data/db_cn_*.db
rm -f ~/TradingAgent/offlineDataManager/data/db_cn_*.db-*  # WAL / SHM 文件
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --full  # 首次全量,~75 分钟
```

### 6.3 重建(只清某个 DB)
```bash
# 例:重拉 news
rm -f ~/TradingAgent/offlineDataManager/data/db_cn_news.db*
/opt/anaconda3/bin/python3 -m scheduler.scheduler_updateData --once service_news  # 会重头拉 7 个 active 源
```

---

## 7. 接下来的路

- **完整架构**: [00_架构文档.md](00_架构文档.md)
- **完整数据设计**: [01_数据详细设计文档.md](01_数据详细设计文档.md)
- **加新数据流程**: [02_新增数据接入指南.md](02_新增数据接入指南.md)
- **改代码前看对应脚本的详细设计**: [代码详细设计/](代码详细设计/)
