# 代码详细设计/scheduler_once.md

`scripts/scheduler/scheduler_once.py` — 一次性测试调度器(2026-09-16 新增)。

## 职责

按**依赖顺序**依次跑完所有离线数据下载任务(共 27 个 service,6 个阶段),用于:
- **首次安装后一次性补齐所有数据**(从空白 DB 跑到最新)
- **新增 service 后批量回填**(无需改 scheduler)
- **批量重置 ctrl 后跑回完整数据**
- **测试 scheduler 调度链路是否完整**

跟 `scheduler_updateData.py` 的区别:

| 维度 | scheduler_updateData | scheduler_once |
|---|---|---|
| 运行方式 | launchd 常驻 + schedule 定时 | **一次性,跑完即退出** |
| 任务选择 | 按时间窗口选 task(morning/full/news/kpl_lp) | **按依赖顺序跑全部/指定** |
| 用途 | 生产环境定时更新 | 测试 / 一次性回填 |
| 跳过逻辑 | 不跳过,全跑 | 支持 `--only` / `--skip` |

## 任务编排(6 个阶段,27 个 service)

| 阶段 | 内容 | service 数 | 依赖 |
|---|---|---|---|
| 0 | 元数据 | 3 (tradecal / basic / index_basic) | 无 |
| 1 | K 线数据 | 4 (daily / adj_factor / week / month) | tradecal + basic |
| 2 | 衍生数据 | 5 (daily_basic / moneyflow / stk_limit / suspend / index_daily) | 日 K |
| 3 | KPL / 开盘啦 | 3 (kpl_list / kpl_concept_cons / kpl_limit_performance) | 无 |
| 4 | 高级数据 | 9 (top_list / top_inst / block_trade / ggt_daily / hsgt_top10 / limit_list / margin / margin_detail / cyq_perf) | 日 K / 部分 basic |
| 5 | 新闻 | 3 (news / major_news / cctv_news) | 无 |

## CLI 用法

```bash
# 1. 默认:跑全部 27 个 service
python -m scheduler.scheduler_once

# 2. 只跑某个阶段
python -m scheduler.scheduler_once --only stage0
python -m scheduler.scheduler_once --only stage3

# 3. 只跑某个 service
python -m scheduler.scheduler_once --only kpl_list

# 4. 跑多个 service(空格分隔,2026-09-16 增强)
python -m scheduler.scheduler_once --only kpl_list daily_basic index_daily

# 5. 跑多个 service(逗号分隔,2026-09-16 增强)
python -m scheduler.scheduler_once --only kpl_list,daily_basic,index_daily

# 6. 跑多个阶段(空格分隔)
python -m scheduler.scheduler_once --only stage0 stage3

# 7. 跳过某些 service(可多次)
python -m scheduler.scheduler_once --skip cctv_news
python -m scheduler.scheduler_once --skip kpl_limit_performance --skip cctv_news

# 8. 失败时继续跑(默认:失败即中止)
python -m scheduler.scheduler_once --continue

# 9. 只看计划,不实际跑
python -m scheduler.scheduler_once --dry-run

# 组合
python -m scheduler.scheduler_once --only stage3 --skip kpl_limit_performance --continue
```

### `--only` 参数解析规则

`--only` 用 `nargs='+'` 接受多个空格分隔的 token(`parse_only_args` 还会把每个 token 内的逗号 split):

- **空格分隔**:`--only kpl_list daily_basic index_daily`
- **逗号分隔**:`--only kpl_list,daily_basic,index_daily`(等价)
- **混合**:`--only stage0,stage3`(多阶段)
- **stage 优先**:如果有 `stageN` token,**该阶段所有 service 都跑**,**忽略其他 service token**
- **依赖顺序**:多个 service 按其在 `STAGES` 中的原始顺序执行(保持依赖)
- **空 `--only`**:不传 → 跑全部 27 个 service

### `--only` 决策矩阵

| 输入 | 效果 |
|---|---|
| 不传 | 全部 27 个 service |
| `--only stage0` | 阶段0 的 3 个 service |
| `--only kpl_list` | 1 个 service(从阶段3 找) |
| `--only kpl_list daily_basic` | 2 个 service(按依赖顺序:daily_basic 在阶段2 先跑,kpl_list 在阶段3 后跑) |
| `--only stage0 stage3` | 6 个 service(阶段0+阶段3 全部) |
| `--only stage3 kpl_list` | 阶段3 全部 3 个(stage 优先,忽略 svc) |
| `--only kpl_list,daily_basic,index_daily` | 3 个 service |
| `--only xxx`(不存在的 service) | **友好错误**,退出码 1 |

## 退出码

| rc | 含义 |
|---|---|
| 0 | 全部成功 |
| 1 | 任一 service 失败(--continue 模式下也可能 rc=1,因为失败列表非空) |
| 2 | 用户中断(Ctrl-C) |

## 关键代码

### STAGES 定义

```python
STAGES = [
    ("阶段 0 — 元数据(无依赖)", [
        ("service_tradecal", ["--trade-date", "20260915"]),
        ("service_basic", []),
        ("service_index_basic", []),
    ]),
    ...
]
```

每项是 `(service_name, extra_args)`,extra_args 是 list(可空)。

### parse_only_args()

把 `--only` 原始 list(空格/逗号分隔)解析为 `(stage_filters, svc_filters)`:

```python
def parse_only_args(only_list) -> tuple:
    """支持 None / 单个 / 多个 / 逗号分隔"""
    if not only_list:
        return [], set()
    raw_tokens = []
    for item in only_list:
        raw_tokens.extend(item.replace(",", " ").split())
    stage_filters, svc_filters = [], set()
    for tok in raw_tokens:
        tok = tok.strip()
        if not tok:
            continue
        if tok.startswith("stage"):
            stage_filters.append(tok)
        else:
            svc_filters.add(tok)
    return stage_filters, svc_filters
```

### build_plan()

根据 `--only` / `--skip` 过滤 `STAGES` 生成执行计划:

- **stage_filters 非空** → 只跑匹配的阶段(忽略 svc_filters)
- **只有 svc_filters** → 跑所有匹配 svc 的阶段
- **都没** → 跑全部
- **--skip**:从已选 plan 中移除指定 service

### spawn_service()

跟 `scheduler_updateData.py` 类似,但**简化**:
- **默认 sync=True**(阻塞等完成)
- **默认 timeout=1800s**(30 分钟)
- **不抛异常**,只返回 rc(由 main 决定是否中止)
- 不使用 `abort_on_failure` 参数
- 子进程 stdout/stderr 重定向到 `logs/service_{name}.log`(追加模式)

### main()

执行循环:
1. 调 `build_plan()` 拿到 plan
2. 调 `print_plan()` 给人看
3. 遍历阶段,每个 service `spawn_service()`
4. 失败时根据 `--continue` 决定是否继续
5. 全部完成打印总结

## 日志

- **主日志**:`~/TradingAgent/offlineDataManager/logs/scheduler_once.log`(20 MB 自动 rotate)
- **每个 service**:自己的 `logs/service_*.log`(由 `spawn_service()` 用 `subprocess` 重定向追加)

## 典型使用场景

### 场景 1:首次安装后回填

```bash
cd ~/TradingAgent/offlineDataManager/scripts
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once
```

预期耗时:~30 分钟(增量 + 部分全量)。

### 场景 2:重置所有 ctrl,从 1993 全量重跑

```bash
# 1. 先清空所有 ctrl
/opt/anaconda3/bin/python3 -c "
import sqlite3
for db in ['db_cn_basic.db', 'db_cn_kpl.db', 'db_cn_index.db']:
    conn = sqlite3.connect(f'/Users/nickzhang/TradingAgent/offlineDataManager/data/{db}')
    for tbl in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%ctrl%'\").fetchall():
        conn.execute(f'DELETE FROM {tbl[0]}')
    conn.commit()
"

# 2. 一次性全跑
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once
```

预期耗时:几小时(全量 1993-2026)。

### 场景 3:补 KPL 数据

```bash
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only stage3
```

预期耗时:~10 分钟(KPL 限流慢)。

### 场景 4:新增 service 后批量跑(无需改 scheduler)

```bash
# 加新 service (例:service_hk_day) 后
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only hk_day
```

**注意**:新 service 需要在 STAGES 列表里。详见 [02_新增数据接入指南.md](../02_新增数据接入指南.md)。

### 场景 5:批量测试多个新 service

```bash
/opt/anaconda3/bin/python3 -m scheduler.scheduler_once --only hk_day etf_day us_day --continue
```

## 跟生产调度的差异

**生产调度**(scheduler_updateData.py):
- 每天 09:05 跑 morning task(stk_limit + kpl_list + margin + margin_detail + cctv_news)
- 每天 16:30 / 20:00 跑 kpl_limit_performance
- 每天 18:00 / 20:00 / 22:00 跑 full task(全部 19 个 service)
- 每 10 分钟跑 news

**scheduler_once.py**:
- 一次性跑完所有任务,严格按依赖顺序
- 不考虑时间窗口(全天都能跑)
- 用于测试 / 回填

## 设计原则

- **依赖顺序严格**:阶段 0→5 不能跳(否则后续 service 找不到上游数据)
- **失败默认中止**:任一 service 失败说明环境有问题,继续跑只会让后续失败
- **可选 --continue**:批量测试场景用,但默认不开启
- **dry-run 永远不写数据**:任何实际跑都写日志 + DB,dry-run 只显示计划
- **stage 优先于 svc**:`--only stage3 kpl_list` 跑 stage3 全部,忽略 kpl_list(避免歧义)