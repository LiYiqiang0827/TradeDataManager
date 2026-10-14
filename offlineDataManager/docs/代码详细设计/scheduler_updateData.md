# 代码详细设计/scheduler_updateData.md

`scripts/scheduler/scheduler_updateData.py` — 多进程调度器主进程。

> 2026-10-08 故障恢复更新：同步子进程的30分钟超时现在转换为既有
> `RuntimeError`，避免未捕获异常使daemon退出、launchd反复重启并从头重跑。
> 周线与月线各自在服务内部原子更新前复权和原始表；其中一项失败时保留旧表，
> 记录错误并继续另一周期和独立的KPL、每日指标、指数等原始数据服务，晚间再重试。
> 日线/复权因子或交易日历失败仍中止依赖链。子进程实际沿用`sys.executable`，
> 目标日从已完成交易日历确定；下文旧示例中的硬编码解释器和“昨日”仅供历史参考。
> 15分钟与1分钟现在都在日线/复权因子更新完成后运行，避免先写入今日分钟价格
> 却缺少今日复权因子。非交易日已有日线/因子的情况下仍允许分钟补跑。

**用 `subprocess.run` 同步 spawn 13 个 service 子进程,按依赖链顺序执行;launchd 拉起后常驻**。

## 相关文档

- **一次性测试调度**([scheduler_once.md](scheduler_once.md),2026-09-16 新增):与本文档对应,用于首次回填 / 批量测试场景
- **跟本文档的区别**:
  - scheduler_updateData:**生产环境,launchd 常驻**,按时间窗口跑 4 个 task
  - scheduler_once:**一次性,跑完退出**,按依赖顺序跑全部/指定 service

## 职责

1. **常驻调度循环**(`run_daemon`)
2. **按时间表触发** 4 个 task(`task_morning_update` / `task_full_update` / `task_news_update` / `task_kpl_limit_performance`)
3. **同步 spawn 子进程** 跑 service
4. **处理子进程返回值**(rc=0/1/2 区分)
5. **提供 CLI 入口**(`--daemon` / `--once` / `--full` / `--status`)

## 入口

- **被 launchd plist 调用**:`python3 -m scheduler.scheduler_updateData --daemon`
- **被手动调用**:`python3 -m scheduler.scheduler_updateData --full` 等

## 时间表(顶部常量)

```python
STR_TIME_FULL_DB_1 = "18:00"  # 完整更新 1(收盘后,基础行情已稳定)
STR_TIME_FULL_DB_2 = "20:00"  # 完整更新 2
STR_TIME_FULL_DB_3 = "22:00"  # 完整更新 3
STR_TIME_MORNING   = "09:05"  # 早上快速更新:涨跌停价 + kpl 涨停榜 + 联播新闻(2026-09-15 新增)
STR_TIME_KPL_LP_1   = "16:30"  # 涨停表现详情 1(收盘后 30 分钟)
STR_TIME_KPL_LP_2   = "20:00"  # 涨停表现详情 2(晚间再补)
NEWS_START          = dtime(8, 0)
NEWS_END            = dtime(5, 0)  # 跨夜
NEWS_INTERVAL_MIN   = 10
```

## 关键函数

### 1. `get_target_date() -> str`
- 收盘后(>= 16:30):用今天
- 否则:用昨天
- 目的:调度时决定要拉哪天的数据

### 2. `is_news_window() -> bool`
- 当前时间 ≥ 08:00 或 ≤ 05:00 → True
- 用于 news 任务(只在新闻时段跑)

### 3. `spawn_service(service_name, extra_args=None, sync=False, abort_on_failure=True)`
```python
def spawn_service(service_name, extra_args=None, sync=False, abort_on_failure=True):
    """启动一个 service 子进程"""
    cmd = ["/opt/anaconda3/bin/python3", "-m", f"service.{service_name}"]
    if extra_args: cmd.extend(extra_args)
    log_file = PROJECT_ROOT / "logs" / f"{service_name}.log"
    SCRIPTS_DIR = PROJECT_ROOT / "scripts"
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT.parent) + ":" + os.environ.get("PYTHONPATH", "")}
    if sync:
        result = subprocess.run(cmd, cwd=str(SCRIPTS_DIR),
                                stdout=open(log_file, "a"),
                                stderr=subprocess.STDOUT,
                                env=env, timeout=1800)
        rc = result.returncode
        if rc != 0 and abort_on_failure:
            err_type = "业务中止" if rc == 2 else "异常退出"
            raise RuntimeError(f"[{service_name}] {err_type} rc={rc},args={extra_args}\n  查看日志: {log_file}")
        return rc
    else:
        return subprocess.Popen(cmd, cwd=str(SCRIPTS_DIR),
                                stdout=open(log_file, "a"),
                                stderr=subprocess.STDOUT, env=env)
```

**关键点**:
- **硬编码 `/opt/anaconda3/bin/python3`**:launchd 环境下 PATH 找不到 anaconda,必须绝对路径
- **env 加 PYTHONPATH**:让子进程能 `import coreClient.tushare_client`
- **cwd=scripts/**:让 `service.xxx` 包能找到(子进程用 `-m` 跑)
- **stdout/stderr 重定向到 `logs/service_xxx.log`**(20-50 MB rotate)
- **timeout=1800**(30 分钟)
- **rc=2 区分"业务中止"**:`raise RuntimeError` 但用 err_type 标识

### 4. `task_morning_update()` — 早上快速更新(2026-09-15 新增)

每天 09:05 触发,只跑 3 个 service(快速补"昨天收盘后的状态"):

```python
def task_morning_update():
    """早上快速更新(每天 09:05)— 只补:涨跌停价 + 开盘啦涨停榜 + 联播新闻

    跟 task_full_update 的区别:
    - task_full_update 太重(基础数据 + 日 K + 周月 K + kpl + news + 涨跌停)
    - 早上 09:00 之前用户已开盘前打开应用,需要快速拿到"昨天收盘后的状态":
      * 涨跌停价(用做今天的判定基准)
      * 开盘啦涨停榜(昨天涨停/跌停/炸板等)
      * 联播新闻(昨天政策/重要事件)
    - 09:05 触发,1 分钟内跑完
    """
    for svc, args in [
        ("service_stk_limit", []),
        ("service_kpl_list", ["--trade-date", target]),
        ("service_margin", []),  # 融资融券交易汇总(2026-09-15 新增,按日循环,挂 task_morning)
        ("service_margin_detail", []),  # 融资融券交易明细(2026-09-15 新增,按月+OFFSET,挂 task_morning)
        ("service_cctv_news", []),  # 联播新闻,早上看昨天汇总
    ]:
        try:
            spawn_service(svc, args, sync=True)
        except RuntimeError as e:
            logger.error(f"[早上更新] {svc} 失败,中止: {e}")
            return
```

**跟 `task_full_update` 的关键区别**:
- 只跑 3 个 service(轻量)
- 任何 service 失败 → 中止不补跑(等 18:00 task_full_update 兜底)
- 早上用户开盘前可用,**耗时 ~3 秒**

### 5. `task_full_update()`
4 阶段同步执行完整更新:

```
阶段 1: service_basic → service_tradecal       (任一失败整体 return)
阶段 2: service_daily → service_adj_factor    (任一失败整体 return)
阶段 3: service_week → service_month          (失败保留旧派生表，继续独立更新)
阶段 4: service_kpl_list → service_kpl_concept_cons
       → service_kpl_limit_performance → service_stk_limit
       → service_suspend → service_top_list → service_top_inst
       → service_block_trade → service_ggt_daily → service_hsgt_top10
       → service_limit_list
       → service_moneyflow
       → service_margin
       → service_margin_detail
       → service_cyq_perf
       → service_daily_basic
       → service_index_basic
       → service_index_daily
       → service_cctv_news
       (任一失败只 logger.error,继续下一项)
```

**依赖链**:`basic/tradecal` 无依赖 → `daily/adj_factor` 互相依赖(daily 先) → `week/month` 强依赖前两者(内部校验) → kpl/cctv 独立。

### 6. `task_news_update()`
- 检查 `is_news_window()`,不在时段直接 return
- 异步 spawn `service_news` + `service_major_news`(不等返回)

### 7. `task_kpl_limit_performance()`
- 同步 spawn `service_kpl_limit_performance`(增量,自动从本周一+ctrl.max_date 取大)
- 失败只 logger.error,继续

### 8. `run_daemon()`
```python
def run_daemon():
    import schedule
    # 注册早上快速更新(09:05)
    schedule.every().day.at(STR_TIME_MORNING).do(task_morning_update)
    # 注册 3 个时间点的 full_update(18:00 / 20:00 / 22:00)
    for t in [STR_TIME_FULL_DB_1, STR_TIME_FULL_DB_2, STR_TIME_FULL_DB_3]:
        schedule.every().day.at(t).do(task_full_update)
    # 注册 2 个时间点的 kpl_limit_performance
    schedule.every().day.at(STR_TIME_KPL_LP_1).do(task_kpl_limit_performance)
    schedule.every().day.at(STR_TIME_KPL_LP_2).do(task_kpl_limit_performance)
    # 注册每 10 分钟的 news_update
    schedule.every(NEWS_INTERVAL_MIN).minutes.do(task_news_update)
    # 启动时立即跑一次 full_update
    task_full_update()
    # 主循环
    while True:
        schedule.run_pending()
        time.sleep(30)
```

### 9. `main()` — CLI 入口
```bash
--daemon  # 调 run_daemon,常驻
--once <name>  # 跑单个 service,如 --once service_daily
--full  # 跑一次 task_full_update
--status  # 调 offline_db_client.show_status,不动数据
(无参数)  # 默认跑一次 task_full_update
```

## 关键设计

### 1. 调度器只负责顺序调用,不掺和业务校验
- 业务校验在 service 内部(如 `service_week` 检查 ctrl 一致性)
- `rc=2`业务中止转换为`RuntimeError`；交易日历、日线与复权失败中止依赖链，
  周/月派生表及其他独立服务失败记录后继续。

### 2. 子进程隔离
- 每个 service 独立 Python 进程,独立 DB 连接,独立日志
- 崩了不影响其他 service

### 3. 异步 vs 同步 spawn
- **同步**(`sync=True`,`subprocess.run`):用于完整更新链路,等子进程跑完
- **异步**(`sync=False`,`subprocess.Popen`):用于 news 高频更新,不阻塞下一轮

### 4. launchd 集成
plist 关键配置:
- `KeepAlive = true`(崩了自动重启)
- `RunAtLoad = true`(登录立即启动)
- `EnvironmentVariables.PYTHONPATH = scripts/`(service 能 import)

## 数据流

```
launchd 启动
  ↓
python3 -m scheduler.scheduler_updateData --daemon
  ↓
run_daemon()
  ↓
schedule 注册任务
  ↓
task_full_update()  # 启动时立即跑一次
  ↓
阶段 1: spawn_service("service_basic", [], sync=True)
  ↓
  subprocess.run(["/opt/anaconda3/bin/python3", "-m", "service.service_basic"], cwd=scripts/)
  ↓
  service_basic 进程跑
  ↓
  写日志到 logs/service_basic.log
  ↓
  返回 rc=0
  ↓
阶段 2: service_daily → service_adj_factor
阶段 3: service_week → service_month
阶段 4: service_kpl_* + service_stk_limit + service_suspend + service_top_* + service_block_trade + service_ggt_daily + service_hsgt_top10 + service_limit_list + service_moneyflow + service_margin + service_margin_detail + service_cyq_perf + service_daily_basic + service_index_basic + service_index_daily + service_cctv_news
  ↓
进入主循环:while True: schedule.run_pending(); time.sleep(30)
  ↓
到 09:00/16:30/20:00/22:00 时 schedule 触发 task
```

## 修改指南

### 加新的定时任务
1. 定义新的 task 函数:`def task_xxx(): ...`
2. 在 `run_daemon()` 注册:
   - 每天固定时间:`schedule.every().day.at("HH:MM").do(task_xxx)`
   - 间隔时间:`schedule.every(N).minutes.do(task_xxx)`
3. 在 `task_full_update` 4 阶段里挑一个加

### 改 spawn_service 默认参数
- Python 路径:`/opt/anaconda3/bin/python3`(必须绝对路径)
- 超时:`timeout=1800`(30 分钟)
- 日志:`logs/service_{name}.log`

## 注意事项

- **`task_daily_update` 已删除**(2026-09-15 修复版,原代码定义了但从未被调用)
- **不要在 scheduler 加业务逻辑**,只负责 spawn
- **subprocess.run 的 stdout 用 `open(log_file, "a")` 追加**,不会覆盖历史日志
- **PYTHONPATH 必须包含 PROJECT_ROOT.parent**,让子进程能 `import coreClient`
- **加新 service 时同步更新多处文档**:
  - `README.md` 关键事实(数字 + 一句话摘要) + 变更记录
  - `00_架构文档.md` 调度时间表 + 关键事实
  - `QuickStart.md` 调度时间表
  - `代码详细设计/README.md` / 项目索引 表格加一行
  - 在 `代码详细设计/` 下新建 `service_xxx.md` 一份详细设计(本目录其余 11 份 service 文档的模板)
