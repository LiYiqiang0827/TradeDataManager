# cronjob_manage 参考配置

通过 hermes 的 cronjob_manage 工具创建 4 个 job(参考实现)。

> **脚本路径选择(2026-10-17 v2)**:
> - **生产路径(推荐)**:`~/TradingDataManager/skills/trading-db-daily-check/scripts/`
>   —— 跟仓库代码同地,跟生产数据/服务路径同源,提交仓库时跟代码一起被 git 跟踪
> - **hermes 个人副本**:`~/.hermes/skills/trading/trading-db-daily-check/scripts/`
>   —— 早期安装位置,跟 hermes 自身同一棵 skills 树;hermes 优先从此处加载
>
> 两个路径**互为同步副本**(同一份代码),选哪个都行,只要保证脚本和 cron 引用一致。
> 本文档示例使用 hermes 副本路径(`~/.hermes/skills/...`),如有变化请用 `${SKILL_SCRIPTS}` 替换。

## Job 1 — 每小时 scheduler 保活

```python
cronjob_manage(action="create", job={
    "name": "trading_db_hourly_live_check",
    "schedule": "0 * * * *",  # 每小时整点
    "command": "python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/hourly_live_check.py --quiet-ok 2>&1 | lark-cli im +messages-send --chat-id oc_a9a10ba69444bf3144cb08e5e353052a --content @-",
    "enabled": True,
})
```

(全天每小时跑,包括周末;因为是保活检查,周末也要看 scheduler 是否挂了)

## Job 2 — 工作日 09:15 + 22:00 offline 数据库日检

```python
cronjob_manage(action="create", job={
    "name": "trading_db_offline_morning_audit",
    "schedule": "15 9 * * 1-5",  # 周一到周五 09:15
    "command": "python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/offline_daily_audit.py 2>&1 | lark-cli im +messages-send --chat-id oc_a9a10ba69444bf3144cb08e5e353052a --content @-",
    "enabled": True,
})
cronjob_manage(action="create", job={
    "name": "trading_db_offline_evening_audit",
    "schedule": "0 22 * * 1-5",  # 周一到周五 22:00
    "command": "python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/offline_daily_audit.py --verbose 2>&1 | lark-cli im +messages-send --chat-id oc_a9a10ba69444bf3144cb08e5e353052a --content @-",
    "enabled": True,
})
```

(晚上 22:00 用 `--verbose` 输出全部表格细节,因为一天数据完整后做最终审查)

## Job 3 — 工作日 16:30 online 数据库落盘检查

```python
cronjob_manage(action="create", job={
    "name": "trading_db_online_audit",
    "schedule": "30 16 * * 1-5",  # 周一到周五 16:30
    "command": "python3 ~/.hermes/skills/trading/trading-db-daily-check/scripts/online_daily_audit.py 2>&1 | lark-cli im +messages-send --chat-id oc_a9a10ba69444bf3144cb08e5e353052a --content @-",
    "enabled": True,
})
```

## 关键决策

- `--quiet-ok` 仅在 hourly 用(只发异常,免得飞书被刷屏)
- offline morning/evening 不加 quiet — 用户希望"确认更新到哪一天"总要看
- online 日检也不加 quiet — 用户希望"看完整情况"
- chat_id 是 Home channel (`oc_a9a10ba69444bf3144cb08e5e353052a`)

## 脚本副本同步约定

`~/TradingDataManager/skills/trading-db-daily-check/`(仓库内,git 跟踪)
与 `~/.hermes/skills/trading/trading-db-daily-check/`(hermes 加载路径)互为副本。
**改完一处,记得同步另一处**(脚本内容应保持完全一致)。
