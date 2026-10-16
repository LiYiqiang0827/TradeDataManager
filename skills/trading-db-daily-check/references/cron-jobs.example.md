# Hermes cron 配置与检查

本页按 Mac mini 当前 Hermes 配置编写。4 个任务使用 `~/.hermes/scripts/` 下的 shell wrapper 和 `--no-agent`；wrapper 调用 Hermes skill 副本中的审计脚本，stdout 由 Hermes 投递到飞书。

## 当前任务

| 任务 | 时间表 | wrapper | 模式 | 投递目标 |
|---|---|---|---|---|
| `trading_db_hourly_live_check` | `0 * * * *` | `trading_db_hourly.sh` | no-agent | `feishu:oc_a9a10ba69444bf3144cb08e5e353052a` |
| `trading_db_offline_audit_0915` | `15 9 * * 1-5` | `trading_db_offline_audit.sh` | no-agent | 同上 |
| `trading_db_offline_audit_2200` | `0 22 * * 1-5` | `trading_db_offline_audit.sh` | no-agent | 同上 |
| `trading_db_online_audit` | `30 16 * * 1-5` | `trading_db_online_audit.sh` | no-agent | 同上 |

检查任务是否已存在及最近执行情况：

```bash
hermes cron list
hermes cron runs <job-id> --limit 10
hermes cron status
```

查看 wrapper 和对应脚本：

```bash
cat ~/.hermes/scripts/trading_db_hourly.sh
cat ~/.hermes/scripts/trading_db_offline_audit.sh
cat ~/.hermes/scripts/trading_db_online_audit.sh
```

## 创建缺失任务

先确认 `hermes cron list` 中没有同名任务。Hermes 版本若有参数差异，先查看 `hermes cron create --help`。创建单个任务的命令格式如下：

```bash
hermes cron create '0 * * * *' \
  --name trading_db_hourly_live_check \
  --deliver feishu:oc_a9a10ba69444bf3144cb08e5e353052a \
  --script trading_db_hourly.sh \
  --no-agent
```

其它任务沿用上表的 schedule、name、wrapper 和投递目标。`--script` 接受 `~/.hermes/scripts/` 下的相对文件名；不要把 skill 脚本路径直接填入 `--script`。

## 退出码与飞书消息

- wrapper 使用 `exec` 调用 Python 脚本，因此审计脚本的退出码会成为 cron 运行状态。
- stdout 会直接作为飞书消息；当前 wrapper 不添加 `⚠️` 前缀。
- 检查脚本应在发现异常时返回非零并输出完整报告；正常时返回 0。hourly 脚本全正常时可以静默。
- 不要再用 `python script | lark-cli ...` 代替此配置：该管道不是当前 Hermes 工作流，也可能让 shell 返回发送命令的退出码而掩盖审计结果。

## 同步位置

- Hermes 加载副本：`~/.hermes/skills/trading/trading-db-daily-check/`
- Git 仓库副本：`~/TradingDataManager/skills/trading-db-daily-check/`
- 两份目录内容应保持一致。修改后对照 `SKILL.md`、`scripts/`、`references/` 和 `prompts/` 文件，再提交仓库。
