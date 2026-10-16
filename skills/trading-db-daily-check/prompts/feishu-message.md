# 飞书消息投递约定

Mac mini 当前审计 cron 使用 Hermes `--no-agent` 和 `~/.hermes/scripts/` 下的 wrapper。Hermes 将 wrapper 的 stdout 原样投递到配置的飞书会话；脚本退出码记录在 cron 运行状态里。该模式不调用本文件作为 prompt，也不会自动根据退出码添加消息前缀。

## 审计脚本输出

- `hourly_live_check.py --quiet-ok`：全正常时不输出，因此 watchdog 不发消息；发现问题时输出完整诊断并以非零码退出。
- `offline_daily_audit.py` 和 `online_daily_audit.py`：输出检查日期、结果表格及异常汇总；完整 stdout 作为飞书消息。
- 若以后改成 agent-driven cron，再将此文件作为消息整理提示，并保持原始审计结果、退出码和数据范围清楚可追溯。
