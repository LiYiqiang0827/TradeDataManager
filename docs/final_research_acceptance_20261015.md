# TradingAgent 研究迁移最终兼容性验收

日期：2026-10-15（Asia/Shanghai）
验收范围：研究目录迁移与 TradingDataManager 统一数据入口的边界；只运行 CLI 帮助和一条固定日期只读数据库查询。

## 结果

- **目录与旧入口：通过。** `policyStudy`、`plot`、`outputs` 分别是指向 `MrLi/policyStudy`、`MrLi/plot`、`MrLi/outputs` 的兼容链接；研究实体仍在 MrLi/MrWu 目录。MrLi stage-one CLI 与 MrWu market-review context-packet CLI 经旧根路径调用 `--help` 均退出 0。未运行策略、模型或 toolbox 任务。
- **统一 provider：通过。** 从主仓根目录执行 `from coreClient import data_provider`，公开模块的解析文件是 `/Users/nickzhang/TradingDataManager/coreClient/data_provider.py`。
- **严格离线日线读取：通过。** 调用 `get_day(ts_code="000001.SZ", trade_date="20261014", qfq=False, source="database_only")` 返回 1 行，trade_date 为 `20261014`。该次查询不使用 online fallback，也没有写数据库。
- **根目录边界：通过。** 默认解析结果分别为研究根 `/Users/nickzhang/TradingAgent/MrLi`、代码根 `/Users/nickzhang/TradingDataManager`、数据库根 `/Users/nickzhang/TradingDatabase`。迁入 TDM 的 `scripts/research_inputs/data_gen.py` 用 manager root 定位代码和 `offlineDataManager/scripts/service`，用 research root 定位默认 watchlist，并通过 `offline_data_dir()` 定位 `TradingDatabase/offline`。默认 watchlist 为 `/Users/nickzhang/TradingAgent/MrLi/policyStudy/policy/题材涨停研究/watchlist`；旧 `data_gen.py --help` 显示此默认路径，`--watchlist-dir` 覆盖选项仍在。`--help` 未读 watchlist、查询数据库或启动服务。旧 `outputs` 入口仍解析到 MrLi 输出目录。
- **Installed skills：通过。** 已安装 `a-share-theme-research` 与 `a-share-kline-structure-analysis` 都解析到 MrLi canonical skill；旧 installed theme skill 完整保存在 `/Users/nickzhang/.codex/skills/archive/a-share-theme-research-20261015`。将旧版独有的 theme-graph 参考并入 canonical，并更新其代码、DB、服务和提示词路径以匹配 manager/database/research 三根；没有改理论内容。两项 canonical skill 的 `quick_validate.py` 均通过。
- **输出忽略规则：通过。** 主仓 `.gitignore` 已镜像 canonical 根规则：`/MrLi/outputs/`、MrLi plot 临时图片、题材研究的 `savepic`、`research`、`presentations` 与 watchlist CSV。`git check-ignore -v` 对真实输出日志、临时图片、研究 JSON、演示文稿和 watchlist CSV 均命中预期规则；研究脚本、MrLi 架构文档、MrWu README 及两个 `.gitkeep` 未被忽略。原有规则保留。

## Dirty 文件保全核对

基线清单 `/private/tmp/trading_restructure_20261014/ownership_manifest.json` 有 165 个原有 dirty 文件，当前逐项检查未发现缺失路径。以原始 SHA 对当前兼容路径/其解析目标复核：120 项仍与原 SHA 相同，41 项匹配 `path_adjustments.json` 记录的路径深度修改后 SHA；另 4 项是数据侧 canonical 迁移或获准的数据根改造所涉内容变化：`offline_db_client.py`、`scheduler_updateData.py`、`test_scheduler_updateData.py` 位于 TDM 唯一实体；`fetch_trend_dividends.py` 已迁入 TDM research_inputs。数据侧 agent 确认前三项的源代码树已逐文件 SHA 归档，抓取脚本原字节也在迁移备份中，当前旧入口指向 canonical 文件。

研究迁移 receipt 汇总记录了 165 项基线、0 项缺失、42 项预期路径深度 hash 更新和 73 个路径调整；本次复验观察到 41 项仍匹配当时记录的 after hash。剩余的变化均可对应到上述获准数据侧改造/脚本迁移及其归档证据，没有发现未映射的文件损失。完整基线和路径调整证据位于 `/private/tmp/trading_restructure_20261014/ownership_manifest.json`、`migration_receipt.json`、`path_adjustments.json` 与 `path_adjustments.diff`；数据侧实体备份清单在 `/private/tmp/trading_restructure_20261014/main_code_backup/backup_manifest.json`。

## 限制

本验收没有运行全量研究、在线 provider、服务、模型或数据下载。数据库检查仅覆盖一个固定日期的一只股票日线；它证明旧入口到 TDM 再到离线数据库的基本只读链路可用，不代表所有 provider 表或历史日期的完整性。
