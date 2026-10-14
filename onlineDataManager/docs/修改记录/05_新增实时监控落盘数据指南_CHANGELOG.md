# 05_新增实时监控落盘数据指南_CHANGELOG

> **本文档历史记录**(只追加,不修改)

---

## v1.0 — 2026-09-15(文档首次创建)

**触发**:用户原话「下一步增加一个和架构文档平级的新文档,主要目的是指导一个 AI 该如何新增加一个实时监控落盘数据,该在哪些地方修改(包括但不限于 数据源头,redis,数据库,service scheduler,test 等等部分)包括 redis 里面常规的 timeline archive stream 组合,stream 落盘方法,redis online 类里面增加获取数据的接口等等」

**变更内容**:
- 新建 `05_新增实时监控落盘数据指南.md`,平级于 `01_架构文档.md` / `02_代码详细设计.md` / `03_数据详细设计.md` / `04_QuickStart.md`
- 15 节结构:
  - §0 文档定位(何时读 / 与 04 §4.3 关系 / 推荐阅读顺序)
  - §1 全链路总览(5 层次鸟瞰图)
  - §2 必须动的 6 类文件清单
  - §3 数据源层(选择哪种数据源 / 加 fetch 函数规范 / 数据时间戳规则)
  - §4 Redis 三件套设计(STREAM + timeline + archive + window 命名规范 + 写入流程 + 滑窗裁剪 + TTL 拍板)
  - §5 redis_online 接口新增规范(3 个一组:put + commit + get + 真值代码模板)
  - §6 SQLite 表 schema 新增(`tbl_<kind>` 命名 + `KIND_SCHEMAS` 注册)
  - §7 落盘链路 `persist_xxx`(游标管理 + commit 后才推进 + v1.23 修复要点)
  - §8 service 层(2 个新文件模板)
  - §9 scheduler 注册(`WRITER_PHASE_SERVICES × SAVEDATA_PHASE_SERVICES` 阶段拍板)
  - §10 check 工具(2 个新检查函数 + 路由表注册)
  - §11 文档体系(2 份详细设计 + 4 份主文档 + 5 份 CHANGELOG)
  - §12 test(端到端验证 + AI 评估节奏)
  - §13 排错清单(7 类常见坑)
  - §14 完整 checklist(9 阶段 50 项打勾)
  - §15 历史

**关键设计约束**(从 memory + v1.23/v1.24 修复继承):
- **统一 encode/decode**:必须用 `self._encode(item)` / `self._decode(v)` 基类方法
- **Redis pipeline 不能有 read 命令**:read 必须单独 `self.r.xxx()`
- **commit 后才推游标**(v1.23 修复:失败返回 -1 不推进)
- **logger 不支持 flush 参数**:所有 `flush=True` 必须删除
- **TTL 动态 > 写死**:根据写入时间点实时算
- **`data_timestamp` 内部全 `YYYY-MM-DD HH:MM:SS.fff`**(v3 双时间戳原则)
- **CHANGELOG 只更新动过的文档**
- **AI 评估节奏**:派 zero-context subagent → 修复 → 重测 scheduler_once → 追加 v1.x

**影响**:
- 任何 AI 被要求"加一个实时监控落盘数据",照本文件 §14 checklist 9 阶段 50 项可端到端接通
- 与现有 04 §4.3 加新 kind 通用模板互补:通用模板覆盖快照型,本文件覆盖实时监控型
- 不需要再回头读 31 份详细设计 md 拼凑接入步骤,直接照 §14 走

**验证**:N/A(本文档无代码)

---

## v1.1 — 2026-09-15(第 22 轮:doc ↔ code 全量一致性校正)

> **触发**:用户第 22 轮指令"再完整扫描一遍代码和文档,看看文档是否与代码一致,尤其是各个详细设计文档"。AI 全量核对,发现 05 §11.1 行数真值点错(刚建 1 小时)。

### 修正清单

**详细设计 doc 行数真值点**:
- §11.1:"参考 `代码详细设计/service_writeredis_auction.md`(145 行)" → **70 行**(`wc -l` 真值)
- §11.1:`service_savedata_auction.md` 没明示行数 → **61 行**(`wc -l` 真值)
- §11.1 文件树注释:`145 行模板` × 2 → **`~70 行模板`** + **`~61 行模板`**(真值)

### 原因

05 文档刚建立时凭印象写 "145 行模板",实际 `service_writeredis_auction.md` 只有 70 行(本指南同时整理了 auction/snapshot 等服务精简后行数减半)。

### 影响

- **AI 照本指南 §11.1 新建 2 份详细设计 doc 时,实际文件大小期望值是 ~70 / ~61 行**(原 145 行期望误导 AI 写过度冗长)
- 与 01/02/03/04 CHANGELOG 同步追加 — **本轮 5 份主文档 CHANGELOG 全部追加 v1.x**

---

## v1.1 — 2026-09-16(snapshot_index 完整实例)

### §12 新增(76 行)

- **§12.1 决策表**:12 项决策(数据源 / 8 只 / 30s / 单独 phase / 时间窗 / 落盘频率 / TTL / 滑窗 / lu_time 替代)
- **§12.2 8 只固定指数代码表**
- **§12.3 实施清单**:对照本指南各章节,列改动模块与行数
- **§12.4 验证结果**:`scheduler_once` 22/22 OK(16.1s)
- **§12.5 实施中遇到的关键坑**(5 个):snapshot_timestamp 是 int unix ms / drop_keys / _flat_record 重命名 / archive TTL 不动态 / envelope 是 data.item

### 原因

snapshot_index 是按本指南走完整流程的第一个新增 kind,把它作为完整示例加入,让未来 AI 加新 kind 时有真实模板参照。

### 影响

- AI 加新 kind 时,有真实决策表 / 实施清单 / 踩坑记录可参考
- snapshot_index 是 v6.10 唯一新增,本指南其余章节未改

### 联动

- `01_架构文档_CHANGELOG.md` v1.1 / `02_代码详细设计_CHANGELOG.md` v1.28 / `03_数据详细设计_CHANGELOG.md` v1.5 — 同步追加 snapshot_index
- `代码详细设计/` 新建 `service_writeredis_snapshot_index.md` + `service_savedata_snapshot_index.md`,5 份 core doc 头部加 v6.10 标记