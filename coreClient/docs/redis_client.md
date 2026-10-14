# redis_client.md

> **版本**:v1.0(2026-09-15 新增)
> **文件路径**:`~/TradingAgent/coreClient/redis_client.py`
> **配置**:`~/TradingAgent/coreClient/redis_config.py`
> **目的**:Redis 通用基础层,跨业务复用(onlineDataManager / monitor / 未来业务)

---

## 1. 功能

Redis 基础层提供:
- **连接管理**:连接 + ping 健康检查
- **JSON 序列化**:完整 JSON pack(dict/list) + STREAM field 单值序列化(自动识别类型还原)
- **通用 HASH meta**:`set_meta / get_meta / incr_meta`(业务游标 key-value 元信息)
- **通用 STREAM 模板**:`_xadd_to_stream / _xadd_many / _decode_stream / _read_stream / _xlen`
- **通用 ZSET 窗口模板**:`_zadd_to_window / _zadd_bulk / _slide_zset_window / _read_window / _read_zset_by_score`
- **通用 SET 模板**:`_set_replace / _set_read / _set_count`(去重 / 池子全量替换)
- **通用 LIST 模板**:`_list_push_trim / _list_read / _list_read_decoded / _list_count`(最近 N 条窗口)
- **通用 HASH 单字段**:`_hset_field / _hgetall / _delete_key`
- **源头展开**:`_flat_record`(从 thsclient 拿到原始 dict 后展开成 STREAM 可用的扁平字段)
- **Key 构建**:`build_key(prefix, *parts)`
- **诊断**:`cleanup(prefix) / info(prefix)`

**关键设计**:**业务层不直接调 `pipe.sadd / pipe.lpush` 等底层命令**,只组合基类通用模板。

---

## 2. 接口

### 2.1 类:`RedisBase`

#### `__init__(host=DEFAULT_HOST, port=DEFAULT_PORT, db=DEFAULT_DB, *, prefix=None)`

初始化 + ping。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `host` | str | `127.0.0.1` | Redis 主机 |
| `port` | int | `6379` | Redis 端口 |
| `db` | int | `0` | Redis DB 号 |
| `prefix` | str \| None | `None` | 业务前缀(子类覆盖 PREFIX 也可) |

**使用方约定**:子类覆盖 `PREFIX: str = "online"` 之类的常量。

---

#### 序列化(4 个静态方法)

```python
@staticmethod
_encode(data) -> str               # 完整 dict/list → JSON 字符串(ZSET member 用)
@staticmethod
_decode(raw) -> Any                # JSON 字符串 → dict/list(失败返 raw)

@staticmethod
_encode_value(v) -> str            # STREAM field 序列化:数字/字符串/布尔 → str;dict/list → JSON 字符串
@staticmethod
_decode_value(v) -> Any            # STREAM field 反序列化:数字字符串 → int/float;JSON → list/dict
```

---

#### HASH meta(`<PREFIX>:meta` 命名空间)

```python
_meta_key(key) -> str              # 业务 HASH key 拼装(目前固定 "<PREFIX>:meta",留 hook 给子类)
set_meta(key, value) -> None       # HSET <PREFIX>:meta key value
get_meta(key, default=None) -> Any # HGET <PREFIX>:meta key(自动 _decode_value)
incr_meta(key) -> int              # HINCRBY <PREFIX>:meta key 1
```

---

#### 源头展开:`_flat_record(ts_code, data, *, drop_keys=DEFAULT_DROP_KEYS, ts_field=None, ts_value=None)`

源头 dict → STREAM 可用的扁平字段。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `ts_code` | str | 必 | 股票代码(顶层 ts_code 字段) |
| `data` | dict | 必 | 同花顺原始 dict |
| `drop_keys` | Iterable[str] | `DEFAULT_DROP_KEYS` | 要 pop 的冗余字段集合 |
| `ts_field` | str \| None | `None` | 数据时间戳字段名(如 `"auction_timestamp"`) |
| `ts_value` | Any | `None` | 数据时间戳值 |

---

#### STREAM 通用(5 个)

```python
_xadd_to_stream(pipe, stream_key, fields, *, maxlen=None, ttl=DEFAULT_STREAM_TTL) -> None
_xadd_many(pipe, stream_key, fields_list, *, maxlen=None, ttl=DEFAULT_STREAM_TTL, trim_after=True) -> int
_decode_stream(raw) -> list[tuple[str, dict]]  # raw 来自 xrevrange,返 [(id, decoded_dict), ...]
_read_stream(stream_key, count=None) -> list[tuple[str, dict]]  # XREVRANGE + _decode_stream
_xlen(stream_key) -> int                       # XLEN(异常返 0)
_scan_count(match_pattern) -> int               # SCAN_ITER + count
```

---

#### ZSET 通用(5 个)

```python
_zadd_to_window(pipe, window_key, member, score, *, ttl=DEFAULT_WINDOW_TTL, slide_window=False) -> None
_zadd_bulk(pipe, zset_key, members_scores, *, expire=None) -> None
_slide_zset_window(pipe, window_key, members_scores, *, ttl) -> None  # 每条按自己 score 滑窗
_read_window(window_key, score_min=0, score_max="+inf") -> list[dict]   # 自动 json.loads
_read_zset_by_score(zset_key, score_min=0, score_max="+inf") -> list     # 返原始字符串(minute 用)
```

---

#### SET 通用(3 个)

```python
_set_replace(pipe, set_key, members, *, expire=None) -> None  # DEL + SADD + 可选 EXPIRE
_set_read(set_key) -> list[str]                              # SMEMBERS
_set_count(set_key) -> int                                    # SCARD
```

---

#### LIST 通用(4 个)

```python
_list_push_trim(pipe, list_key, payload, *, max_size=500, ttl=DEFAULT_STREAM_TTL) -> None  # LPUSH + LTRIM
_list_read(list_key) -> list                                                          # LRANGE(原始字符串)
_list_read_decoded(list_key) -> list[dict]                                            # LRANGE + JSON decode
_list_count(list_key) -> int                                                          # LLEN
```

---

#### HASH 单字段 / DEL(3 个)

```python
_hset_field(hash_key, field, value) -> None    # HSET + 自动 _encode_value
_hgetall(hash_key) -> dict                    # HGETALL(空时返 {})
_delete_key(key) -> None                      # DEL
```

---

#### Key 构建:`build_key(prefix, *parts) -> str`

```python
build_key("online", "zt", "pool")                # → "online:zt:pool"
build_key("online", "snapshot", "window", ts_code) # → "online:snapshot:window:000001.SZ"
```

---

#### 清理 & 诊断

```python
cleanup(prefix=None) -> int   # 按前缀清理所有 key(谨慎,通常只在调试时用)
info(prefix=None) -> dict     # keyspace 概览({"prefix", "ping"})
```

---

## 3. 用法示例

### 3.1 业务层继承

```python
# onlineDataManager 业务侧(参考 scripts/core/redis_online.py)
from coreClient.redis_client import RedisBase

class OnlineRedis(RedisBase):
    PREFIX = "online"
    def __init__(self):
        super().__init__()
        # 业务侧只组合基类通用模板

    def put_auction_snapshots(self, ts_codes):
        # 1. 源头展开
        items = fetch_auction_snapshots(ts_codes)
        # 2. STREAM 批量写入
        stream_key = self.build_key(self.PREFIX, "auction", "stream")
        with self.r.pipeline(transaction=False) as pipe:
            for item in items:
                flat = self._flat_record(item["ts_code"], item, ts_field="auction_timestamp", ts_value=item.get("auction_timestamp"))
                self._xadd_to_stream(pipe, stream_key, flat, maxlen=1000)
            pipe.execute()
```

### 3.2 滑窗 ZSET

```python
# snapshot 滑窗(每条按自己 score 滑窗)
window_key = self.build_key("online", "snapshot", "window", ts_code)
with self.r.pipeline() as pipe:
    self._slide_zset_window(pipe, window_key, {member: score}, ttl=300)
    pipe.execute()

# 读最近 5 分钟
data = self._read_window(window_key, score_min=time.time() - 300)
```

### 3.3 HASH meta(业务游标)

```python
# 业务游标 last_id
self.set_meta("auction_last_id", "1757933234567-0")
last_id = self.get_meta("auction_last_id")
```

---

## 4. config 文件说明:redis_config.py

**位置**:`~/TradingAgent/coreClient/redis_config.py`

| 配置项 | 值 | 说明 |
|---|---|---|
| `DEFAULT_HOST` | `"127.0.0.1"` | Redis 主机 |
| `DEFAULT_PORT` | `6379` | Redis 端口 |
| `DEFAULT_DB` | `0` | Redis DB 号 |
| `META_LAST_ID_PREFIX` | `"last_stream_id:"` | STREAM 落盘游标前缀(业务自行拼接) |
| `META_KEY_LAST_ID` | `"last_id"` | last_id 字段名 |
| `META_KEY_COUNT` | `"count"` | count 字段名 |
| `META_KEY_TRADE_DATE` | `"trade_date"` | trade_date 字段名 |
| `DEFAULT_DROP_KEYS` | frozenset | STREAM 字段精简集合:`{thscode, ticker, data_timestamp, data_timestamp_iso, save_timestamp, save_timestamp_iso, snap_ts_unix, snap_ts, fetch_timestamp, raw}`(**v6.13/v6.14**: `snap_ts` / `snap_ts_unix` 已从 service 写入端删除,基类 drop set 保留以防历史数据回放,新数据永远不会触发)|
| `DEFAULT_STREAM_TTL` | `3600 * 6`(6 小时) | STREAM 默认 TTL(秒),业务可覆盖 |
| `DEFAULT_WINDOW_TTL` | `300`(5 分钟) | ZSET window 默认 TTL(秒) |
| `DEFAULT_LIST_MAX_SIZE` | `500` | LIST 默认 max_size |
| `REDIS_RETRY_MAX` | `3` | 连接/拉数据失败重试次数 |
| `REDIS_RETRY_SLEEP` | `0.5` | 重试基础间隔(秒) |
| `REDIS_PING_TIMEOUT` | `2` | ping 超时(秒) |

**关键设计**:**不包含业务前缀**(业务前缀由各业务的 `redis_*.py` 自行管理)。

---

## 5. 使用方

| 业务 | 文件 | 用途 |
|---|---|---|
| **onlineDataManager** | `scripts/core/redis_online.py`(`OnlineRedis` 继承 `RedisBase`) | 所有 10 kind 数据的 Redis 读写(auction / snapshot / orderbook / zt / break / anomaly / hot / limitperformance / watchlist / minute) |
| **monitor**(未来) | `~/TradingAgent/monitor/.../redis_monitor.py` | 监控数据(预留,目前未实现) |

---

## 6. 关键设计原则

1. **业务层只组合通用模板,不直接调底层命令**(`pipe.sadd / pipe.lpush`)
2. **`PREFIX` 由子类覆盖**:onlineDataManager → `"online"`,monitor → `"monitor"`
3. **JSON 序列化两层**:完整 dict 用 `_encode/_decode`,STREAM field 单值用 `_encode_value/_decode_value`(自动识别类型还原)
4. **drop_keys 兜底**:基类 `DEFAULT_DROP_KEYS` 处理常见冗余字段,业务可追加
6. **时间戳字段命名**:`<kind>_timestamp`(snapshot_timestamp / auction_timestamp / ...),由源头或 client 兜底打

---

## 7. 验证清单

- [x] `redis_client.py` 现有代码 + 文档完整对齐(2026-09-15)
- [x] `RedisBase` 类所有方法签名 + 参数 + 返回值文档化
- [x] 5 类数据通用模板(STREAM / ZSET / SET / LIST / HASH)全覆盖
- [x] config 文件 14 项配置全部说明
- [x] onlineDataManager 使用方文档化