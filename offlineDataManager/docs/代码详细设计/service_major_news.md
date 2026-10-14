# 代码详细设计/service_major_news.md

`scripts/service/service_major_news.py` — 长新闻增量更新 service(每 10 分钟)。

## 职责

1. 无 CLI 参数
2. 调 `down.update_major_news()`
3. 写日志到 `logs/service_major_news.log`

## 入口

```bash
python3 -m service.service_major_news
```

## 关键流程

```python
def main():
    log_file = PROJECT_ROOT / "logs" / "service_major_news.log"
    logger.add(str(log_file), rotation="20 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        result = down.update_major_news()
        logger.info(f"[service_major_news] 完成: +{result.get('inserted', 0):,} 行, 用时 {result.get('elapsed', 0):.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_major_news] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **无 CLI 参数**
- **`update_major_news()` 默认**:
  - `start_date=None` → 走 `tbl_news_ctrl[major_news]` 断点(YYYY-MM-DD HH:MM:SS),或 fallback "1 天前 + 00:00:00"
  - `end_date` 默认 `now()`
- **md5 基于 `title`**(跟 `tbl_news` 用 content 算 md5 不一致)
- **每源独立断点**:`tbl_news_ctrl[major_news]`(单一行,虽然 ctrl 表是 src 主键)
- **log rotation=20 MB**

## 数据流

```
service_major_news.main()
  ↓
down = CNDataDown()
  ↓
result = down.update_major_news()
  ↓
update_major_news 内部:
  - last = _get_news_ctrl_value(conn_news, "major_news")
  - sd = last if last else "1 天前 00:00:00"
  - ed = now()
  - df = self.client.pro.major_news(start_date=sd, end_date=ed, limit=2000)
  - df["md5"] = df["title"].apply(hashlib.md5)  # 基于 title
  - df["datetime"] = df["pub_time"].astype(str)
  - df["snap_ts"] = snap_ts()
  - upsert_df(conn_news, df, "tbl_major_news", key_cols=["datetime", "src", "md5"])
  - _update_news_ctrl_value(conn_news, "major_news", max_pub_time)
  ↓
return {"inserted": int, "elapsed": float}
```

## 调度位置

- **每 10 分钟**(跟 `service_news` 一起,异步 spawn)
- **不阻塞下一轮**

## 注意事项

- **单次整体拉**(`start_date` ~ `end_date` 一次调用),不像 `update_news` 按日循环
- **md5 基于 title**(`tbl_news` 用 content)
- **断点 key 是 `"major_news"` 字符串**,不是表名 `tbl_major_news`
- **datetime 来源**:`pub_time` 字段(原样转字符串)
- **默认 1 天前**:冷启动时拉 1 天前到现在的数据
