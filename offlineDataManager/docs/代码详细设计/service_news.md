# 代码详细设计/service_news.md

`scripts/service/service_news.py` — 新闻快讯增量更新 service(每 10 分钟)。

## 职责

1. 无 CLI 参数(全默认)
2. 调 `down.update_news()`(默认 1 天前起,所有 active 源)
3. 写日志到 `logs/service_news.log`

## 入口

```bash
python3 -m service.service_news
```

## 关键流程

```python
def main():
    log_file = PROJECT_ROOT / "logs" / "service_news.log"
    logger.add(str(log_file), rotation="50 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        # update_news 默认 1 天前起(增量)
        result = down.update_news()
        total = sum(v for v in result.values() if isinstance(v, int))
        logger.info(f"[service_news] 完成: +{total:,} 行, 各源: {result}, 用时 {time.time()-t0:.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_news] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **无 CLI 参数**,全部默认
- **`update_news()` 默认**:
  - `src_list=None` → 走 `get_active_news_srcs()`(9 源 - 2 disabled = 7 源)
  - `start_date=None, end_date=None` → 走每源独立断点(`tbl_news_ctrl[src]`)
- **每源独立断点**:`tbl_news_ctrl[src]`,datetime 粒度
- **写日志**:`{src: 新增行数} dict`
- **log rotation=50 MB**

## 数据流

```
service_news.main()
  ↓
down = CNDataDown()
  ↓
result = down.update_news()
  ↓
update_news 内部:
  - 默认 src_list = get_active_news_srcs()  # 7 源
  - 对每源 src:
    last = _get_news_ctrl_value(conn_news, src)  # 读 tbl_news_ctrl[src]
    sd = last[:10] if last else "2020-01-01"  # datetime 转 date
    按日期循环 cur=sd..today
      对每天 td:
        ed = td + 1 day
        df = self.client.news(src=src, start_date=td, end_date=ed, limit=1500, offset=offset)
        df["md5"] = df["content"].apply(hashlib.md5)  # 基于 content
        df["snap_ts"] = snap_ts()
        upsert_df(conn_news, df, "tbl_news", key_cols=["datetime", "src", "md5"])
        offset += 1500 if len(df) == 1500 else break
      cur += 1 day
    _update_news_ctrl_value(conn_news, src, last_success_dt)
  ↓
return {src: inserted_count}
```

## 调度位置

- **每 10 分钟**(08:00 - 次日 05:00)由 `task_news_update` 异步 spawn
- **不阻塞下一轮**(用 `Popen` 而非 `run`)

## 注意事项

- **9 源 - 2 disabled = 7 源**:`yuncaijing` / `fenghuang` 实测 7 天连续 0 行,scheduler 默认跳过
- **md5 基于 content**(`tbl_news` 用 content 算 md5,跟 `tbl_major_news` 用 title 算 md5 不一致)
- **datetime 格式**:写入的是 `YYYY-MM-DD HH:MM:SS` 完整时间戳
- **默认起始 2020-01-01**(`tbl_news_ctrl` 无该源记录时)
- **高频任务**:异步启动,后台跑不等返回(崩了不影响其他)
