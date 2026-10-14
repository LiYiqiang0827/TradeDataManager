# 代码详细设计/service_cctv_news.md

`scripts/service/service_cctv_news.py` — CCTV 新闻联播增量更新 service(每天 1 次)。

## 职责

1. 无 CLI 参数
2. 调 `down.update_cctv_news()`
3. 写日志到 `logs/service_cctv_news.log`

## 入口

```bash
python3 -m service.service_cctv_news
```

## 关键流程

```python
def main():
    log_file = PROJECT_ROOT / "logs" / "service_cctv_news.log"
    logger.add(str(log_file), rotation="10 MB", level="INFO", enqueue=True)
    
    t0 = time.time()
    try:
        down = CNDataDown()
        result = down.update_cctv_news()
        logger.info(f"[service_cctv_news] 完成: +{result.get('inserted', 0):,} 行, 用时 {result.get('elapsed', 0):.1f}s")
        return 0
    except Exception as e:
        logger.error(f"[service_cctv_news] 失败: {e}", exc_info=True)
        return 1
```

## 关键点

- **无 CLI 参数**
- **`update_cctv_news()` 默认**:
  - `start_date=None` → 走 `tbl_news_ctrl[cctv_news]` 断点(YYYYMMDD),或 fallback "7 天前"
- **每源独立断点**:`tbl_news_ctrl[cctv_news]`
- **datetime 是 YYYYMMDD 不带时间戳**(cctv 接口无时间字段,跟 `tbl_news` / `tbl_major_news` 完全不同)
- **log rotation=10 MB**

## 数据流

```
service_cctv_news.main()
  ↓
down = CNDataDown()
  ↓
result = down.update_cctv_news()
  ↓
update_cctv_news 内部:
  - last = _get_news_ctrl_value(conn_news, "cctv_news")
  - sd = (last + 1 day) if last else "7 天前"
  - ed = today()
  - 按日期循环 cur=sd..ed
    对每天 td:
      df = self.client.pro.cctv_news(date=td)
      if df not empty:
        df["datetime"] = td  # 覆盖为 YYYYMMDD
        df["snap_ts"] = snap_ts()
        upsert_df(conn_news, df, "tbl_cctv_news", key_cols=["datetime", "md5"])
        if inserted > 0: last_success_date = td
  - _update_news_ctrl_value(conn_news, "cctv_news", last_success_date)
  ↓
return {"inserted": int, "elapsed": float}
```

## 调度位置

**挂两个 task**(2026-09-15):
- `task_morning_update`(09:05):早上快速补,用户开盘前看昨天联播
- `task_full_update` 第 4 阶段(18:00 / 20:00 / 22:00):每天 3 次兜底

**不在 news 高频调度**(`task_news_update` 不调它,news 是快讯,联播是日级)

## 注意事项

- **datetime 格式差异**:`tbl_cctv_news.datetime` 是 `YYYYMMDD` 8 位数字,跟 `tbl_news` / `tbl_major_news` 的 `YYYY-MM-DD HH:MM:SS` 不同
- **`get_cctv_news` 不接受 `*_datetime` 参数**(因为数据格式就是 YYYYMMDD)
- **`update_cctv_news` 不显式算 md5 字段**,df 里没 md5 列,upsert 时只插 datetime/title/content/src/snap_ts。SQLite 主键检查时 md5 字段为 NULL,实际主键约束退化成只按 datetime 判重(由 `service_cctv_news` 按日期循环的调度保证不重)
- **每天 1 次**即可(cctv 联播是固定节目,实时性不强)
- **log rotation=10 MB**(其他 service 是 20-50 MB,这里数据量小)
