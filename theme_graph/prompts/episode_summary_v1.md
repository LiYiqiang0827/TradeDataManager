任务：只根据给定日期范围内的热度、高度、宽度、炸板和催化证据总结一次题材行情段。

输出结构：
{
  "phase_summary": "不超过150字",
  "key_dates": [{"date":"YYYYMMDD","role":"start|acceleration|divergence|climax|repair|retreat","evidence_ids":["E1"]}],
  "market_cores": [{"ts_code":"000001.SZ","role":"height_core|first_mover|capacity_core|follower","evidence_ids":["E2"]}],
  "catalysts": [{"summary":"文字","evidence_ids":["E3"]}],
  "unresolved": ["待确认问题"],
  "confidence": 0.0,
  "needs_review": true
}

周期结束日可以是事后分段结果，但对任一历史日期的阶段描述不得引用其后的数据。产业核心不能仅凭市场高度认定。
