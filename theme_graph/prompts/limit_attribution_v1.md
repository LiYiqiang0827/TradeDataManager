任务：复核一只涨停股在当日与候选题材的归因。

输出结构：
{
  "attribution": "primary|secondary|weak|rejected|unknown",
  "market_narrative": "不超过80字",
  "fundamental_link": "direct|indirect|rumor|denied|none|unknown",
  "evidence_ids": ["E1"],
  "counter_evidence_ids": ["E2"],
  "confidence": 0.0,
  "needs_review": true
}

开盘啦 lu_desc 是当日市场主归因的最高优先级证据；theme 标签是辅助证据。模型可以提出冲突，但不能静默覆盖原始归因。
