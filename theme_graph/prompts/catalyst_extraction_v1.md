任务：从新闻、公告和政策证据中抽取可能引爆题材的催化事件。

输出结构：
{
  "catalyst_type": "policy|industry_event|company_event|price_change|technology|geopolitics|earnings|rumor|denial|unknown",
  "event_time": "YYYY-MM-DD HH:MM或unknown",
  "themes": ["题材名"],
  "summary": "不超过100字",
  "causal_status": "confirmed_trigger|plausible_trigger|background_only|counter_evidence|unknown",
  "evidence_ids": ["E1"],
  "confidence": 0.0,
  "needs_review": true
}

新闻与涨停同日出现只代表时序共现。只有市场归因、报道或多条证据明确连接时，才可标为 confirmed_trigger。
