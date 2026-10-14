任务：依据已经筛选的新闻全文、题材日度行情和涨停股事实，判断本轮行情为何启动、原因如何演化，以及哪些解释证据不足。

输出结构：
{
  "episode_id": "原样返回",
  "reason_status": "identified|multiple_competing|no_reliable_reason",
  "dominant_reason": {
    "summary": "本轮最主要炒作原因；无可靠原因时明确写未知，不超过120字",
    "mechanism": "事件如何转化为该题材的市场叙事，不超过160字",
    "causal_status": "confirmed_trigger|plausible_trigger|background_only|unknown",
    "event_time": "YYYY-MM-DD HH:MM或unknown",
    "first_known_at": "YYYY-MM-DD HH:MM或unknown",
    "evidence_ids": ["N1"],
    "market_evidence_ids": ["M1","S1"],
    "counter_evidence_ids": []
  },
  "catalysts": [
    {
      "catalyst_type": "policy|industry_event|company_event|price_change|technology|geopolitics|earnings|rumor|denial|unknown",
      "summary": "事件事实，不超过100字",
      "role": "initial_trigger|reinforcement|branch_rotation|late_explanation|counter",
      "causal_status": "confirmed_trigger|plausible_trigger|background_only|counter_evidence|unknown",
      "event_time": "YYYY-MM-DD HH:MM或unknown",
      "evidence_ids": ["N1"],
      "market_evidence_ids": ["M1"]
    }
  ],
  "narrative_timeline": [
    {"date":"YYYYMMDD","stage":"pre_start|start|acceleration|divergence|repair|retreat","narrative":"当时可见叙事","evidence_ids":["N1","M1"]}
  ],
  "alternative_explanations": [
    {"summary":"竞争解释","evidence_ids":["N2"],"why_not_dominant":"证据不足或时间不符"}
  ],
  "unresolved": ["仍不能回答的问题"],
  "confidence": 0.0,
  "needs_review": true
}

判断顺序：先检查新闻时间是否领先启动，再检查题材宽度和高度是否响应，然后检查是否存在更早或更直接的竞争解释。不能因为一篇盘后文章使用“受某消息刺激”就自动认定confirmed_trigger。
不得把market_recap或个股异动本身写入dominant_reason。只有异动稿中可独立核验的政策、公告、产业、价格或技术事件可以作为候选原因，且必须按事件的最早可见时间与行情比较。若主因只剩异动稿自我归因，输出no_reliable_reason。
每个catalysts和narrative_timeline项目都必须有非空evidence_ids。没有新闻的行情日应引用M或S证据，不得输出无引用叙述。
