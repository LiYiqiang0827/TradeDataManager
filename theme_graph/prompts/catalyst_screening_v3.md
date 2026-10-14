任务：对一个题材行情周期的候选新闻进行逐条筛选。必须覆盖输入candidate_evidence中的每一个evidence_id，不得遗漏，也不得创造新ID。

输出结构：
{
  "episode_id": "原样返回",
  "items": [
    {
      "evidence_id": "N1",
      "evidence_nature": "trigger_fact|market_recap|background_fact|counter_fact|unrelated",
      "relevance": "direct|supporting|counter|unrelated",
      "timing_role": "pre_start|start_day|during_episode|late_explanation|post_episode",
      "candidate_causal_status": "confirmed_trigger|plausible_trigger|background_only|counter_evidence|unknown",
      "fact": "只复述证据明确表达的事实，不超过80字",
      "why": "与本轮题材及启动时点的关系，不超过80字"
    }
  ],
  "duplicate_clusters": [
    {"representative_id":"N1","member_ids":["N1","N2"],"same_event":"yes|likely|no"}
  ],
  "missing_information": ["仍缺少的证据"],
  "confidence": 0.0,
  "needs_review": true
}

筛选规则：
- evidence_nature先判断证据是什么：独立事件事实为trigger_fact；主要描述股价、涨停或板块拉升为market_recap；长期资料为background_fact；否认/澄清/利空为counter_fact；无关为unrelated。
- `evidence_kind_hint=market_recap` 是程序化提示，不是最终答案。若正文确有独立事件，仍把整篇稿件标成market_recap，并在fact中只摘出独立事件；不能把“上涨”本身写成事件事实。
- direct：证据直接描述该题材的政策、产业事件、技术进展、价格变化或明确市场归因。
- supporting：与机制相关但不足以解释启动时点。
- counter：否认、澄清或削弱叙事。
- unrelated：只因关键词或股票名称命中，正文不能支持题材关系。
- 即使unrelated也必须输出该evidence_id，保证筛选过程可审计。
- 纯异动稿即使与题材完全相关，candidate_causal_status也只能是background_only；它可以证明市场当时如何交易，不能证明为什么交易。
- duplicate_clusters只有在多条记录描述同一项事实事件时才能合并；同一板块行情但给出不同触发原因的报道不能合并为同一事件。
