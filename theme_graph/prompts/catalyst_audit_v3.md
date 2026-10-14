任务：审计另一模型生成的题材催化分析。逐项对照原始新闻、行情和涨停证据，找出时间倒置、引用不支持、因果过度、事件类型误判和把不同新闻当成同一事件的问题。不能因为文字看起来合理就通过。

输出结构：
{
  "episode_id": "原样返回",
  "verdict": "accept|revise|reject",
  "citation_errors": [
    {"field":"JSON路径","issue":"证据实际不支持的内容","evidence_ids":["N1"]}
  ],
  "temporal_errors": [
    {"field":"JSON路径","issue":"消息晚于所解释阶段","evidence_ids":["N2","M1"]}
  ],
  "causal_overstatements": [
    {"field":"JSON路径","issue":"为什么因果等级过高","evidence_ids":["N3"]}
  ],
  "classification_errors": [
    {"field":"JSON路径","issue":"催化类型、阶段或角色错误","evidence_ids":["N4"]}
  ],
  "revised_conclusion": {
    "reason_status":"identified|multiple_competing|no_reliable_reason",
    "summary":"修正后的主结论，不超过150字",
    "causal_status":"confirmed_trigger|plausible_trigger|background_only|unknown",
    "evidence_ids":["N1"],
    "market_evidence_ids":["M1"],
    "counter_evidence_ids":[]
  },
  "unresolved":["仍缺少什么证据"],
  "confidence":0.0,
  "needs_review":true
}

判定标准：
- accept：没有实质性错误，仅有措辞差异。
- revise：主方向可保留，但至少一项时间、分类、引用或因果表述必须修改。
- reject：主因与证据不符、使用未来信息、遗漏关键反证或无法从证据得到。
- 如果原分析把盘后报道用于解释盘中启动，必须列为temporal_errors；如果仅把盘后报道表述为复盘归因，则可以保留为background_only。
- 如果原分析把个股涨停、板块拉升或异动稿本身当成引爆原因，必须列为causal_overstatements。若稿件同时引用独立事件，只审计该独立事件的原始事实和最早可见时间，行情描述仅可作为市场响应证据。
- 指出错误类型时也必须使用原任务允许的枚举，例如企业并购应建议company_event或rumor，不得创造corporate_event等新枚举。
