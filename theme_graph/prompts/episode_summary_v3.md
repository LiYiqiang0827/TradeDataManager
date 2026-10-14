任务：把已审查的催化判断与题材行情数据合并成一次题材周期摘要。只总结证据，不重新发明催化。

输出结构：
{
  "episode_id": "原样返回",
  "phase_summary": "概括启动、加强、分歧或退潮，不超过180字",
  "key_dates": [
    {"date":"YYYYMMDD","role":"start|acceleration|divergence|climax|repair|retreat","evidence_ids":["M1","S1"]}
  ],
  "market_cores": [
    {"ts_code":"000001.SZ","name":"公司名","role":"height_core|first_mover|capacity_core|follower","evidence_ids":["S1"]}
  ],
  "dominant_reason": {
    "summary":"沿用催化分析结论",
    "causal_status":"confirmed_trigger|plausible_trigger|background_only|unknown",
    "evidence_ids":["N1"]
  },
  "catalysts": [
    {"date":"YYYYMMDD或unknown","role":"initial_trigger|reinforcement|branch_rotation|late_explanation|counter","summary":"文字","evidence_ids":["N1"]}
  ],
  "reason_market_alignment": "原因出现与宽度、高度、核心股变化是否同步；不超过160字",
  "unresolved": ["待确认问题"],
  "confidence": 0.0,
  "needs_review": true
}

行情阶段可按完整周期事后分段，但解释某一天时不得引用之后才出现的消息。市场核心按涨停时间、高度、持续性和容量事实识别；产业核心不能仅凭连板高度认定。
周期摘要不能把个股异动、涨停或板块拉升本身写成炒作原因。异动稿只能描述市场响应或保存其中可独立核验的原始事件；若审计结论仍只有异动稿自我归因，应明确写“未找到可靠归因”。
