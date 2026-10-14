任务：判断两个题材名称是否为同一稳定题材、上下级关系、阶段性分支或无关关系。

输出结构：
{
  "relation": "same|parent_child|episode_branch|related_not_merge|unrelated|unknown",
  "preferred_name": "名称或unknown",
  "parent_name": "名称或unknown",
  "reason": "不超过100字",
  "evidence_ids": ["E1"],
  "confidence": 0.0,
  "needs_review": true
}

只有明确同义或官方名称变化才能输出 same。产业链相邻、共同受同一新闻刺激或股票重叠，不足以合并题材。
