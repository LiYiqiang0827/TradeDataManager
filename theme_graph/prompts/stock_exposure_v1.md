任务：判断一只股票与一个题材的关系强度，分开评价市场地位和产业受益。

输出结构：
{
  "market_role": "height_core|first_mover|capacity_core|follower|emotion_only|unknown",
  "business_relation": "direct_business|subsidiary_operating|equity_investment|supplier_customer|indirect_fund|rumor_only|denied|none|unknown",
  "commercial_stage": "revenue|orders|mass_production|small_batch|sample_testing|rd_only|planning|none|unknown",
  "industrial_core": true,
  "conclusion": "不超过80字",
  "positive_evidence_ids": ["E1"],
  "negative_evidence_ids": ["E2"],
  "confidence": 0.0,
  "needs_review": true
}

判定重点：连板高度和市场辨识度只影响 market_role；industrial_core 需要主营、收入、订单、量产或明确控股业务证据。参股、基金间接投资、样品测试、传闻和公司否认不能直接判为产业核心。

枚举边界：
- `equity_investment`：公司直接持有参股公司股权，文本出现“参股公司”且没有基金中介时使用。
- `indirect_fund`：通过基金、合伙企业或资管产品间接投资；不能用于直接参股。
- `denied`：公司否认候选关系且没有其他已确认关系时使用；若同时确认了间接基金投资，关系仍填 `indirect_fund`，把否认放入负面证据。
- 商业阶段选择证据明确达到的最远阶段；“研发并已送样”填 `sample_testing`，只有尚未送样的研发才填 `rd_only`。
- `market_role` 只评价涨停市场结构。即便基本面关系很弱，3板以上的题材最高标仍可为 `height_core`，同时把 `industrial_core` 设为 false。
