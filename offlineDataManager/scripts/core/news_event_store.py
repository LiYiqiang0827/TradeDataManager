"""Major News 清洗、事件聚类与 CCTV 主键修复。

原始 SQLite 新闻库保持不变；Major News 的清洗结果写入独立 DuckDB。这样过滤
规则和聚类版本可以重建，也不会因清洗删除原始证据。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import html
import json
from pathlib import Path
import re
import sqlite3
import unicodedata
from typing import Any, Iterable, Optional

import duckdb
import pandas as pd

try:  # service 以 scripts 为 PYTHONPATH；统一接口则从项目根包导入。
    from config.settings import DB_PATH_BASIC, DB_PATH_NEWS, NEWS_EVENT_DB_PATH
except ModuleNotFoundError:  # pragma: no cover - 由不同入口决定
    from offlineDataManager.scripts.config.settings import (
        DB_PATH_BASIC, DB_PATH_NEWS, NEWS_EVENT_DB_PATH,
    )


PROCESSOR_VERSION = "major-news-clean-v1"
NEWS_LLM_PROMPT_VERSION = "news-refinement-v2"
DEFAULT_WINDOW_HOURS = 36
DEFAULT_SOURCE_TABLE = "tbl_major_news"
ALLOWED_SOURCE_TABLES = {"tbl_major_news", "tbl_news"}

MOVE_RE = re.compile(
    r"(?:快速|直线|大幅|突然|集体|震荡)?(?:异动|拉升|上涨|上扬|走强|走弱|活跃|冲高|翻红|高开|"
    r"涨超|大涨|飙升|封板|涨停|触及涨停|打开涨停|炸板|跳水|下挫|下跌|走低|低开|"
    r"跌超|大跌|跌停|触及跌停|跌幅扩大|涨幅扩大|回落|回升|回暖|爆发|调整|承压|分化)"
)
DOMESTIC_RE = re.compile(
    r"(?:A股|沪指|上证(?:指数|综指)?|深证成指|深成指|创业板指|科创50|北证50|"
    r"上证50|沪深300|中证\d+|两市|个股|股票|板块|概念|题材)"
)
SECTOR_RE = re.compile(r"(?:板块|概念|题材).{0,10}" + MOVE_RE.pattern)
PROTECTED_FACT_RE = re.compile(
    r"(?:美股|纳指|纳斯达克|标普|道指|日经|韩国综合|恒生|港股|欧洲股市|德国DAX|"
    r"法国CAC|富时|美元指数|人民币|汇率|国债|债券|比特币|原油|油价|黄金|白银|"
    r"铜价|铝价|锌价|镍价|锂价|粮价|农产品价格|玉米|小麦|大豆|铁矿|焦煤|焦炭|"
    r"螺纹钢|LME|COMEX|WTI|布伦特|期货)"
)
INDEPENDENT_FACT_RE = re.compile(
    r"(?:发布|印发|公告|签署|中标|获批|批准|立项|发射|首飞|投产|停产|减产|增产|"
    r"限产|复产|收购|并购|重组|上调|下调|提价|涨价|降价|突破|量产|开工|订单|"
    r"政策|会议决定|正式启动|推出|达成|关税|制裁|出口|进口|库存|产量|销量|"
    r"营收|净利润|同比|环比|施行|生效|数据(?:显示|公布)|回应|澄清|调查|起诉|回购|增持|减持)"
)
MARKET_METRIC_RE = re.compile(
    r"(?:(?:A股|沪深两市|两市|ETF两市).{0,15}(?:成交额|成交金额)|"
    r"(?:A股|沪深|两市|股市|市场).{0,8}(?:午评|收评|盘中快评|早盘快评|午盘)|"
    r"(?:连板股(?:追踪|分析)|涨停复盘|短线情绪))"
)
AI_RECAP_RE = re.compile(r"(?:概念联动.*(?:连板|涨停).*背后逻辑揭晓|金融界App\s*AI线索挖掘)")
LEADING_LABEL_RE = re.compile(r"^(?:【[^】]{1,60}】)+")
HTML_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^0-9a-zA-Z\u4e00-\u9fff%]+")
PERCENT_RE = re.compile(r"\d+(?:\.\d+)?%")
MEASURE_RE = re.compile(r"\d+(?:\.\d+)?(?:美元|元|亿元|万吨|万桶|吨|点|基点|倍)")

GENERIC_BIGRAMS = {
    "快讯", "消息", "最新", "今日", "昨日", "记者", "报道", "公司", "市场",
    "板块", "概念", "午后", "早盘", "盘中", "截至", "目前", "表示", "相关",
}


def _text(value: Any) -> str:
    if value is None:
        return ""
    value = html.unescape(str(value))
    value = HTML_RE.sub(" ", value)
    return SPACE_RE.sub(" ", unicodedata.normalize("NFKC", value)).strip()


def _compact(value: str) -> str:
    return PUNCT_RE.sub("", _text(value).lower())


def _headline(title: str, content: str) -> str:
    title, content = _text(title), _text(content)
    if title:
        return title[:160]
    match = re.match(r"^【([^】]{2,100})】", content)
    if match:
        return match.group(1)
    return re.split(r"[。！？；;]", content, maxsplit=1)[0][:120]


def _exact_hash(title: str, content: str) -> str:
    body = _compact(content or title)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _item_id(src: str, published_at: str, source_md5: str, exact_hash: str) -> str:
    raw = f"{src}\n{published_at}\n{source_md5 or exact_hash}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def stable_cctv_md5(trade_date: str, title: Any, content: Any) -> str:
    raw = f"{trade_date}\n{_text(title)}\n{_text(content)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _iso_date(value: str) -> str:
    digits = re.sub(r"\D", "", str(value))[:8]
    if len(digits) != 8:
        raise ValueError(f"日期必须是 YYYYMMDD 或 YYYY-MM-DD: {value}")
    return datetime.strptime(digits, "%Y%m%d").date().isoformat()


class StockNameMatcher:
    """只在行情动词前的短主语中匹配股票名，避免对每条新闻扫描五千只股票。"""

    def __init__(self, names: Iterable[str] = ()):
        self.names = {_text(name) for name in names if 2 <= len(_text(name)) <= 12}
        self.lengths = sorted({len(name) for name in self.names}, reverse=True)

    @classmethod
    def from_database(cls, path: Path = DB_PATH_BASIC) -> "StockNameMatcher":
        if not Path(path).exists():
            return cls()
        conn = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
        try:
            names = [row[0] for row in conn.execute(
                "SELECT name FROM tbl_cn_basic WHERE name IS NOT NULL AND trim(name)<>''"
            )]
        finally:
            conn.close()
        return cls(names)

    def subject_is_stock(self, headline: str) -> bool:
        for match in MOVE_RE.finditer(headline):
            prefix = re.sub(r"[【】\[\]（）()，,：:、]", "", headline[max(0, match.start() - 16):match.start()])
            for length in self.lengths:
                if len(prefix) >= length and prefix[-length:] in self.names:
                    return True
        return False


def classify_news(title: Any, content: Any, stock_matcher: Optional[StockNameMatcher] = None) -> dict[str, Any]:
    """识别纯A股行情结果稿，同时保留独立事实、商品和海外市场价格事实。"""
    title_text, content_text = _text(title), _text(content)
    headline = _headline(title_text, content_text)
    combined = f"{headline} {content_text[:500]}"
    has_fact = bool(INDEPENDENT_FACT_RE.search(combined))
    has_move = bool(MOVE_RE.search(headline))
    domestic = bool(DOMESTIC_RE.search(headline))
    sector_move = bool(SECTOR_RE.search(headline))
    stock_move = bool(stock_matcher and stock_matcher.subject_is_stock(headline))
    # 没有股票清单时仍识别“XX股份/科技/集团快速拉升”这类明确句式。
    generic_stock_move = bool(re.search(
        r"[\u4e00-\u9fffA-Za-z0-9*]{2,12}(?:股份|科技|集团|证券|银行|药业|能源)"
        r"(?:快速拉升|直线拉升|触及涨停|封板|快速下跌|跳水|触及跌停|跌停)",
        headline,
    ))
    market_metric = bool(MARKET_METRIC_RE.search(headline))
    ai_recap = bool(AI_RECAP_RE.search(combined))
    recap = (
        (has_move and (domestic or sector_move or stock_move or generic_stock_move))
        or market_metric
        or ai_recap
    )
    move_match = MOVE_RE.search(headline)
    protected_match = PROTECTED_FACT_RE.search(headline)
    protected_is_primary = bool(
        protected_match and (not move_match or protected_match.start() <= move_match.start())
    )
    # 商品和海外市场事实按标题主语保护，例如“国债期货午盘上涨”“美股收评”；
    # A股成交额/盘面复盘与 AI 涨停模板仍直接剔除。
    if protected_is_primary:
        return {
            "filter_class": "protected_market_fact", "excluded": False,
            "reason": "commodity_or_overseas_market_fact", "has_independent_fact": has_fact,
        }
    if market_metric or ai_recap:
        return {
            "filter_class": "a_share_market_result", "excluded": True,
            "reason": "a_share_market_result", "has_independent_fact": has_fact,
        }
    # 只有标题先陈述独立事实、随后顺带提到股价时才保留；“板块拉升，消息面上…”
    # 以及金融界 AI 涨停归因模板仍属于结果稿，底层事实应由独立报道进入事件库。
    fact_match = INDEPENDENT_FACT_RE.search(headline)
    fact_is_primary = bool(fact_match and (not move_match or fact_match.start() < move_match.start()))
    if recap and has_fact and fact_is_primary and not market_metric and not ai_recap:
        return {
            "filter_class": "fact_with_market_recap", "excluded": False,
            "reason": "independent_fact_preserved", "has_independent_fact": True,
        }
    if recap:
        return {
            "filter_class": "a_share_market_result", "excluded": True,
            "reason": "a_share_market_result", "has_independent_fact": has_fact,
        }
    return {
        "filter_class": "factual_news", "excluded": False,
        "reason": "", "has_independent_fact": has_fact,
    }


def _grams(value: str) -> set[str]:
    value = _compact(value)
    if len(value) < 2:
        return {value} if value else set()
    grams = {value[i:i + 2] for i in range(len(value) - 1)}
    return {gram for gram in grams if gram not in GENERIC_BIGRAMS}


def _number_signature(value: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return tuple(sorted(set(PERCENT_RE.findall(value)))), tuple(sorted(set(MEASURE_RE.findall(value))))


def _near_duplicate(left: str, right: str) -> bool:
    if not left or not right:
        return False
    lnum, rnum = _number_signature(left), _number_signature(right)
    # 价格、涨跌幅不同的快讯保留为不同事件，避免压掉用户关心的数值更新。
    if lnum[0] and rnum[0] and lnum[0] != rnum[0]:
        return False
    if lnum[1] and rnum[1] and lnum[1] != rnum[1]:
        return False
    lg, rg = _grams(left), _grams(right)
    if not lg or not rg:
        return False
    common = len(lg & rg)
    jaccard = common / len(lg | rg)
    containment = common / min(len(lg), len(rg))
    return jaccard >= 0.56 or (containment >= 0.78 and jaccard >= 0.40)


@dataclass
class EventState:
    event_id: str
    event_date: str
    first_at: datetime
    last_at: datetime
    representative_item_id: str
    representative_src: str
    title: str
    content: str
    headline: str
    filter_class: str
    excluded: bool
    exclusion_reason: str
    has_independent_fact: bool
    item_count: int = 0
    sources: set[str] = field(default_factory=set)
    exact_hashes: set[str] = field(default_factory=set)
    exact_member_count: int = 0
    near_member_count: int = 0
    representative_score: float = 0.0


class NewsEventStore:
    def __init__(
        self,
        database: Path = NEWS_EVENT_DB_PATH,
        source: Path = DB_PATH_NEWS,
        basic: Path = DB_PATH_BASIC,
        window_hours: int = DEFAULT_WINDOW_HOURS,
        source_table: str = DEFAULT_SOURCE_TABLE,
    ):
        self.database = Path(database)
        self.source = Path(source)
        self.basic = Path(basic)
        self.window = timedelta(hours=window_hours)
        if source_table not in ALLOWED_SOURCE_TABLES:
            raise ValueError(f"不支持的新闻源表: {source_table}")
        self.source_table = source_table
        self.source_kind = "major_news" if source_table == "tbl_major_news" else "news"
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self, read_only: bool = False):
        return duckdb.connect(str(self.database), read_only=read_only)

    def _ensure_schema(self) -> None:
        conn = self._connect()
        try:
            self._create_schema(conn)
        finally:
            conn.close()

    @staticmethod
    def _create_schema(conn) -> None:
        conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_state(
                  key VARCHAR PRIMARY KEY, value VARCHAR, updated_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS fact_news_event(
                  event_id VARCHAR PRIMARY KEY,
                  event_date VARCHAR NOT NULL,
                  first_published_at TIMESTAMP NOT NULL,
                  last_published_at TIMESTAMP NOT NULL,
                  representative_item_id VARCHAR NOT NULL,
                  representative_src VARCHAR,
                  title VARCHAR,
                  content VARCHAR,
                  normalized_headline VARCHAR,
                  filter_class VARCHAR,
                  excluded BOOLEAN,
                  exclusion_reason VARCHAR,
                  has_independent_fact BOOLEAN,
                  item_count INTEGER,
                  source_count INTEGER,
                  sources_json VARCHAR,
                  exact_member_count INTEGER,
                  near_member_count INTEGER,
                  representative_score DOUBLE,
                  processor_version VARCHAR,
                  updated_at TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_news_event_date ON fact_news_event(event_date);
                CREATE INDEX IF NOT EXISTS idx_news_event_last ON fact_news_event(last_published_at);
                CREATE TABLE IF NOT EXISTS rel_news_event_member(
                  item_id VARCHAR PRIMARY KEY,
                  event_id VARCHAR NOT NULL,
                  published_at TIMESTAMP NOT NULL,
                  event_date VARCHAR NOT NULL,
                  src VARCHAR,
                  source_md5 VARCHAR,
                  title VARCHAR,
                  content VARCHAR,
                  normalized_headline VARCHAR,
                  exact_hash VARCHAR,
                  duplicate_kind VARCHAR,
                  filter_class VARCHAR,
                  excluded BOOLEAN,
                  exclusion_reason VARCHAR,
                  has_independent_fact BOOLEAN,
                  representative_score DOUBLE,
                  snap_ts VARCHAR,
                  processor_version VARCHAR,
                  processed_at TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_news_member_date ON rel_news_event_member(event_date);
                CREATE INDEX IF NOT EXISTS idx_news_member_event ON rel_news_event_member(event_id);
                CREATE INDEX IF NOT EXISTS idx_news_member_time ON rel_news_event_member(published_at);
                CREATE TABLE IF NOT EXISTS fact_news_insight(
                  event_id VARCHAR PRIMARY KEY,
                  event_date VARCHAR NOT NULL,
                  llm_keep BOOLEAN NOT NULL,
                  importance_score INTEGER,
                  category VARCHAR,
                  summary VARCHAR,
                  key_facts_json VARCHAR,
                  themes_json VARCHAR,
                  entities_json VARCHAR,
                  horizon VARCHAR,
                  novelty VARCHAR,
                  reason VARCHAR,
                  provider VARCHAR,
                  model VARCHAR,
                  prompt_version VARCHAR,
                  input_hash VARCHAR,
                  processed_at TIMESTAMP,
                  run_id VARCHAR
                );
                CREATE INDEX IF NOT EXISTS idx_news_insight_date ON fact_news_insight(event_date);
                CREATE TABLE IF NOT EXISTS news_llm_run(
                  run_id VARCHAR PRIMARY KEY,
                  start_date VARCHAR,
                  end_date VARCHAR,
                  provider VARCHAR,
                  prompt_version VARCHAR,
                  candidate_events INTEGER,
                  processed_events INTEGER,
                  kept_events INTEGER,
                  batch_count INTEGER,
                  status VARCHAR,
                  error VARCHAR,
                  started_at TIMESTAMP,
                  finished_at TIMESTAMP
                );
            """)
        # 旧的派生库向前兼容；DuckDB支持IF NOT EXISTS，不影响新建库。
        conn.execute("ALTER TABLE fact_news_insight ADD COLUMN IF NOT EXISTS run_id VARCHAR")

    @staticmethod
    def _representative_score(src: str, title: str, content: str) -> float:
        source_bonus = {
            "cls": 18, "yicai": 16, "eastmoney": 12, "10jqka": 10,
            "wallstreetcn": 10, "sina": 8, "jinrongjie": 6,
        }.get(src, 0)
        return min(len(_text(content)), 1200) + (80 if _text(title) else 0) + source_bonus

    @staticmethod
    def _event_id(item_id: str, event_date: str) -> str:
        return f"NE-{event_date.replace('-', '')}-{item_id[:20]}"

    def _clear(self, conn) -> None:
        # DuckDB 在包含大主键 ART index 的表上逐行 DELETE 成本高，异常中断后
        # 也可能触发 index delete 错误。派生库完全可重建，重建时直接重建表。
        conn.execute("DROP TABLE IF EXISTS rel_news_event_member")
        conn.execute("DROP TABLE IF EXISTS fact_news_event")
        conn.execute("DROP TABLE IF EXISTS fact_news_insight")
        conn.execute("DROP TABLE IF EXISTS news_llm_run")
        conn.execute("DROP TABLE IF EXISTS pipeline_state")
        for table in (
            "rel_major_news_theme", "rel_major_news_entity", "fact_major_news_analysis",
            "rel_major_news_semantic_member", "fact_major_news_semantic_event",
            "fact_major_news_daily_analysis", "dim_major_news_theme_tag", "dim_major_news_entity",
        ):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        self._create_schema(conn)

    def _load_active(self, conn, cutoff: datetime) -> dict[str, EventState]:
        rows = conn.execute(
            "SELECT * FROM fact_news_event WHERE last_published_at>=?", [cutoff]
        ).fetchdf()
        states: dict[str, EventState] = {}
        for row in rows.to_dict("records"):
            states[row["event_id"]] = EventState(
                event_id=row["event_id"], event_date=row["event_date"],
                first_at=pd.Timestamp(row["first_published_at"]).to_pydatetime(),
                last_at=pd.Timestamp(row["last_published_at"]).to_pydatetime(),
                representative_item_id=row["representative_item_id"],
                representative_src=row.get("representative_src") or "",
                title=row.get("title") or "", content=row.get("content") or "",
                headline=row.get("normalized_headline") or "",
                filter_class=row.get("filter_class") or "factual_news",
                excluded=bool(row.get("excluded")), exclusion_reason=row.get("exclusion_reason") or "",
                has_independent_fact=bool(row.get("has_independent_fact")),
                item_count=int(row.get("item_count") or 0),
                sources=set(json.loads(row.get("sources_json") or "[]")),
                exact_member_count=int(row.get("exact_member_count") or 0),
                near_member_count=int(row.get("near_member_count") or 0),
                representative_score=float(row.get("representative_score") or 0),
            )
        if states:
            ids = list(states)
            conn.register("_active_ids", pd.DataFrame({"event_id": ids}))
            hashes = conn.execute(
                """SELECT m.event_id,m.exact_hash FROM rel_news_event_member m
                   JOIN _active_ids a USING(event_id) GROUP BY m.event_id,m.exact_hash"""
            ).fetchall()
            conn.unregister("_active_ids")
            for event_id, exact_hash in hashes:
                if exact_hash:
                    states[event_id].exact_hashes.add(exact_hash)
        return states

    @staticmethod
    def _indices(states: dict[str, EventState]):
        exact: dict[str, list[str]] = defaultdict(list)
        token: dict[str, set[str]] = defaultdict(set)
        for event_id, state in states.items():
            for value in state.exact_hashes:
                exact[value].append(event_id)
            for gram in _grams(state.headline):
                token[gram].add(event_id)
        return exact, token

    def _match_event(
        self, states: dict[str, EventState], exact_index: dict[str, list[str]],
        token_index: dict[str, set[str]], exact_hash: str, headline: str,
        published_at: datetime, src: str, excluded: bool,
    ) -> tuple[Optional[EventState], str]:
        for event_id in reversed(exact_index.get(exact_hash, [])):
            state = states.get(event_id)
            if state and state.excluded == excluded and published_at - state.last_at <= self.window:
                kind = "same_source_exact" if src in state.sources else "cross_source_exact"
                return state, kind
        grams = _grams(headline)
        candidate_ids: set[str] = set()
        for gram in sorted(grams, key=lambda value: len(token_index.get(value, ())))[:8]:
            candidate_ids.update(token_index.get(gram, ()))
        candidates = sorted(
            (states[event_id] for event_id in candidate_ids if event_id in states),
            key=lambda state: state.last_at, reverse=True,
        )
        for state in candidates:
            if state.excluded != excluded or published_at - state.last_at > self.window:
                continue
            if _near_duplicate(headline, state.headline):
                return state, "near_duplicate"
        return None, "representative"

    def build(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        rebuild: bool = False,
    ) -> dict[str, Any]:
        """处理 `[start_date, end_date]` 自然日；无起点时按断点回看一天增量运行。"""
        raw = sqlite3.connect(f"file:{self.source.resolve()}?mode=ro", uri=True)
        raw.row_factory = sqlite3.Row
        conn = self._connect()
        matcher = StockNameMatcher.from_database(self.basic)
        try:
            if rebuild:
                self._clear(conn)
            raw_min, raw_max = raw.execute(
                f"SELECT MIN(substr(datetime,1,10)),MAX(substr(datetime,1,10)) FROM {self.source_table}"
            ).fetchone()
            if not raw_min or not raw_max:
                return {"status": "empty", "source_kind": self.source_kind}
            checkpoint_key = f"{self.source_kind}_max_datetime"
            checkpoint = conn.execute(
                "SELECT value FROM pipeline_state WHERE key=?", [checkpoint_key]
            ).fetchone()
            stored_source = conn.execute(
                "SELECT value FROM pipeline_state WHERE key='source_kind'"
            ).fetchone()
            stored_version = conn.execute(
                "SELECT value FROM pipeline_state WHERE key='processor_version'"
            ).fetchone()
            if not rebuild and stored_source and stored_source[0] != self.source_kind:
                raise RuntimeError(
                    f"派生库的数据源是 {stored_source[0]}，当前请求是 {self.source_kind}，"
                    "请使用独立数据库或 --rebuild"
                )
            if not rebuild and stored_version and stored_version[0] != PROCESSOR_VERSION:
                raise RuntimeError(
                    f"新闻清洗规则已由 {stored_version[0]} 升级为 {PROCESSOR_VERSION}，"
                    "请指定日期范围并使用 --rebuild 重建派生库"
                )
            if start_date:
                start = datetime.strptime(start_date.replace("-", "")[:8], "%Y%m%d")
            elif checkpoint and checkpoint[0]:
                start = datetime.fromisoformat(str(checkpoint[0])[:19]) - timedelta(days=1)
                start = datetime.combine(start.date(), datetime.min.time())
            else:
                start = datetime.strptime(raw_min, "%Y-%m-%d")
            end = datetime.strptime((end_date or raw_max).replace("-", "")[:8], "%Y%m%d")
            if end < start:
                return {"status": "up_to_date", "start_date": start.date().isoformat(), "end_date": end.date().isoformat()}

            states = self._load_active(conn, start - self.window) if not rebuild else {}
            totals = defaultdict(int, {
                "raw_rows": 0, "processed_items": 0, "skipped_existing": 0,
                "events_created": 0, "same_source_exact": 0,
                "cross_source_exact": 0, "near_duplicate": 0,
                "excluded_items": 0, "days_processed": 0,
            })
            current = start
            latest_raw_datetime = None
            while current <= end:
                day = current.date().isoformat()
                next_day = (current + timedelta(days=1)).date().isoformat()
                # 只保留窗口内事件，防止索引随全历史增长。
                cutoff = current - self.window
                states = {key: value for key, value in states.items() if value.last_at >= cutoff}
                exact_index, token_index = self._indices(states)
                existing = set()
                if not rebuild:
                    existing = {row[0] for row in conn.execute(
                        "SELECT item_id FROM rel_news_event_member WHERE event_date=?", [day]
                    ).fetchall()}
                rows = raw.execute(
                    f"""SELECT datetime,src,title,content,md5,snap_ts FROM {self.source_table}
                       WHERE datetime>=? AND datetime<? ORDER BY datetime,src,md5""",
                    (f"{day} 00:00:00", f"{next_day} 00:00:00"),
                ).fetchall()
                if rows:
                    day_latest = str(rows[-1]["datetime"])
                    if latest_raw_datetime is None or day_latest > latest_raw_datetime:
                        latest_raw_datetime = day_latest
                totals["raw_rows"] += len(rows)
                members: list[dict[str, Any]] = []
                changed: dict[str, EventState] = {}
                now = datetime.now()
                for row in rows:
                    published_text = str(row["datetime"])
                    published = datetime.fromisoformat(published_text)
                    title, content = _text(row["title"]), _text(row["content"])
                    headline = _headline(title, content)
                    normalized_headline = _compact(headline)
                    exact_hash = _exact_hash(title, content)
                    item_id = _item_id(str(row["src"]), published_text, str(row["md5"] or ""), exact_hash)
                    if item_id in existing:
                        totals["skipped_existing"] += 1
                        continue
                    classification = classify_news(title, content, matcher)
                    state, duplicate_kind = self._match_event(
                        states, exact_index, token_index, exact_hash, normalized_headline,
                        published, str(row["src"]), bool(classification["excluded"]),
                    )
                    score = self._representative_score(str(row["src"]), title, content)
                    if state is None:
                        event_id = self._event_id(item_id, day)
                        state = EventState(
                            event_id=event_id, event_date=day, first_at=published, last_at=published,
                            representative_item_id=item_id, representative_src=str(row["src"]),
                            title=title, content=content, headline=normalized_headline,
                            filter_class=classification["filter_class"],
                            excluded=bool(classification["excluded"]),
                            exclusion_reason=classification["reason"],
                            has_independent_fact=bool(classification["has_independent_fact"]),
                            representative_score=score,
                        )
                        states[event_id] = state
                        for gram in _grams(normalized_headline):
                            token_index[gram].add(event_id)
                        totals["events_created"] += 1
                    state.first_at = min(state.first_at, published)
                    state.last_at = max(state.last_at, published)
                    state.item_count += 1
                    state.sources.add(str(row["src"]))
                    state.exact_hashes.add(exact_hash)
                    state.has_independent_fact = state.has_independent_fact or bool(classification["has_independent_fact"])
                    if duplicate_kind in {"same_source_exact", "cross_source_exact"}:
                        state.exact_member_count += 1
                        totals[duplicate_kind] += 1
                    elif duplicate_kind == "near_duplicate":
                        state.near_member_count += 1
                        totals["near_duplicate"] += 1
                    if score > state.representative_score:
                        state.representative_item_id = item_id
                        state.representative_src = str(row["src"])
                        state.title, state.content = title, content
                        state.headline = normalized_headline
                        state.filter_class = classification["filter_class"]
                        state.exclusion_reason = classification["reason"]
                        state.representative_score = score
                        for gram in _grams(normalized_headline):
                            token_index[gram].add(state.event_id)
                    exact_index.setdefault(exact_hash, []).append(state.event_id)
                    changed[state.event_id] = state
                    members.append({
                        "item_id": item_id, "event_id": state.event_id,
                        "published_at": published, "event_date": day, "src": str(row["src"]),
                        "source_md5": str(row["md5"] or ""), "title": title, "content": content,
                        "normalized_headline": normalized_headline, "exact_hash": exact_hash,
                        "duplicate_kind": duplicate_kind,
                        "filter_class": classification["filter_class"],
                        "excluded": bool(classification["excluded"]),
                        "exclusion_reason": classification["reason"],
                        "has_independent_fact": bool(classification["has_independent_fact"]),
                        "representative_score": score, "snap_ts": str(row["snap_ts"] or ""),
                        "processor_version": PROCESSOR_VERSION, "processed_at": now,
                    })
                    totals["processed_items"] += 1
                    if classification["excluded"]:
                        totals["excluded_items"] += 1
                    if latest_raw_datetime is None or published_text > latest_raw_datetime:
                        latest_raw_datetime = published_text

                conn.execute("BEGIN")
                try:
                    if members:
                        member_df = pd.DataFrame(members)
                        conn.register("_member_batch", member_df)
                        conn.execute("INSERT OR IGNORE INTO rel_news_event_member SELECT * FROM _member_batch")
                        conn.unregister("_member_batch")
                    if changed:
                        event_rows = []
                        for state in changed.values():
                            event_rows.append({
                                "event_id": state.event_id, "event_date": state.event_date,
                                "first_published_at": state.first_at, "last_published_at": state.last_at,
                                "representative_item_id": state.representative_item_id,
                                "representative_src": state.representative_src,
                                "title": state.title, "content": state.content,
                                "normalized_headline": state.headline, "filter_class": state.filter_class,
                                "excluded": state.excluded, "exclusion_reason": state.exclusion_reason,
                                "has_independent_fact": state.has_independent_fact,
                                "item_count": state.item_count, "source_count": len(state.sources),
                                "sources_json": json.dumps(sorted(state.sources), ensure_ascii=False),
                                "exact_member_count": state.exact_member_count,
                                "near_member_count": state.near_member_count,
                                "representative_score": state.representative_score,
                                "processor_version": PROCESSOR_VERSION, "updated_at": now,
                            })
                        event_df = pd.DataFrame(event_rows)
                        conn.register("_event_batch", event_df)
                        conn.execute("INSERT OR REPLACE INTO fact_news_event SELECT * FROM _event_batch")
                        conn.unregister("_event_batch")
                    checkpoint_value = latest_raw_datetime or f"{day} 23:59:59"
                    # 指定历史日期回填时不能把日常增量断点倒写到过去，
                    # 否则下一次定时更新会重新扫描几个月的数据。
                    if checkpoint and checkpoint[0]:
                        checkpoint_value = max(checkpoint_value, str(checkpoint[0]))
                    conn.execute(
                        "INSERT OR REPLACE INTO pipeline_state VALUES (?, ?, CURRENT_TIMESTAMP)",
                        [checkpoint_key, checkpoint_value],
                    )
                    conn.execute(
                        """INSERT OR REPLACE INTO pipeline_state VALUES
                           ('source_kind', ?, CURRENT_TIMESTAMP)""", [self.source_kind]
                    )
                    conn.execute(
                        """INSERT OR REPLACE INTO pipeline_state VALUES
                           ('processor_version', ?, CURRENT_TIMESTAMP)""", [PROCESSOR_VERSION]
                    )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                totals["days_processed"] += 1
                current += timedelta(days=1)

            totals["start_date"] = start.date().isoformat()
            totals["end_date"] = end.date().isoformat()
            totals["processor_version"] = PROCESSOR_VERSION
            totals["source_kind"] = self.source_kind
            totals["source_table"] = self.source_table
            totals["status"] = "ok"
            return dict(totals)
        finally:
            raw.close()
            conn.close()

    def query_events(
        self,
        start_date: str,
        end_date: str,
        include_excluded: bool = False,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        conn = self._connect(read_only=True)
        try:
            where = "event_date BETWEEN ? AND ?"
            params: list[Any] = [_iso_date(start_date), _iso_date(end_date)]
            if not include_excluded:
                where += " AND NOT excluded"
            sql = f"SELECT * FROM fact_news_event WHERE {where} ORDER BY first_published_at,event_id"
            if limit is not None:
                sql += " LIMIT ?"
                params.append(int(limit))
            return conn.execute(sql, params).fetchdf()
        finally:
            conn.close()

    def query_daily_stats(self, start_date: str, end_date: str) -> pd.DataFrame:
        conn = self._connect(read_only=True)
        try:
            return conn.execute(
                """WITH members AS (
                       SELECT event_date,COUNT(*) AS raw_items,
                              SUM(CASE WHEN excluded THEN 1 ELSE 0 END) AS excluded_items
                       FROM rel_news_event_member WHERE event_date BETWEEN ? AND ? GROUP BY event_date
                   ), events AS (
                       SELECT event_date,COUNT(*) AS event_count,
                              SUM(CASE WHEN NOT excluded THEN 1 ELSE 0 END) AS effective_events,
                              SUM(CASE WHEN excluded THEN 1 ELSE 0 END) AS excluded_events,
                              SUM(item_count-1) AS duplicate_items
                       FROM fact_news_event WHERE event_date BETWEEN ? AND ? GROUP BY event_date
                   )
                   SELECT m.event_date,m.raw_items,m.excluded_items,e.event_count,e.effective_events,
                          e.excluded_events,e.duplicate_items,
                          ROUND(1-e.effective_events::DOUBLE/NULLIF(m.raw_items,0),4) AS total_reduction_rate
                   FROM members m JOIN events e USING(event_date) ORDER BY m.event_date""",
                [_iso_date(start_date), _iso_date(end_date), _iso_date(start_date), _iso_date(end_date)],
            ).fetchdf()
        finally:
            conn.close()

    def query_insights(
        self,
        start_date: str,
        end_date: str,
        *,
        min_importance: int = 0,
        include_dropped: bool = False,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        conn = self._connect(read_only=True)
        try:
            where = "i.event_date BETWEEN ? AND ? AND i.importance_score>=?"
            params: list[Any] = [_iso_date(start_date), _iso_date(end_date), int(min_importance)]
            if not include_dropped:
                where += " AND i.llm_keep"
            sql = f"""SELECT e.first_published_at,e.representative_src,e.title,e.content,
                              e.item_count,e.source_count,i.* EXCLUDE(event_id,event_date)
                       FROM fact_news_insight i JOIN fact_news_event e USING(event_id)
                       WHERE {where}
                       ORDER BY i.importance_score DESC,e.first_published_at,i.event_id"""
            if limit is not None:
                sql += " LIMIT ?"
                params.append(int(limit))
            return conn.execute(sql, params).fetchdf()
        finally:
            conn.close()

    def query_llm_daily_stats(self, start_date: str, end_date: str) -> pd.DataFrame:
        conn = self._connect(read_only=True)
        try:
            return conn.execute(
                """SELECT e.event_date,
                          COUNT(*) FILTER (WHERE NOT e.excluded) AS deterministic_events,
                          COUNT(i.event_id) AS llm_processed_events,
                          COUNT(*) FILTER (WHERE i.llm_keep) AS llm_effective_events,
                          COUNT(*) FILTER (WHERE i.llm_keep AND i.importance_score>=45) AS material_events,
                          COUNT(*) FILTER (WHERE i.llm_keep AND i.importance_score>=60) AS high_importance_events,
                          ROUND(COUNT(*) FILTER (WHERE i.llm_keep)::DOUBLE /
                                NULLIF(COUNT(i.event_id),0),4) AS llm_keep_rate
                   FROM fact_news_event e
                   LEFT JOIN fact_news_insight i
                     ON e.event_id=i.event_id AND i.prompt_version=?
                   WHERE e.event_date BETWEEN ? AND ?
                   GROUP BY e.event_date ORDER BY e.event_date""",
                [NEWS_LLM_PROMPT_VERSION, _iso_date(start_date), _iso_date(end_date)],
            ).fetchdf()
        finally:
            conn.close()


def repair_cctv_news(database: Path = DB_PATH_NEWS) -> dict[str, int]:
    """按日期、标题去重 CCTV，保留正文更完整版本并补稳定 md5。"""
    conn = sqlite3.connect(str(database))
    conn.row_factory = sqlite3.Row
    try:
        total, invalid = conn.execute(
            """SELECT COUNT(*),
                      SUM(CASE WHEN md5 IS NULL OR trim(md5)='' OR src IS NULL OR trim(src)='' THEN 1 ELSE 0 END)
               FROM tbl_cctv_news"""
        ).fetchone()
        duplicate = conn.execute(
            """SELECT 1 FROM tbl_cctv_news
               GROUP BY datetime,title HAVING COUNT(*)>1 LIMIT 1"""
        ).fetchone()
        # 修复后的日常增量无需重写整张 CCTV 表；只在发现旧主键或重复时治理。
        if not invalid and duplicate is None:
            return {"before": int(total), "after": int(total), "removed": 0}
        rows = conn.execute(
            "SELECT datetime,title,content,src,snap_ts FROM tbl_cctv_news ORDER BY datetime,rowid"
        ).fetchall()
        unique: dict[tuple[str, str], dict[str, str]] = {}
        for row in rows:
            date_value, title_value, content_value = str(row["datetime"]), _text(row["title"]), _text(row["content"])
            key = (date_value, title_value)
            candidate = {
                "datetime": date_value, "title": title_value, "content": content_value,
                "src": _text(row["src"]) or "cctv",
                "md5": stable_cctv_md5(date_value, title_value, content_value),
                "snap_ts": _text(row["snap_ts"]),
            }
            current = unique.get(key)
            if current is None or len(candidate["content"]) > len(current["content"]):
                unique[key] = candidate
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM tbl_cctv_news")
        conn.executemany(
            """INSERT INTO tbl_cctv_news(datetime,title,content,src,md5,snap_ts)
               VALUES(:datetime,:title,:content,:src,:md5,:snap_ts)""",
            list(unique.values()),
        )
        conn.commit()
        return {"before": len(rows), "after": len(unique), "removed": len(rows) - len(unique)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
