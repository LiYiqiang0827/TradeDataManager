"""Build a dated research task or generate a draft with the existing model transports.

No database writes. Generated text always needs the report/evidence acceptance step.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def build_prompt(packet_path: Path, evidence_path: Path | None = None,
                 previous_reports: list[Path] | None = None) -> str:
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    day = str(packet["trade_date"])
    datetime.strptime(day, "%Y%m%d")
    if len(day) != 8 or not packet.get("limitups"):
        raise ValueError("A non-empty dated limit-up packet is required")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8")) if evidence_path else []
    if not isinstance(evidence, list):
        raise ValueError("Evidence must be an array with published_at, title, url and text")
    for item in evidence:
        published = datetime.fromisoformat(item["published_at"])
        if published.tzinfo is None:
            raise ValueError("Evidence timestamps must include a timezone")
        from zoneinfo import ZoneInfo
        if published.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d") > day:
            raise ValueError("Future evidence is prohibited: " + item.get("title", ""))
    previous = []
    for path in previous_reports or []:
        match = re.search(r"20\d{6}", path.name)
        if not match or match[0] >= day:
            raise ValueError("Previous reports must be dated before the target day")
        previous.append({"date": match[0], "report": path.read_text(encoding="utf-8")})
    template = (ROOT / "theme_graph/prompts/limit_attribution_v3_2.md").read_text(encoding="utf-8")
    return (template + "\n\n## 本次任务输入（替换模板中的日期与示例）\n"
            + f"仅分析 {day}。当日盘后报道可作盘后解释，不可倒写成封板前催化；禁止跨日信息。\n"
            + "本接口不提供网页检索。仅使用给定证据；缺少证据必须明示检索缺口，不能声称已经搜索。\n"
            + "连板可借鉴之前日报并补充当日新信息、盘面和反证。只输出完整 Markdown 日报，不输出围栏。\n"
            + "题材标题须为 ## 一、题材名；每只股票恰好一次；表头须含 股票、涨停状态、为什么涨停、个股与故事的桥梁、兑现程度/反证、信心、资料来源。\n"
            + json.dumps({"market_packet": packet, "evidence": evidence, "previous_reports": previous}, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--previous-report", type=Path, action="append", default=[])
    parser.add_argument("--prompt-out", type=Path, required=True)
    parser.add_argument("--provider", choices=["prompt", "qwen", "hermes"], default="prompt")
    parser.add_argument("--model")
    parser.add_argument("--base-url", help="Qwen/vLLM OpenAI-compatible /v1 endpoint")
    parser.add_argument("--hermes-provider", default="minimax-cn")
    parser.add_argument("--out", type=Path, help="Draft YYYYMMDD.md; never imported automatically")
    parser.add_argument("--max-tokens", type=int, default=24000)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()
    prompt = build_prompt(args.packet, args.evidence, args.previous_report)
    args.prompt_out.parent.mkdir(parents=True, exist_ok=True)
    args.prompt_out.write_text(prompt, encoding="utf-8")
    if args.provider == "prompt":
        print(json.dumps({"prompt": str(args.prompt_out), "model_called": False}))
        return 0
    if not args.out or args.out.exists():
        parser.error("Generation requires --out pointing to a new draft file")
    day = str(json.loads(args.packet.read_text(encoding="utf-8"))["trade_date"])
    if args.out.name != day + ".md":
        parser.error("Draft filename must be the packet's YYYYMMDD.md")
    from theme_graph.model_runner import call_qwen, call_hermes
    if args.provider == "qwen":
        text, model = call_qwen(prompt, args.base_url, args.model, args.timeout, args.max_tokens)
    else:
        text, model = call_hermes(prompt, args.hermes_provider, args.model or "MiniMax-M3", args.timeout)
    text = re.sub(r"\A```(?:markdown|md)?\s*|\s*```\Z", "", text.strip())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text + "\n", encoding="utf-8")
    print(json.dumps({"draft": str(args.out), "model": model, "needs_review": True}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
