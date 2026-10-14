"""在人工金标 JSONL 上评估本地题材模型；不会修改题材事实表。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from model_runner import run_task


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path(__file__).parent / "eval" / "stock_exposure_seed.jsonl")
    parser.add_argument("--provider", choices=["qwen-vllm", "minimax-hermes"], default="qwen-vllm")
    parser.add_argument("--model")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cases = [json.loads(line) for line in args.dataset.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows, exact_fields, total_fields = [], 0, 0
    for case in cases:
        started = time.perf_counter()
        try:
            result = run_task(case["task"], case["packet"], args.provider, args.model, save=False)
            output, error = result["output"], None
        except Exception as exc:
            output, error = None, str(exc)
        elapsed = round(time.perf_counter() - started, 3)
        matches = {}
        for key, expected in case["expected"].items():
            total_fields += 1
            match = output is not None and output.get(key) == expected
            exact_fields += int(match)
            matches[key] = match
        rows.append({"case_id": case["case_id"], "provider": args.provider, "elapsed_seconds": elapsed,
                     "matches": matches, "output": output, "error": error})
    report = {"provider": args.provider, "cases": len(cases), "valid_json_cases": sum(r["output"] is not None for r in rows),
              "exact_field_accuracy": exact_fields / total_fields if total_fields else None, "results": rows}
    target = args.output or (args.dataset.parent / f"result_{args.provider}.json")
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
