"""将全市场每日1分钟CSV导入按交易日分区的Parquet数据集。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from config.settings import ONE_MIN_CATALOG_PATH, ONE_MIN_CSV_ROOT, ONE_MIN_PARQUET_ROOT
from coreClient.csv_client import CSVClient


def main() -> int:
    parser = argparse.ArgumentParser(description="导入全市场1分钟OHLC CSV到Parquet")
    parser.add_argument("--csv", help="导入单个CSV；省略时导入--csv-root目录")
    parser.add_argument("--csv-root", default=str(ONE_MIN_CSV_ROOT))
    parser.add_argument("--output-root", default=str(ONE_MIN_PARQUET_ROOT))
    parser.add_argument("--catalog", default=str(ONE_MIN_CATALOG_PATH))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--scan-only", action="store_true", help="只刷新DuckDB目录表，不转换")
    args = parser.parse_args()
    if args.scan_only:
        result = CSVClient(args.csv_root, args.output_root, args.catalog).scan_oneMinute(
            args.start_date, args.end_date
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.csv:
        # 单文件模式仍先登记目录表，再由日期范围增量转换。
        day = Path(args.csv).name[:8]
        results = [CSVClient(Path(args.csv).parent, args.output_root, args.catalog).convert_oneMinute_csv(
            args.csv, overwrite=args.overwrite
        )]
    else:
        results = CSVClient(args.csv_root, args.output_root, args.catalog).update_oneMinute(
            args.start_date, args.end_date, overwrite=args.overwrite,
        )
    summary = {
        "output_root": args.output_root,
        "files": len(results),
        "imported": sum(item.get("action") == "imported" for item in results),
        "skipped": sum(item.get("action") == "skipped_existing" for item in results),
        "failed": sum(item.get("action") == "failed" for item in results),
        "rows": sum(int(item.get("rows", 0)) for item in results),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
