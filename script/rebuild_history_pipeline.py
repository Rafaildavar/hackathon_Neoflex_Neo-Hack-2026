from __future__ import annotations

import argparse
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Full historical rebuild: reset -> load raw -> transform -> refresh aggregates."
    )
    parser.add_argument(
        "--tickers",
        default="SBER,GAZP,LKOH,YDEX,VTBR,ROSN,NVTK,TATN,GMKN,NLMK",
        help="Comma-separated MOEX tickers.",
    )
    parser.add_argument(
        "--from-date", required=True, help="Start date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--till-date", required=True, help="End date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--page-size", type=int, default=500, help="Page size for MOEX candles API."
    )
    parser.add_argument(
        "--max-pages", type=int, default=1000, help="Max pages per ticker."
    )
    parser.add_argument(
        "--include-mart",
        action="store_true",
        help="Also truncate mart tables during reset.",
    )
    parser.add_argument(
        "--skip-reset",
        action="store_true",
        help="Skip reset step and append data to existing pipeline tables.",
    )
    return parser.parse_args()


def run_step(name: str, cmd: list[str]) -> None:
    print(f"\n=== {name} ===")
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    args = parse_args()

    py = sys.executable

    if not args.skip_reset:
        reset_cmd = [py, "./script/reset_pipeline_data.py"]
        if args.include_mart:
            reset_cmd.append("--include-mart")
        run_step("Reset tables", reset_cmd)

    run_step(
        "Load raw MOEX candles",
        [
            py,
            "./script/load_raw_moex_candles.py",
            "--tickers",
            args.tickers,
            "--from-date",
            args.from_date,
            "--till-date",
            args.till_date,
            "--page-size",
            str(args.page_size),
            "--max-pages",
            str(args.max_pages),
        ],
    )

    run_step(
        "Transform raw to minute and refresh aggregates",
        [
            py,
            "./script/transform_raw_to_candles.py",
            "--from-date",
            args.from_date,
            "--till-date",
            args.till_date,
            "--refresh-aggregates",
        ],
    )

    run_step("Check row counts", [py, "./script/verify_pipeline_counts.py"])
    print("\nPipeline rebuild completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
