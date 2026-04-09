from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analyze.lstm_forecast import DbConfig, predict_with_trained_models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily inference with trained LSTM models (no retraining)"
    )
    parser.add_argument("--ticker", default=None, help="Ticker filter, e.g. SBER")
    parser.add_argument("--timesteps", type=int, default=60, help="Fallback window size")
    parser.add_argument("--model-version", default="lstm_v1", help="Model version tag")
    parser.add_argument(
        "--model-dir",
        default=str(PROJECT_ROOT / "models" / "lstm_daily"),
        help="Directory with saved .weights.h5 and metadata",
    )
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "moex_dwh"), help="PostgreSQL database")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "moex"), help="PostgreSQL user")
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "moex_pass"), help="PostgreSQL password")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    db_cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    stats = predict_with_trained_models(
        db_config=db_cfg,
        ticker=args.ticker,
        model_dir=Path(args.model_dir),
        fallback_timesteps=args.timesteps,
        model_version=args.model_version,
    )

    print(
        "Done:",
        f"total={stats.total_tickers}",
        f"success={stats.success_tickers}",
        f"skipped={stats.skipped_tickers}",
        f"errors={stats.errors_tickers}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

