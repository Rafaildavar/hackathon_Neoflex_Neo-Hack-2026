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

from analyze.lstm_forecast import DbConfig, train_and_predict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train LSTM per ticker on daily candles and upsert one-day-ahead prediction"
    )
    parser.add_argument("--ticker", default=None, help="Ticker filter, e.g. SBER")
    parser.add_argument("--timesteps", type=int, default=60, help="Window size for LSTM")
    parser.add_argument("--epochs", type=int, default=200, help="Max training epochs")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size")
    parser.add_argument("--min-rows", type=int, default=120, help="Minimum rows per ticker")
    parser.add_argument("--verbose", type=int, default=1, choices=[0, 1, 2], help="Keras fit verbosity")
    parser.add_argument("--model-version", default="lstm_v1", help="Model version tag")
    parser.add_argument(
        "--model-dir",
        default=str(PROJECT_ROOT / "models" / "lstm_daily"),
        help="Directory for .weights.h5 and metadata",
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

    stats = train_and_predict(
        db_config=db_cfg,
        ticker=args.ticker,
        timesteps=args.timesteps,
        epochs=args.epochs,
        batch_size=args.batch_size,
        model_dir=Path(args.model_dir),
        min_rows=args.min_rows,
        verbose=args.verbose,
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

