from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2

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

from analyze.lstm_forecast import PREDICTION_TABLE_DDL


def main() -> int:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
        user=os.getenv("POSTGRES_USER", "moex"),
        password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(PREDICTION_TABLE_DDL)
    finally:
        conn.close()

    print("Table mart.lstm_daily_predictions is ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
