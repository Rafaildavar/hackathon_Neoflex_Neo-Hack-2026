import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def main() -> None:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
        user=os.getenv("POSTGRES_USER", "moex"),
        password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
    )

    # refresh_continuous_aggregate требует autocommit режима
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    queries = [
        "CALL refresh_continuous_aggregate('core.hourly_candles', NULL, NULL);",
        "CALL refresh_continuous_aggregate('core.daily_candles', NULL, NULL);",
        "CALL refresh_continuous_aggregate('core.weekly_candles', NULL, NULL);",
    ]

    cur = conn.cursor()
    for query in queries:
        cur.execute(query)
    cur.close()

    conn.close()
    print("Aggregates refreshed: hourly, daily, weekly")


if __name__ == "__main__":
    main()
