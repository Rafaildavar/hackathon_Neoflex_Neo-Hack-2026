from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import psycopg2
import requests
from psycopg2.extras import Json

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Загружаем переменные из .env, если он существует
if load_dotenv is not None:
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


DEFAULT_MOEX_BASE = "https://iss.moex.com/iss/"
DEFAULT_ENDPOINTS = [
    "engines/stock/markets/shares/securities/{ticker}/candles.json",
    "engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json",
]


@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class LoadStats:
    inserted_rows: int = 0
    requests_done: int = 0
    candles_fetched: int = 0


class RateLimiter:
    """Simple client-side rate limiter by max requests per second."""

    def __init__(self, max_rps: float) -> None:
        if max_rps <= 0:
            raise ValueError("max_rps must be > 0")
        self._min_interval = 1.0 / max_rps
        self._last_ts = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_ts
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_ts = time.monotonic()


def parse_args() -> argparse.Namespace:
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="Load raw MOEX candles payloads into stg.raw_moex_data"
    )
    parser.add_argument(
        "--tickers",
        default=os.getenv("MOEX_TICKERS", "SBER"),
        help="Comma-separated tickers, e.g. SBER,GAZP,LKOH",
    )
    parser.add_argument(
        "--from-date",
        dest="from_date",
        default=os.getenv("MOEX_FROM_DATE", yesterday),
        help="Start date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--till-date",
        dest="till_date",
        default=os.getenv("MOEX_TILL_DATE", yesterday),
        help="End date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("MOEX_INTERVAL", "1")),
        help="MOEX candle interval (1, 10, 60, 24)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("MOEX_TIMEOUT_SECONDS", "20")),
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--db-host",
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="PostgreSQL host",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="PostgreSQL port",
    )
    parser.add_argument(
        "--db-name",
        default=os.getenv("POSTGRES_DB", "moex_dwh"),
        help="PostgreSQL database",
    )
    parser.add_argument(
        "--db-user",
        default=os.getenv("POSTGRES_USER", "moex"),
        help="PostgreSQL user",
    )
    parser.add_argument(
        "--db-password",
        default=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
        help="PostgreSQL password",
    )
    parser.add_argument(
        "--moex-base-url",
        default=os.getenv("MOEX_BASE_URL", DEFAULT_MOEX_BASE),
        help="MOEX ISS base URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch payloads without inserting into DB",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=int(os.getenv("MOEX_PAGE_SIZE", "500")),
        help="MOEX page size for pagination",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=int(os.getenv("MOEX_MAX_PAGES", "500")),
        help="Safety limit for pagination pages per ticker",
    )
    parser.add_argument(
        "--max-rps",
        type=float,
        default=float(os.getenv("MOEX_MAX_RPS", "10")),
        help="Client-side rate limit in requests/second (MOEX hard limit is 20)",
    )
    parser.add_argument(
        "--daily-request-limit",
        type=int,
        default=int(os.getenv("MOEX_DAILY_REQUEST_LIMIT", "100000")),
        help="Safety daily request cap (MOEX guideline ~100000/day)",
    )
    return parser.parse_args()


def fetch_with_fallback(
    ticker: str,
    from_date: str,
    till_date: str,
    interval: int,
    timeout_seconds: int,
    moex_base_url: str,
    start: int,
    page_size: int,
) -> tuple[str, dict[str, Any]]:
    params = {
        "from": from_date,
        "till": till_date,
        "interval": interval,
        "start": start,
    }
    if page_size > 0:
        params["limit"] = page_size
    errors: list[str] = []

    for endpoint_template in DEFAULT_ENDPOINTS:
        endpoint = endpoint_template.format(ticker=ticker)
        url = urljoin(moex_base_url, endpoint)

        try:
            response = requests.get(url, params=params, timeout=timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
            continue

        candles = payload.get("candles")
        if isinstance(candles, dict) and "data" in candles:
            return url, payload

        errors.append(f"{url}: unexpected payload format")

    raise RuntimeError("All MOEX endpoints failed:\n- " + "\n- ".join(errors))


def insert_raw_payload(
    conn: psycopg2.extensions.connection,
    source_endpoint: str,
    ticker: str,
    request_params: dict[str, Any],
    payload: dict[str, Any],
) -> bool:
    """
    Insert raw payload row.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stg.raw_moex_data (
                ticker,
                source_endpoint,
                request_params,
                payload,
                ingested_at
            ) VALUES (%s, %s, %s, %s, NOW())
            """,
            (ticker, source_endpoint, Json(request_params), Json(payload)),
        )
        return True


def get_today_ingested_request_count(conn: psycopg2.extensions.connection) -> int:
    """
    Count already ingested raw requests in UTC day.
    Used as approximation for API requests done today by this pipeline.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM stg.raw_moex_data
            WHERE ingested_at >= %s
            """,
            (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),),
        )
        return int(cur.fetchone()[0])


def main() -> int:
    args = parse_args()
    if args.max_rps > 20:
        raise ValueError("MOEX ISS limit is 20 requests/second. Set --max-rps <= 20")

    print(
        "Notice: MOEX data in this pipeline is used for educational purposes only "
        "(not for profit extraction)."
    )
    tickers = [part.strip().upper() for part in args.tickers.split(",") if part.strip()]

    if not tickers:
        raise ValueError("No tickers provided")

    db_cfg = DbConfig(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    stats = LoadStats()
    limiter = RateLimiter(args.max_rps)

    conn = None
    if not args.dry_run:
        conn = psycopg2.connect(
            host=db_cfg.host,
            port=db_cfg.port,
            dbname=db_cfg.dbname,
            user=db_cfg.user,
            password=db_cfg.password,
        )
        today_count = get_today_ingested_request_count(conn)
        print(
            f"Already ingested today (UTC): {today_count} requests. "
            f"Configured daily cap: {args.daily_request_limit}."
        )
        if today_count >= args.daily_request_limit:
            raise RuntimeError(
                "Daily request limit already reached before run start. "
                "Abort to avoid MOEX overuse."
            )

    try:
        for ticker in tickers:
            page = 0
            start = 0
            total_rows_for_ticker = 0

            while page < args.max_pages:
                # Respect MOEX per-second limit with client-side throttling.
                limiter.wait()
                source_endpoint, payload = fetch_with_fallback(
                    ticker=ticker,
                    from_date=args.from_date,
                    till_date=args.till_date,
                    interval=args.interval,
                    timeout_seconds=args.timeout_seconds,
                    moex_base_url=args.moex_base_url,
                    start=start,
                    page_size=args.page_size,
                )
                stats.requests_done += 1
                if stats.requests_done >= args.daily_request_limit:
                    print(
                        "WARN Daily request safety cap reached for this run. "
                        "Stopping further polling."
                    )
                    break
                page += 1

                candles_data = payload.get("candles", {}).get("data", [])
                batch_size = len(candles_data)
                total_rows_for_ticker += batch_size
                stats.candles_fetched += batch_size

                print(
                    f"Fetched {batch_size} rows for {ticker} "
                    f"(page {page}, start={start}) from {source_endpoint} "
                    f"for {args.from_date}..{args.till_date}"
                )

                if not args.dry_run:
                    inserted = insert_raw_payload(
                        conn=conn,
                        source_endpoint=source_endpoint,
                        ticker=ticker,
                        request_params={
                            "from": args.from_date,
                            "till": args.till_date,
                            "interval": args.interval,
                            "start": start,
                            "page": page,
                            "page_size": args.page_size,
                        },
                        payload=payload,
                    )
                    if inserted:
                        stats.inserted_rows += 1

                # Empty or short page means there is no next page.
                if batch_size == 0 or batch_size < args.page_size:
                    break

                start += args.page_size

            if page >= args.max_pages:
                print(
                    f"  WARN Reached max pages ({args.max_pages}) for {ticker}; "
                    "consider increasing --max-pages if needed."
                )

            if stats.requests_done >= args.daily_request_limit:
                break

            if not args.dry_run:
                print(
                    f"  OK Upserted {ticker}: pages={page}, candles={total_rows_for_ticker}"
                )

        if conn is not None:
            conn.commit()
    finally:
        if conn is not None:
            conn.close()

    print(
        "\nDone. "
        f"HTTP requests: {stats.requests_done}, "
        f"upserted: {stats.inserted_rows}, "
        f"candles fetched: {stats.candles_fetched}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

