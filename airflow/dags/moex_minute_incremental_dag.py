from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import Json

DEFAULT_MOEX_BASE = "https://iss.moex.com/iss/"
DEFAULT_ENDPOINTS = [
    "engines/stock/markets/shares/boards/TQBR/securities/{ticker}/candles.json",
    "engines/stock/markets/shares/securities/{ticker}/candles.json",
]
DEFAULT_TICKERS = "SBER,GAZP,LKOH,YDEX,VTBR,ROSN,NVTK,TATN,GMKN,NLMK"


@dataclass
class IncrementalStats:
    tickers_processed: int = 0
    raw_payloads_saved: int = 0
    candles_upserted: int = 0
    requests_done: int = 0
    min_bucket: datetime | None = None
    max_bucket: datetime | None = None


def _split_tickers(raw: str) -> list[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _get_db_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
        user=os.getenv("POSTGRES_USER", "moex"),
        password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
    )


def _fetch_moex_payload(
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
        "iss.meta": "off",
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


def _parse_candles(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candles_section = payload.get("candles", {})
    columns = candles_section.get("columns", [])
    data = candles_section.get("data", [])
    if not columns or not data:
        return []

    idx = {col: i for i, col in enumerate(columns)}
    required = {"open", "close", "high", "low", "volume", "begin"}
    if not required.issubset(idx.keys()):
        return []

    rows: list[dict[str, Any]] = []
    for item in data:
        try:
            # MOEX returns begin like "2026-04-09 10:32:00", assume UTC.
            bucket = datetime.fromisoformat(item[idx["begin"]]).replace(tzinfo=timezone.utc)
            rows.append(
                {
                    "bucket": bucket,
                    "open": float(item[idx["open"]]) if item[idx["open"]] is not None else None,
                    "high": float(item[idx["high"]]) if item[idx["high"]] is not None else None,
                    "low": float(item[idx["low"]]) if item[idx["low"]] is not None else None,
                    "close": float(item[idx["close"]]) if item[idx["close"]] is not None else None,
                    "volume": float(item[idx["volume"]]) if item[idx["volume"]] is not None else None,
                }
            )
        except (IndexError, TypeError, ValueError):
            continue
    return rows


def _get_last_bucket(cur: psycopg2.extensions.cursor, ticker: str) -> datetime | None:
    cur.execute(
        """
        SELECT max(bucket)
        FROM core.minute_candles
        WHERE ticker = %s
        """,
        (ticker,),
    )
    return cur.fetchone()[0]


def _save_raw_payload(
    cur: psycopg2.extensions.cursor,
    ticker: str,
    source_endpoint: str,
    request_params: dict[str, Any],
    payload: dict[str, Any],
) -> None:
    cur.execute(
        """
        INSERT INTO stg.raw_moex_data (source_endpoint, ticker, request_params, payload, ingested_at)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (source_endpoint, ticker, Json(request_params), Json(payload)),
    )


def _upsert_candles(
    cur: psycopg2.extensions.cursor,
    ticker: str,
    candles: list[dict[str, Any]],
    last_bucket: datetime | None,
) -> tuple[int, datetime | None, datetime | None]:
    inserted = 0
    min_bucket: datetime | None = None
    max_bucket: datetime | None = None

    for candle in candles:
        bucket = candle["bucket"]
        if last_bucket is not None and bucket <= last_bucket:
            continue

        cur.execute(
            """
            INSERT INTO core.minute_candles (bucket, ticker, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, bucket) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """,
            (
                bucket,
                ticker,
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                candle["volume"],
            ),
        )
        inserted += 1
        min_bucket = bucket if min_bucket is None else min(min_bucket, bucket)
        max_bucket = bucket if max_bucket is None else max(max_bucket, bucket)

    return inserted, min_bucket, max_bucket


def _refresh_aggregates_for_range(
    conn: psycopg2.extensions.connection,
    start_ts: datetime,
    end_ts: datetime,
) -> None:
    hour_start = start_ts.replace(minute=0, second=0, microsecond=0)
    hour_end = end_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    day_start = start_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = end_ts.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    original_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "CALL refresh_continuous_aggregate('core.hourly_candles', %s, %s)",
                (hour_start, hour_end),
            )
            cur.execute(
                "CALL refresh_continuous_aggregate('core.daily_candles', %s, %s)",
                (day_start, day_end),
            )
    finally:
        conn.autocommit = original_autocommit


def run_incremental_minute_sync() -> None:
    tickers = _split_tickers(os.getenv("MOEX_TICKERS", DEFAULT_TICKERS))
    timeout_seconds = int(os.getenv("MOEX_TIMEOUT_SECONDS", "20"))
    page_size = int(os.getenv("MOEX_PAGE_SIZE", "500"))
    max_pages = int(os.getenv("MOEX_MAX_PAGES", "500"))
    moex_base_url = os.getenv("MOEX_BASE_URL", DEFAULT_MOEX_BASE)
    interval = 1
    now_utc = datetime.now(timezone.utc)
    fallback_from_days = int(os.getenv("MOEX_INCREMENTAL_FALLBACK_DAYS", "1"))

    stats = IncrementalStats()
    conn = _get_db_conn()

    try:
        with conn.cursor() as cur:
            for ticker in tickers:
                stats.tickers_processed += 1
                last_bucket = _get_last_bucket(cur, ticker)
                if last_bucket is None:
                    from_dt = (now_utc - timedelta(days=fallback_from_days)).date().isoformat()
                else:
                    # Candle endpoint works with dates; take last known day and filter by bucket after fetch.
                    from_dt = last_bucket.date().isoformat()
                till_dt = now_utc.date().isoformat()

                page = 0
                start = 0

                while page < max_pages:
                    source_endpoint, payload = _fetch_moex_payload(
                        ticker=ticker,
                        from_date=from_dt,
                        till_date=till_dt,
                        interval=interval,
                        timeout_seconds=timeout_seconds,
                        moex_base_url=moex_base_url,
                        start=start,
                        page_size=page_size,
                    )
                    stats.requests_done += 1
                    page += 1

                    _save_raw_payload(
                        cur,
                        ticker=ticker,
                        source_endpoint=source_endpoint,
                        request_params={
                            "mode": "minute_incremental",
                            "from": from_dt,
                            "till": till_dt,
                            "interval": interval,
                            "start": start,
                            "page": page,
                            "page_size": page_size,
                        },
                        payload=payload,
                    )
                    stats.raw_payloads_saved += 1

                    candles = _parse_candles(payload)
                    upserted, min_b, max_b = _upsert_candles(cur, ticker, candles, last_bucket)
                    stats.candles_upserted += upserted
                    if min_b is not None:
                        stats.min_bucket = min_b if stats.min_bucket is None else min(stats.min_bucket, min_b)
                    if max_b is not None:
                        stats.max_bucket = max_b if stats.max_bucket is None else max(stats.max_bucket, max_b)

                    batch_size = len(payload.get("candles", {}).get("data", []))
                    if batch_size == 0 or batch_size < page_size:
                        break
                    start += page_size

            conn.commit()

        if stats.min_bucket is not None and stats.max_bucket is not None:
            refresh_end = stats.max_bucket.replace(second=0, microsecond=0) + timedelta(minutes=1)
            _refresh_aggregates_for_range(conn, stats.min_bucket, refresh_end)

        print(
            "Incremental sync done: "
            f"tickers={stats.tickers_processed}, "
            f"requests={stats.requests_done}, "
            f"raw_payloads={stats.raw_payloads_saved}, "
            f"candles_upserted={stats.candles_upserted}"
        )
    finally:
        conn.close()


with DAG(
    dag_id="moex_minute_incremental_sync",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["moex", "minute", "incremental"],
) as dag:
    PythonOperator(
        task_id="fetch_and_upsert_minutes_incremental",
        python_callable=run_incremental_minute_sync,
    )

