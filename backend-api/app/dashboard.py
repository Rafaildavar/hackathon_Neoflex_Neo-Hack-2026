from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any

from fastapi import APIRouter, HTTPException, status

from .db import read_cursor
from .schemas import (
    AnomalyRow,
    CandlestickPoint,
    DashboardFiltersRequest,
    DashboardSnapshotResponse,
    KpiCardData,
    LeaderRow,
    LeadersData,
    SeverityLevel,
    TimeRange,
    TimeResolution,
)
from .settings import get_settings

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])

MOSCOW_TZ = timezone(timedelta(hours=3))
RANGE_TO_DAYS: dict[TimeRange, int] = {
    TimeRange.day_7: 7,
    TimeRange.day_30: 30,
    TimeRange.day_90: 90,
}


def range_to_days(value: TimeRange) -> int:
    return RANGE_TO_DAYS.get(value, 30)


def normalize_ticker(value: str) -> str:
    return value.strip().upper()


def compact_number(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} млрд"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f} млн"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f} тыс"
    return f"{value:.0f}"


def format_number(value: float, precision: int = 2) -> str:
    return f"{value:.{precision}f}".replace(".", ",")


def format_signed_percent(value: float, precision: int = 2) -> str:
    prefix = "+" if value > 0 else "-" if value < 0 else ""
    return f"{prefix}{format_number(abs(value), precision)}%"


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_moscow(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MOSCOW_TZ)


def format_label(timestamp: datetime, resolution: TimeResolution) -> str:
    local_dt = as_moscow(timestamp)
    if resolution == TimeResolution.minute:
        return local_dt.strftime("%H:%M")
    if resolution == TimeResolution.hour:
        return local_dt.strftime("%d.%m %H:%M")
    return local_dt.strftime("%d.%m")


def resolution_limit(resolution: TimeResolution, days: int) -> int:
    if resolution == TimeResolution.minute:
        return min(360, max(120, days * 40))
    if resolution == TimeResolution.hour:
        return min(260, max(72, days * 12))
    return max(7, days)


def resolution_table(resolution: TimeResolution) -> str:
    if resolution == TimeResolution.minute:
        return "core.minute_candles"
    if resolution == TimeResolution.hour:
        return "core.hourly_candles"
    return "core.daily_candles"


def fetch_available_tickers(monitored_tickers: list[str]) -> list[str]:
    with read_cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ticker
            FROM (
                SELECT ticker FROM core.daily_candles WHERE ticker = ANY(%s)
                UNION
                SELECT ticker FROM core.minute_candles WHERE ticker = ANY(%s)
            ) AS unified
            ORDER BY ticker
            """,
            (monitored_tickers, monitored_tickers),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows]


def fetch_daily_metrics_rows(tickers: list[str], days: int) -> list[dict[str, Any]]:
    with read_cursor(dict_rows=True) as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    trade_date,
                    ticker,
                    close::float8 AS close,
                    volume::float8 AS volume,
                    COALESCE(volatility_pct::float8, 0) AS volatility,
                    COALESCE(price_change_pct::float8, 0) AS price_change_pct,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS rn
                FROM mart.daily_metrics
                WHERE ticker = ANY(%s)
            )
            SELECT
                trade_date::text AS trade_date,
                ticker,
                close,
                volume,
                volatility,
                price_change_pct
            FROM ranked
            WHERE rn <= %s
            ORDER BY trade_date ASC, ticker ASC
            """,
            (tickers, days),
        )
        rows = cur.fetchall()

    if rows:
        return rows

    with read_cursor(dict_rows=True) as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    bucket::date AS trade_date,
                    ticker,
                    close::float8 AS close,
                    volume::float8 AS volume,
                    COALESCE(volatility::float8, 0) AS volatility,
                    LAG(close::float8) OVER (PARTITION BY ticker ORDER BY bucket) AS prev_close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY bucket DESC) AS rn
                FROM core.daily_candles
                WHERE ticker = ANY(%s)
            )
            SELECT
                trade_date::text AS trade_date,
                ticker,
                close,
                volume,
                volatility,
                CASE
                    WHEN prev_close IS NULL OR prev_close = 0 THEN 0
                    ELSE ((close - prev_close) / prev_close) * 100
                END AS price_change_pct
            FROM ranked
            WHERE rn <= %s
            ORDER BY trade_date ASC, ticker ASC
            """,
            (tickers, days),
        )
        return cur.fetchall()


def fetch_resolution_rows(
    tickers: list[str],
    resolution: TimeResolution,
    days: int,
    max_points: int,
) -> list[dict[str, Any]]:
    table_name = resolution_table(resolution)

    with read_cursor(dict_rows=True) as cur:
        cur.execute(
            f"""
            WITH ranked AS (
                SELECT
                    bucket,
                    ticker,
                    open::float8 AS open,
                    high::float8 AS high,
                    low::float8 AS low,
                    close::float8 AS close,
                    volume::float8 AS volume,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY bucket DESC) AS rn
                FROM {table_name}
                WHERE ticker = ANY(%s)
                  AND bucket >= NOW() - make_interval(days => %s)
            )
            SELECT
                bucket,
                ticker,
                open,
                high,
                low,
                close,
                volume,
                CASE
                    WHEN close IS NULL OR close = 0 THEN 0
                    ELSE ((high - low) / NULLIF(close, 0)) * 100
                END AS volatility
            FROM ranked
            WHERE rn <= %s
            ORDER BY bucket ASC
            """,
            (tickers, days, max_points),
        )
        rows = cur.fetchall()

    if rows or resolution == TimeResolution.minute:
        return rows

    trunc_unit = "hour" if resolution == TimeResolution.hour else "day"
    with read_cursor(dict_rows=True) as cur:
        cur.execute(
            f"""
            WITH base AS (
                SELECT
                    bucket,
                    ticker,
                    open::float8 AS open,
                    high::float8 AS high,
                    low::float8 AS low,
                    close::float8 AS close,
                    volume::float8 AS volume
                FROM core.minute_candles
                WHERE ticker = ANY(%s)
                  AND bucket >= NOW() - make_interval(days => %s)
            ),
            aggregated AS (
                SELECT
                    date_trunc(%s, bucket) AS bucket,
                    ticker,
                    (ARRAY_AGG(open ORDER BY bucket ASC))[1] AS open,
                    MAX(high) AS high,
                    MIN(low) AS low,
                    (ARRAY_AGG(close ORDER BY bucket DESC))[1] AS close,
                    SUM(volume) AS volume
                FROM base
                GROUP BY 1, 2
            ),
            ranked AS (
                SELECT
                    bucket,
                    ticker,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY bucket DESC) AS rn
                FROM aggregated
            )
            SELECT
                bucket,
                ticker,
                open,
                high,
                low,
                close,
                volume,
                CASE
                    WHEN close IS NULL OR close = 0 THEN 0
                    ELSE ((high - low) / NULLIF(close, 0)) * 100
                END AS volatility
            FROM ranked
            WHERE rn <= %s
            ORDER BY bucket ASC
            """,
            (tickers, days, trunc_unit, max_points),
        )
        return cur.fetchall()


def group_daily_by_ticker(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["ticker"]].append(
            {
                "date": row["trade_date"],
                "close": safe_float(row["close"]),
                "volume": safe_float(row["volume"]),
                "volatility": safe_float(row["volatility"]),
                "price_change_pct": safe_float(row["price_change_pct"]),
            }
        )
    return grouped


def group_resolution_by_ticker(
    rows: list[dict[str, Any]],
    resolution: TimeResolution,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        bucket = row["bucket"]
        if not isinstance(bucket, datetime):
            continue
        grouped[row["ticker"]].append(
            {
                "timestamp": bucket.isoformat(),
                "label": format_label(bucket, resolution),
                "open": safe_float(row["open"]),
                "high": safe_float(row["high"]),
                "low": safe_float(row["low"]),
                "close": safe_float(row["close"]),
                "volume": safe_float(row["volume"]),
                "volatility": safe_float(row["volatility"]),
            }
        )
    return grouped


def build_price_volume_series(
    ticker_points: dict[str, list[dict[str, Any]]],
    selected_tickers: list[str],
) -> list[dict[str, Any]]:
    timeline = sorted(
        {
            point["timestamp"]
            for ticker in selected_tickers
            for point in ticker_points.get(ticker, [])
        }
    )

    lookup: dict[str, dict[str, dict[str, Any]]] = {
        ticker: {item["timestamp"]: item for item in ticker_points.get(ticker, [])}
        for ticker in selected_tickers
    }

    output: list[dict[str, Any]] = []
    for timestamp in timeline:
        row: dict[str, Any] = {"date": "", "totalVolume": 0.0}
        for ticker in selected_tickers:
            point = lookup.get(ticker, {}).get(timestamp)
            if not point:
                continue
            if not row["date"]:
                row["date"] = point["label"]
            row[ticker] = point["close"]
            row["totalVolume"] += point["volume"]

        if row["date"]:
            output.append(row)

    return output


def build_volatility_series(
    ticker_points: dict[str, list[dict[str, Any]]],
    selected_tickers: list[str],
) -> list[dict[str, Any]]:
    timeline = sorted(
        {
            point["timestamp"]
            for ticker in selected_tickers
            for point in ticker_points.get(ticker, [])
        }
    )

    lookup: dict[str, dict[str, dict[str, Any]]] = {
        ticker: {item["timestamp"]: item for item in ticker_points.get(ticker, [])}
        for ticker in selected_tickers
    }

    output: list[dict[str, Any]] = []
    for timestamp in timeline:
        row: dict[str, Any] = {"date": ""}
        for ticker in selected_tickers:
            point = lookup.get(ticker, {}).get(timestamp)
            if not point:
                continue
            if not row["date"]:
                row["date"] = point["label"]
            row[ticker] = point["volatility"]
        if row["date"]:
            output.append(row)

    return output


def build_kpis(daily_by_ticker: dict[str, list[dict[str, Any]]]) -> list[KpiCardData]:
    latest_rows: list[dict[str, Any]] = []
    previous_rows: list[dict[str, Any]] = []

    for series in daily_by_ticker.values():
        if not series:
            continue
        latest_rows.append(series[-1])
        if len(series) > 1:
            previous_rows.append(series[-2])

    if not latest_rows:
        return [
            KpiCardData(title="Средняя цена закрытия", value="0", delta="0%", trend="neutral"),
            KpiCardData(title="Суммарный объем", value="0", delta="0 тикеров", trend="neutral"),
            KpiCardData(title="Лидер движения", value="-", delta="0%", trend="neutral"),
            KpiCardData(title="Макс. волатильность", value="0%", delta="дневной диапазон", trend="neutral"),
        ]

    latest_average_close = mean(item["close"] for item in latest_rows)
    previous_average_close = (
        mean(item["close"] for item in previous_rows) if previous_rows else latest_average_close
    )
    average_delta = (
        ((latest_average_close - previous_average_close) / previous_average_close) * 100
        if previous_average_close
        else 0.0
    )

    total_volume = sum(item["volume"] for item in latest_rows)
    max_volatility = max(item["volatility"] for item in latest_rows)
    movers = sorted(
        [
            {"ticker": ticker, "delta": series[-1]["price_change_pct"]}
            for ticker, series in daily_by_ticker.items()
            if series
        ],
        key=lambda item: item["delta"],
        reverse=True,
    )
    top_mover = movers[0] if movers else None

    return [
        KpiCardData(
            title="Средняя цена закрытия",
            value=format_number(latest_average_close, 2),
            delta=format_signed_percent(average_delta, 2),
            trend="up" if average_delta > 0 else "down" if average_delta < 0 else "neutral",
        ),
        KpiCardData(
            title="Суммарный объем",
            value=compact_number(total_volume),
            delta=f"{len(latest_rows)} тикеров",
            trend="neutral",
        ),
        KpiCardData(
            title="Лидер движения",
            value=top_mover["ticker"] if top_mover else "-",
            delta=format_signed_percent(top_mover["delta"], 2) if top_mover else "0%",
            trend="down" if top_mover and top_mover["delta"] < 0 else "up" if top_mover else "neutral",
        ),
        KpiCardData(
            title="Макс. волатильность",
            value=f"{format_number(max_volatility, 2)}%",
            delta="дневной диапазон",
            trend="up" if max_volatility > 4 else "neutral",
        ),
    ]


def build_leaders(daily_by_ticker: dict[str, list[dict[str, Any]]]) -> LeadersData:
    rows: list[LeaderRow] = []
    for ticker, series in daily_by_ticker.items():
        if not series:
            continue
        latest = series[-1]
        rows.append(
            LeaderRow(
                ticker=ticker,
                changePct=safe_float(latest["price_change_pct"]),
                volume=safe_float(latest["volume"]),
            )
        )

    gainers = sorted(rows, key=lambda item: item.changePct, reverse=True)[:5]
    losers = sorted(rows, key=lambda item: item.changePct)[:5]
    return LeadersData(gainers=gainers, losers=losers)


def standard_deviation(values: list[float]) -> float:
    if not values:
        return 0.0
    values_mean = mean(values)
    variance = sum((value - values_mean) ** 2 for value in values) / len(values)
    return variance ** 0.5


def load_anomalies_from_mart(tickers: list[str], days: int) -> list[AnomalyRow]:
    with read_cursor(dict_rows=True) as cur:
        cur.execute(
            """
            SELECT
                event_id,
                detected_at,
                ticker,
                anomaly_type,
                COALESCE(severity, 'medium') AS severity,
                COALESCE(metric_value::float8, 0) AS metric_value,
                COALESCE(threshold_value::float8, 0) AS threshold_value,
                details
            FROM mart.anomaly_events
            WHERE ticker = ANY(%s)
              AND detected_at >= NOW() - make_interval(days => %s)
            ORDER BY detected_at DESC
            LIMIT 120
            """,
            (tickers, days),
        )
        rows = cur.fetchall()

    mapped: list[AnomalyRow] = []
    for row in rows:
        anomaly_type = str(row["anomaly_type"]).lower()
        normalized_type = "volume_spike" if "volume" in anomaly_type else "price_jump"

        details = row.get("details") if isinstance(row, dict) else None
        if isinstance(details, dict) and details.get("trade_date"):
            event_ts = str(details["trade_date"])
        else:
            detected = row["detected_at"]
            event_ts = detected.date().isoformat() if isinstance(detected, datetime) else str(detected)

        severity_raw = str(row.get("severity", "medium")).lower()
        severity = SeverityLevel.high if severity_raw == "high" else SeverityLevel.medium

        mapped.append(
            AnomalyRow(
                id=f"db-{row['event_id']}",
                eventTs=event_ts,
                ticker=row["ticker"],
                anomalyType=normalized_type,
                metricValue=safe_float(row["metric_value"]),
                threshold=safe_float(row["threshold_value"]),
                severity=severity,
            )
        )

    return mapped


def build_anomalies_from_daily(daily_by_ticker: dict[str, list[dict[str, Any]]]) -> list[AnomalyRow]:
    anomalies: list[AnomalyRow] = []
    for ticker, series in daily_by_ticker.items():
        if not series:
            continue

        volume_values = [safe_float(point["volume"]) for point in series]
        volume_avg = mean(volume_values) if volume_values else 0.0
        volume_std = standard_deviation(volume_values)
        threshold = volume_avg + 3 * volume_std

        for point in series:
            event_ts = point["date"]
            volume = safe_float(point["volume"])
            price_change = abs(safe_float(point["price_change_pct"]))

            if volume > threshold and volume_std > 0:
                severity = SeverityLevel.high if volume > volume_avg + 4 * volume_std else SeverityLevel.medium
                anomalies.append(
                    AnomalyRow(
                        id=f"{ticker}-{event_ts}-volume",
                        eventTs=event_ts,
                        ticker=ticker,
                        anomalyType="volume_spike",
                        metricValue=volume,
                        threshold=threshold,
                        severity=severity,
                    )
                )

            if price_change > 2:
                severity = SeverityLevel.high if price_change > 4 else SeverityLevel.medium
                anomalies.append(
                    AnomalyRow(
                        id=f"{ticker}-{event_ts}-price",
                        eventTs=event_ts,
                        ticker=ticker,
                        anomalyType="price_jump",
                        metricValue=safe_float(point["price_change_pct"]),
                        threshold=2,
                        severity=severity,
                    )
                )

    anomalies.sort(key=lambda item: item.eventTs, reverse=True)
    return anomalies[:30]


def build_candlestick_series(
    ticker_points: dict[str, list[dict[str, Any]]],
    ticker: str,
) -> list[CandlestickPoint]:
    output: list[CandlestickPoint] = []
    for item in ticker_points.get(ticker, []):
        output.append(
            CandlestickPoint(
                timestamp=item["timestamp"],
                label=item["label"],
                open=safe_float(item["open"]),
                high=safe_float(item["high"]),
                low=safe_float(item["low"]),
                close=safe_float(item["close"]),
                volume=safe_float(item["volume"]),
            )
        )
    return output


def normalize_main_tickers(raw_tickers: list[str], available_tickers: list[str]) -> list[str]:
    normalized = [normalize_ticker(value) for value in raw_tickers if value.strip()]
    selected = [ticker for ticker in normalized if ticker in available_tickers]
    if selected:
        return selected
    return available_tickers[:3] if len(available_tickers) >= 3 else available_tickers[:]


@router.post("/snapshot", response_model=DashboardSnapshotResponse)
def get_dashboard_snapshot(filters: DashboardFiltersRequest) -> DashboardSnapshotResponse:
    settings = get_settings()
    available_tickers = fetch_available_tickers(settings.monitored_tickers)
    if not available_tickers:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "В DWH пока нет данных по тикерам. Сначала загрузите данные MOEX "
                "через ETL (load_raw_moex_candles.py + transform_raw_to_candles.py)."
            ),
        )

    main_tickers = normalize_main_tickers(filters.mainTickers, available_tickers)
    if not main_tickers:
        raise HTTPException(status_code=400, detail="Не удалось определить список тикеров для основного графика.")

    candidate_candle_ticker = normalize_ticker(filters.candlestickTicker or "")
    candlestick_ticker = (
        candidate_candle_ticker
        if candidate_candle_ticker in available_tickers
        else main_tickers[0]
    )

    requested_tickers = sorted(set(main_tickers + [candlestick_ticker]))
    max_days = max(range_to_days(filters.mainRange), range_to_days(filters.candlestickRange))

    daily_rows = fetch_daily_metrics_rows(requested_tickers, max_days)
    if not daily_rows:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Недостаточно данных в core/mart для построения дашборда.",
        )

    daily_by_ticker = group_daily_by_ticker(daily_rows)
    main_daily_by_ticker = {ticker: daily_by_ticker.get(ticker, []) for ticker in main_tickers}

    main_days = range_to_days(filters.mainRange)
    main_limit = resolution_limit(filters.mainResolution, main_days)
    main_rows = fetch_resolution_rows(
        main_tickers,
        filters.mainResolution,
        main_days,
        main_limit,
    )
    main_points_by_ticker = group_resolution_by_ticker(main_rows, filters.mainResolution)

    candle_days = range_to_days(filters.candlestickRange)
    candle_limit = resolution_limit(filters.candlestickResolution, candle_days)
    candle_rows = fetch_resolution_rows(
        [candlestick_ticker],
        filters.candlestickResolution,
        candle_days,
        candle_limit,
    )
    candle_points_by_ticker = group_resolution_by_ticker(candle_rows, filters.candlestickResolution)

    kpis = build_kpis(main_daily_by_ticker)
    leaders = build_leaders(main_daily_by_ticker)
    anomalies = load_anomalies_from_mart(main_tickers, max_days)
    if not anomalies:
        anomalies = build_anomalies_from_daily(main_daily_by_ticker)

    price_volume_series = build_price_volume_series(main_points_by_ticker, main_tickers)
    volatility_series = build_volatility_series(main_points_by_ticker, main_tickers)
    candlestick_series = build_candlestick_series(candle_points_by_ticker, candlestick_ticker)

    if not candlestick_series:
        fallback_rows = fetch_resolution_rows(
            [candlestick_ticker],
            TimeResolution.day,
            candle_days,
            max(7, candle_days),
        )
        candlestick_series = build_candlestick_series(
            group_resolution_by_ticker(fallback_rows, TimeResolution.day),
            candlestick_ticker,
        )

    return DashboardSnapshotResponse(
        generatedAt=datetime.now(timezone.utc).isoformat(),
        delayedByMinutes=settings.moex_delay_minutes,
        availableTickers=available_tickers,
        kpis=kpis,
        priceVolumeSeries=price_volume_series,
        volatilitySeries=volatility_series,
        candlestickTicker=candlestick_ticker,
        candlestickSeries=candlestick_series,
        leaders=leaders,
        anomalies=anomalies[:30],
    )
