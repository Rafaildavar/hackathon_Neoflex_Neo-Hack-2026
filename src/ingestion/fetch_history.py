from datetime import date, timedelta
from typing import Any

from src.config.settings import settings
from src.ingestion.moex_client import MoexClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _to_daily_rows(payload: dict[str, Any], ticker: str) -> list[dict[str, Any]]:
    history = payload.get("history", {})
    columns = history.get("columns", [])
    data = history.get("data", [])
    rows: list[dict[str, Any]] = []

    for item in data:
        raw = dict(zip(columns, item))
        open_price = raw.get("OPEN")
        close_price = raw.get("CLOSE")
        high_price = raw.get("HIGH")
        low_price = raw.get("LOW")

        rows.append(
            {
                "name": ticker,
                "date": raw.get("TRADEDATE"),
                "high": high_price,
                "open": open_price,
                "close": close_price,
                "low": low_price,
                "valume": raw.get("VOLUME"),
            }
        )
    return rows


def fetch_history_for_ticker(ticker: str, days_back: int = 30) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    client = MoexClient()
    date_till = date.today().isoformat()
    date_from = (date.today() - timedelta(days=days_back)).isoformat()
    payload = client.get_history(ticker=ticker, date_from=date_from, date_till=date_till)
    return payload, _to_daily_rows(payload, ticker)


def run() -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for ticker in settings.moex_tickers:
        payload, rows = fetch_history_for_ticker(ticker)
        result[ticker] = rows
        logger.info("Fetched history for %s: %s records", ticker, len(rows))
        _ = payload
    return result


if __name__ == "__main__":
    run()
