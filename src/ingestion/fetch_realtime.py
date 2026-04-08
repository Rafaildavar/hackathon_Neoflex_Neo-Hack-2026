import time
from datetime import datetime, timezone
from typing import Any

from src.config.settings import settings
from src.ingestion.moex_client import MoexClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


def fetch_realtime_snapshot(ticker: str) -> dict[str, Any]:
    client = MoexClient()
    payload = client.get_security(ticker)
    return {
        "ticker": ticker,
        "event_ts": datetime.now(timezone.utc),
        "payload": payload,
    }


def run_forever() -> None:
    while True:
        for ticker in settings.moex_tickers:
            snapshot = fetch_realtime_snapshot(ticker)
            logger.info("Realtime snapshot fetched for %s", snapshot["ticker"])
        time.sleep(settings.moex_poll_seconds)


if __name__ == "__main__":
    run_forever()
