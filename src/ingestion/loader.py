from src.config.settings import settings
from src.ingestion.fetch_history import fetch_history_for_ticker
from src.ingestion.fetch_realtime import fetch_realtime_snapshot
from src.storage.db import get_session
from src.storage.repository import MoexRepository
from src.utils.logger import get_logger

logger = get_logger(__name__)


def load_history(days_back: int = 30) -> None:
    with get_session() as session:
        repo = MoexRepository(session)
        for ticker in settings.moex_tickers:
            payload, rows = fetch_history_for_ticker(ticker=ticker, days_back=days_back)
            repo.save_raw(
                ticker=ticker,
                source_endpoint="history",
                payload=payload,
            )
            for row in rows:
                repo.upsert_daily_candle(row)
            logger.info("Loaded history for %s: %s rows", ticker, len(rows))


def load_realtime_once() -> None:
    with get_session() as session:
        repo = MoexRepository(session)
        for ticker in settings.moex_tickers:
            snapshot = fetch_realtime_snapshot(ticker=ticker)
            repo.save_raw(
                ticker=ticker,
                source_endpoint="realtime",
                payload=snapshot["payload"],
                event_ts=snapshot["event_ts"],
            )
            logger.info("Loaded realtime snapshot for %s", ticker)
