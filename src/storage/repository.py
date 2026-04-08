import json
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.storage.db import engine


def get_daily_candles(ticket: str) -> pd.DataFrame:
    """Дневные свечи из core.daily_candles (не staging)."""
    query = text(
        """
        SELECT
            name,
            date,
            high,
            open,
            close,
            low,
            valume
        FROM core.daily_candles
        WHERE name = :ticket
        ORDER BY date DESC
        """
    )
    return pd.read_sql(query, engine, params={"ticket": ticket.strip()})


def get_raw_data(ticket: str) -> pd.DataFrame:
    """Алиас к get_daily_candles; данные только из core.daily_candles."""
    return get_daily_candles(ticket)


def get_stg_raw_moex(ticket: str) -> pd.DataFrame:
    """Сырой JSON из stg.raw_moex_data по тикеру (если нужен именно staging)."""
    query = text(
        "SELECT * FROM stg.raw_moex_data WHERE ticker = :ticket ORDER BY load_ts DESC"
    )
    return pd.read_sql(query, engine, params={"ticket": ticket.strip()})


class MoexRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_raw(
        self,
        ticker: str,
        source_endpoint: str,
        payload: dict[str, Any],
        event_ts: datetime | None = None,
    ) -> None:
        self.session.execute(
            text(
                """
                INSERT INTO stg.raw_moex_data (source_endpoint, ticker, event_ts, payload_json)
                VALUES (:source_endpoint, :ticker, :event_ts, CAST(:payload_json AS JSONB))
                """
            ),
            {
                "source_endpoint": source_endpoint,
                "ticker": ticker,
                "event_ts": event_ts,
                "payload_json": json.dumps(payload, ensure_ascii=False),
            },
        )

    def upsert_daily_candle(self, row: dict[str, Any]) -> None:
        self.session.execute(
            text(
                """
                INSERT INTO core.daily_candles
                (name, date, high, open, close, low, valume)
                VALUES (:name, :date, :high, :open, :close, :low, :valume)
                ON CONFLICT (name, date) DO UPDATE SET
                    high = EXCLUDED.high,
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    low = EXCLUDED.low,
                    valume = EXCLUDED.valume
                """
            ),
            row,
        )
