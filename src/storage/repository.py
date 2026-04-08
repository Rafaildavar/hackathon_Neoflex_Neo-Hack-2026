import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


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
                (trade_date, ticker, open, close, high, low, volume, price_change_pct, range_pct)
                VALUES (:trade_date, :ticker, :open, :close, :high, :low, :volume, :price_change_pct, :range_pct)
                ON CONFLICT (trade_date, ticker) DO UPDATE SET
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    volume = EXCLUDED.volume,
                    price_change_pct = EXCLUDED.price_change_pct,
                    range_pct = EXCLUDED.range_pct
                """
            ),
            row,
        )
