from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import JSON, BigInteger, Date, DateTime, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawMoexData(Base):
    __tablename__ = "raw_moex_data"
    __table_args__ = {"schema": "stg"}

    id: Mapped[int] = mapped_column(primary_key=True)
    load_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source_endpoint: Mapped[str] = mapped_column(Text)
    ticker: Mapped[str] = mapped_column(String(20))
    event_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payload_json: Mapped[dict] = mapped_column(JSON)


class DailyCandle(Base):
    __tablename__ = "daily_candles"
    __table_args__ = {"schema": "core"}

    name: Mapped[str] = mapped_column(String(20), primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    high: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    open: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    close: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    low: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    valume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
