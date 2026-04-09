from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TimeRange(str, Enum):
    day_7 = "7d"
    day_30 = "30d"
    day_90 = "90d"


class TimeResolution(str, Enum):
    minute = "minute"
    hour = "hour"
    day = "day"


class MetricType(str, Enum):
    price = "price"
    volume = "volume"
    volatility = "volatility"


class SeverityLevel(str, Enum):
    medium = "medium"
    high = "high"


class AuthRequest(BaseModel):
    email: str = Field(min_length=1, max_length=256)
    password: str = Field(min_length=1, max_length=1024)


class AuthResponse(BaseModel):
    email: str


class DashboardFiltersRequest(BaseModel):
    mainTickers: list[str]
    mainRange: TimeRange
    mainResolution: TimeResolution
    mainMetricType: MetricType
    candlestickTicker: str
    candlestickRange: TimeRange
    candlestickResolution: TimeResolution


class KpiCardData(BaseModel):
    title: str
    value: str
    delta: str
    trend: str


class LeaderRow(BaseModel):
    ticker: str
    changePct: float
    volume: float


class LeadersData(BaseModel):
    gainers: list[LeaderRow]
    losers: list[LeaderRow]


class AnomalyRow(BaseModel):
    id: str
    eventTs: str
    ticker: str
    anomalyType: str
    metricValue: float
    threshold: float
    severity: SeverityLevel


class CandlestickPoint(BaseModel):
    timestamp: str
    label: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class DashboardSnapshotResponse(BaseModel):
    generatedAt: str
    delayedByMinutes: int
    availableTickers: list[str]
    kpis: list[KpiCardData]
    priceVolumeSeries: list[dict[str, Any]]
    volatilitySeries: list[dict[str, Any]]
    candlestickTicker: str
    candlestickSeries: list[CandlestickPoint]
    leaders: LeadersData
    anomalies: list[AnomalyRow]

