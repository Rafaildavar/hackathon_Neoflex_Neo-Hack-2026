from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

if load_dotenv is not None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    api_host: str
    api_port: int
    cors_origins: list[str]
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    monitored_tickers: list[str]
    moex_delay_minutes: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    monitored = _parse_csv(
        os.getenv(
            "MOEX_TICKERS",
            "SBER,GAZP,LKOH,YDEX,VTBR,ROSN,NVTK",
        )
    )
    if not monitored:
        monitored = ["SBER", "GAZP", "LKOH", "YDEX", "VTBR", "ROSN", "NVTK"]

    cors_origins = _parse_csv(os.getenv("CORS_ORIGINS", "http://localhost:5173"))
    if not cors_origins:
        cors_origins = ["*"]

    return Settings(
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("API_PORT", os.getenv("AUTH_API_PORT", "8001"))),
        cors_origins=cors_origins,
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "moex_dwh"),
        postgres_user=os.getenv("POSTGRES_USER", "moex"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
        monitored_tickers=[ticker.upper() for ticker in monitored],
        moex_delay_minutes=int(os.getenv("MOEX_DELAY_MINUTES", "15")),
    )
