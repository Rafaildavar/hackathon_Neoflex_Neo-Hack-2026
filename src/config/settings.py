import os
from dataclasses import dataclass


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    postgres_db: str = os.getenv("POSTGRES_DB", "neo_invest")
    postgres_user: str = os.getenv("POSTGRES_USER", "neo")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "neo_pass")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))

    moex_base_url: str = os.getenv("MOEX_BASE_URL", "https://iss.moex.com/iss")
    moex_timeout_sec: int = int(os.getenv("MOEX_TIMEOUT_SEC", "20"))
    moex_poll_seconds: int = int(os.getenv("MOEX_POLL_SECONDS", "10"))
    moex_tickers: list[str] = _split_csv(
        os.getenv("MOEX_TICKERS", "SBER,GAZP,LKOH,YNDX,VTBR,ROSN,NVTK")
    )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
