from typing import Any

import requests

from src.config.settings import settings


class MoexClient:
    def __init__(self, base_url: str | None = None, timeout_sec: int | None = None) -> None:
        self.base_url = base_url or settings.moex_base_url
        self.timeout_sec = timeout_sec or settings.moex_timeout_sec
        self.session = requests.Session()

    def get_history(self, ticker: str, date_from: str, date_till: str) -> dict[str, Any]:
        url = (
            f"{self.base_url}/history/engines/stock/markets/shares/"
            f"securities/{ticker}.json"
        )
        params = {"from": date_from, "till": date_till, "iss.meta": "off"}
        response = self.session.get(url, params=params, timeout=self.timeout_sec)
        response.raise_for_status()
        return response.json()

    def get_security(self, ticker: str) -> dict[str, Any]:
        url = f"{self.base_url}/engines/stock/markets/shares/securities/{ticker}.json"
        params = {"iss.meta": "off"}
        response = self.session.get(url, params=params, timeout=self.timeout_sec)
        response.raise_for_status()
        return response.json()
