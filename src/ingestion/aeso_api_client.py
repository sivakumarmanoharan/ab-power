import time
from datetime import date

import requests 

BASE_URL ="https://apimgw.aeso.ca/public"

ENDPOINTS = {
    "pool_price": ("poolprice-api/v1.1/price/poolPrice", "range"),
    "load": ("actualforecast-api/v1/load/albertaInternalLoad", "range"),
    "generation": ("currentsupplydemand-api/v1/csd/generation/assets/current", "current"),
    "assets": ("assetlist-api/v1/assetlist", "current"),
}

RETRYABLE_VALUES = {429,500,502,503,504}

class AESOClient:
    def __init__(
        self,
        api_key:str,
        max_retries:int=4,
        timeout:int=60
    ):
        if not api_key:
            raise ValueError("AESO API Key required. If not there, please check docs of AESO website and store in ab_power secret scope")
        self.session = requests.Session()
        self.session.headers.update(
            {'API-KEY': api_key,
            'Accept': 'application/json'}
        )
        self.max_retries = max_retries
        self.timeout=timeout

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{BASE_URL}/{path}"
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                # Network-level failure: retry unless this was the last attempt
                if attempt == self.max_retries:
                    raise RuntimeError(f"Network failure calling {url}") from exc
            else:
                # Runs only when the request itself succeeded
                if response.status_code == 200:
                    return response.json()
                if response.status_code not in RETRYABLE_VALUES or attempt == self.max_retries:
                    raise RuntimeError(
                        f"AESO API {response.status_code} for {url}: {response.text[:300]}"
                    )
            time.sleep(2 ** attempt)  # back off, then retry
        raise RuntimeError(f"Exhausted retries for {url}")
    def fetch(self, 
              dataset: str, 
              start: date | None = None, 
              end: date | None = None
    ) -> dict:
        if dataset not in ENDPOINTS:
            raise KeyError(f"Unknown dataset '{dataset}'. Options: {list(ENDPOINTS)}")
        path, mode = ENDPOINTS[dataset]
        if mode == "range":
            if start is None:
                raise ValueError(f"'{dataset}' needs a start date")
            params = {"startDate": start.isoformat()}
            if end is not None:
                params["endDate"] = end.isoformat()
            return self._get(path, params)
        return self._get(path)

        
