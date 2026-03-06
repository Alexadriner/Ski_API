import abc
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests


LIFT_STATUS_VALUES = {"open", "closed", "hold", "scheduled", "unknown"}
SLOPE_STATUS_VALUES = {"open", "closed", "partial", "grooming", "scheduled", "unknown"}
GROOMING_STATUS_VALUES = {"groomed", "not_groomed", "unknown"}


@dataclass
class ScraperConfig:
    scraper_name: str
    base_url: str
    timeout_seconds: int = 20
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5
    min_request_interval_seconds: float = 0.6
    user_agent: str = "SkiAPIWebsiteScraper/1.0 (+https://local.ski-api)"


class WebsiteScraperBase(abc.ABC):
    """
    Base class for all resort website scrapers.

    Rules enforced by this base class:
    - Always rate-limit outgoing requests.
    - Always retry transient request failures.
    - Always normalize output to one common schema.
    - Always validate status enum values before returning data.
    """

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = logging.getLogger(f"website_scraper.{config.scraper_name}")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})
        self._last_request_ts = 0.0

    def run(self, resort_id: str) -> dict[str, Any]:
        """
        Execute scraper pipeline for one resort.

        Returns normalized payload:
        {
          "meta": {...},
          "resort": {...},
          "lifts": [...],
          "slopes": [...]
        }
        """
        started_at = time.time()
        self._validate_resort_id(resort_id)

        raw_payload = self.fetch_raw_payload(resort_id)
        normalized = self.normalize_payload(resort_id, raw_payload)
        self._validate_normalized_payload(normalized)

        normalized["meta"] = {
            "scraper_name": self.config.scraper_name,
            "base_url": self.config.base_url,
            "resort_id": resort_id,
            "scraped_at_unix": int(time.time()),
            "duration_ms": int((time.time() - started_at) * 1000),
        }
        return normalized

    @abc.abstractmethod
    def fetch_raw_payload(self, resort_id: str) -> dict[str, Any]:
        """
        Fetch raw website data (JSON and/or HTML) for one resort.
        Subclasses define endpoints and source-specific rules.
        """

    @abc.abstractmethod
    def normalize_payload(self, resort_id: str, raw_payload: dict[str, Any]) -> dict[str, Any]:
        """
        Convert raw source data into standard output schema.
        """

    def get_json(self, url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self._request("GET", url, params=params)
        return response.json()

    def get_html(self, url: str, *, params: dict[str, Any] | None = None) -> str:
        response = self._request("GET", url, params=params)
        return response.text

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self._respect_rate_limit()
        last_error: Exception | None = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.config.timeout_seconds,
                    **kwargs,
                )

                if response.status_code >= 500:
                    raise requests.HTTPError(
                        f"Server error {response.status_code}: {response.text[:200]}",
                        response=response,
                    )

                response.raise_for_status()
                self._last_request_ts = time.time()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                sleep_s = self.config.retry_backoff_seconds * attempt
                self.logger.warning(
                    "Request failed (attempt %s/%s) for %s: %s",
                    attempt,
                    self.config.max_retries,
                    url,
                    exc,
                )
                time.sleep(sleep_s)

        raise RuntimeError(f"Request failed after retries for {url}: {last_error}")

    def _respect_rate_limit(self) -> None:
        now = time.time()
        delta = now - self._last_request_ts
        min_interval = self.config.min_request_interval_seconds
        if delta < min_interval:
            time.sleep(min_interval - delta)

    def _validate_resort_id(self, resort_id: str) -> None:
        if not resort_id or not isinstance(resort_id, str):
            raise ValueError("resort_id must be a non-empty string")

    def _validate_normalized_payload(self, payload: dict[str, Any]) -> None:
        required_root = {"resort", "lifts", "slopes"}
        missing = required_root.difference(payload.keys())
        if missing:
            raise ValueError(f"Missing required root keys: {sorted(missing)}")

        if not isinstance(payload["lifts"], list):
            raise ValueError("lifts must be a list")
        if not isinstance(payload["slopes"], list):
            raise ValueError("slopes must be a list")
        if not isinstance(payload["resort"], dict):
            raise ValueError("resort must be an object")

        for lift in payload["lifts"]:
            self._validate_lift_entry(lift)

        for slope in payload["slopes"]:
            self._validate_slope_entry(slope)

    def _validate_lift_entry(self, lift: dict[str, Any]) -> None:
        required = {"source_entity_id", "operational_status"}
        missing = required.difference(lift.keys())
        if missing:
            raise ValueError(f"Lift entry missing keys: {sorted(missing)}")

        status = str(lift["operational_status"]).lower()
        if status not in LIFT_STATUS_VALUES:
            raise ValueError(f"Invalid lift operational_status: {status}")

    def _validate_slope_entry(self, slope: dict[str, Any]) -> None:
        required = {"source_entity_id", "operational_status"}
        missing = required.difference(slope.keys())
        if missing:
            raise ValueError(f"Slope entry missing keys: {sorted(missing)}")

        status = str(slope["operational_status"]).lower()
        if status not in SLOPE_STATUS_VALUES:
            raise ValueError(f"Invalid slope operational_status: {status}")

        grooming = slope.get("grooming_status")
        if grooming is not None:
            grooming_value = str(grooming).lower()
            if grooming_value not in GROOMING_STATUS_VALUES:
                raise ValueError(f"Invalid slope grooming_status: {grooming_value}")
