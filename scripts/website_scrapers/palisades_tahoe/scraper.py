import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

from scripts.website_scrapers.base import ScraperConfig, WebsiteScraperBase


class PalisadesTahoeScraper(WebsiteScraperBase):
    """
    Scraper for https://www.palisadestahoe.com/mountain-information/mountain-report

    Data flow discovered from site source:
    - mountain-report HTML contains liftsAndTrailsConfig with:
      - liftsAndTrailsBuilderBasePath (v4.mtnfeed.com)
      - resortPath (palisades-tahoe)
    - v4 config contains bearer token and resortIds
    - live feed is loaded from mtnpowder feed API
    """

    MOUNTAIN_REPORT_PATH = "/mountain-information/mountain-report"
    FALLBACK_BUILDER_BASE = "https://v4.mtnfeed.com/"
    FALLBACK_RESORT_PATH = "palisades-tahoe"

    def __init__(self) -> None:
        super().__init__(
            ScraperConfig(
                scraper_name="palisades_tahoe",
                base_url="https://www.palisadestahoe.com",
            )
        )
        self.logger = logging.getLogger("website_scraper.palisades_tahoe")
        self._robot_parser = RobotFileParser()
        self._robots_loaded = False

    def fetch_raw_payload(self, resort_id: str) -> dict[str, Any]:
        report_url = urljoin(self.config.base_url, self.MOUNTAIN_REPORT_PATH)
        self._load_robots()

        if not self._is_allowed(report_url):
            self.logger.warning("robots.txt disallows %s", report_url)
            return {
                "report_url": report_url,
                "feed_json": None,
                "builder_base": None,
                "resort_path": None,
            }

        html = self.get_html(report_url)
        cfg = self._extract_mtnfeed_config(html)
        builder_base = cfg.get("builder_base") or self.FALLBACK_BUILDER_BASE
        resort_path = cfg.get("resort_path") or self.FALLBACK_RESORT_PATH

        resort_cfg_url = urljoin(builder_base, f"./resorts/{resort_path}.json")
        resort_cfg = self.get_json(resort_cfg_url)

        bearer = resort_cfg.get("bearerToken")
        resort_ids = resort_cfg.get("resortIds") or []
        if not bearer or not resort_ids:
            self.logger.warning("Missing bearer/resortIds in %s", resort_cfg_url)
            return {
                "report_url": report_url,
                "feed_json": None,
                "builder_base": builder_base,
                "resort_path": resort_path,
                "resort_cfg": resort_cfg,
            }

        feed_json = self.get_json(
            "https://mtnpowder.com/feed/v3.json",
            params={"bearer_token": bearer, "resortId[]": resort_ids},
        )

        return {
            "report_url": report_url,
            "feed_json": feed_json,
            "builder_base": builder_base,
            "resort_path": resort_path,
            "resort_cfg": resort_cfg,
        }

    def normalize_payload(self, resort_id: str, raw_payload: dict[str, Any]) -> dict[str, Any]:
        report_url = raw_payload.get("report_url")
        feed = raw_payload.get("feed_json") or {}
        resorts = feed.get("Resorts") or []
        data = resorts[0] if resorts else {}

        snow = data.get("SnowReport") or {}
        current = data.get("CurrentConditions") or {}

        resort = {
            "official_website": self.config.base_url,
            "lift_status_url": report_url,
            "slope_status_url": report_url,
            "snow_report_url": report_url,
            "weather_url": report_url,
            "status_provider": "palisades_tahoe_mtnfeed",
            "status_last_scraped_at": feed.get("LastUpdate"),
            "lifts_open_count": self._to_int(snow.get("TotalOpenLifts")),
            "slopes_open_count": self._to_int(snow.get("TotalOpenTrails")),
            "snow_depth_valley_cm": self._to_int((snow.get("BaseArea") or {}).get("BaseCm")),
            "snow_depth_mountain_cm": self._resolve_mountain_base_cm(snow),
            "new_snow_24h_cm": self._to_int((snow.get("AllMountain") or {}).get("Last24HoursCm")),
            "temperature_valley_c": self._to_float((current.get("Base") or {}).get("TemperatureC")),
            "temperature_mountain_c": self._to_float(
                (current.get("MidMountain") or {}).get("TemperatureC")
                or (current.get("Summit") or {}).get("TemperatureC")
            ),
        }

        lifts: list[dict[str, Any]] = []
        slopes: list[dict[str, Any]] = []

        for area in data.get("MountainAreas") or []:
            for lift in area.get("Lifts") or []:
                hours = self._extract_today_hours(lift.get("Hours"))
                lifts.append(
                    {
                        "source_entity_id": self._to_str(lift.get("Id")),
                        "name": lift.get("Name"),
                        "operational_status": self._map_lift_status(
                            lift.get("StatusEnglish") or lift.get("Status")
                        ),
                        "operational_note": lift.get("Status"),
                        "planned_open_time": hours.get("open"),
                        "planned_close_time": hours.get("close"),
                        "status_updated_at": lift.get("UpdateDate"),
                        "status_source_url": report_url,
                        "wait_time_min": self._to_int(lift.get("WaitTime")),
                    }
                )

            for trail in area.get("Trails") or []:
                slopes.append(
                    {
                        "source_entity_id": self._to_str(trail.get("Id")),
                        "name": trail.get("Name"),
                        "operational_status": self._map_slope_status(
                            trail.get("StatusEnglish") or trail.get("Status")
                        ),
                        "grooming_status": self._map_grooming_status(trail.get("Grooming")),
                        "operational_note": trail.get("Status"),
                        "status_updated_at": trail.get("UpdateDate"),
                        "status_source_url": report_url,
                    }
                )

        lifts = self._deduplicate_entities(lifts)
        slopes = self._deduplicate_entities(slopes)

        return {"resort": resort, "lifts": lifts, "slopes": slopes}

    def _extract_mtnfeed_config(self, html: str) -> dict[str, str | None]:
        base_match = re.search(
            r'liftsAndTrailsBuilderBasePath:\s*"([^"]+)"', html, re.IGNORECASE
        )
        resort_match = re.search(r'resortPath:\s*"([^"]+)"', html, re.IGNORECASE)
        return {
            "builder_base": base_match.group(1).strip() if base_match else None,
            "resort_path": resort_match.group(1).strip() if resort_match else None,
        }

    def _load_robots(self) -> None:
        if self._robots_loaded:
            return
        robots_url = urljoin(self.config.base_url, "/robots.txt")
        try:
            self._robot_parser.set_url(robots_url)
            self._robot_parser.read()
            delay = self._robot_parser.crawl_delay(self.config.user_agent)
            if delay is None:
                delay = self._robot_parser.crawl_delay("*")
            if delay is not None:
                self.config.min_request_interval_seconds = max(
                    self.config.min_request_interval_seconds, float(delay)
                )
            self._robots_loaded = True
        except Exception as exc:
            self.logger.warning("Could not load robots.txt (%s): %s", robots_url, exc)
            self._robots_loaded = True

    def _is_allowed(self, url: str) -> bool:
        ua = self.config.user_agent
        if self._robot_parser.can_fetch(ua, url):
            return True
        return self._robot_parser.can_fetch("*", url)

    def _resolve_mountain_base_cm(self, snow: dict[str, Any]) -> int | None:
        mid = self._to_int((snow.get("MidMountainArea") or {}).get("BaseCm"))
        if mid is not None and mid > 0:
            return mid
        summit = self._to_int((snow.get("SummitArea") or {}).get("BaseCm"))
        return summit

    def _extract_today_hours(self, hours: Any) -> dict[str, Any]:
        if not isinstance(hours, dict):
            return {"open": None, "close": None}
        weekday = datetime.utcnow().strftime("%A")
        # Feed uses Sunday..Saturday keys.
        day_hours = hours.get(weekday) or {}
        if not isinstance(day_hours, dict):
            return {"open": None, "close": None}
        open_v = day_hours.get("Open")
        close_v = day_hours.get("Close")
        return {
            "open": open_v if self._is_time_like(open_v) else None,
            "close": close_v if self._is_time_like(close_v) else None,
        }

    def _is_time_like(self, value: Any) -> bool:
        if value is None:
            return False
        text = str(value).strip()
        if not text or text == "--":
            return False
        return bool(re.search(r"\d", text))

    def _map_lift_status(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if "open" in text:
            return "open"
        if "hold" in text or "delay" in text:
            return "hold"
        if "sched" in text:
            return "scheduled"
        if "closed" in text:
            return "closed"
        return "unknown"

    def _map_slope_status(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if "open" in text:
            return "open"
        if "partial" in text or "limited" in text:
            return "partial"
        if "groom" in text:
            return "grooming"
        if "sched" in text:
            return "scheduled"
        if "closed" in text:
            return "closed"
        return "unknown"

    def _map_grooming_status(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in ("yes", "true", "1"):
            return "groomed"
        if text in ("no", "false", "0"):
            return "not_groomed"
        if "groom" in text and "not" not in text:
            return "groomed"
        return "unknown"

    def _to_int(self, value: Any) -> int | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text == "--":
            return None
        if "-" in text and " - " in text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text == "--":
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _to_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _deduplicate_entities(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for row in rows:
            source_id = row.get("source_entity_id") or ""
            name = str(row.get("name") or "").strip().lower()
            key = f"{source_id}|{name}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped
