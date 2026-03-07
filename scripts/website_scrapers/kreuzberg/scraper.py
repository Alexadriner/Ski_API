import logging
import re
import unicodedata
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

from scripts.website_scrapers.base import ScraperConfig, WebsiteScraperBase


class KreuzbergScraper(WebsiteScraperBase):
    """
    Scraper for https://www.skilifte-kreuzberg.de/

    Source currently exposes lift status directly in homepage HTML:
    - Lift names in <h5> tags (Blicklift, Rothang, Dreitannen)
    - Status text below each lift (e.g. "geoeffnet"/"geschlossen")
    """

    HOME_PATH = "/"

    def __init__(self) -> None:
        super().__init__(
            ScraperConfig(
                scraper_name="kreuzberg",
                base_url="https://www.skilifte-kreuzberg.de",
            )
        )
        self.logger = logging.getLogger("website_scraper.kreuzberg")
        self._robot_parser = RobotFileParser()
        self._robots_loaded = False
        self._robots_unavailable = False

    def fetch_raw_payload(self, resort_id: str) -> dict[str, Any]:
        home_url = urljoin(self.config.base_url, self.HOME_PATH)
        self._load_robots()

        if not self._is_allowed(home_url):
            self.logger.warning("robots.txt disallows %s", home_url)
            return {"home_url": home_url, "html": ""}

        html = self.get_html(home_url)
        return {"home_url": home_url, "html": html}

    def normalize_payload(self, resort_id: str, raw_payload: dict[str, Any]) -> dict[str, Any]:
        html = raw_payload.get("html") or ""
        home_url = raw_payload.get("home_url") or urljoin(self.config.base_url, self.HOME_PATH)

        lifts = self._extract_lifts(html, home_url)
        weather = self._extract_weather_metrics(html)
        note, note_date = self._extract_news(html)
        lifts_open_count = sum(1 for lift in lifts if lift.get("operational_status") == "open")

        snow_min_cm, snow_max_cm = self._parse_snow_range_cm(weather.get("snow_depth_text"))

        resort = {
            "official_website": self.config.base_url,
            "lift_status_url": home_url,
            "slope_status_url": home_url,
            "snow_report_url": home_url,
            "weather_url": home_url,
            "status_provider": "skilifte_kreuzberg_homepage",
            "status_last_scraped_at": note_date,
            "lifts_open_count": lifts_open_count,
            "slopes_open_count": None,
            "snow_depth_valley_cm": snow_min_cm,
            "snow_depth_mountain_cm": snow_max_cm,
            "new_snow_24h_cm": None,
            "temperature_valley_c": self._parse_temperature_c(weather.get("temperature_text")),
            "temperature_mountain_c": None,
        }

        if note:
            resort["status_note"] = note

        return {"resort": resort, "lifts": lifts, "slopes": []}

    def _extract_lifts(self, html: str, status_url: str) -> list[dict[str, Any]]:
        pattern = re.compile(
            r"<h5[^>]*>\s*(?P<name>[^<]+?)\s*</h5>.*?"
            r"<p[^>]*>\s*(?P<status>[^<]+?)\s*</p>",
            flags=re.IGNORECASE | re.DOTALL,
        )

        rows: list[dict[str, Any]] = []
        seen: set[str] = set()

        for match in pattern.finditer(html):
            name = self._clean_text(match.group("name"))
            status_raw = self._clean_text(match.group("status"))
            if not name:
                continue

            normalized_name = self._normalize_for_match(name)
            if normalized_name not in {"blicklift", "rothang", "dreitannen"}:
                continue
            if normalized_name in seen:
                continue

            seen.add(normalized_name)
            rows.append(
                {
                    "source_entity_id": normalized_name,
                    "name": name,
                    "operational_status": self._map_lift_status(status_raw),
                    "operational_note": status_raw or None,
                    "status_updated_at": None,
                    "status_source_url": status_url,
                }
            )

        return rows

    def _extract_weather_metrics(self, html: str) -> dict[str, str | None]:
        # Four metrics are shown in one row; currently:
        # weather icon text, visibility, temperature, snow depth.
        values = re.findall(
            r'<div class="col-3 text-center"[^>]*>.*?<p[^>]*>\s*(.*?)\s*</p>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        clean_values = [self._clean_text(v) for v in values if self._clean_text(v)]

        return {
            "weather_text": clean_values[0] if len(clean_values) > 0 else None,
            "visibility_text": clean_values[1] if len(clean_values) > 1 else None,
            "temperature_text": clean_values[2] if len(clean_values) > 2 else None,
            "snow_depth_text": clean_values[3] if len(clean_values) > 3 else None,
        }

    def _extract_news(self, html: str) -> tuple[str | None, str | None]:
        note_match = re.search(
            r"<h2[^>]*>\s*Neues\s*</h2>\s*<p[^>]*>(?P<note>.*?)</p>",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        note = self._clean_text(note_match.group("note")) if note_match else None

        date_match = re.search(
            r"<p[^>]*>\s*(?P<date>\d{2}\.\d{2}\.\d{4})\s*</p>",
            html,
            flags=re.IGNORECASE,
        )
        date_value = None
        if date_match:
            try:
                dt = datetime.strptime(date_match.group("date"), "%d.%m.%Y")
                date_value = dt.strftime("%Y-%m-%d 00:00:00")
            except ValueError:
                date_value = None

        return note, date_value

    def _map_lift_status(self, value: str | None) -> str:
        text = self._normalize_for_match(value or "")
        if "offen" in text or "geoffnet" in text:
            return "open"
        if "zu" == text or "geschlossen" in text:
            return "closed"
        if "spater" in text or "spaeter" in text or "geplant" in text:
            return "scheduled"
        return "unknown"

    def _parse_temperature_c(self, value: str | None) -> float | None:
        if not value:
            return None
        match = re.search(r"-?\d+(?:[.,]\d+)?", value)
        if not match:
            return None
        return float(match.group(0).replace(",", "."))

    def _parse_snow_range_cm(self, value: str | None) -> tuple[int | None, int | None]:
        if not value:
            return None, None
        numbers = [int(n) for n in re.findall(r"\d+", value)]
        if not numbers:
            return None, None
        if len(numbers) == 1:
            return numbers[0], numbers[0]
        return min(numbers), max(numbers)

    def _clean_text(self, value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value)
        text = unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _normalize_for_match(self, value: str) -> str:
        text = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
            .lower()
            .strip()
        )
        return re.sub(r"[^a-z0-9]+", "", text)

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
        except Exception as exc:
            self.logger.warning("Could not load robots.txt (%s): %s", robots_url, exc)
            self._robots_unavailable = True
        finally:
            self._robots_loaded = True

    def _is_allowed(self, url: str) -> bool:
        if self._robots_unavailable:
            return True
        ua = self.config.user_agent
        if self._robot_parser.can_fetch(ua, url):
            return True
        return self._robot_parser.can_fetch("*", url)
