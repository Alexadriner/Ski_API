import argparse
import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests

from scripts.website_scrapers.kreuzberg.scraper import KreuzbergScraper


ROOT_DIR = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT_DIR / "checkpoints" / "website_scrapers" / "kreuzberg"
LOG_DIR = ROOT_DIR / "logs" / "website_scrapers" / "kreuzberg"
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")
API_KEY = os.getenv("API_KEY", "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA")
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}


def configure_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"collector_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def append_jsonl(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=True) + "\n")


def api_get(path: str) -> dict | list:
    url = f"{API_BASE_URL}{path}"
    r = requests.get(url, params={"api_key": API_KEY}, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def api_put(path: str, payload: dict) -> None:
    url = f"{API_BASE_URL}{path}"
    r = requests.put(url, json=payload, headers=HEADERS, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT {path} failed: {r.status_code} {r.text}")


def api_post(path: str, payload: dict) -> dict | None:
    url = f"{API_BASE_URL}{path}"
    r = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"POST {path} failed: {r.status_code} {r.text}")
    if not r.text.strip():
        return None
    try:
        return r.json()
    except ValueError:
        return None


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    text = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_datetime(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text or text == "--":
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def sync_resort_status_to_api(resort_id: str, snapshot: dict) -> None:
    resort = api_get(f"/resorts/{resort_id}")
    live = snapshot.get("resort", {})

    payload = {
        "name": resort.get("name"),
        "country": (resort.get("geography") or {}).get("country") or resort.get("country"),
        "region": (resort.get("geography") or {}).get("region") or resort.get("region"),
        "continent": (resort.get("geography") or {}).get("continent") or resort.get("continent"),
        "latitude": ((resort.get("geography") or {}).get("coordinates") or {}).get("latitude")
        if resort.get("geography")
        else resort.get("latitude"),
        "longitude": ((resort.get("geography") or {}).get("coordinates") or {}).get("longitude")
        if resort.get("geography")
        else resort.get("longitude"),
        "village_altitude_m": (resort.get("altitude") or {}).get("village_m")
        if resort.get("altitude")
        else resort.get("village_altitude_m"),
        "min_altitude_m": (resort.get("altitude") or {}).get("min_m")
        if resort.get("altitude")
        else resort.get("min_altitude_m"),
        "max_altitude_m": (resort.get("altitude") or {}).get("max_m")
        if resort.get("altitude")
        else resort.get("max_altitude_m"),
        "ski_area_name": (resort.get("ski_area") or {}).get("name")
        if resort.get("ski_area")
        else resort.get("ski_area_name"),
        "ski_area_type": (resort.get("ski_area") or {}).get("area_type")
        if resort.get("ski_area")
        else resort.get("ski_area_type")
        or "alpine",
        "official_website": (resort.get("sources") or {}).get("official_website") or live.get("official_website"),
        "lift_status_url": live.get("lift_status_url"),
        "slope_status_url": live.get("slope_status_url"),
        "snow_report_url": live.get("snow_report_url"),
        "weather_url": live.get("weather_url"),
        "status_provider": live.get("status_provider"),
        "status_last_scraped_at": normalize_datetime(live.get("status_last_scraped_at")),
        "lifts_open_count": live.get("lifts_open_count"),
        "slopes_open_count": live.get("slopes_open_count"),
        "snow_depth_valley_cm": live.get("snow_depth_valley_cm"),
        "snow_depth_mountain_cm": live.get("snow_depth_mountain_cm"),
        "new_snow_24h_cm": live.get("new_snow_24h_cm"),
        "temperature_valley_c": live.get("temperature_valley_c"),
        "temperature_mountain_c": live.get("temperature_mountain_c"),
    }

    api_put(f"/resorts/{resort_id}", payload)


def build_lift_payload(existing: dict, scraped: dict, resort_id: str) -> dict:
    display = existing.get("display") or {}
    geometry = existing.get("geometry") or {}
    start = geometry.get("start") or {}
    end = geometry.get("end") or {}
    specs = existing.get("specs") or {}
    source = existing.get("source") or {}
    status = existing.get("status") or {}

    return {
        "resort_id": resort_id,
        "name": existing.get("name"),
        "lift_type": display.get("lift_type") or existing.get("lift_type"),
        "capacity_per_hour": specs.get("capacity_per_hour"),
        "seats": specs.get("seats"),
        "bubble": specs.get("bubble"),
        "heated_seats": specs.get("heated_seats"),
        "year_built": specs.get("year_built"),
        "altitude_start_m": specs.get("altitude_start_m"),
        "altitude_end_m": specs.get("altitude_end_m"),
        "lat_start": start.get("latitude"),
        "lon_start": start.get("longitude"),
        "lat_end": end.get("latitude"),
        "lon_end": end.get("longitude"),
        "source_system": source.get("system") or existing.get("source_system"),
        "source_entity_id": source.get("entity_id"),
        "name_normalized": display.get("normalized_name") or existing.get("name_normalized"),
        "operational_status": scraped.get("operational_status") or status.get("operational_status") or "unknown",
        "operational_note": scraped.get("operational_note") or status.get("note"),
        "planned_open_time": status.get("planned_open_time"),
        "planned_close_time": status.get("planned_close_time"),
        "status_updated_at": normalize_datetime(scraped.get("status_updated_at"))
        or normalize_datetime(status.get("updated_at")),
        "status_source_url": scraped.get("status_source_url") or source.get("source_url"),
    }


def build_new_lift_payload(scraped: dict, resort_id: str) -> dict:
    name = scraped.get("name")
    return {
        "resort_id": resort_id,
        "name": name,
        "lift_type": "draglift",
        "capacity_per_hour": None,
        "seats": None,
        "bubble": False,
        "heated_seats": False,
        "year_built": None,
        "altitude_start_m": None,
        "altitude_end_m": None,
        "lat_start": None,
        "lon_start": None,
        "lat_end": None,
        "lon_end": None,
        "source_system": "osm",
        "source_entity_id": scraped.get("source_entity_id"),
        "name_normalized": normalize_name(name) or None,
        "operational_status": scraped.get("operational_status") or "unknown",
        "operational_note": scraped.get("operational_note"),
        "planned_open_time": None,
        "planned_close_time": None,
        "status_updated_at": normalize_datetime(scraped.get("status_updated_at")),
        "status_source_url": scraped.get("status_source_url"),
    }


def sync_lifts_to_api(resort_id: str, snapshot: dict) -> None:
    existing_lifts = api_get(f"/lifts/by_resort/{resort_id}")

    lifts_by_source = {}
    lifts_by_name = {}
    for item in existing_lifts if isinstance(existing_lifts, list) else []:
        source_id = ((item.get("source") or {}).get("entity_id") or "").strip()
        name = normalize_name(item.get("name"))
        if source_id:
            lifts_by_source[source_id] = item
        if name:
            lifts_by_name[name] = item

    updates = 0
    creates = 0
    for scraped in snapshot.get("lifts", []):
        source_id = (scraped.get("source_entity_id") or "").strip()
        name_key = normalize_name(scraped.get("name"))
        existing = lifts_by_source.get(source_id) or lifts_by_name.get(name_key)
        if not existing:
            try:
                payload = build_new_lift_payload(scraped, resort_id)
                created = api_post("/lifts", payload) or {}
                creates += 1
                created_id = created.get("id")
                logging.info(
                    "Created new lift for %s: name=%s source_entity_id=%s id=%s",
                    resort_id,
                    scraped.get("name"),
                    source_id or None,
                    created_id,
                )
            except Exception as exc:
                logging.warning(
                    "Lift create failed for resort=%s name=%s source_entity_id=%s: %s",
                    resort_id,
                    scraped.get("name"),
                    source_id or None,
                    exc,
                )
            continue

        payload = build_lift_payload(existing, scraped, resort_id)
        current_status = (existing.get("status") or {}).get("operational_status")
        current_note = (existing.get("status") or {}).get("note")
        if (
            payload.get("operational_status") == current_status
            and (payload.get("operational_note") or None) == (current_note or None)
        ):
            continue

        try:
            api_put(f"/lifts/{existing['id']}", payload)
            updates += 1
        except Exception as exc:
            logging.warning(
                "Lift sync failed for id=%s name=%s: %s",
                existing.get("id"),
                existing.get("name"),
                exc,
            )

    logging.info("API sync updated %s lifts and created %s lifts for %s", updates, creates, resort_id)


def run_collection_loop(resort_id: str, interval_seconds: int, once: bool, sync_api: bool) -> None:
    scraper = KreuzbergScraper()
    output_file = OUT_DIR / f"{resort_id}_status.jsonl"

    while True:
        started = time.time()
        try:
            payload = scraper.run(resort_id=resort_id)
            payload["meta"]["collected_at_utc"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            append_jsonl(output_file, payload)
            if sync_api:
                sync_resort_status_to_api(resort_id, payload)
                sync_lifts_to_api(resort_id, payload)
            logging.info(
                "Collected snapshot for %s: lifts_open=%s lifts_total=%s",
                resort_id,
                payload.get("resort", {}).get("lifts_open_count"),
                len(payload.get("lifts", [])),
            )
        except Exception as exc:
            logging.exception("Collector iteration failed: %s", exc)

        if once:
            break

        elapsed = time.time() - started
        sleep_seconds = max(0, interval_seconds - int(elapsed))
        time.sleep(sleep_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect Kreuzberg website status snapshots every N seconds."
    )
    parser.add_argument("--resort-id", default="kreuzberg")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-sync-api", action="store_true")
    args = parser.parse_args()

    configure_logging()
    run_collection_loop(
        resort_id=args.resort_id,
        interval_seconds=max(60, args.interval_seconds),
        once=args.once,
        sync_api=not args.no_sync_api,
    )


if __name__ == "__main__":
    main()
