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

from scripts.website_scrapers.palisades_tahoe.scraper import PalisadesTahoeScraper


ROOT_DIR = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT_DIR / "checkpoints" / "website_scrapers" / "palisades_tahoe"
LOG_DIR = ROOT_DIR / "logs" / "website_scrapers" / "palisades_tahoe"
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")
API_KEY = os.getenv("API_KEY", "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA")
HEADERS = {"Content-Type": "application/json"}


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
    r = requests.put(url, params={"api_key": API_KEY}, json=payload, headers=HEADERS, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT {path} failed: {r.status_code} {r.text}")

def api_delete(path: str) -> None:
    url = f"{API_BASE_URL}{path}"
    r = requests.delete(url, params={"api_key": API_KEY}, headers=HEADERS, timeout=30)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"DELETE {path} failed: {r.status_code} {r.text}")


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


def resolve_by_name(scraped_name: str | None, by_name: dict[str, dict]) -> dict | None:
    key = normalize_name(scraped_name)
    if not key:
        return None
    if key in by_name:
        return by_name[key]

    # Fuzzy fallback: if exactly one near-match exists.
    candidates = []
    for existing_key, existing in by_name.items():
        if key in existing_key or existing_key in key:
            candidates.append(existing)
    if len(candidates) == 1:
        return candidates[0]
    return None


def normalize_time(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text or text == "--":
        return None

    for fmt in ("%I:%M %p", "%I:%M:%S %p", "%H:%M", "%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%H:%M:%S")
        except ValueError:
            pass

    return None


def normalize_datetime(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text or text == "--":
        return None

    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
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
        "official_website": (resort.get("sources") or {}).get("official_website"),
        "lift_status_url": (resort.get("sources") or {}).get("lift_status_url") or live.get("lift_status_url"),
        "slope_status_url": (resort.get("sources") or {}).get("slope_status_url") or live.get("slope_status_url"),
        "snow_report_url": (resort.get("sources") or {}).get("snow_report_url") or live.get("snow_report_url"),
        "weather_url": (resort.get("sources") or {}).get("weather_url") or live.get("weather_url"),
        "status_provider": live.get("status_provider") or (resort.get("sources") or {}).get("status_provider"),
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
        "lift_type": display.get("lift_type") or "chairlift",
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
        "source_system": source.get("system") or "website",
        "source_entity_id": source.get("entity_id"),
        "name_normalized": display.get("normalized_name") or normalize_name(existing.get("name")),
        "operational_status": scraped.get("operational_status") or status.get("operational_status") or "unknown",
        "operational_note": scraped.get("operational_note") or status.get("note"),
        "planned_open_time": normalize_time(scraped.get("planned_open_time")) or normalize_time(
            status.get("planned_open_time")
        ),
        "planned_close_time": normalize_time(scraped.get("planned_close_time")) or normalize_time(
            status.get("planned_close_time")
        ),
        "status_updated_at": normalize_datetime(scraped.get("status_updated_at"))
        or normalize_datetime(status.get("updated_at")),
        "status_source_url": scraped.get("status_source_url") or source.get("source_url"),
    }


def build_slope_payload(existing: dict, scraped: dict, resort_id: str) -> dict:
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
        "difficulty": display.get("difficulty") or "blue",
        "length_m": specs.get("length_m"),
        "vertical_drop_m": specs.get("vertical_drop_m"),
        "average_gradient": specs.get("average_gradient"),
        "max_gradient": specs.get("max_gradient"),
        "snowmaking": specs.get("snowmaking"),
        "night_skiing": specs.get("night_skiing"),
        "family_friendly": specs.get("family_friendly"),
        "race_slope": specs.get("race_slope"),
        "lat_start": start.get("latitude"),
        "lon_start": start.get("longitude"),
        "lat_end": end.get("latitude"),
        "lon_end": end.get("longitude"),
        "source_system": source.get("system") or "website",
        "source_entity_id": source.get("entity_id"),
        "name_normalized": display.get("normalized_name") or normalize_name(existing.get("name")),
        "operational_status": scraped.get("operational_status") or status.get("operational_status") or "unknown",
        "grooming_status": scraped.get("grooming_status") or status.get("grooming_status") or "unknown",
        "operational_note": scraped.get("operational_note") or status.get("note"),
        "status_updated_at": normalize_datetime(scraped.get("status_updated_at"))
        or normalize_datetime(status.get("updated_at")),
        "status_source_url": scraped.get("status_source_url") or source.get("source_url"),
    }


def sync_entities_to_api(resort_id: str, snapshot: dict) -> None:
    existing_lifts = api_get(f"/lifts/by_resort/{resort_id}")
    existing_slopes = api_get(f"/slopes/by_resort/{resort_id}")

    lifts_by_source = {}
    lifts_by_name = {}
    for item in existing_lifts if isinstance(existing_lifts, list) else []:
        source_id = ((item.get("source") or {}).get("entity_id") or "").strip()
        name = normalize_name(item.get("name"))
        if source_id:
            lifts_by_source[source_id] = item
        if name:
            lifts_by_name[name] = item

    slopes_by_source = {}
    slopes_by_name = {}
    for item in existing_slopes if isinstance(existing_slopes, list) else []:
        source_id = ((item.get("source") or {}).get("entity_id") or "").strip()
        name = normalize_name(item.get("name"))
        if source_id:
            slopes_by_source[source_id] = item
        if name:
            slopes_by_name[name] = item

    lift_updates = 0
    slope_updates = 0
    matched_lift_ids: set[int] = set()
    matched_slope_ids: set[int] = set()

    for scraped in snapshot.get("lifts", []):
        source_id = (scraped.get("source_entity_id") or "").strip()
        name_key = normalize_name(scraped.get("name"))
        existing = lifts_by_source.get(source_id) or lifts_by_name.get(name_key) or resolve_by_name(
            scraped.get("name"), lifts_by_name
        )
        if not existing:
            continue
        matched_lift_ids.add(existing["id"])
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
            lift_updates += 1
        except Exception as exc:
            logging.warning(
                "Lift sync failed for id=%s name=%s: %s",
                existing.get("id"),
                existing.get("name"),
                exc,
            )

    for scraped in snapshot.get("slopes", []):
        source_id = (scraped.get("source_entity_id") or "").strip()
        name_key = normalize_name(scraped.get("name"))
        existing = slopes_by_source.get(source_id) or slopes_by_name.get(name_key) or resolve_by_name(
            scraped.get("name"), slopes_by_name
        )
        if not existing:
            continue
        matched_slope_ids.add(existing["id"])
        payload = build_slope_payload(existing, scraped, resort_id)
        current = existing.get("status") or {}
        if (
            payload.get("operational_status") == current.get("operational_status")
            and payload.get("grooming_status") == current.get("grooming_status")
            and (payload.get("operational_note") or None) == (current.get("note") or None)
        ):
            continue
        try:
            api_put(f"/slopes/{existing['id']}", payload)
            slope_updates += 1
        except Exception as exc:
            logging.warning(
                "Slope sync failed for id=%s name=%s: %s",
                existing.get("id"),
                existing.get("name"),
                exc,
            )

    # Remove unmatched entities for this resort when they still have no useful status.
    # This keeps the resort clean from OSM leftovers that are not part of website feed.
    deleted_lifts = 0
    deleted_slopes = 0

    for item in existing_lifts if isinstance(existing_lifts, list) else []:
        item_id = item.get("id")
        if item_id in matched_lift_ids:
            continue
        status = ((item.get("status") or {}).get("operational_status") or "").strip().lower()
        has_status = status not in ("", "unknown")
        if has_status:
            continue
        try:
            api_delete(f"/lifts/{item_id}")
            deleted_lifts += 1
        except Exception as exc:
            logging.warning("Lift delete failed for id=%s name=%s: %s", item_id, item.get("name"), exc)

    for item in existing_slopes if isinstance(existing_slopes, list) else []:
        item_id = item.get("id")
        if item_id in matched_slope_ids:
            continue
        status_obj = item.get("status") or {}
        status = (status_obj.get("operational_status") or "").strip().lower()
        grooming = (status_obj.get("grooming_status") or "").strip().lower()
        has_status = (status not in ("", "unknown")) or (grooming not in ("", "unknown"))
        if has_status:
            continue
        try:
            api_delete(f"/slopes/{item_id}")
            deleted_slopes += 1
        except Exception as exc:
            logging.warning(
                "Slope delete failed for id=%s name=%s: %s", item_id, item.get("name"), exc
            )

    logging.info(
        "API sync updated %s lifts and %s slopes for %s (deleted %s lifts / %s slopes)",
        lift_updates,
        slope_updates,
        resort_id,
        deleted_lifts,
        deleted_slopes,
    )


def run_collection_loop(resort_id: str, interval_seconds: int, once: bool, sync_api: bool) -> None:
    scraper = PalisadesTahoeScraper()
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
                sync_entities_to_api(resort_id, payload)
            logging.info(
                "Collected snapshot for %s: lifts_open=%s slopes_open=%s",
                resort_id,
                payload.get("resort", {}).get("lifts_open_count"),
                payload.get("resort", {}).get("slopes_open_count"),
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
        description="Collect Palisades Tahoe website status snapshots every N seconds."
    )
    parser.add_argument("--resort-id", default="palisades-tahoe")
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
