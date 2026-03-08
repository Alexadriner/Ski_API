import json
import logging
import os
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import requests


# =========================
# PATHS
# =========================
ROOT_DIR = Path(__file__).resolve().parents[2]
COORD_DIR = ROOT_DIR / "checkpoints" / "coordinates"

# =========================
# CONFIG
# =========================
API_BASE_URL = "http://localhost:8080"
API_KEY = "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}

SLEEP = 0.2
API_TIMEOUT = 120
API_RETRIES = 4
API_RETRY_BACKOFF = 1.5

# =========================
# LOGGING
# =========================
LOG_DIR = ROOT_DIR / "logs" / "cleanup"
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("cleanup_ski_data")
logger.setLevel(logging.INFO)

# =========================
# CHECKPOINT
# =========================
CHECKPOINT_DIR = ROOT_DIR / "checkpoints" / "cleanup"
CHECKPOINT_FILE = "progress.txt"

os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CHECKPOINT_PATH = CHECKPOINT_DIR / CHECKPOINT_FILE


# =========================
# HELPERS
# =========================
def normalize_name(name):
    if not name:
        return None

    name = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def generate_fallback_name(entity, difficulty=None, lift_type=None, osm_id=None):
    if entity == "lift" and lift_type:
        return f"{lift_type.title()} Lift {osm_id}"

    if entity == "slope" and difficulty:
        return f"{difficulty.title()} Slope {osm_id}"

    return None


def generate_coordinate_name(entity, start_lat=None, start_lon=None, difficulty=None, lift_type=None, osm_id=None):
    if start_lat is None or start_lon is None:
        return generate_fallback_name(entity, difficulty=difficulty, lift_type=lift_type, osm_id=osm_id)

    lat_s = f"{float(start_lat):.5f}"
    lon_s = f"{float(start_lon):.5f}"

    if entity == "lift":
        kind = (lift_type or "unknown").title()
        return f"{kind} Lift [{lat_s},{lon_s}]"

    kind = (difficulty or "unknown").title()
    return f"{kind} Slope [{lat_s},{lon_s}]"


def is_previous_fallback_name(name, entity_type):
    if not name:
        return False

    normalized_entity_type = str(entity_type).lower()
    if normalized_entity_type in ("lift", "lifts"):
        return bool(re.match(r"^[A-Za-z_]+ Lift \d+$", name))

    return bool(re.match(r"^[A-Za-z_]+ Slope \d+$", name))


def build_point_key(lat, lon):
    if lat is None or lon is None:
        return None
    return f"{float(lat):.5f},{float(lon):.5f}"


def build_segment_key(start_lat, start_lon, end_lat, end_lon):
    p1 = build_point_key(start_lat, start_lon)
    p2 = build_point_key(end_lat, end_lon)
    if not p1 and not p2:
        return None
    if not p1:
        p1 = p2
    if not p2:
        p2 = p1
    # Direction-independent key, so reversed geometries count as same segment.
    a, b = sorted([p1, p2])
    return f"{a}|{b}"


def load_coordinate_index():
    by_entity = {"lifts": {}, "slopes": {}}

    if not COORD_DIR.exists():
        logger.warning("No coordinate directory found. Cleanup runs without coordinate enrichment.")
        return by_entity

    files = sorted(COORD_DIR.glob("worker_*.jsonl"))
    loaded = 0

    for file_path in files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue

                    try:
                        item = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    entity_type = item.get("entity_type")
                    if entity_type not in by_entity:
                        continue

                    entity_id = item.get("id")
                    if entity_id is None:
                        continue

                    try:
                        entity_id = int(entity_id)
                    except (TypeError, ValueError):
                        continue

                    by_entity[entity_type][entity_id] = {
                        "start_lat": item.get("start_lat"),
                        "start_lon": item.get("start_lon"),
                        "end_lat": item.get("end_lat"),
                        "end_lon": item.get("end_lon"),
                        "difficulty": item.get("difficulty"),
                        "lift_type": item.get("lift_type"),
                        "name": item.get("name"),
                    }
                    loaded += 1
        except OSError as e:
            logger.warning(f"Could not read coordinate file {file_path}: {e}")

    logger.info(f"Loaded {loaded} coordinate entries from {len(files)} files.")
    return by_entity


def configure_logging(worker_id=None, total_workers=None):
    if logger.handlers:
        return

    suffix = "single"
    if worker_id is not None and total_workers is not None:
        suffix = f"worker_{worker_id}_of_{total_workers}"

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    log_path = LOG_DIR / f"cleanup_{suffix}_{timestamp}.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


def shard_items(items, worker_id, total_workers):
    ordered = sorted(
        items,
        key=lambda item: (
            str(item.get("resort_id", "")),
            str(item.get("id", "")),
        ),
    )
    return [item for idx, item in enumerate(ordered) if idx % total_workers == worker_id]


def api_get(path):
    url = f"{API_BASE_URL}{path}"
    sep = "&" if "?" in path else "?"
    url_with_key = f"{url}{sep}api_key={API_KEY}"

    last_error = None
    for attempt in range(1, API_RETRIES + 1):
        try:
            r = requests.get(
                url_with_key,
                headers=HEADERS,
                timeout=API_TIMEOUT,
            )
            if r.status_code == 408 or r.status_code >= 500:
                raise requests.HTTPError(
                    f"Transient HTTP {r.status_code} for {path}",
                    response=r,
                )
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, requests.HTTPError) as exc:
            last_error = exc
            if attempt >= API_RETRIES:
                break
            sleep_s = API_RETRY_BACKOFF * attempt
            logger.warning(
                f"GET retry {attempt}/{API_RETRIES} for {path} after error: {exc}"
            )
            time.sleep(sleep_s)

    raise last_error


def api_put(path, payload):
    r = requests.put(
        f"{API_BASE_URL}{path}?api_key={API_KEY}",
        json=payload,
        headers=HEADERS,
    )
    if r.status_code not in (200, 201):
        logger.error(f"PUT ERROR {r.status_code}: {r.text}")


def api_delete(path):
    r = requests.delete(
        f"{API_BASE_URL}{path}?api_key={API_KEY}",
        headers=HEADERS,
    )
    if r.status_code not in (200, 204):
        logger.error(f"DELETE ERROR {r.status_code}: {r.text}")


# =========================
# CHECKPOINT HELPERS
# =========================
def write_checkpoint(data):
    """Schreibt Checkpoint atomar, um korrupte Dateien zu vermeiden."""
    tmp_path = Path(f"{CHECKPOINT_PATH}.tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, CHECKPOINT_PATH)


def save_checkpoint(entity_type, index, entity_id):
    data = {
        "entity_type": entity_type,
        "index": index,
        "entity_id": entity_id,
        "timestamp": time.time(),
    }

    write_checkpoint(data)
    logger.debug(f"Checkpoint saved: {data}")


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_PATH):
        return None

    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            raw = f.read()

        if not raw.strip():
            logger.warning("Checkpoint empty. Ignoring.")
            return None

        data = json.loads(raw)

        if not isinstance(data, dict):
            logger.warning("Checkpoint has invalid format. Ignoring.")
            return None

        required = {"entity_type", "index", "entity_id", "timestamp"}
        if not required.issubset(data.keys()):
            logger.warning("Checkpoint missing required keys. Ignoring.")
            return None

        return data
    except Exception as e:
        logger.error(f"Checkpoint corrupted: {e}")
        return None


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)
        logger.info("Checkpoint cleared")


def save_phase(phase):
    checkpoint = load_checkpoint()
    if not checkpoint:
        return

    checkpoint["phase"] = phase
    write_checkpoint(checkpoint)


# =========================
# LOAD DATA
# =========================
def load_all():
    resorts = api_get("/resorts?summary=true")
    lifts_raw = api_get("/lifts")
    slopes_raw = api_get("/slopes")

    lifts = [normalize_lift_payload(item) for item in lifts_raw]
    slopes = [normalize_slope_payload(item) for item in slopes_raw]

    logger.info(f"Loaded {len(resorts)} resorts")
    logger.info(f"Loaded {len(lifts)} lifts")
    logger.info(f"Loaded {len(slopes)} slopes")

    return resorts, lifts, slopes


def normalize_lift_payload(item):
    # Supports both legacy flat API payloads and the current nested response shape.
    display = item.get("display", {}) if isinstance(item.get("display"), dict) else {}
    geometry = item.get("geometry", {}) if isinstance(item.get("geometry"), dict) else {}
    specs = item.get("specs", {}) if isinstance(item.get("specs"), dict) else {}
    source = item.get("source", {}) if isinstance(item.get("source"), dict) else {}
    status = item.get("status", {}) if isinstance(item.get("status"), dict) else {}
    start = geometry.get("start", {}) if isinstance(geometry.get("start"), dict) else {}
    end = geometry.get("end", {}) if isinstance(geometry.get("end"), dict) else {}

    return {
        "id": item.get("id"),
        "resort_id": item.get("resort_id"),
        "name": item.get("name"),
        "lift_type": item.get("lift_type") or display.get("lift_type") or "unknown",
        "capacity_per_hour": item.get("capacity_per_hour", specs.get("capacity_per_hour")),
        "seats": item.get("seats", specs.get("seats")),
        "bubble": item.get("bubble", specs.get("bubble")),
        "heated_seats": item.get("heated_seats", specs.get("heated_seats")),
        "year_built": item.get("year_built", specs.get("year_built")),
        "altitude_start_m": item.get("altitude_start_m", specs.get("altitude_start_m")),
        "altitude_end_m": item.get("altitude_end_m", specs.get("altitude_end_m")),
        "lat_start": item.get("lat_start", start.get("latitude")),
        "lon_start": item.get("lon_start", start.get("longitude")),
        "lat_end": item.get("lat_end", end.get("latitude")),
        "lon_end": item.get("lon_end", end.get("longitude")),
        "source_system": item.get("source_system", source.get("system")),
        "source_entity_id": item.get("source_entity_id", source.get("entity_id")),
        "name_normalized": item.get("name_normalized", display.get("normalized_name")),
        "operational_status": item.get("operational_status", status.get("operational_status")),
        "operational_note": item.get("operational_note", status.get("note")),
        "planned_open_time": item.get("planned_open_time", status.get("planned_open_time")),
        "planned_close_time": item.get("planned_close_time", status.get("planned_close_time")),
        "status_updated_at": item.get("status_updated_at", status.get("updated_at")),
        "status_source_url": item.get("status_source_url", source.get("source_url")),
    }


def normalize_slope_payload(item):
    # Supports both legacy flat API payloads and the current nested response shape.
    display = item.get("display", {}) if isinstance(item.get("display"), dict) else {}
    geometry = item.get("geometry", {}) if isinstance(item.get("geometry"), dict) else {}
    specs = item.get("specs", {}) if isinstance(item.get("specs"), dict) else {}
    source = item.get("source", {}) if isinstance(item.get("source"), dict) else {}
    status = item.get("status", {}) if isinstance(item.get("status"), dict) else {}
    start = geometry.get("start", {}) if isinstance(geometry.get("start"), dict) else {}
    end = geometry.get("end", {}) if isinstance(geometry.get("end"), dict) else {}
    path = geometry.get("path")
    slope_path_json = None
    if isinstance(path, list):
        slope_path_json = json.dumps(path)

    return {
        "id": item.get("id"),
        "resort_id": item.get("resort_id"),
        "name": item.get("name"),
        "difficulty": item.get("difficulty") or display.get("difficulty") or "unknown",
        "length_m": item.get("length_m", specs.get("length_m")),
        "vertical_drop_m": item.get("vertical_drop_m", specs.get("vertical_drop_m")),
        "average_gradient": item.get("average_gradient", specs.get("average_gradient")),
        "max_gradient": item.get("max_gradient", specs.get("max_gradient")),
        "snowmaking": item.get("snowmaking", specs.get("snowmaking")),
        "night_skiing": item.get("night_skiing", specs.get("night_skiing")),
        "family_friendly": item.get("family_friendly", specs.get("family_friendly")),
        "race_slope": item.get("race_slope", specs.get("race_slope")),
        "lat_start": item.get("lat_start", start.get("latitude")),
        "lon_start": item.get("lon_start", start.get("longitude")),
        "lat_end": item.get("lat_end", end.get("latitude")),
        "lon_end": item.get("lon_end", end.get("longitude")),
        "source_system": item.get("source_system", source.get("system")),
        "source_entity_id": item.get("source_entity_id", source.get("entity_id")),
        "name_normalized": item.get("name_normalized", display.get("normalized_name")),
        "operational_status": item.get("operational_status", status.get("operational_status")),
        "grooming_status": item.get("grooming_status", status.get("grooming_status")),
        "operational_note": item.get("operational_note", status.get("note")),
        "status_updated_at": item.get("status_updated_at", status.get("updated_at")),
        "status_source_url": item.get("status_source_url", source.get("source_url")),
        "slope_path_json": item.get("slope_path_json", slope_path_json),
    }


# =========================
# CLEANUP
# =========================
def cleanup_entities(entities, resorts, entity_type, coord_index):
    seen = set()
    seen_coord = set()
    valid = []
    to_delete = []

    for e in entities:
        resort_id = e.get("resort_id")
        osm_id = e.get("id")
        key = (resort_id, osm_id)

        if key in seen:
            to_delete.append(e)
            continue
        seen.add(key)

        resort = next((r for r in resorts if r["id"] == resort_id), None)
        if not resort:
            to_delete.append(e)
            continue

        name = normalize_name(e.get("name"))
        fallback_like = is_previous_fallback_name(name, entity_type)

        coord_meta = {}
        try:
            coord_meta = coord_index.get(int(osm_id), {})
        except (TypeError, ValueError):
            coord_meta = {}

        start_lat = e.get("lat_start")
        start_lon = e.get("lon_start")
        end_lat = e.get("lat_end")
        end_lon = e.get("lon_end")

        if start_lat is None:
            start_lat = coord_meta.get("start_lat")
        if start_lon is None:
            start_lon = coord_meta.get("start_lon")
        if end_lat is None:
            end_lat = coord_meta.get("end_lat")
        if end_lon is None:
            end_lon = coord_meta.get("end_lon")

        segment_key = build_segment_key(start_lat, start_lon, end_lat, end_lon)

        if not name or fallback_like:
            if entity_type in ("lift", "lifts"):
                name = generate_coordinate_name(
                    "lift",
                    start_lat=start_lat,
                    start_lon=start_lon,
                    lift_type=e.get("lift_type"),
                    osm_id=osm_id,
                )
            else:
                name = generate_coordinate_name(
                    "slope",
                    start_lat=start_lat,
                    start_lon=start_lon,
                    difficulty=e.get("difficulty"),
                    osm_id=osm_id,
                )

            if not name:
                to_delete.append(e)
                continue

        e["name"] = name
        e["lat_start"] = start_lat
        e["lon_start"] = start_lon
        e["lat_end"] = end_lat
        e["lon_end"] = end_lon

        if segment_key:
            type_value = e.get("lift_type") if entity_type in ("lift", "lifts") else e.get("difficulty")
            location_key = (resort_id, type_value, segment_key)
            if location_key in seen_coord:
                to_delete.append(e)
                continue
            seen_coord.add(location_key)

        valid.append(e)

    return valid, to_delete


# =========================
# APPLY
# =========================
def apply_changes(valid, delete, entity_type, checkpoint=None, use_checkpoint=True):
    logger.info(f"Processing {entity_type}")

    start_index_update = 0
    start_index_delete = 0

    def index_from_entity_id(items, entity_id):
        for idx, item in enumerate(items):
            if item.get("id") == entity_id:
                return idx
        return None

    if use_checkpoint and checkpoint and checkpoint.get("entity_type") == entity_type:
        phase = checkpoint.get("phase", "update")
        index = checkpoint.get("index", 0)
        checkpoint_id = checkpoint.get("entity_id")

        try:
            index = int(index)
        except (TypeError, ValueError):
            index = 0

        if phase == "update":
            resolved_index = index_from_entity_id(valid, checkpoint_id)
            start_index_update = resolved_index if resolved_index is not None else index
        elif phase == "delete":
            start_index_update = len(valid)
            resolved_index = index_from_entity_id(delete, checkpoint_id)
            start_index_delete = resolved_index if resolved_index is not None else index
        else:
            logger.warning(
                f"Unknown checkpoint phase '{phase}'. Starting {entity_type} from beginning."
            )

        logger.info(
            f"Resuming {entity_type} (update={start_index_update}, delete={start_index_delete})"
        )

    start_index_update = max(0, min(start_index_update, len(valid)))
    start_index_delete = max(0, min(start_index_delete, len(delete)))

    logger.info(f"Updating {len(valid)} entries")
    for i in range(start_index_update, len(valid)):
        e = valid[i]

        if use_checkpoint:
            save_checkpoint(entity_type, i, e["id"])
            save_phase("update")

        logger.info(f"Updating {entity_type} ID={e['id']}")
        api_put(f"/{entity_type}/{e['id']}", e)
        time.sleep(SLEEP)

    logger.info(f"Deleting {len(delete)} entries")
    for i in range(start_index_delete, len(delete)):
        e = delete[i]

        if use_checkpoint:
            save_checkpoint(entity_type, i, e["id"])
            save_phase("delete")

        logger.warning(f"Deleting {entity_type} ID={e['id']}")
        api_delete(f"/{entity_type}/{e['id']}")
        time.sleep(SLEEP)


# =========================
# MAIN
# =========================
def main():
    worker_id = int(sys.argv[1]) if len(sys.argv) > 1 else None
    total_workers = int(sys.argv[2]) if len(sys.argv) > 2 else None

    parallel_mode = (
        worker_id is not None
        and total_workers is not None
        and total_workers > 1
        and worker_id >= 0
    )

    if parallel_mode and worker_id >= total_workers:
        raise ValueError("worker_id must be smaller than total_workers")

    configure_logging(worker_id=worker_id if parallel_mode else None, total_workers=total_workers if parallel_mode else None)
    logger.info("=== Cleanup Script Started ===")

    checkpoint = None
    if parallel_mode:
        logger.info(f"Parallel cleanup mode active (worker {worker_id}/{total_workers}). Checkpoint disabled.")
    else:
        checkpoint = load_checkpoint()
        if checkpoint:
            logger.warning(f"Resuming from checkpoint: {checkpoint}")
        else:
            logger.info("No checkpoint found. Starting fresh.")

    coord_index = load_coordinate_index()
    resorts, lifts, slopes = load_all()

    clean_lifts, del_lifts = cleanup_entities(
        lifts,
        resorts,
        "lifts",
        coord_index["lifts"],
    )
    clean_slopes, del_slopes = cleanup_entities(
        slopes,
        resorts,
        "slopes",
        coord_index["slopes"],
    )

    if parallel_mode:
        clean_lifts = shard_items(clean_lifts, worker_id, total_workers)
        del_lifts = shard_items(del_lifts, worker_id, total_workers)
        clean_slopes = shard_items(clean_slopes, worker_id, total_workers)
        del_slopes = shard_items(del_slopes, worker_id, total_workers)
        logger.info(
            "Worker shard sizes | "
            f"lifts update={len(clean_lifts)}, lifts delete={len(del_lifts)}, "
            f"slopes update={len(clean_slopes)}, slopes delete={len(del_slopes)}"
        )

    entities = {
        "lifts": (clean_lifts, del_lifts),
        "slopes": (clean_slopes, del_slopes),
    }

    process_order = ["lifts", "slopes"]
    if checkpoint:
        checkpoint_entity = checkpoint.get("entity_type")
        if checkpoint_entity in entities:
            process_order = [checkpoint_entity] + [
                e for e in process_order if e != checkpoint_entity
            ]
            logger.info(f"Processing order for resume: {process_order}")
        else:
            logger.warning(
                f"Unknown checkpoint entity_type '{checkpoint_entity}'. "
                "Using default processing order."
            )

    for entity_type in process_order:
        valid, delete = entities[entity_type]
        entity_checkpoint = (
            checkpoint
            if checkpoint and checkpoint.get("entity_type") == entity_type
            else None
        )
        apply_changes(
            valid,
            delete,
            entity_type,
            entity_checkpoint,
            use_checkpoint=not parallel_mode,
        )

    if not parallel_mode:
        clear_checkpoint()
    logger.info("Cleanup finished successfully.")


if __name__ == "__main__":
    main()
