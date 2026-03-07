import ast
import csv
import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import requests

# =========================
# PATHS
# =========================
ROOT_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT_DIR / "logs" / "scraper"
COORD_DIR = ROOT_DIR / "checkpoints" / "coordinates"

# =========================
# CONFIG
# =========================
CSV_FILE = ROOT_DIR / "ski-resorts.csv"
LAST_RESORT_FILE = ROOT_DIR / "last_resort.txt"

OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"
OVERPASS_RADIUS = 8000
MAX_RETRIES = 3
NAME_QUERY_CHUNK_SIZE = 20

API_BASE_URL = "http://localhost:8080"
API_KEY = "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}

WORKER_COORD_FILE = None
logger = logging.getLogger("ski_scraper")

# =========================
# ENUM MAPPINGS
# =========================
PISTE_DIFFICULTY_MAP = {
    "novice": "green",
    "easy": "blue",
    "intermediate": "red",
    "advanced": "black",
    "expert": "black",
}

AERIALWAY_LIFT_MAP = {
    "gondola": "gondola",
    "cable_car": "cable_car",
    "chair_lift": "chairlift",
    "mixed_lift": "chairlift",
    "t-bar": "draglift",
    "j-bar": "draglift",
    "platter": "draglift",
    "rope_tow": "draglift",
    "magic_carpet": "magic_carpet",
}

# =========================
# UTILS
# =========================
def normalize_id(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.lower()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"\s+", "-", name)
    return name.strip("-")


def normalize_name(name):
    if not name:
        return None
    return unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")


def normalize_lookup_name(name):
    normalized = normalize_name(name)
    if not normalized:
        return None
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized or None


# =========================
# CSV PARSING
# =========================
def parse_csv(filepath):
    resorts = []

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            name = row.get("name")
            if not name:
                continue

            lat = lon = None
            coord_raw = row.get("location_coordinate")
            if coord_raw:
                try:
                    coord = ast.literal_eval(coord_raw)
                    lat = float(coord.get("lat"))
                    lon = float(coord.get("long"))
                except Exception:
                    print(f"Ungueltige Koordinaten fuer {name}")

            resorts.append(
                {
                    "id": normalize_id(name),
                    "name": name,
                    "country": row.get("location_country"),
                    "region": row.get("location_region"),
                    "continent": None,
                    "lat": lat,
                    "lon": lon,
                    "ski_area_name": name,
                }
            )

    print(f"{len(resorts)} Resorts geladen")
    return resorts


# =========================
# OVERPASS
# =========================
def overpass_request(query):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(OVERPASS_URL, data=query, timeout=60)

            if r.status_code == 504:
                print(f"Overpass Fehler 504 Gateway Timeout bei Versuch {attempt}/{MAX_RETRIES}")
                time.sleep(5 * attempt)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            wait = 5 * attempt
            print(f"Overpass Fehler ({attempt}/{MAX_RETRIES}): {e}")
            time.sleep(wait)

    return None


def chunked(values, size):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def build_lift_name_query(lat, lon, names):
    escaped = [re.escape(name) for name in names]
    pattern = "|".join(escaped)
    return f"""
        [out:json][timeout:60];
        way["aerialway"]["name"~"^({pattern})$",i](around:{OVERPASS_RADIUS},{lat},{lon});
        out geom tags;
    """


def build_slope_name_query(lat, lon, names):
    escaped = [re.escape(name) for name in names]
    pattern = "|".join(escaped)
    return f"""
        [out:json][timeout:60];
        way["piste:type"="downhill"]["name"~"^({pattern})$",i](around:{OVERPASS_RADIUS},{lat},{lon});
        out geom tags;
    """


def build_lift_unnamed_query(lat, lon):
    return f"""
        [out:json][timeout:60];
        way["aerialway"][!"name"](around:{OVERPASS_RADIUS},{lat},{lon});
        out geom tags;
    """


def build_slope_unnamed_query(lat, lon):
    return f"""
        [out:json][timeout:60];
        way["piste:type"="downhill"][!"name"](around:{OVERPASS_RADIUS},{lat},{lon});
        out geom tags;
    """


def build_lift_broad_query(lat, lon):
    return f"""
        [out:json][timeout:60];
        way["aerialway"](around:{OVERPASS_RADIUS},{lat},{lon});
        out geom tags;
    """


def build_slope_broad_query(lat, lon):
    return f"""
        [out:json][timeout:90];
        (
          way["piste:type"="downhill"](around:{OVERPASS_RADIUS},{lat},{lon});
          relation["route"="piste"]["piste:type"="downhill"](around:{OVERPASS_RADIUS},{lat},{lon});
        );
        out geom tags;
        >;
        out skel qt;
    """


def merge_overpass_results(results):
    merged = {"elements": []}
    seen_ids = set()

    for result in results:
        if not result:
            continue
        for element in result.get("elements", []):
            element_id = element.get("id")
            if element_id in seen_ids:
                continue
            seen_ids.add(element_id)
            merged["elements"].append(element)

    return merged


def fetch_by_known_names(lat, lon, names, query_builder, unnamed_query_builder, broad_query_builder):
    normalized_names = sorted({name for name in names if name})
    results = []

    if normalized_names:
        for name_chunk in chunked(normalized_names, NAME_QUERY_CHUNK_SIZE):
            results.append(overpass_request(query_builder(lat, lon, name_chunk)))
        results.append(overpass_request(unnamed_query_builder(lat, lon)))
        merged = merge_overpass_results(results)
        if merged.get("elements"):
            return merged

    return overpass_request(broad_query_builder(lat, lon))


def load_existing_name_index():
    index = {"lifts": {}, "slopes": {}}

    try:
        lifts_res = requests.get(
            f"{API_BASE_URL}/lifts?api_key={API_KEY}",
            headers=HEADERS,
            timeout=30,
        )
        lifts_res.raise_for_status()
        lifts = lifts_res.json()

        slopes_res = requests.get(
            f"{API_BASE_URL}/slopes?api_key={API_KEY}",
            headers=HEADERS,
            timeout=30,
        )
        slopes_res.raise_for_status()
        slopes = slopes_res.json()
    except requests.exceptions.RequestException as exc:
        logger.warning(f"Could not load existing names from API: {exc}")
        return index

    for lift in lifts if isinstance(lifts, list) else []:
        resort_id = lift.get("resort_id")
        name = normalize_lookup_name(lift.get("name"))
        if resort_id and name:
            index["lifts"].setdefault(resort_id, set()).add(name)

    for slope in slopes if isinstance(slopes, list) else []:
        resort_id = slope.get("resort_id")
        name = normalize_lookup_name(slope.get("name"))
        if resort_id and name:
            index["slopes"].setdefault(resort_id, set()).add(name)

    return index


def fetch_osm_data(resort, existing_name_index):
    if resort["lat"] is None or resort["lon"] is None:
        return None

    lat, lon = resort["lat"], resort["lon"]
    resort_id = resort["id"]

    known_lift_names = existing_name_index["lifts"].get(resort_id, set())
    known_slope_names = existing_name_index["slopes"].get(resort_id, set())

    lifts_data = fetch_by_known_names(
        lat,
        lon,
        known_lift_names,
        build_lift_name_query,
        build_lift_unnamed_query,
        build_lift_broad_query,
    )
    slopes_data = fetch_by_known_names(
        lat,
        lon,
        known_slope_names,
        build_slope_name_query,
        build_slope_unnamed_query,
        build_slope_broad_query,
    )

    return {
        "lifts": lifts_data,
        "slopes": slopes_data,
    }


# =========================
# API CLIENT
# =========================
def create_or_update_resort(resort):
    payload = {
        "id": resort["id"],
        "name": resort["name"],
        "country": resort["country"] or "unknown",
        "region": resort["region"],
        "continent": resort["continent"],
        "latitude": resort["lat"],
        "longitude": resort["lon"],
        "village_altitude_m": None,
        "min_altitude_m": None,
        "max_altitude_m": None,
        "ski_area_name": resort["ski_area_name"],
        "ski_area_type": "alpine",
    }

    r = requests.post(
        f"{API_BASE_URL}/resorts?api_key={API_KEY}",
        json=payload,
        headers=HEADERS,
    )

    if r.status_code not in (200, 201):
        requests.put(
            f"{API_BASE_URL}/resorts/{resort['id']}?api_key={API_KEY}",
            json=payload,
            headers=HEADERS,
        )


# =========================
# LIFTS & SLOPES
# =========================
processed_lift_ids = set()
processed_slope_ids = set()


def setup_worker_logging(worker_id: int):
    import logging
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = datetime.now().strftime(f"scraper_worker_{worker_id}_%Y-%m-%d_%H-%M-%S.log")
    log_path = LOG_DIR / log_filename

    # Remove all existing handlers (wenn du mehrfach testest)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(str(log_path), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )



def extract_coordinates(element):
    center = element.get("center") or {}
    lat = center.get("lat", element.get("lat"))
    lon = center.get("lon", element.get("lon"))
    return lat, lon


def extract_endpoints(element):
    geometry = element.get("geometry")

    if isinstance(geometry, list) and len(geometry) >= 2:
        first = geometry[0]
        last = geometry[-1]

        return (
            first.get("lat"),
            first.get("lon"),
            last.get("lat"),
            last.get("lon"),
        )

    # Fallback
    center = element.get("center")
    if center:
        return (
            center.get("lat"),
            center.get("lon"),
            center.get("lat"),
            center.get("lon"),
        )

    return None, None, None, None



def append_coordinate_event(entity_type, resort_id, osm_id, name, extra, start_lat, start_lon, end_lat, end_lon):
    if WORKER_COORD_FILE is None:
        return

    event = {
        "entity_type": entity_type,
        "resort_id": resort_id,
        "id": int(osm_id),
        "name": name,
        "start_lat": start_lat,
        "start_lon": start_lon,
        "end_lat": end_lat,
        "end_lon": end_lon,
        "timestamp": time.time(),
    }
    event.update(extra)

    with open(WORKER_COORD_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=True) + "\n")


def save_entity(entity_type, osm_id, payload):
    """Prueft, ob Entity existiert, dann POST oder PUT."""
    r = requests.get(f"{API_BASE_URL}/{entity_type}/{osm_id}?api_key={API_KEY}", headers=HEADERS)

    if r.status_code == 200:
        r2 = requests.put(
            f"{API_BASE_URL}/{entity_type}/{osm_id}?api_key={API_KEY}",
            json=payload,
            headers=HEADERS,
        )
    elif r.status_code == 404:
        r2 = requests.post(
            f"{API_BASE_URL}/{entity_type}?api_key={API_KEY}",
            json=payload,
            headers=HEADERS,
        )
    else:
        print(f"Fehler beim Pruefen von {entity_type} {osm_id}: {r.status_code}, {r.text}")
        return

    if r2.status_code not in (200, 201):
        print(
            f"Fehler beim Speichern von {entity_type} {osm_id}: "
            f"{r2.status_code}, {r2.text}"
        )


def send_lift(resort_id, tags, osm_id, element):
    if osm_id in processed_lift_ids:
        return
    processed_lift_ids.add(osm_id)

    lift_type = AERIALWAY_LIFT_MAP.get(tags.get("aerialway"))
    if not lift_type:
        return

    start_lat, start_lon, end_lat, end_lon = extract_endpoints(element)
    normalized_name = normalize_name(tags.get("name"))

    append_coordinate_event(
        entity_type="lifts",
        resort_id=resort_id,
        osm_id=osm_id,
        name=normalized_name,
        extra={"lift_type": lift_type},
        start_lat=start_lat,
        start_lon=start_lon,
        end_lat=end_lat,
        end_lon=end_lon,
    )

    payload = {
        "id": osm_id,
        "resort_id": resort_id,
        "name": normalized_name,
        "lift_type": lift_type,
        "capacity_per_hour": None,
        "seats": None,
        "bubble": False,
        "heated_seats": False,
        "year_built": None,
        "altitude_start_m": None,
        "altitude_end_m": None,
        "lat_start": start_lat,
        "lon_start": start_lon,
        "lat_end": end_lat,
        "lon_end": end_lon,
    }

    save_entity("lifts", osm_id, payload)


def send_slope(resort_id, tags, osm_id, element):
    if osm_id in processed_slope_ids:
        return
    processed_slope_ids.add(osm_id)

    raw_difficulty = tags.get("piste:difficulty")
    difficulty = PISTE_DIFFICULTY_MAP.get(raw_difficulty)

    if not difficulty:
        logger.debug(
            f"Slope {osm_id} ignored (difficulty='{raw_difficulty}') in resort {resort_id}"
        )
        return

    start_lat, start_lon, end_lat, end_lon = extract_endpoints(element)
    normalized_name = normalize_name(tags.get("name"))

    logger.info(
        f"Slope detected: {normalized_name} | diff={difficulty} "
        f"| start=({start_lat},{start_lon}) end=({end_lat},{end_lon})"
    )

    append_coordinate_event(
        entity_type="slopes",
        resort_id=resort_id,
        osm_id=osm_id,
        name=normalized_name,
        extra={"difficulty": difficulty},
        start_lat=start_lat,
        start_lon=start_lon,
        end_lat=end_lat,
        end_lon=end_lon,
    )

    payload = {
        "id": osm_id,
        "resort_id": resort_id,
        "name": normalized_name,
        "difficulty": difficulty,
        "length_m": None,
        "lat_start": start_lat,
        "lon_start": start_lon,
        "lat_end": end_lat,
        "lon_end": end_lon,
    }

    save_entity("slopes", osm_id, payload)



def build_relation_geometries(elements):
    way_geometries = {}
    relations = []

    for el in elements:
        if el.get("type") == "way" and isinstance(el.get("geometry"), list):
            way_geometries[el["id"]] = el["geometry"]

    for el in elements:
        if el.get("type") == "relation":
            relations.append(el)

    for rel in relations:
        combined = []

        for member in rel.get("members", []):
            if member.get("type") == "way":
                geom = way_geometries.get(member.get("ref"))
                if geom:
                    combined.extend(geom)

        if len(combined) >= 2:
            rel["geometry"] = combined

    return elements


def delete_existing_entities_for_resort(resort_id):
    for entity in ["lifts", "slopes"]:
        try:
            r = requests.delete(
                f"{API_BASE_URL}/{entity}/by_resort/{resort_id}?api_key={API_KEY}",
                headers=HEADERS,
                timeout=30,
            )
            if r.status_code not in (200, 204):
                logger.warning(f"Delete failed for {entity} {resort_id}: {r.status_code}")
        except Exception as e:
            logger.warning(f"Delete error for {entity} {resort_id}: {e}")




# =========================
# PROCESSING
# =========================
def process_osm_data(osm_data, resort_id):
    global processed_lift_ids, processed_slope_ids

    processed_lift_ids = set()
    processed_slope_ids = set()

    # ----- LIFTS -----
    if osm_data.get("lifts"):
        lift_elements = osm_data["lifts"].get("elements", [])
        logger.info(f"{len(lift_elements)} Lift-Elemente gefunden")

        for el in lift_elements:
            if el.get("type") == "way":
                send_lift(resort_id, el.get("tags", {}), el["id"], el)

    # ----- SLOPES -----
    if osm_data.get("slopes"):
        slope_elements = osm_data["slopes"].get("elements", [])
        logger.info(f"{len(slope_elements)} Slope-Elemente gefunden (raw)")

        slope_elements = build_relation_geometries(slope_elements)

        processed_counter = 0

        for el in slope_elements:
            if el.get("type") in ("way", "relation"):
                before = len(processed_slope_ids)
                send_slope(resort_id, el.get("tags", {}), el["id"], el)
                if len(processed_slope_ids) > before:
                    processed_counter += 1

        logger.info(f"{processed_counter} Pisten verarbeitet")





# =========================
# HELPER
# =========================
def save_last_index(index):
    with open(LAST_RESORT_FILE, "w", encoding="utf-8") as f:
        f.write(str(index))


def load_last_index():
    try:
        with open(LAST_RESORT_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def load_last_resort():
    try:
        with open(LAST_RESORT_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


# =========================
# MAIN
# =========================
def main():
    import sys

    global WORKER_COORD_FILE

    worker_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    total_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    if total_workers <= 0:
        raise ValueError("total_workers must be greater than 0")
    if worker_id < 0 or worker_id >= total_workers:
        raise ValueError("worker_id must be between 0 and total_workers - 1")

    step = total_workers

    os.makedirs(COORD_DIR, exist_ok=True)
    WORKER_COORD_FILE = COORD_DIR / f"worker_{worker_id}.jsonl"

    setup_worker_logging(worker_id)

    resorts = parse_csv(CSV_FILE)

    base_index = load_last_index()
    start = base_index + worker_id

    logger.info(f"Worker {worker_id}/{total_workers} startet bei Index {start}")

    for i in range(start, len(resorts), step):
        resort = resorts[i]

        logger.info(f"Worker {worker_id} -> {resort['name']} (#{i})")

        try:
            # Resort updaten
            create_or_update_resort(resort)

            # ALTEN DATENMÜLL LÖSCHEN
            delete_existing_entities_for_resort(resort["id"])

            # OSM neu laden
            osm_data = fetch_osm_data(resort, {"lifts": {}, "slopes": {}})

            if osm_data:
                process_osm_data(osm_data, resort["id"])

            if worker_id == 0:
                save_last_index(i)

        except Exception as e:
            logger.error(f"Fehler bei Resort {resort['name']}: {e}")

        time.sleep(1.5)



if __name__ == "__main__":
    main()
