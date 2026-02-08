import csv
import requests
import time
import ast
import unicodedata
import re

# =========================
# CONFIG
# =========================
CSV_FILE = "ski-resorts.csv"
LAST_RESORT_FILE = "last_resort.txt"

OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"
OVERPASS_RADIUS = 7000
MAX_RETRIES = 3

API_BASE_URL = "http://localhost:8080"
API_KEY = "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA"

HEADERS = {
    "Content-Type": "application/json"
}

# =========================
# ENUM MAPPINGS
# =========================
PISTE_DIFFICULTY_MAP = {
    "novice": "green",
    "easy": "blue",
    "intermediate": "red",
    "advanced": "black",
    "expert": "black"
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
    "magic_carpet": "magic_carpet"
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
                    print(f"⚠️ Ungültige Koordinaten für {name}")

            resorts.append({
                "id": normalize_id(name),
                "name": name,
                "country": row.get("location_country"),
                "region": row.get("location_region"),
                "continent": None,
                "lat": lat,
                "lon": lon,
                "ski_area_name": name
            })

    print(f"ℹ️ {len(resorts)} Resorts geladen")
    return resorts


# =========================
# OVERPASS
# =========================
def overpass_request(query):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(OVERPASS_URL, data=query, timeout=60)
            
            if r.status_code == 504:
                print(f"⚠️ Overpass Fehler 504 Gateway Timeout beim Versuch {attempt}/{MAX_RETRIES}")
                time.sleep(5 * attempt)
                continue
            
            r.raise_for_status()
            return r.json()
        
        except requests.exceptions.RequestException as e:
            wait = 5 * attempt
            print(f"⚠️ Overpass Fehler ({attempt}/{MAX_RETRIES}): {e}")
            time.sleep(wait)

    return None


def fetch_osm_data(resort):
    if resort["lat"] is None or resort["lon"] is None:
        return None

    lat, lon = resort["lat"], resort["lon"]

    return {
        "lifts": overpass_request(f"""
            [out:json][timeout:60];
            way["aerialway"](around:{OVERPASS_RADIUS},{lat},{lon});
            out tags;
        """),
        "slopes": overpass_request(f"""
            [out:json][timeout:60];
            way["piste:type"="downhill"](around:{OVERPASS_RADIUS},{lat},{lon});
            out tags;
        """)
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
        "ski_area_type": "alpine"
    }

    r = requests.post(
        f"{API_BASE_URL}/resorts?api_key={API_KEY}",
        json=payload,
        headers=HEADERS
    )

    if r.status_code not in (200, 201):
        requests.put(
            f"{API_BASE_URL}/resorts/{resort['id']}?api_key={API_KEY}",
            json=payload,
            headers=HEADERS
        )


# =========================
# LIFTS & SLOPES MIT DUPLIKAT-PRÜFUNG
# =========================
processed_lift_ids = set()
processed_slope_ids = set()

def save_entity(entity_type, osm_id, payload):
    """ Prüft, ob Entity existiert, dann POST oder PUT """
    r = requests.get(f"{API_BASE_URL}/{entity_type}/{osm_id}?api_key={API_KEY}", headers=HEADERS)
    
    if r.status_code == 200:
        r2 = requests.put(f"{API_BASE_URL}/{entity_type}/{osm_id}?api_key={API_KEY}", json=payload, headers=HEADERS)
    elif r.status_code == 404:
        r2 = requests.post(f"{API_BASE_URL}/{entity_type}?api_key={API_KEY}", json=payload, headers=HEADERS)
    else:
        print(f"⚠️ Fehler beim Prüfen von {entity_type} {osm_id}: {r.status_code}, {r.text}")
        return

    if r2.status_code not in (200, 201):
        print(f"⚠️ Fehler beim Speichern von {entity_type} {osm_id}: {r2.status_code}, {r2.text}")


def send_lift(resort_id, tags, osm_id):
    if osm_id in processed_lift_ids:
        return  # Duplikat überspringen
    processed_lift_ids.add(osm_id)

    lift_type = AERIALWAY_LIFT_MAP.get(tags.get("aerialway"))
    if not lift_type:
        return

    payload = {
        "id": osm_id,
        "resort_id": resort_id,
        "name": normalize_name(tags.get("name")),
        "lift_type": lift_type,
        "capacity_per_hour": None,
        "seats": None,
        "bubble": False,
        "heated_seats": False,
        "year_built": None,
        "altitude_start_m": None,
        "altitude_end_m": None
    }

    save_entity("lifts", osm_id, payload)


def send_slope(resort_id, tags, osm_id):
    if osm_id in processed_slope_ids:
        return  # Duplikat überspringen
    processed_slope_ids.add(osm_id)

    difficulty = PISTE_DIFFICULTY_MAP.get(tags.get("piste:difficulty"))
    if not difficulty:
        return

    payload = {
        "id": osm_id,
        "resort_id": resort_id,
        "name": normalize_name(tags.get("name")),
        "difficulty": difficulty,
        "length_m": None
    }

    save_entity("slopes", osm_id, payload)


# =========================
# PROCESSING
# =========================
def process_osm_data(osm_data, resort_id):
    if osm_data.get("lifts"):
        for el in osm_data["lifts"].get("elements", []):
            send_lift(resort_id, el.get("tags", {}), el["id"])

    if osm_data.get("slopes"):
        for el in osm_data["slopes"].get("elements", []):
            send_slope(resort_id, el.get("tags", {}), el["id"])


# =========================
# HELPER: Fortschritt speichern und laden
# =========================
def save_last_index(index):
    with open(LAST_RESORT_FILE, "w", encoding="utf-8") as f:
        f.write(str(index))


def load_last_index():
    try:
        with open(LAST_RESORT_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except:
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

    # Worker Nummer (0,1,2,3...)
    worker_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    # Abstand
    step = 20

    resorts = parse_csv(CSV_FILE)

    # Resume Index laden
    base_index = load_last_index()

    # Start für diesen Worker
    start = base_index + worker_id

    print(f"Worker {worker_id} startet bei Index {start}")

    for i in range(start, len(resorts), step):

        resort = resorts[i]

        print(f"\n🏔️ Worker {worker_id} → {resort['name']} (#{i})")

        create_or_update_resort(resort)

        osm_data = fetch_osm_data(resort)

        if osm_data:
            process_osm_data(osm_data, resort["id"])

        # Nur Worker 0 speichert Fortschritt
        if worker_id == 0:
            save_last_index(i)

        time.sleep(2)


if __name__ == "__main__":
    main()
