import requests
import unicodedata
import re
import time

import logging
import os
from datetime import datetime

import json


# =========================
# CONFIG
# =========================

API_BASE_URL = "http://localhost:8080"
API_KEY = "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA"

HEADERS = {
    "Content-Type": "application/json"
}

SLEEP = 0.2

# =========================
# LOGGING
# =========================

LOG_DIR = "logs"

# Ordner erstellen falls nicht vorhanden
os.makedirs(LOG_DIR, exist_ok=True)

# Dateiname mit Zeitstempel
log_filename = datetime.now().strftime("cleanup_%Y-%m-%d_%H-%M-%S.log")
log_path = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler()  # Optional: zeigt Logs auch im Terminal
    ]
)

logger = logging.getLogger(__name__)

# =========================
# CHECKPOINT
# =========================

CHECKPOINT_DIR = "checkpoints"
CHECKPOINT_FILE = "progress.txt"

os.makedirs(CHECKPOINT_DIR, exist_ok=True)

CHECKPOINT_PATH = os.path.join(CHECKPOINT_DIR, CHECKPOINT_FILE)


# =========================
# HELPERS
# =========================

def normalize_name(name):

    if not name:
        return None

    name = unicodedata.normalize("NFKD", name)\
        .encode("ascii", "ignore")\
        .decode("ascii")

    name = re.sub(r"\s+", " ", name)

    return name.strip()


def generate_fallback_name(entity, difficulty=None, lift_type=None, osm_id=None):

    if entity == "lift" and lift_type:
        return f"{lift_type.title()} Lift {osm_id}"

    if entity == "slope" and difficulty:
        return f"{difficulty.title()} Slope {osm_id}"

    return None


def belongs_to_resort(entity, resort):

    resort_name = resort["name"].lower()

    candidates = [
        entity.get("name"),
        entity.get("piste:resort"),
        entity.get("ski_resort"),
        entity.get("resort"),
    ]

    for c in candidates:
        if c and resort_name in c.lower():
            return True

    return False


def api_get(path):

    r = requests.get(
        f"{API_BASE_URL}{path}?api_key={API_KEY}",
        headers=HEADERS
    )

    r.raise_for_status()

    return r.json()


def api_put(path, payload):

    r = requests.put(
        f"{API_BASE_URL}{path}?api_key={API_KEY}",
        json=payload,
        headers=HEADERS
    )

    if r.status_code not in (200, 201):
        logger.error(f"PUT ERROR {r.status_code}: {r.text}")


def api_delete(path):

    r = requests.delete(
        f"{API_BASE_URL}{path}?api_key={API_KEY}",
        headers=HEADERS
    )

    if r.status_code not in (200, 204):
        logger.error(f"DELETE ERROR {r.status_code}: {r.text}")


# =========================
# CHECKPOINT HELPERS
# =========================

def write_checkpoint(data):
    """
    Schreibt Checkpoint atomar, damit keine korrupten/NUL-Dateien entstehen.
    """
    tmp_path = f"{CHECKPOINT_PATH}.tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, CHECKPOINT_PATH)


def save_checkpoint(entity_type, index, entity_id):
    """
    Speichert aktuellen Stand
    """
    data = {
        "entity_type": entity_type,
        "index": index,
        "entity_id": entity_id,
        "timestamp": time.time()
    }

    write_checkpoint(data)

    logger.debug(f"Checkpoint saved: {data}")


def load_checkpoint():
    """
    Lädt letzten Stand (falls vorhanden)
    """
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
    """
    Löscht Checkpoint nach Erfolg
    """
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)
        logger.info("Checkpoint cleared")


def save_phase(phase):
    """
    Update/Delete Phase speichern
    """
    checkpoint = load_checkpoint()
    if not checkpoint:
        return

    checkpoint["phase"] = phase
    write_checkpoint(checkpoint)




# =========================
# LOAD DATA
# =========================

def load_all():

    resorts = api_get("/resorts")
    lifts = api_get("/lifts")
    slopes = api_get("/slopes")

    logger.info(f"Loaded {len(resorts)} resorts")
    logger.info(f"Loaded {len(lifts)} lifts")
    logger.info(f"Loaded {len(slopes)} slopes")

    return resorts, lifts, slopes


# =========================
# CLEANUP
# =========================

def cleanup_entities(entities, resorts, entity_type):
    seen = set()
    valid = []
    to_delete = []

    for e in entities:
        resort_id = e.get("resort_id")
        osm_id = e.get("id")
        key = (resort_id, osm_id)

        # -----------------------
        # Duplikate entfernen
        # -----------------------
        if key in seen:
            to_delete.append(e)
            continue
        seen.add(key)

        # -----------------------
        # Name normalisieren
        # -----------------------
        name = normalize_name(e.get("name"))
        if not name:
            if entity_type in ("lift", "lifts"):
                name = generate_fallback_name(
                    "lift",
                    lift_type=e.get("lift_type"),
                    osm_id=osm_id
                )
            else:
                name = generate_fallback_name(
                    "slope",
                    difficulty=e.get("difficulty"),
                    osm_id=osm_id
                )

            if not name:
                to_delete.append(e)
                continue

        e["name"] = name

        # -----------------------
        # Resort exists check
        # -----------------------
        resort = next((r for r in resorts if r["id"] == resort_id), None)
        if not resort:
            to_delete.append(e)
            continue

        valid.append(e)

    return valid, to_delete  # <-- sehr wichtig!


# =========================
# APPLY
# =========================

def apply_changes(valid, delete, entity_type, checkpoint=None):

    logger.info(f"Processing {entity_type}")

    start_index_update = 0
    start_index_delete = 0

    def index_from_entity_id(items, entity_id):
        for idx, item in enumerate(items):
            if item.get("id") == entity_id:
                return idx
        return None

    # Resume-Logik
    if checkpoint and checkpoint.get("entity_type") == entity_type:

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
            # Update-Phase ist bereits durch, nur Delete fortsetzen.
            start_index_update = len(valid)
            resolved_index = index_from_entity_id(delete, checkpoint_id)
            start_index_delete = resolved_index if resolved_index is not None else index
        else:
            logger.warning(
                f"Unknown checkpoint phase '{phase}'. Starting {entity_type} from beginning."
            )

        logger.info(
            f"Resuming {entity_type} "
            f"(update={start_index_update}, delete={start_index_delete})"
        )

    start_index_update = max(0, min(start_index_update, len(valid)))
    start_index_delete = max(0, min(start_index_delete, len(delete)))

    # ---------------- UPDATE ----------------

    logger.info(f"Updating {len(valid)} entries")

    for i in range(start_index_update, len(valid)):

        e = valid[i]

        save_checkpoint(
            entity_type,
            i,
            e["id"]
        )

        save_phase("update")

        logger.info(f"Updating {entity_type} ID={e['id']}")

        api_put(f"/{entity_type}/{e['id']}", e)
        time.sleep(SLEEP)

    # ---------------- DELETE ----------------

    logger.info(f"Deleting {len(delete)} entries")

    for i in range(start_index_delete, len(delete)):

        e = delete[i]

        save_checkpoint(
            entity_type,
            i,
            e["id"]
        )

        save_phase("delete")

        logger.warning(f"Deleting {entity_type} ID={e['id']}")

        api_delete(f"/{entity_type}/{e['id']}")
        time.sleep(SLEEP)



# =========================
# MAIN
# =========================

def main():

    logger.info("=== Cleanup Script Started ===")

    checkpoint = load_checkpoint()

    if checkpoint:
        logger.warning(
            f"Resuming from checkpoint: "
            f"{checkpoint}"
        )
    else:
        logger.info("No checkpoint found. Starting fresh.")

    resorts, lifts, slopes = load_all()

    clean_lifts, del_lifts = cleanup_entities(
        lifts,
        resorts,
        "lifts"
    )

    clean_slopes, del_slopes = cleanup_entities(
        slopes,
        resorts,
        "slopes"
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
            entity_checkpoint
        )

    clear_checkpoint()

    logger.info("Cleanup finished successfully.")




if __name__ == "__main__":
    main()


