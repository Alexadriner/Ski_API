import argparse
import json
import logging
import math
import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime

import requests


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")
API_KEY = os.getenv("API_KEY", "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA")
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}

logger = logging.getLogger("merge_similar_slopes")


def normalize_name(value):
    if not value:
        return ""
    text = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def haversine_m(a_lat, a_lon, b_lat, b_lon):
    r = 6371000.0
    p1 = math.radians(a_lat)
    p2 = math.radians(b_lat)
    dp = math.radians(b_lat - a_lat)
    dl = math.radians(b_lon - a_lon)
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(x), math.sqrt(1 - x))


def extract_endpoints(slope):
    start = ((slope.get("geometry") or {}).get("start")) or {}
    end = ((slope.get("geometry") or {}).get("end")) or {}

    points = []
    for p in (start, end):
        lat = to_float(p.get("latitude"))
        lon = to_float(p.get("longitude"))
        if lat is None or lon is None:
            continue
        points.append((lat, lon))
    return points


def min_endpoint_distance_m(slope_a, slope_b):
    pts_a = extract_endpoints(slope_a)
    pts_b = extract_endpoints(slope_b)
    if not pts_a or not pts_b:
        return None

    best = None
    for a in pts_a:
        for b in pts_b:
            d = haversine_m(a[0], a[1], b[0], b[1])
            if best is None or d < best:
                best = d
    return best


def connected_components(rows, distance_threshold_m):
    n = len(rows)
    graph = [[] for _ in range(n)]

    for i in range(n):
        for j in range(i + 1, n):
            d = min_endpoint_distance_m(rows[i], rows[j])
            if d is not None and d <= distance_threshold_m:
                graph[i].append(j)
                graph[j].append(i)

    seen = set()
    components = []
    for i in range(n):
        if i in seen:
            continue
        stack = [i]
        comp_idx = []
        seen.add(i)
        while stack:
            cur = stack.pop()
            comp_idx.append(cur)
            for nxt in graph[cur]:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        components.append([rows[idx] for idx in comp_idx])
    return components


def choose_status(status_values):
    rank = {
        "open": 6,
        "partial": 5,
        "grooming": 4,
        "scheduled": 3,
        "closed": 2,
        "unknown": 1,
    }
    best = "unknown"
    best_rank = 0
    for v in status_values:
        key = str(v or "unknown").strip().lower()
        r = rank.get(key, 0)
        if r > best_rank:
            best = key
            best_rank = r
    return best


def choose_grooming(grooming_values):
    values = [str(v or "").strip().lower() for v in grooming_values]
    if "groomed" in values:
        return "groomed"
    if "not_groomed" in values:
        return "not_groomed"
    return "unknown"


def parse_iso(ts):
    if not ts:
        return None
    text = str(ts).strip()
    fmts = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S")
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def build_merge_payload(component, resort_id):
    def length_val(s):
        return ((s.get("specs") or {}).get("length_m")) or 0

    base = max(component, key=length_val)

    endpoints = []
    for s in component:
        endpoints.extend(extract_endpoints(s))

    start_lat = end_lat = start_lon = end_lon = None
    if len(endpoints) >= 2:
        far_best = None
        far_pair = None
        for i in range(len(endpoints)):
            for j in range(i + 1, len(endpoints)):
                d = haversine_m(
                    endpoints[i][0], endpoints[i][1], endpoints[j][0], endpoints[j][1]
                )
                if far_best is None or d > far_best:
                    far_best = d
                    far_pair = (endpoints[i], endpoints[j])
        if far_pair:
            (start_lat, start_lon), (end_lat, end_lon) = far_pair

    specs = [s.get("specs") or {} for s in component]
    source = base.get("source") or {}
    display = base.get("display") or {}
    status_entries = [s.get("status") or {} for s in component]

    lengths = [x.get("length_m") for x in specs if x.get("length_m") is not None]
    verticals = [x.get("vertical_drop_m") for x in specs if x.get("vertical_drop_m") is not None]
    avg_pairs = [
        (x.get("average_gradient"), x.get("length_m"))
        for x in specs
        if x.get("average_gradient") is not None and x.get("length_m") not in (None, 0)
    ]
    max_grads = [x.get("max_gradient") for x in specs if x.get("max_gradient") is not None]

    weighted_avg = None
    if avg_pairs:
        w_sum = sum(length for _, length in avg_pairs)
        if w_sum:
            weighted_avg = sum(avg * length for avg, length in avg_pairs) / w_sum

    notes = []
    for st in status_entries:
        note = st.get("note")
        if note:
            note_text = str(note).strip()
            if note_text and note_text not in notes:
                notes.append(note_text)

    updated_times = [parse_iso((st.get("updated_at"))) for st in status_entries]
    updated_times = [t for t in updated_times if t is not None]
    latest_updated = max(updated_times).strftime("%Y-%m-%d %H:%M:%S") if updated_times else None

    src_url = next((source_url for source_url in [source.get("source_url")] if source_url), None)
    if not src_url:
        for st in status_entries:
            if st.get("source_url"):
                src_url = st.get("source_url")
                break

    base_path = (((base.get("geometry") or {}).get("path")) or [])
    merged_path = None
    if base_path:
        merged_path = [
            {
                "latitude": p.get("latitude"),
                "longitude": p.get("longitude"),
            }
            for p in base_path
            if p.get("latitude") is not None and p.get("longitude") is not None
        ]
    elif start_lat is not None and start_lon is not None and end_lat is not None and end_lon is not None:
        merged_path = [
            {"latitude": start_lat, "longitude": start_lon},
            {"latitude": end_lat, "longitude": end_lon},
        ]

    return {
        "resort_id": resort_id,
        "name": base.get("name"),
        "difficulty": display.get("difficulty") or "blue",
        "length_m": sum(lengths) if lengths else None,
        "vertical_drop_m": sum(verticals) if verticals else None,
        "average_gradient": weighted_avg,
        "max_gradient": max(max_grads) if max_grads else None,
        "snowmaking": any(bool(x.get("snowmaking")) for x in specs),
        "night_skiing": any(bool(x.get("night_skiing")) for x in specs),
        "family_friendly": any(bool(x.get("family_friendly")) for x in specs),
        "race_slope": any(bool(x.get("race_slope")) for x in specs),
        "lat_start": start_lat,
        "lon_start": start_lon,
        "lat_end": end_lat,
        "lon_end": end_lon,
        "source_system": source.get("system") or "osm",
        "source_entity_id": source.get("entity_id"),
        "name_normalized": display.get("normalized_name") or normalize_name(base.get("name")),
        "operational_status": choose_status([st.get("operational_status") for st in status_entries]),
        "grooming_status": choose_grooming([st.get("grooming_status") for st in status_entries]),
        "operational_note": " | ".join(notes) if notes else None,
        "status_updated_at": latest_updated,
        "status_source_url": src_url,
        "slope_path_json": json.dumps(merged_path, ensure_ascii=True) if merged_path else None,
    }


def api_get(path):
    url = f"{API_BASE_URL}{path}"
    r = requests.get(url, params={"api_key": API_KEY}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def api_put(path, payload):
    url = f"{API_BASE_URL}{path}"
    r = requests.put(url, json=payload, headers=HEADERS, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT {path} failed: {r.status_code} {r.text}")


def api_delete(path):
    url = f"{API_BASE_URL}{path}"
    r = requests.delete(url, headers=HEADERS, timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"DELETE {path} failed: {r.status_code} {r.text}")


def load_resort_ids(resort_ids_arg):
    if resort_ids_arg:
        return [x.strip() for x in resort_ids_arg.split(",") if x.strip()]
    summaries = api_get("/resorts?summary=true")
    return [item["id"] for item in summaries if item.get("id")]


def merge_for_resort(resort_id, distance_threshold_m, apply_changes):
    slopes = api_get(f"/slopes/by_resort/{resort_id}")
    if not isinstance(slopes, list) or not slopes:
        logger.info("Resort %s: no slopes found", resort_id)
        return {"groups": 0, "components": 0, "merged": 0}

    grouped = defaultdict(list)
    for slope in slopes:
        name = normalize_name(slope.get("name"))
        difficulty = ((slope.get("display") or {}).get("difficulty") or "").strip().lower()
        if not name:
            continue
        grouped[(name, difficulty)].append(slope)

    groups_count = 0
    components_count = 0
    merged_count = 0

    for (name_key, difficulty), rows in grouped.items():
        if len(rows) < 2:
            continue
        groups_count += 1
        components = connected_components(rows, distance_threshold_m)
        for comp in components:
            if len(comp) < 2:
                continue
            components_count += 1
            comp_sorted = sorted(comp, key=lambda x: x.get("id"))
            keep = comp_sorted[0]
            delete_rows = comp_sorted[1:]
            payload = build_merge_payload(comp_sorted, resort_id)
            ids_all = [s.get("id") for s in comp_sorted]

            logger.info(
                "Resort %s merge candidate name='%s' diff='%s' ids=%s keep=%s delete=%s",
                resort_id,
                name_key,
                difficulty or "n/a",
                ids_all,
                keep.get("id"),
                [s.get("id") for s in delete_rows],
            )

            if not apply_changes:
                continue

            try:
                api_put(f"/slopes/{keep['id']}", payload)
                for row in delete_rows:
                    api_delete(f"/slopes/{row['id']}")
                merged_count += 1
            except Exception as exc:
                logger.warning(
                    "Resort %s merge failed for ids=%s: %s",
                    resort_id,
                    ids_all,
                    exc,
                )

    return {
        "groups": groups_count,
        "components": components_count,
        "merged": merged_count,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Merge same-name slope segments with near-identical endpoints."
    )
    parser.add_argument(
        "--resort-ids",
        default="",
        help="Comma-separated resort IDs. If omitted, all resorts are processed.",
    )
    parser.add_argument(
        "--distance-m",
        type=float,
        default=45.0,
        help="Endpoint distance threshold in meters for linking segments (default: 45).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (PUT + DELETE). Without this flag it runs as dry-run.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger.info("Starting slope merge. mode=%s", "apply" if args.apply else "dry-run")

    resort_ids = load_resort_ids(args.resort_ids)
    logger.info("Processing %s resorts", len(resort_ids))

    total_groups = 0
    total_components = 0
    total_merged = 0

    for idx, resort_id in enumerate(resort_ids, start=1):
        logger.info("[%s/%s] resort=%s", idx, len(resort_ids), resort_id)
        stats = merge_for_resort(
            resort_id=resort_id,
            distance_threshold_m=args.distance_m,
            apply_changes=args.apply,
        )
        total_groups += stats["groups"]
        total_components += stats["components"]
        total_merged += stats["merged"]

    logger.info(
        "Done. groups=%s connected_components=%s merged=%s mode=%s",
        total_groups,
        total_components,
        total_merged,
        "apply" if args.apply else "dry-run",
    )


if __name__ == "__main__":
    main()

