import argparse
import json
import logging
import os
import re
import time
import unicodedata

import requests


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")
API_KEY = os.getenv("API_KEY", "R3StTY4OfadeFJZurXdZ1pZMVbWB3zWuL6FnuPGIbvA")
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}",
}
OVERPASS_URL = os.getenv("OVERPASS_URL", "https://overpass-api.de/api/interpreter")

logger = logging.getLogger("enrich_slope_paths_from_osm")


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


def to_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def haversine_m(a_lat, a_lon, b_lat, b_lon):
    import math

    r = 6371000.0
    p1 = math.radians(a_lat)
    p2 = math.radians(b_lat)
    dp = math.radians(b_lat - a_lat)
    dl = math.radians(b_lon - a_lon)
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(x), math.sqrt(1 - x))


def slope_endpoints(slope):
    geometry = slope.get("geometry") or {}
    start = geometry.get("start") or {}
    end = geometry.get("end") or {}
    a = (to_float(start.get("latitude")), to_float(start.get("longitude")))
    b = (to_float(end.get("latitude")), to_float(end.get("longitude")))
    return a, b


def escape_overpass_value(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def overpass_request(query, timeout_s):
    r = requests.post(OVERPASS_URL, data=query.encode("utf-8"), timeout=timeout_s)
    r.raise_for_status()
    return r.json()


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


def extract_line_points(element):
    geometry = element.get("geometry")
    if not isinstance(geometry, list) or len(geometry) < 2:
        return []
    points = []
    for p in geometry:
        lat = to_float(p.get("lat"))
        lon = to_float(p.get("lon"))
        if lat is None or lon is None:
            continue
        points.append((lat, lon))
    return points


def endpoint_match_distance(candidate_points, slope_a, slope_b):
    if len(candidate_points) < 2:
        return None
    c_start = candidate_points[0]
    c_end = candidate_points[-1]

    a_ok = slope_a[0] is not None and slope_a[1] is not None
    b_ok = slope_b[0] is not None and slope_b[1] is not None
    if not a_ok or not b_ok:
        return None

    d1 = haversine_m(c_start[0], c_start[1], slope_a[0], slope_a[1]) + haversine_m(
        c_end[0], c_end[1], slope_b[0], slope_b[1]
    )
    d2 = haversine_m(c_start[0], c_start[1], slope_b[0], slope_b[1]) + haversine_m(
        c_end[0], c_end[1], slope_a[0], slope_a[1]
    )
    return min(d1, d2)


def choose_best_candidate(elements, slope):
    slope_name = normalize_name(slope.get("name"))
    slope_diff = ((slope.get("display") or {}).get("difficulty") or "").strip().lower()
    s_a, s_b = slope_endpoints(slope)

    best = None
    best_score = None

    for el in elements:
        tags = el.get("tags") or {}
        points = extract_line_points(el)
        if len(points) < 2:
            continue

        score = 0.0
        d = endpoint_match_distance(points, s_a, s_b)
        if d is not None:
            score += d
        else:
            score += 5000.0

        if slope_name:
            c_name = normalize_name(tags.get("name"))
            if c_name == slope_name:
                score -= 1000.0
            elif slope_name and slope_name in c_name:
                score -= 250.0

        c_diff_raw = (tags.get("piste:difficulty") or "").lower()
        diff_map = {
            "easy": "blue",
            "intermediate": "red",
            "advanced": "black",
            "expert": "black",
            "novice": "green",
            "beginner": "green",
            "freeride": "black",
        }
        c_diff = diff_map.get(c_diff_raw, c_diff_raw)
        if slope_diff and c_diff and c_diff != slope_diff:
            score += 500.0

        if best_score is None or score < best_score:
            best_score = score
            best = points

    return best


def overpass_query_for_slope(resort_lat, resort_lon, slope, radius_m):
    name = slope.get("name")
    diff = (slope.get("display") or {}).get("difficulty")
    diff_filter = ""
    if diff:
        reverse = {
            "green": "novice|beginner",
            "blue": "easy",
            "red": "intermediate",
            "black": "advanced|expert|freeride",
        }
        mapped = reverse.get(str(diff).lower())
        if mapped:
            diff_filter = f'["piste:difficulty"~"^({mapped})$"]'

    if name:
        escaped = escape_overpass_value(name)
        return f"""
        [out:json][timeout:45];
        (
          way(around:{int(radius_m)},{resort_lat},{resort_lon})["piste:type"="downhill"]["name"="{escaped}"]{diff_filter};
          relation(around:{int(radius_m)},{resort_lat},{resort_lon})["piste:type"="downhill"]["name"="{escaped}"]{diff_filter};
        );
        out geom;
        """

    # unnamed fallback: search around endpoints
    (a_lat, a_lon), (b_lat, b_lon) = slope_endpoints(slope)
    if a_lat is None or a_lon is None:
        a_lat, a_lon = resort_lat, resort_lon
    if b_lat is None or b_lon is None:
        b_lat, b_lon = resort_lat, resort_lon

    return f"""
    [out:json][timeout:45];
    (
      way(around:600,{a_lat},{a_lon})["piste:type"="downhill"]{diff_filter};
      way(around:600,{b_lat},{b_lon})["piste:type"="downhill"]{diff_filter};
      relation(around:600,{a_lat},{a_lon})["piste:type"="downhill"]{diff_filter};
      relation(around:600,{b_lat},{b_lon})["piste:type"="downhill"]{diff_filter};
    );
    out geom;
    """


def slope_payload_from_existing(slope):
    display = slope.get("display") or {}
    geometry = slope.get("geometry") or {}
    start = geometry.get("start") or {}
    end = geometry.get("end") or {}
    specs = slope.get("specs") or {}
    source = slope.get("source") or {}
    status = slope.get("status") or {}

    return {
        "resort_id": slope.get("resort_id"),
        "name": slope.get("name"),
        "difficulty": display.get("difficulty") or slope.get("difficulty") or "blue",
        "length_m": specs.get("length_m"),
        "vertical_drop_m": specs.get("vertical_drop_m"),
        "average_gradient": specs.get("average_gradient"),
        "max_gradient": specs.get("max_gradient"),
        "snowmaking": bool(specs.get("snowmaking")),
        "night_skiing": bool(specs.get("night_skiing")),
        "family_friendly": bool(specs.get("family_friendly")),
        "race_slope": bool(specs.get("race_slope")),
        "lat_start": start.get("latitude"),
        "lon_start": start.get("longitude"),
        "lat_end": end.get("latitude"),
        "lon_end": end.get("longitude"),
        "source_system": source.get("system") or "osm",
        "source_entity_id": source.get("entity_id"),
        "name_normalized": display.get("normalized_name"),
        "operational_status": status.get("operational_status") or "unknown",
        "grooming_status": status.get("grooming_status") or "unknown",
        "operational_note": status.get("note"),
        "status_updated_at": status.get("updated_at"),
        "status_source_url": source.get("source_url"),
        "slope_path_json": None,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Find slope paths in OSM and store them into API/database before merge."
    )
    parser.add_argument("--resort-ids", default="")
    parser.add_argument("--radius-m", type=float, default=12000.0)
    parser.add_argument("--overpass-timeout", type=float, default=60.0)
    parser.add_argument("--request-delay", type=float, default=0.3)
    parser.add_argument("--refresh", action="store_true", help="Refresh paths even if already present.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    resorts = api_get("/resorts")
    if args.resort_ids:
        wanted = {x.strip() for x in args.resort_ids.split(",") if x.strip()}
        resorts = [r for r in resorts if r.get("id") in wanted]

    updates = 0
    scanned = 0

    for i, resort in enumerate(resorts, start=1):
        rid = resort.get("id")
        r_lat = to_float(((resort.get("geography") or {}).get("coordinates") or {}).get("latitude"))
        r_lon = to_float(((resort.get("geography") or {}).get("coordinates") or {}).get("longitude"))
        if r_lat is None or r_lon is None:
            logger.info("[%s/%s] %s skipped: missing resort coordinates", i, len(resorts), rid)
            continue

        detail = api_get(f"/resorts/{rid}")
        slopes = detail.get("slopes") or []
        logger.info("[%s/%s] %s slopes=%s", i, len(resorts), rid, len(slopes))

        for slope in slopes:
            scanned += 1
            path_exists = bool(((slope.get("geometry") or {}).get("path")))
            if path_exists and not args.refresh:
                continue

            query = overpass_query_for_slope(r_lat, r_lon, slope, args.radius_m)
            try:
                osm = overpass_request(query, args.overpass_timeout)
            except Exception as exc:
                logger.warning("Slope #%s resort=%s overpass failed: %s", slope.get("id"), rid, exc)
                continue

            elements = build_relation_geometries(osm.get("elements", []))
            best_points = choose_best_candidate(elements, slope)
            if not best_points or len(best_points) < 2:
                continue

            payload = slope_payload_from_existing(slope)
            payload["lat_start"] = best_points[0][0]
            payload["lon_start"] = best_points[0][1]
            payload["lat_end"] = best_points[-1][0]
            payload["lon_end"] = best_points[-1][1]
            payload["slope_path_json"] = json.dumps(
                [{"latitude": p[0], "longitude": p[1]} for p in best_points],
                ensure_ascii=True,
            )

            logger.info(
                "Path match slope #%s resort=%s name=%s points=%s",
                slope.get("id"),
                rid,
                slope.get("name"),
                len(best_points),
            )
            if not args.dry_run:
                try:
                    api_put(f"/slopes/{slope['id']}", payload)
                    updates += 1
                except Exception as exc:
                    logger.warning("Slope update failed #%s: %s", slope.get("id"), exc)
            time.sleep(args.request_delay)

    logger.info(
        "Done. scanned=%s updated=%s mode=%s",
        scanned,
        updates,
        "dry-run" if args.dry_run else "apply",
    )


if __name__ == "__main__":
    main()

