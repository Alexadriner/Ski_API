import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# =========================
# CONFIG
# =========================
NUM_WORKERS = 10
START_DELAY = 2
SCRIPT_PATH = Path(__file__).resolve().parent / "cleanup_ski_data.py"
REASSIGN_SCRIPT_PATH = Path(__file__).resolve().parent / "reassign_entities_by_resort_cluster.py"
UPDATE_RESORT_COORDS_SCRIPT_PATH = Path(__file__).resolve().parent / "update_resort_coordinates.py"
ENRICH_SLOPE_PATHS_SCRIPT_PATH = Path(__file__).resolve().parent / "enrich_slope_paths_from_osm.py"
MERGE_SCRIPT_PATH = Path(__file__).resolve().parent / "merge_similar_slopes.py"
BASE_DIR = Path(__file__).resolve().parents[2]
CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "cleanup"
PROGRESS_FILE = CHECKPOINT_DIR / "launcher_progress.txt"

# =========================
# PYTHON EXECUTABLE
# =========================
PYTHON = sys.executable
STAGES = [
    "cleanup_workers",
    "resort_coords",
    "reassign",
    "enrich_slope_paths",
    "merge",
    "done",
]


def save_progress(stage, status="running", last_value=None):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "stage": stage,
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "last_value": last_value,
    }
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True))


def load_progress():
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return STAGES[0]

            # Backward compatibility: old format used plain stage text.
            if raw in STAGES:
                return raw

            data = json.loads(raw)
            stage = str(data.get("stage", "")).strip()
            if stage in STAGES:
                return stage
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return STAGES[0]


def next_stage(current_stage):
    try:
        idx = STAGES.index(current_stage)
    except ValueError:
        return STAGES[0]
    if idx >= len(STAGES) - 1:
        return STAGES[-1]
    return STAGES[idx + 1]


def run_cleanup_workers(num_workers, start_delay):
    processes = []

    print(f"Starte {num_workers} Cleanup-Worker...\n")

    for i in range(num_workers):
        print(f"-> Starte Cleanup-Worker {i}")

        cmd = [
            PYTHON,
            str(SCRIPT_PATH),
            str(i),
            str(num_workers),
        ]

        p = subprocess.Popen(cmd, cwd=str(BASE_DIR))
        processes.append(p)
        time.sleep(start_delay)

    print("\nAlle Cleanup-Worker gestartet.")

    rc = 0
    for p in processes:
        code = p.wait()
        if code != 0 and rc == 0:
            rc = code

    print("\nAlle Cleanup-Worker beendet.")
    return rc


def run_cluster_reassign(cluster_km, switch_margin_m):
    print("\nStarte Cluster-Reassignment (eindeutige Resort-Zuordnung)...")
    cmd = [
        PYTHON,
        str(REASSIGN_SCRIPT_PATH),
        "--cluster-km",
        str(cluster_km),
        "--switch-margin-m",
        str(switch_margin_m),
    ]
    return subprocess.call(cmd, cwd=str(BASE_DIR))


def run_resort_coordinate_update():
    print("\nStarte Resort-Koordinaten-Update (OSM -> Zentrum -> unveraendert)...")
    cmd = [
        PYTHON,
        str(UPDATE_RESORT_COORDS_SCRIPT_PATH),
    ]
    return subprocess.call(cmd, cwd=str(BASE_DIR))


def run_merge_similar_slopes(merge_distance_m):
    print("\nStarte Merge Similar Slopes...")
    cmd = [
        PYTHON,
        str(MERGE_SCRIPT_PATH),
        "--distance-m",
        str(merge_distance_m),
        "--apply",
    ]
    return subprocess.call(cmd, cwd=str(BASE_DIR))


def run_enrich_slope_paths():
    print("\nStarte OSM-Pistenverlauf-Enrichment...")
    cmd = [
        PYTHON,
        str(ENRICH_SLOPE_PATHS_SCRIPT_PATH),
    ]
    return subprocess.call(cmd, cwd=str(BASE_DIR))


def main():
    parser = argparse.ArgumentParser(
        description="Run cleanup workers, then cluster reassignment and slope merge."
    )
    parser.add_argument("--workers", type=int, default=NUM_WORKERS)
    parser.add_argument("--start-delay", type=float, default=START_DELAY)
    parser.add_argument("--cluster-km", type=float, default=9.0)
    parser.add_argument("--switch-margin-m", type=float, default=250.0)
    parser.add_argument("--merge-distance-m", type=float, default=45.0)
    parser.add_argument("--skip-resort-coords", action="store_true")
    parser.add_argument("--skip-reassign", action="store_true")
    parser.add_argument("--skip-enrich-slope-paths", action="store_true")
    parser.add_argument("--skip-merge", action="store_true")
    parser.add_argument("--reset-progress", action="store_true")
    args = parser.parse_args()

    if args.reset_progress:
        save_progress(
            STAGES[0],
            status="reset",
            last_value={"last_completed_stage": None},
        )

    start_stage = load_progress()
    save_progress(
        start_stage,
        status="resume",
        last_value={"message": "launcher_started_or_resumed"},
    )

    if start_stage == "done":
        print("\nCleanup-Launcher ist laut Fortschrittsdatei bereits abgeschlossen.")
        print(f"Fuer Neustart: --reset-progress (Datei: {PROGRESS_FILE})")
        return

    print(f"\nFortschritt: starte/resume ab Stage '{start_stage}' ({PROGRESS_FILE})")

    stage_runners = [
        (
            "cleanup_workers",
            True,
            lambda: run_cleanup_workers(
                num_workers=max(1, args.workers),
                start_delay=max(0.0, args.start_delay),
            ),
            "Cleanup-Worker failed",
        ),
        (
            "resort_coords",
            not args.skip_resort_coords,
            run_resort_coordinate_update,
            "Resort-Koordinaten-Update fehlgeschlagen",
        ),
        (
            "reassign",
            not args.skip_reassign,
            lambda: run_cluster_reassign(
                cluster_km=args.cluster_km,
                switch_margin_m=args.switch_margin_m,
            ),
            "Cluster-Reassignment fehlgeschlagen",
        ),
        (
            "enrich_slope_paths",
            not args.skip_enrich_slope_paths,
            run_enrich_slope_paths,
            "OSM-Pistenverlauf-Enrichment fehlgeschlagen",
        ),
        (
            "merge",
            not args.skip_merge,
            lambda: run_merge_similar_slopes(args.merge_distance_m),
            "Merge Similar Slopes fehlgeschlagen",
        ),
    ]

    running = False
    for stage_name, enabled, runner, error_prefix in stage_runners:
        if not running:
            if stage_name != start_stage:
                continue
            running = True

        if not enabled:
            print(f"\nStage '{stage_name}' wird per Flag uebersprungen.")
            save_progress(
                next_stage(stage_name),
                status="skipped",
                last_value={"last_completed_stage": stage_name},
            )
            continue

        save_progress(
            stage_name,
            status="running",
            last_value={"current_stage": stage_name},
        )
        rc = runner()
        if rc != 0:
            save_progress(
                stage_name,
                status="failed",
                last_value={"failed_stage": stage_name, "exit_code": rc},
            )
            print(f"\n{error_prefix} (exit={rc}).")
            print(f"Fortschritt gespeichert bei Stage '{stage_name}'.")
            sys.exit(rc)

        save_progress(
            next_stage(stage_name),
            status="ok",
            last_value={"last_completed_stage": stage_name},
        )

    save_progress(
        "done",
        status="ok",
        last_value={"last_completed_stage": "merge"},
    )
    print("\nCleanup-Launcher abgeschlossen.")


if __name__ == "__main__":
    main()
