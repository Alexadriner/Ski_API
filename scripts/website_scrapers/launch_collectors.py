import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def discover_collectors() -> list[str]:
    collectors: list[str] = []
    for collector_file in sorted(BASE_DIR.glob("*/collector.py")):
        resort_slug = collector_file.parent.name
        if resort_slug.startswith("_"):
            continue
        collectors.append(resort_slug)
    return collectors


def build_command(
    resort_slug: str, interval_seconds: int, once: bool, no_sync_api: bool
) -> list[str]:
    module = f"scripts.website_scrapers.{resort_slug}.collector"
    # Do not force --resort-id from folder name.
    # Some collectors use a different canonical resort id than the module slug.
    cmd = [sys.executable, "-m", module]
    if interval_seconds is not None:
        cmd.extend(["--interval-seconds", str(interval_seconds)])
    if once:
        cmd.append("--once")
    if no_sync_api:
        cmd.append("--no-sync-api")
    return cmd


def terminate_processes(processes: dict[str, subprocess.Popen]) -> None:
    for name, proc in processes.items():
        if proc.poll() is None:
            print(f"[launcher] Stopping {name} (pid={proc.pid}) ...")
            proc.terminate()

    deadline = time.time() + 8
    while time.time() < deadline:
        if all(proc.poll() is not None for proc in processes.values()):
            return
        time.sleep(0.2)

    for name, proc in processes.items():
        if proc.poll() is None:
            print(f"[launcher] Killing {name} (pid={proc.pid}) ...")
            proc.kill()


def parse_csv_list(value: str | None) -> set[str]:
    if not value:
        return set()
    return {part.strip() for part in value.split(",") if part.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch all website collector.py instances in parallel."
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=300,
        help="Polling interval passed to each collector (default: 300).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run each collector only once.",
    )
    parser.add_argument(
        "--no-sync-api",
        action="store_true",
        help="Pass --no-sync-api to all collectors.",
    )
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma-separated resort slugs to start (e.g. kreuzberg,palisades_tahoe).",
    )
    parser.add_argument(
        "--skip",
        type=str,
        default="",
        help="Comma-separated resort slugs to skip.",
    )
    args = parser.parse_args()

    all_collectors = discover_collectors()
    if not all_collectors:
        print("[launcher] No collector.py files found in scripts/website_scrapers/*")
        return 1

    only = parse_csv_list(args.only)
    skip = parse_csv_list(args.skip)

    selected = [name for name in all_collectors if (not only or name in only) and name not in skip]
    if not selected:
        print("[launcher] No collectors selected after applying --only/--skip filters.")
        return 1

    print(f"[launcher] Found collectors: {', '.join(all_collectors)}")
    print(f"[launcher] Starting: {', '.join(selected)}")

    processes: dict[str, subprocess.Popen] = {}

    def _handle_shutdown(signum: int, _frame) -> None:
        print(f"\n[launcher] Received signal {signum}, shutting down collectors ...")
        terminate_processes(processes)
        raise SystemExit(130)

    signal.signal(signal.SIGINT, _handle_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_shutdown)

    try:
        for resort_slug in selected:
            cmd = build_command(
                resort_slug=resort_slug,
                interval_seconds=max(60, args.interval_seconds),
                once=args.once,
                no_sync_api=args.no_sync_api,
            )
            proc = subprocess.Popen(cmd, cwd=str(BASE_DIR.parents[1]))
            processes[resort_slug] = proc
            print(f"[launcher] Started {resort_slug} (pid={proc.pid})")

        if args.once:
            exit_code = 0
            for resort_slug, proc in processes.items():
                code = proc.wait()
                print(f"[launcher] {resort_slug} exited with code {code}")
                if code != 0:
                    exit_code = code
            return exit_code

        while True:
            time.sleep(2)
            for resort_slug, proc in processes.items():
                code = proc.poll()
                if code is not None:
                    print(f"[launcher] {resort_slug} exited unexpectedly with code {code}.")
                    terminate_processes(processes)
                    return code
    finally:
        terminate_processes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
