import subprocess
import sys
import time
from pathlib import Path

# =========================
# CONFIG
# =========================
NUM_WORKERS = 10
START_DELAY = 2
SCRIPT_PATH = Path(__file__).resolve().parent / "cleanup_ski_data.py"
BASE_DIR = Path(__file__).resolve().parents[2]

# =========================
# PYTHON EXECUTABLE
# =========================
PYTHON = sys.executable


def main():
    processes = []

    print(f"Starte {NUM_WORKERS} Cleanup-Worker...\n")

    for i in range(NUM_WORKERS):
        print(f"-> Starte Cleanup-Worker {i}")

        cmd = [
            PYTHON,
            str(SCRIPT_PATH),
            str(i),
            str(NUM_WORKERS),
        ]

        p = subprocess.Popen(cmd, cwd=str(BASE_DIR))
        processes.append(p)
        time.sleep(START_DELAY)

    print("\nAlle Cleanup-Worker gestartet.")

    for p in processes:
        p.wait()

    print("\nAlle Cleanup-Worker beendet.")


if __name__ == "__main__":
    main()
