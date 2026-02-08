import subprocess
import sys
import time

# =========================
# CONFIG
# =========================
NUM_WORKERS = 20         # Anzahl Worker
SCRIPT_NAME = "ski_scraper.py"  # Dein Hauptskript
START_DELAY = 15          # Sekunden zwischen Starts (wichtig für Overpass)

# =========================
# PYTHON EXECUTABLE
# =========================
PYTHON = sys.executable  # nutzt denselben Python wie launcher.py


def main():

    processes = []

    print(f"Starte {NUM_WORKERS} Worker...\n")

    for i in range(NUM_WORKERS):

        print(f"→ Starte Worker {i}")

        cmd = [
            PYTHON,
            SCRIPT_NAME,
            str(i)
        ]

        p = subprocess.Popen(cmd)

        processes.append(p)

        # kleine Pause (wichtig gegen API-Spam)
        time.sleep(START_DELAY)

    print("\nAlle Worker gestartet.")

    # Optional: Warten bis alle fertig sind
    for p in processes:
        p.wait()

    print("\nAlle Worker beendet.")


if __name__ == "__main__":
    main()
