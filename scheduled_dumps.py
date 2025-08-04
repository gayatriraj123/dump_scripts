import subprocess
import schedule
import time
from datetime import datetime, timedelta
import os

# Path to your venv Python
VENV_PYTHON = r"C:\Prushal\dump_download_scrape\.venv\Scripts\python.exe"

# Folders where dumps are stored
DUMP_FOLDERS = {
    "bopo": "BOPO_test",
    "ext_test": "EXT_test",
    "ext_prod": "EXT_production"
}

# Keep dumps for last 10 days
KEEP_LAST = 10

def cleanup_old_dumps(folder, KEEP_LAST=10):
    """Keep only the latest N .sql files, delete the rest."""
    if not os.path.exists(folder):
        return
    
    # List all .sql files in folder
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".sql")]
    
    # Sort by modification time (newest first)
    files.sort(key=os.path.getmtime, reverse=True)
    
    # Delete all files except the most recent `keep_last`
    for old_file in files[KEEP_LAST:]:
        print(f"[INFO] Deleting old dump: {old_file}")
        os.remove(old_file)


def run_all_dumps():
    print(f"[{datetime.now()}] === Starting scheduled database dumps ===")

    # BOPO
    cleanup_old_dumps(DUMP_FOLDERS["bopo"])
    print("=== Running BOPO dump ===")
    subprocess.run([VENV_PYTHON, "download_bopo_dump.py"])

    # EXT Test
    cleanup_old_dumps(DUMP_FOLDERS["ext_test"])
    print("=== Running EXT Test dump ===")
    subprocess.run([VENV_PYTHON, "download_EXT_dump.py"])

    # EXT Production
    cleanup_old_dumps(DUMP_FOLDERS["ext_prod"])
    print("=== Running EXT Production dump ===")
    subprocess.run([VENV_PYTHON, "download_ext_production_dump.py"])

    print(f"[{datetime.now()}] === All dumps download completed ===\n")

# Schedule time (24-hour format)
schedule.every().day.at("02:00").do(run_all_dumps)  # Runs every day at 2 AM
# schedule.every(0.5).minutes.do(run_all_dumps)  # run every 1 minute for testing


print("[INFO] Scheduler started. Waiting for scheduled time...")

while True:
    schedule.run_pending()
    time.sleep(10)  # check every 30 seconds
