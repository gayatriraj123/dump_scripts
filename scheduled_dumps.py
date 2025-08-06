import subprocess
import schedule
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

load_dotenv()

import os
import sys

# Dynamically get current venv python executable
VENV_PYTHON = sys.executable

# # Path to your venv Python
# VENV_PYTHON = r"C:\Prushal\dump_download_scrape\.venv\Scripts\python.exe"


# Folders where dumps are stored
DUMP_FOLDERS = {
    "bopo": "BOPO_test",
    "ext_test": "EXT_test",
    "ext_prod": "EXT_production"
}

# ===== Google Drive folder IDs =====
DRIVE_FOLDERS = {
    "bopo": [
        os.getenv("DRIVE_BOPO_FOLDER_ID"),
        os.getenv("DRIVE_BOPO_BACKUP_FOLDER_ID_1"),
        os.getenv("DRIVE_BOPO_BACKUP_FOLDER_ID_2")
    ],
    "ext_test": [
        os.getenv("DRIVE_EXT_TEST_FOLDER_ID"),
        os.getenv("DRIVE_EXT_TEST_BACKUP_FOLDER_ID_1"),
        os.getenv("DRIVE_EXT_TEST_BACKUP_FOLDER_ID_2")
    ],
    "ext_prod": [
        os.getenv("DRIVE_EXT_PROD_FOLDER_ID"),
        os.getenv("DRIVE_EXT_PROD_BACKUP_FOLDER_ID_1"),
        os.getenv("DRIVE_EXT_PROD_BACKUP_FOLDER_ID_2")
    ]
}


# Keep dumps for last 10 days
KEEP_LAST = 10
RETENTION_DAYS = 10 

# ===== Authenticate Google Drive =====
gauth = GoogleAuth()
gauth.LoadCredentialsFile("token.json")

if not gauth.credentials:
    gauth.LocalWebserverAuth()
    gauth.SaveCredentialsFile("token.json")  # ✅ Save token after first login
else:
    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile("token.json")  # ✅ Save refreshed token
    else:
        gauth.Authorize()
drive = GoogleDrive(gauth)


def cleanup_old_local_dumps(folder):
    """Keep only the latest N .sql files, delete the rest."""
    if not os.path.exists(folder):
        return
    
    # List all .sql files in folder
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".sql")]
    files.sort(key=os.path.getmtime, reverse=True)
    
    # Delete all files except the most recent `keep_last`
    for old_file in files[KEEP_LAST:]:
        print(f"[INFO] Deleting old dump: {old_file}")
        os.remove(old_file)

def upload_to_drive(local_file, folder_id):
    """Upload file to Google Drive."""
    file_name = os.path.basename(local_file)
    gfile = drive.CreateFile({"parents": [{"id": folder_id}], "title": file_name})
    gfile.SetContentFile(local_file)
    gfile.Upload()
    print(f"[INFO] Uploaded to Google Drive folder {folder_id}: {file_name}")


def cleanup_old_drive_files(folder_id, keep_last=10):
    """Keep only the latest keep_last files in Google Drive folder."""
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
    # Sort by modifiedDate (newest first)
    file_list.sort(key=lambda x: x['modifiedDate'], reverse=True)

    for old_file in file_list[keep_last:]:
        print(f"[INFO] Deleting old Google Drive file: {old_file['title']}")
        old_file.Delete()

def process_dump(db_key, script_name):
    """Run dump, upload to multiple folders, cleanup."""
    cleanup_old_local_dumps(DUMP_FOLDERS[db_key])

    print(f"=== Running {db_key.upper()} dump ===")
    subprocess.run([VENV_PYTHON, script_name])

    # Find latest local dump
    latest_dump = max(
        [os.path.join(DUMP_FOLDERS[db_key], f) for f in os.listdir(DUMP_FOLDERS[db_key]) if f.endswith(".sql")],
        key=os.path.getmtime
    )

    # Upload to all configured Google Drive folders
    for folder_id in DRIVE_FOLDERS[db_key]:
        if folder_id:
            upload_to_drive(latest_dump, folder_id)
            cleanup_old_drive_files(folder_id, KEEP_LAST)

def run_all_dumps():
    print(f"\n[{datetime.now()}] === Starting scheduled database dumps ===")

    process_dump("bopo", "download_bopo_dump.py")
    process_dump("ext_test", "download_EXT_dump.py")
    process_dump("ext_prod", "download_ext_production_dump.py")

    print(f"[{datetime.now()}] === All dumps completed ===\n")


# (24-hour format)
# schedule.every().day.at("02:00").do(run_all_dumps)  # every day at 2 AM
schedule.every(0.5).minutes.do(run_all_dumps)  # run every 1 minute for testing


print("[INFO] Scheduler started. Waiting for scheduled time...")

while True:
    schedule.run_pending()
    time.sleep(10)  # check every 30 seconds
