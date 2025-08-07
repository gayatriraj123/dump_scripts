import subprocess
import schedule
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import logging
import sys

# ==================== Logging Setup ====================
os.makedirs("logs", exist_ok=True)  # Create logs dir if it doesn't exist

logging.basicConfig(
    filename="logs/dump_scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("=== Script started ===")
# =======================================================

load_dotenv()

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
# RETENTION_DAYS = 10 

# ===== Authenticate Google Drive =====
gauth = GoogleAuth()
gauth.LoadCredentialsFile("token.json")

try:
    if not gauth.credentials:
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile("token.json")
    elif gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile("token.json")
    else:
        gauth.Authorize()
except Exception as e:
    logging.error("Google Drive authentication failed", exc_info=True)
    sys.exit(1)
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
        logging.info(f"Deleting old local dump: {old_file}")
        os.remove(old_file)

def upload_to_drive(local_file, folder_id):
    file_name = os.path.basename(local_file)
    try:
        gfile = drive.CreateFile({"parents": [{"id": folder_id}], "title": file_name})
        gfile.SetContentFile(local_file)
        gfile.Upload()
        logging.info(f"Uploaded to Google Drive folder {folder_id}: {file_name}")
    except Exception as e:
        logging.error(f"Failed to upload {file_name} to folder {folder_id}", exc_info=True)



def cleanup_old_drive_files(folder_id, keep_last=10):
    try:
        file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
        file_list.sort(key=lambda x: x['modifiedDate'], reverse=True)

        for old_file in file_list[keep_last:]:
            logging.info(f"Deleting old Google Drive file: {old_file['title']}")
            old_file.Delete()
    except Exception as e:
        logging.error(f"Drive cleanup failed for folder {folder_id}", exc_info=True)


def process_dump(db_key, script_name):
    cleanup_old_local_dumps(DUMP_FOLDERS[db_key])
    logging.info(f"=== Running {db_key.upper()} dump ===")

    try:
        subprocess.run([VENV_PYTHON, script_name], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Script failed: {script_name}", exc_info=True)
        return

    try:
        latest_dump = max(
            [os.path.join(DUMP_FOLDERS[db_key], f) for f in os.listdir(DUMP_FOLDERS[db_key]) if f.endswith(".sql")],
            key=os.path.getmtime
        )
    except Exception as e:
        logging.error(f"Could not find latest dump for {db_key}", exc_info=True)
        return

    for folder_id in DRIVE_FOLDERS[db_key]:
        if folder_id:
            upload_to_drive(latest_dump, folder_id)
            cleanup_old_drive_files(folder_id, KEEP_LAST)


def run_all_dumps():
    logging.info("=== Starting scheduled database dumps ===")
    process_dump("bopo", "download_bopo_dump.py")
    process_dump("ext_test", "download_EXT_dump.py")
    process_dump("ext_prod", "download_ext_production_dump.py")
    logging.info("=== All dumps completed ===\n")


# (24-hour format)
# schedule.every().day.at("02:00").do(run_all_dumps)  # every day at 2 AM
schedule.every(0.5).minutes.do(run_all_dumps)  # run every 1 minute for testing


logging.info("Scheduler started. Waiting for scheduled time...")

while True:
    schedule.run_pending()
    time.sleep(10)  # check every 30 seconds
