import paramiko
from scp import SCPClient
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env_bop
load_dotenv(".env")

# ====== Load credentials ======
BOPO_SSH_HOST = os.getenv("BOPO_SSH_HOST")
BOPO_SSH_PORT = int(os.getenv("BOPO_SSH_PORT"))
BOPO_SSH_USER = os.getenv("BOPO_SSH_USER")
BOPO_SSH_PASSWORD = os.getenv("BOPO_SSH_PASSWORD")

BOPO_DB_NAME = os.getenv("BOPO_DB_NAME")
BOPO_DB_USER = os.getenv("BOPO_DB_USER")
BOPO_DB_PASSWORD = os.getenv("BOPO_DB_PASSWORD")
BOPO_DB_HOST = os.getenv("BOPO_DB_HOST")
BOPO_DB_PORT = os.getenv("BOPO_DB_PORT")

# ====== Folder to store dumps ======
DUMP_FOLDER = "BOPO_test"
os.makedirs(DUMP_FOLDER, exist_ok=True)

# Create a timestamped dump file name
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
DUMP_FILE_REMOTE = f"dump_{timestamp}.sql"
DUMP_FILE_LOCAL = os.path.join(DUMP_FOLDER, DUMP_FILE_REMOTE)

def create_ssh_client():
    """Create SSH connection using username & password."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        BOPO_SSH_HOST,
        port=BOPO_SSH_PORT,
        username=BOPO_SSH_USER,
        password=BOPO_SSH_PASSWORD
    )
    return client

def download_db_dump():
    ssh = create_ssh_client()
    print("Connected to server.")

    # Run mysqldump on remote server
    dump_cmd = f"mysqldump -u {BOPO_DB_USER} -p'{BOPO_DB_PASSWORD}' -h {BOPO_DB_HOST} -P {BOPO_DB_PORT} {BOPO_DB_NAME} > {DUMP_FILE_REMOTE}"
    stdin, stdout, stderr = ssh.exec_command(dump_cmd)
    exit_status = stdout.channel.recv_exit_status()

    if exit_status == 0:
        print("Database dumped successfully on server.")
    else:
        print("[ERROR] mysqldump failed:", stderr.read().decode())
        ssh.close()
        return

    # Download dump file
    scp = SCPClient(ssh.get_transport())
    scp.get(DUMP_FILE_REMOTE, DUMP_FILE_LOCAL)
    scp.close()
    print(f"Dump file downloaded locally at '{DUMP_FILE_LOCAL}'.")

    # Optional: delete dump file from server
    ssh.exec_command(f"rm {DUMP_FILE_REMOTE}")
    ssh.close()

if __name__ == "__main__":
    download_db_dump()
