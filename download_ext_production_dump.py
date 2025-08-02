import paramiko
from scp import SCPClient
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# ====== Load Credentials ======
EXT_PRO_SSH_KEY_PATH = os.getenv("EXT_PRO_SSH_KEY_PATH")
EXT_PRO_SSH_USER = os.getenv("EXT_PRO_SSH_USER")
EXT_PRO_SSH_HOST = os.getenv("EXT_PRO_SSH_HOST")

EXT_PRO_DB_NAME = os.getenv("EXT_PRO_DB_NAME")
EXT_PRO_DB_USER = os.getenv("EXT_PRO_DB_USER")
EXT_PRO_DB_PASSWORD = os.getenv("EXT_PRO_DB_PASSWORD")
EXT_PRO_DB_HOST = os.getenv("EXT_PRO_DB_HOST")
EXT_PRO_DB_PORT = os.getenv("EXT_PRO_DB_PORT")

DUMP_FOLDER = "EXT_production"
os.makedirs(DUMP_FOLDER, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
DUMP_FILE_REMOTE = f"dump_{timestamp}.sql"
DUMP_FILE_LOCAL = os.path.join(DUMP_FOLDER, DUMP_FILE_REMOTE)

def create_ssh_client():
    key = paramiko.RSAKey.from_private_key_file(EXT_PRO_SSH_KEY_PATH)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(EXT_PRO_SSH_HOST, username=EXT_PRO_SSH_USER, pkey=key)
    return client

def download_db_dump():
    ssh = create_ssh_client()
    print("Connected to server.")

    # Run mysqldump on the remote server
    dump_cmd = f"mysqldump -u {EXT_PRO_DB_USER} -p'{EXT_PRO_DB_PASSWORD}' -h {EXT_PRO_DB_HOST} -P {EXT_PRO_DB_PORT} {EXT_PRO_DB_NAME} > {DUMP_FILE_REMOTE}"
    stdin, stdout, stderr = ssh.exec_command(dump_cmd)
    exit_status = stdout.channel.recv_exit_status()
    
    if exit_status == 0:
        print("Database dumped successfully on server.")
    else:
        print("[ERROR] mysqldump failed:", stderr.read().decode())
        ssh.close()
        return

    # Download file from server
    scp = SCPClient(ssh.get_transport())
    scp.get(DUMP_FILE_REMOTE, DUMP_FILE_LOCAL)
    scp.close()

    print(f"[INFO] Dump file downloaded locally at '{DUMP_FILE_LOCAL}'.")

    # Remove dump from server (optional)
    ssh.exec_command(f"rm {DUMP_FILE_REMOTE}")
    ssh.close()

if __name__ == "__main__":
    download_db_dump()
