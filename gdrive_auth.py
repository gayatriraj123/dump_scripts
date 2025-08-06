from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

gauth = GoogleAuth("settings.yaml")
gauth.LoadCredentialsFile("token.json")

if not gauth.credentials:
    print("[AUTH] No saved credentials. Starting first-time auth...")
    gauth.LocalWebserverAuth()  # browser-based login
elif gauth.access_token_expired:
    print("[AUTH] Token expired. Refreshing...")
    gauth.Refresh()
else:
    print("[AUTH] Token valid. Using saved credentials.")
    gauth.Authorize()

gauth.SaveCredentialsFile("token.json")
drive = GoogleDrive(gauth)
