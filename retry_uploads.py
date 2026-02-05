#!/usr/bin/env python3
import os
import sqlite3
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

CREDS_PATH = os.getenv('GOOGLE_API_CREDENTIALS_PATH', 'credentials.json')
OAUTH_TOKEN_PATH = os.getenv('GOOGLE_OAUTH_TOKEN_PATH', 'token.json')
DB_PATH = os.getenv('FOOD_DB_PATH', 'food.db')
TARGET_FOLDER_ID = os.getenv('TARGET_FOLDER_ID', '1dt-L4A68Wu4KVuydb-zZi8b88sc1L5PH')


def get_creds():
    if OAUTH_TOKEN_PATH and os.path.exists(OAUTH_TOKEN_PATH):
        creds = UserCredentials.from_authorized_user_file(OAUTH_TOKEN_PATH, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(OAUTH_TOKEN_PATH, 'w', encoding='utf-8') as f:
                f.write(creds.to_json())
        return creds
    return Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)


def get_drive_service():
    return build('drive', 'v3', credentials=get_creds())


def ensure_folder(service, name, parent_id):
    q = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    res = service.files().list(q=q, fields='files(id,name)', pageSize=1).execute()
    files = res.get('files', [])
    if files:
        return files[0]['id']
    meta = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        meta['parents'] = [parent_id]
    created = service.files().create(body=meta, fields='id').execute()
    return created['id']


def upload_file(service, path, name, parent_id):
    media = MediaFileUpload(path, resumable=True)
    meta = {'name': name, 'parents': [parent_id]}
    file = service.files().create(body=meta, media_body=media, fields='id').execute()
    return file['id']


def main():
    service = get_drive_service()
    base_folder = ensure_folder(service, 'Контракты', TARGET_FOLDER_ID)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, reestr_number, file_name, local_path FROM attachments WHERE status LIKE 'pending_upload%'")
    rows = cur.fetchall()

    for row in rows:
        row_id, reestr_number, file_name, local_path = row
        if not local_path or not os.path.exists(local_path):
            continue
        contract_folder = ensure_folder(service, reestr_number, base_folder)
        files_folder = ensure_folder(service, 'files', contract_folder)
        source_folder = ensure_folder(service, 'source', contract_folder)
        parent = source_folder if file_name == 'document-info.html' else files_folder

        try:
            drive_id = upload_file(service, local_path, file_name, parent)
            cur.execute("UPDATE attachments SET drive_file_id = ?, status = 'uploaded' WHERE id = ?", (drive_id, row_id))
            conn.commit()
            print(f"Uploaded: {file_name}")
        except Exception as e:
            cur.execute("UPDATE attachments SET retry_count = retry_count + 1 WHERE id = ?", (row_id,))
            conn.commit()
            print(f"Failed: {file_name} -> {e}")

    conn.close()


if __name__ == '__main__':
    main()
