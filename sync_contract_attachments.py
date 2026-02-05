#!/usr/bin/env python3
import os
import re
import json
import sqlite3
import time
import tarfile
import tempfile
import subprocess
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

load_dotenv()

CREDS_PATH = os.getenv('GOOGLE_API_CREDENTIALS_PATH', 'credentials.json')
OAUTH_CREDS_PATH = os.getenv('GOOGLE_OAUTH_CREDENTIALS_PATH', 'oauth_credentials.json')
OAUTH_TOKEN_PATH = os.getenv('GOOGLE_OAUTH_TOKEN_PATH', 'token.json')
DB_PATH = os.getenv('FOOD_DB_PATH', 'food.db')
TARGET_FOLDER_ID = os.getenv('TARGET_FOLDER_ID', '1dt-L4A68Wu4KVuydb-zZi8b88sc1L5PH')
LOCAL_ATTACHMENTS_ROOT = os.getenv('LOCAL_ATTACHMENTS_ROOT', 'attachments')


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


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reestr_number TEXT,
            file_name TEXT,
            url TEXT,
            drive_file_id TEXT,
            size INTEGER,
            content_type TEXT,
            status TEXT,
            local_path TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TEXT
        )
    ''')
    # Backward compatibility for older schema
    for col, col_def in [
        ('local_path', 'TEXT'),
        ('retry_count', 'INTEGER DEFAULT 0')
    ]:
        try:
            cur.execute(f"ALTER TABLE attachments ADD COLUMN {col} {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def save_attachment_meta(meta):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO attachments (reestr_number, file_name, url, drive_file_id, size, content_type, status, local_path, retry_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ''', (
        meta.get('reestr_number'),
        meta.get('file_name'),
        meta.get('url'),
        meta.get('drive_file_id'),
        meta.get('size'),
        meta.get('content_type'),
        meta.get('status'),
        meta.get('local_path'),
        meta.get('retry_count', 0)
    ))
    conn.commit()
    conn.close()


def fetch_contract_info_id(reestr_number):
    url = f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber={reestr_number}"
    resp = requests.get(url, timeout=30)
    m = re.search(r"contractInfoId=(\d+)", resp.text)
    return m.group(1) if m else None


def save_html_copy(reestr_number, dest_path):
    cid = fetch_contract_info_id(reestr_number)
    if cid:
        url = f"https://zakupki.gov.ru/epz/contract/contractCard/document-info.html?reestrNumber={reestr_number}&contractInfoId={cid}"
    else:
        url = f"https://zakupki.gov.ru/epz/contract/contractCard/document-info.html?reestrNumber={reestr_number}"
    resp = requests.get(url, timeout=30)
    with open(dest_path, 'w', encoding='utf-8') as f:
        f.write(resp.text)
    return url


def fetch_attachments_via_ssh(reestr_number):
    cmd = (
        "ssh ussr "
        f"\"~/zakupki-parser/venv/bin/python ~/zakupki-parser/fetch_contract_attachments.py "
        f"{reestr_number} 'https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber={reestr_number}'\""
    )
    output = os.popen(cmd).read().strip()
    return json.loads(output)


def pull_attachments_archive(reestr_number, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tmp:
        archive_path = tmp.name
    cmd = [
        'ssh', 'ussr',
        f"tar -czf - ~/zakupki-parser/attachments/{reestr_number}"
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    with open(archive_path, 'wb') as f:
        for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b''):
            f.write(chunk)
    proc.wait(timeout=120)
    if proc.returncode != 0:
        err = proc.stderr.read().decode('utf-8')
        raise RuntimeError(f"SSH tar failed: {err}")

    with tarfile.open(archive_path, 'r:gz') as tar:
        tar.extractall(path=local_dir)
    os.remove(archive_path)


def download_file(url, path):
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        return None, resp.status_code, None
    with open(path, 'wb') as f:
        f.write(resp.content)
    return resp.headers, resp.status_code, len(resp.content)


def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: sync_contract_attachments.py <reestr_number>")
        return

    reestr_number = sys.argv[1]
    init_db()

    # Prepare local storage
    local_contract_dir = os.path.join(LOCAL_ATTACHMENTS_ROOT, reestr_number)
    os.makedirs(local_contract_dir, exist_ok=True)

    # Fetch attachments list from ussr (ensures server-side download)
    payload = fetch_attachments_via_ssh(reestr_number)

    # Pull archive from ussr to local
    try:
        pull_attachments_archive(reestr_number, LOCAL_ATTACHMENTS_ROOT)
    except Exception as e:
        print(f"WARN: failed to pull attachments archive: {e}")

    # Try Drive upload (if Google reachable)
    try:
        service = get_drive_service()
    except Exception as e:
        service = None
        print(f"WARN: Drive unavailable: {e}")

    if service:
        base_folder = ensure_folder(service, 'Контракты', TARGET_FOLDER_ID)
        contract_folder = ensure_folder(service, reestr_number, base_folder)
        files_folder = ensure_folder(service, 'files', contract_folder)
        source_folder = ensure_folder(service, 'source', contract_folder)
    else:
        base_folder = contract_folder = files_folder = source_folder = None

    # Save HTML copy locally
    html_path = os.path.join(local_contract_dir, 'document-info.html')
    html_url = save_html_copy(reestr_number, html_path)

    # Upload HTML copy if possible
    html_file_id = None
    if service and source_folder:
        try:
            html_file_id = upload_file(service, html_path, 'document-info.html', source_folder)
            status = 'uploaded'
        except Exception as e:
            status = f"pending_upload: {e}"
    else:
        status = 'pending_upload'

    save_attachment_meta({
        'reestr_number': reestr_number,
        'file_name': 'document-info.html',
        'url': html_url,
        'drive_file_id': html_file_id,
        'size': os.path.getsize(html_path),
        'content_type': 'text/html',
        'status': status,
        'local_path': html_path
    })

    # Process files from payload
    for item in payload.get('files', []):
        url = item.get('url')
        file_name = item.get('file_name') or 'file.bin'
        local_path = os.path.join(local_contract_dir, file_name)
        # If file already exists from archive, skip download
        if not os.path.exists(local_path):
            headers, status_code, size = download_file(url, local_path)
        else:
            headers, status_code, size = None, 200, os.path.getsize(local_path)

        if status_code != 200:
            save_attachment_meta({
                'reestr_number': reestr_number,
                'file_name': file_name,
                'url': url,
                'drive_file_id': None,
                'size': size,
                'content_type': None,
                'status': f"download_failed_{status_code}",
                'local_path': local_path
            })
            continue

        if service and files_folder:
            try:
                drive_id = upload_file(service, local_path, file_name, files_folder)
                status = 'uploaded'
            except Exception as e:
                drive_id = None
                status = f"pending_upload: {e}"
        else:
            drive_id = None
            status = 'pending_upload'

        save_attachment_meta({
            'reestr_number': reestr_number,
            'file_name': file_name,
            'url': url,
            'drive_file_id': drive_id,
            'size': size,
            'content_type': headers.get('Content-Type') if headers else None,
            'status': status,
            'local_path': local_path
        })
        time.sleep(0.2)

    # Auto run recognition on local files
    try:
        import subprocess
        subprocess.run([
            os.path.join(os.path.dirname(__file__), 'venv', 'bin', 'python'),
            os.path.join(os.path.dirname(__file__), 'recognize_attachments.py'),
            reestr_number
        ], check=False)
        subprocess.run([
            os.path.join(os.path.dirname(__file__), 'venv', 'bin', 'python'),
            os.path.join(os.path.dirname(__file__), 'compare_parsed.py'),
            reestr_number
        ], check=False)
    except Exception as e:
        print(f"WARN: recognition failed: {e}")

    print("Done")


if __name__ == '__main__':
    main()
