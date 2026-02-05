import os
import logging
import re
import traceback
import subprocess
import json
import sqlite3
import datetime
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import telebot
from telebot import types
import gspread
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
# from ai_service import ai_service  # Temporarily disabled

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Dedicated parsing logger
parse_logger = logging.getLogger("parser")
if not parse_logger.handlers:
    parse_handler = logging.FileHandler(os.getenv('PARSE_LOG_PATH', 'parse.log'))
    parse_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    parse_logger.addHandler(parse_handler)
    parse_logger.setLevel(logging.INFO)

token = os.getenv('TELEGRAM_TOKEN')
super_admin_id = os.getenv('SUPER_ADMIN_ID')
admin_ids_str = os.getenv('ADMIN_IDS')
creds_path = os.getenv('GOOGLE_API_CREDENTIALS_PATH', 'credentials.json')
oauth_creds_path = os.getenv('GOOGLE_OAUTH_CREDENTIALS_PATH', 'oauth_credentials.json')
oauth_token_path = os.getenv('GOOGLE_OAUTH_TOKEN_PATH', 'token.json')
DB_PATH = os.getenv('FOOD_DB_PATH', 'food.db')

# --- CONFIGURATION ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# --- SETUP ---
if not token:
    logging.error("TELEGRAM_TOKEN not found in .env")
    exit(1)

# Parse IDs
try:
    SUPER_ADMIN_ID = int(super_admin_id) if super_admin_id else None
    ADMIN_IDS = [int(id_str.strip()) for id_str in admin_ids_str.split(',')] if admin_ids_str else []
except ValueError:
    logging.error("Invalid ID format in .env")
    SUPER_ADMIN_ID = None
    ADMIN_IDS = []

bot = telebot.TeleBot(token)

# Global variables
current_sheet_id = None
# ID мастер-таблицы, в которой создаем листы
MASTER_SHEET_ID = "1GABj9RzjYIIXLnUTULQq9MnMsiwCldeyr-IVLdz_Kxc"
TARGET_FOLDER_ID = "1dt-L4A68Wu4KVuydb-zZi8b88sc1L5PH"

# Global state for batch processing
user_states = {}

# --- GOOGLE SERVICES HELPER ---
def get_creds():
    # Prefer OAuth user creds if token exists
    try:
        if oauth_token_path and os.path.exists(oauth_token_path):
            creds = UserCredentials.from_authorized_user_file(oauth_token_path, SCOPES)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(oauth_token_path, "w", encoding="utf-8") as f:
                    f.write(creds.to_json())
            return creds
    except Exception as e:
        logging.error(f"Failed to load OAuth token: {e}")

    # Fallback to service account
    try:
        return Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    except Exception as e:
        logging.error(f"Failed to load service account credentials: {e}")
        return None

def get_gc():
    creds = get_creds()
    if creds:
        try:
            return gspread.authorize(creds)
        except Exception as e:
             logging.error(f"Failed to authorize gspread: {e}")
    return None

def get_drive_service():
    creds = get_creds()
    if creds:
        try:
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"Failed to build drive service: {e}")
    return None

def check_drive_folder_access():
    if not TARGET_FOLDER_ID:
        return False, "TARGET_FOLDER_ID not set"
    service = get_drive_service()
    if not service:
        return False, "Drive service unavailable"
    try:
        meta = service.files().get(
            fileId=TARGET_FOLDER_ID,
            fields="id,name,mimeType"
        ).execute()
        if meta.get("mimeType") != "application/vnd.google-apps.folder":
            return False, "TARGET_FOLDER_ID is not a folder"
        # Try a lightweight list to confirm access
        service.files().list(
            q=f"'{TARGET_FOLDER_ID}' in parents and trashed=false",
            pageSize=1,
            fields="files(id)"
        ).execute()
        return True, meta.get("name", "")
    except Exception as e:
        logging.error(f"Drive folder access check failed: {e}")
        return False, str(e)

# --- SQLITE HELPERS ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            reestr_number TEXT PRIMARY KEY,
            customer TEXT,
            price_clean REAL,
            price_source TEXT,
            date_start TEXT,
            date_end TEXT,
            url TEXT,
            objects_hash TEXT,
            requisites_hash TEXT,
            last_checked TEXT,
            last_changed TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS requisites_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reestr_number TEXT,
            changed_at TEXT,
            requisites_json TEXT,
            requisites_hash TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS objects_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reestr_number TEXT,
            changed_at TEXT,
            objects_json TEXT,
            objects_hash TEXT,
            objects_total_clean REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reestr_number TEXT,
            checked_at TEXT,
            price_clean REAL,
            objects_hash TEXT,
            requisites_hash TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rss_state (
            reestr_number TEXT PRIMARY KEY,
            feed_url TEXT,
            last_guid TEXT,
            last_pubdate TEXT,
            last_checked TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_last_hashes(reestr_number):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT objects_hash, requisites_hash FROM contracts WHERE reestr_number = ?", (reestr_number,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0] or "", row[1] or ""
    return "", ""

def get_last_changed(reestr_number):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT last_changed FROM contracts WHERE reestr_number = ?", (reestr_number,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        return row[0]
    return None

def record_check(data):
    now = datetime.datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO checks (reestr_number, checked_at, price_clean, objects_hash, requisites_hash) VALUES (?, ?, ?, ?, ?)",
        (
            data.get("reestr_number", ""),
            now,
            float(data.get("price_clean", 0) or 0),
            data.get("objects_hash", ""),
            data.get("requisites_hash", "")
        )
    )
    conn.commit()
    conn.close()

def upsert_contract(data, objects_changed, requisites_changed):
    now = datetime.datetime.utcnow().isoformat()
    last_changed = now if (objects_changed or requisites_changed) else get_last_changed(data.get("reestr_number", ""))
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO contracts (
            reestr_number, customer, price_clean, price_source, date_start, date_end, url,
            objects_hash, requisites_hash, last_checked, last_changed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(reestr_number) DO UPDATE SET
            customer=excluded.customer,
            price_clean=excluded.price_clean,
            price_source=excluded.price_source,
            date_start=excluded.date_start,
            date_end=excluded.date_end,
            url=excluded.url,
            objects_hash=excluded.objects_hash,
            requisites_hash=excluded.requisites_hash,
            last_checked=excluded.last_checked,
            last_changed=excluded.last_changed
    """, (
        data.get("reestr_number", ""),
        data.get("customer", ""),
        float(data.get("price_clean", 0) or 0),
        data.get("price_source", ""),
        data.get("date_start", ""),
        data.get("date_end", ""),
        data.get("url", ""),
        data.get("objects_hash", ""),
        data.get("requisites_hash", ""),
        now,
        last_changed
    ))
    conn.commit()
    conn.close()

def record_history(data, objects_changed, requisites_changed):
    now = datetime.datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if objects_changed:
        cur.execute(
            "INSERT INTO objects_history (reestr_number, changed_at, objects_json, objects_hash, objects_total_clean) VALUES (?, ?, ?, ?, ?)",
            (
                data.get("reestr_number", ""),
                now,
                json.dumps(data.get("objects", []), ensure_ascii=False),
                data.get("objects_hash", ""),
                float(data.get("objects_total_clean", 0) or 0),
            )
        )
    if requisites_changed:
        cur.execute(
            "INSERT INTO requisites_history (reestr_number, changed_at, requisites_json, requisites_hash) VALUES (?, ?, ?, ?)",
            (
                data.get("reestr_number", ""),
                now,
                json.dumps(data.get("requisites", {}), ensure_ascii=False),
                data.get("requisites_hash", ""),
            )
        )
    conn.commit()
    conn.close()

def determine_changes(data):
    last_objects_hash, last_requisites_hash = get_last_hashes(data.get("reestr_number", ""))
    objects_changed = (data.get("objects_hash", "") != last_objects_hash)
    requisites_changed = (data.get("requisites_hash", "") != last_requisites_hash)
    return objects_changed, requisites_changed

def get_contract_numbers_from_db():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT reestr_number FROM contracts")
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows if row and row[0]]

def get_contract_numbers_from_registry():
    gc = get_gc()
    if not gc:
        return []
    try:
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = get_registry_worksheet(sh)
        values = ws.col_values(1)
        if not values:
            return []
        numbers = []
        for val in values[1:]:
            val = (val or "").strip()
            if is_valid_contract_number(val):
                numbers.append(val)
        return list(dict.fromkeys(numbers))
    except Exception as e:
        logging.warning(f"Failed to read registry sheet: {e}")
        return []

def ensure_contract_stub(reestr_number):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO contracts (reestr_number) VALUES (?)",
        (reestr_number,)
    )
    cur.execute(
        "INSERT OR IGNORE INTO rss_state (reestr_number, feed_url) VALUES (?, ?)",
        (reestr_number, f"https://zakupki.gov.ru/epz/contract/contractCard/rss?reestrNumber={reestr_number}")
    )
    conn.commit()
    conn.close()

def add_contracts_to_registry(contract_numbers):
    for number in contract_numbers:
        ensure_contract_stub(number)
        upsert_registry_row({"reestr_number": number}, False, False)

def remove_contract_from_registry(reestr_number):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM contracts WHERE reestr_number = ?", (reestr_number,))
    cur.execute("DELETE FROM objects_history WHERE reestr_number = ?", (reestr_number,))
    cur.execute("DELETE FROM requisites_history WHERE reestr_number = ?", (reestr_number,))
    cur.execute("DELETE FROM checks WHERE reestr_number = ?", (reestr_number,))
    conn.commit()
    conn.close()

    gc = get_gc()
    if not gc:
        return
    try:
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = get_registry_worksheet(sh)
        cell = ws.find(reestr_number)
        if cell:
            ws.delete_rows(cell.row)
    except gspread.exceptions.CellNotFound:
        return
    except Exception as e:
        logging.warning(f"Failed to remove from registry sheet: {e}")

def clear_registry():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM contracts")
    cur.execute("DELETE FROM objects_history")
    cur.execute("DELETE FROM requisites_history")
    cur.execute("DELETE FROM checks")
    conn.commit()
    conn.close()

    gc = get_gc()
    if not gc:
        return
    try:
        sh = gc.open_by_key(MASTER_SHEET_ID)
        ws = get_registry_worksheet(sh)
        ws.resize(rows=1)
    except Exception as e:
        logging.warning(f"Failed to clear registry sheet: {e}")
def safe_send(chat_id, text, **kwargs):
    if not chat_id:
        return
    bot.send_message(chat_id, text, **kwargs)

# --- GOOGLE SHEETS HELPERS ---
def get_registry_worksheet(sh):
    try:
        return sh.worksheet("Registry")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Registry", rows=1000, cols=20)
        header = [
            "Reestr Number", "Customer", "Price", "Price Source",
            "Date Start", "Date End", "URL",
            "Objects Hash", "Requisites Hash",
            "Last Checked (UTC)", "Last Changed (UTC)"
        ]
        ws.append_row(header)
        return ws

def upsert_registry_row(data, objects_changed, requisites_changed):
    gc = get_gc()
    if not gc:
        return
    sh = gc.open_by_key(MASTER_SHEET_ID)
    ws = get_registry_worksheet(sh)
    reestr_number = data.get("reestr_number", "")
    now = datetime.datetime.utcnow().isoformat()
    row = [
        reestr_number,
        data.get("customer", ""),
        data.get("price_clean", 0),
        data.get("price_source", ""),
        data.get("date_start", ""),
        data.get("date_end", ""),
        data.get("url", ""),
        data.get("objects_hash", ""),
        data.get("requisites_hash", ""),
        now,
        now if (objects_changed or requisites_changed) else ""
    ]
    try:
        cell = ws.find(reestr_number)
        if cell:
            ws.update(f"A{cell.row}:K{cell.row}", [row])
        else:
            ws.append_row(row)
    except gspread.exceptions.CellNotFound:
        ws.append_row(row)

def create_contract_spreadsheet(gc, contract_number):
    sh = gc.create(contract_number)
    # Remove default sheet to keep only our sheets
    try:
        for ws in sh.worksheets():
            if ws.title in ("Sheet1", "Лист1", "Лист 1"):
                sh.del_worksheet(ws)
                break
    except Exception as e:
        logging.warning(f"Failed to remove default sheet: {e}")
    # Move to target folder if configured
    try:
        service = get_drive_service()
        if service and TARGET_FOLDER_ID:
            file_id = sh.id
            # Add folder and remove root
            service.files().update(
                fileId=file_id,
                addParents=TARGET_FOLDER_ID,
                removeParents="root",
                fields="id, parents"
            ).execute()
    except Exception as e:
        logging.warning(f"Failed to move sheet to target folder: {e}")
    return sh

def find_existing_contract_sheet_id(contract_number):
    service = get_drive_service()
    if not service:
        return None
    try:
        query = (
            f"name = '{contract_number}' and "
            f"mimeType = 'application/vnd.google-apps.spreadsheet' and "
            f"trashed = false"
        )
        result = service.files().list(
            q=query,
            orderBy="modifiedTime desc",
            fields="files(id, name, modifiedTime)",
            pageSize=5
        ).execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
    except Exception as e:
        logging.warning(f"Failed to find existing sheet by name: {e}")
    return None

# --- REMOTE PARSER ---
def fetch_contract_data_via_ssh(url, max_retries=3, timeout=60):
    """
    Executes a remote script on 'ussr' to fetch FULL contract data as JSON.
    """
    for attempt in range(max_retries):
        try:
            start_ts = time.time()
            ssh_command = [
                "ssh",
                "-o", "ConnectTimeout=30",
                "-o", "ServerAliveInterval=30",
                "-o", "ServerAliveCountMax=2",
                "ussr",
                f"~/zakupki-parser/venv/bin/python ~/zakupki-parser/fetch_contract_data.py '{url}'"
            ]
            parse_logger.info(f"START fetch_contract_data url={url} attempt={attempt+1}")
            result = subprocess.run(ssh_command, capture_output=True, text=False, timeout=timeout)
            
            if result.returncode != 0:
                error_msg = result.stderr.decode('utf-8')
                parse_logger.error(f"FAIL fetch_contract_data url={url} attempt={attempt+1} err={error_msg}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
                continue
                
            json_output = result.stdout.decode('utf-8')
            duration = time.time() - start_ts
            parse_logger.info(f"OK fetch_contract_data url={url} seconds={duration:.2f} attempt={attempt+1}")
            return json.loads(json_output)
            
        except subprocess.TimeoutExpired:
            parse_logger.error(f"TIMEOUT fetch_contract_data url={url} attempt={attempt+1} timeout={timeout}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
        except Exception as e:
            parse_logger.error(f"EXCEPTION fetch_contract_data url={url} attempt={attempt+1} err={e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
    
    return None

def fetch_contract_preview_via_ssh(contract_numbers, max_retries=3, timeout=60):
    """
    Fetches preview information for multiple contracts via SSH.
    Returns list of contracts with basic info.
    """
    for attempt in range(max_retries):
        try:
            start_ts = time.time()
            numbers_str = ','.join(contract_numbers)
            ssh_command = [
                "ssh",
                "-o", "ConnectTimeout=30",
                "-o", "ServerAliveInterval=30",
                "-o", "ServerAliveCountMax=2",
                "ussr",
                f"~/zakupki-parser/venv/bin/python ~/zakupki-parser/fetch_contracts_preview.py '{numbers_str}'"
            ]
            parse_logger.info(f"START fetch_contract_preview count={len(contract_numbers)} attempt={attempt+1}")
            result = subprocess.run(ssh_command, capture_output=True, text=False, timeout=timeout)
            
            if result.returncode != 0:
                error_msg = result.stderr.decode('utf-8')
                parse_logger.error(f"FAIL fetch_contract_preview attempt={attempt+1} err={error_msg}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
                continue
                
            json_output = result.stdout.decode('utf-8')
            duration = time.time() - start_ts
            parse_logger.info(f"OK fetch_contract_preview count={len(contract_numbers)} seconds={duration:.2f} attempt={attempt+1}")
            return json.loads(json_output)
            
        except subprocess.TimeoutExpired:
            parse_logger.error(f"TIMEOUT fetch_contract_preview attempt={attempt+1} timeout={timeout}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
        except Exception as e:
            parse_logger.error(f"EXCEPTION fetch_contract_preview attempt={attempt+1} err={e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(2 ** attempt)
    
    return None

# --- INPUT ANALYSIS ---
def analyze_user_input(text):
    """
    Analyzes user input and determines the type of request.
    Returns dict with type and data.
    """
    text = text.strip()
    
    # 1. Check for URL (existing functionality)
    if 'zakupki.gov.ru' in text:
        url_match = re.search(r'https?://[^\\s]+zakupki\.gov\.ru[^\\s]*', text)
        if url_match:
            return {'type': 'url', 'data': url_match.group()}
    
    # 2. Extract contract numbers
    numbers = extract_contract_numbers(text)
    if not numbers:
        return {'type': 'unknown', 'data': None}
    
    if len(numbers) == 1:
        return {'type': 'single_number', 'data': numbers[0]}
    else:
        return {'type': 'multiple_numbers', 'data': numbers}

def extract_contract_numbers(text):
    """
    Extracts contract numbers from text.
    Contract numbers are typically 19+ digits.
    """
    # Pattern for contract numbers (19+ digits)
    pattern = r'\b(\d{19,})\b'
    numbers = re.findall(pattern, text)
    
    # Validate and deduplicate
    valid_numbers = []
    for num in set(numbers):
        if is_valid_contract_number(num):
            valid_numbers.append(num)
    
    return valid_numbers

def is_valid_contract_number(number):
    """
    Validates if a number looks like a contract registry number.
    """
    if len(number) < 19:
        return False
    
    # Additional validation rules can be added here
    # For now, just check length and format
    return number.isdigit()

def get_contract_url_from_number(number):
    """
    Converts contract number to full URL.
    """
    return f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber={number}"

def format_contract_list_preview(contracts):
    """
    Formats contract list preview for Telegram.
    """
    if not contracts:
        return "❌ Контракты не найдены"
    
    # Group by year
    years = {}
    for contract in contracts:
        year = contract.get('year', 'Unknown')
        if year not in years:
            years[year] = []
        years[year].append(contract)
    
    msg = f"📋 **Найдено {len(contracts)} контрактов:**\\n\\n"
    
    for year, year_contracts in sorted(years.items(), reverse=True):
        msg += f"🗓 **{year} год:** {len(year_contracts)} шт.\\n"
        # Show first 2 contracts as examples
        for i, contract in enumerate(year_contracts[:2]):
            short_num = contract['number'][-6:]  # Last 6 digits
            customer = contract.get('customer', 'N/A')[:30] + ('...' if len(contract.get('customer', '')) > 30 else '')
            msg += f"  • К-{short_num}: {customer}\\n"
        
        if len(year_contracts) > 2:
            msg += f"  • ...и еще {len(year_contracts) - 2}\\n"
        msg += "\\n"
    
    return msg

# --- UTILS ---
def clean_number(value_str):
    """
    Cleans price/quantity strings to pure numbers.
    Example: "1 200,00 ₽" -> 1200.00
    Handles multiline data correctly.
    """
    if not value_str:
        return 0.0
    
    # Split into lines for multiline data processing
    lines = value_str.split('\n')
    
    # Search for numbers in each line
    for line in lines:
        # Clean the line
        clean_line = line.replace('₽', '').replace('RUB', '').replace('ДЕТ ДН', '').replace('УСЛ ЕД', '')
        clean_line = clean_line.replace(' ', '').replace('\xa0', '').replace(',', '.')
        
        # Look for number in this line
        match = re.search(r'(\d+\.?\d*)', clean_line)
        if match:
            try:
                number = float(match.group(1))
                if number > 0:  # Return first positive number found
                    logging.info(f"clean_number: Found {number} in line: {line[:50]}...")
                    return number
            except ValueError:
                continue
    
    # Fallback: try original logic on first line
    try:
        first_line_clean = lines[0].replace('₽', '').replace('RUB', '').replace('ДЕТ ДН', '').replace('УСЛ ЕД', '')
        first_line_clean = first_line_clean.replace(' ', '').replace('\xa0', '').replace(',', '.')
        match = re.search(r'(\d+\.?\d*)', first_line_clean)
        if match:
            result = float(match.group(1))
            logging.info(f"clean_number: Fallback to first line, got {result}")
            return result
    except:
        pass
    
    logging.warning(f"clean_number: No valid number found in: {value_str[:100]}...")
    return 0.0

def extract_number_and_unit(value_str):
    """
    Extracts number and unit of measurement from string.
    Examples:
        "3 000 ДЕТ ДН" -> (3000.0, "ДЕТ ДН")
        "2 233 843,92 Ставка НДС: Без НДС" -> (2233843.92, "Ставка НДС: Без НДС")
        "1 200,00 ₽" -> (1200.0, "₽")
        "5" -> (5.0, "")
    """
    if not value_str:
        return 0.0, ""
    
    # First line usually contains the main value
    main_line = value_str.split('\n')[0].strip()
    
    # Find the number
    number_match = re.search(r'(\d+[.,\s\d]*\d*)', main_line)
    if not number_match:
        return 0.0, ""
    
    number_str = number_match.group(1)
    
    # Clean and convert the number
    try:
        # Remove spaces (thousands separator)
        number_str = number_str.replace(' ', '').replace('\xa0', '')
        # Replace comma with dot for decimal
        number_str = number_str.replace(',', '.')
        number = float(number_str)
    except ValueError:
        try:
            match = re.search(r'\d+\.?\d*', number_str)
            if match:
                number = float(match.group())
            else:
                return 0.0, ""
        except:
            return 0.0, ""
    
    # Extract unit (everything after the number)
    unit_part = main_line[number_match.end():].strip()
    
    # Keep currency symbols if they are the only unit
    if unit_part and not unit_part.replace('₽', '').replace('RUB', '').strip():
        # Only currency symbols - keep one
        if '₽' in unit_part:
            unit_part = '₽'
        elif 'RUB' in unit_part:
            unit_part = 'RUB'
        else:
            unit_part = ''
    else:
        # Remove common currency symbols to extract real unit
        unit_part = unit_part.replace('₽', '').replace('RUB', '').strip()
    
    return number, unit_part

def is_total_row(name):
    """
    Determines if a row is a total/summary row.
    """
    if not name:
        return False
    
    total_keywords = ["итого", "всего", "total", "сумма", "合计", "정리", "подитог", "общий"]
    name_lower = name.lower().strip()
    
    return any(keyword in name_lower for keyword in total_keywords)

def parse_price_info(obj):
    """
    Parses price information from object and returns clean data.
    Returns dict with: name, category, price, price_unit, total_sum, total_unit, qty
    """
    name = obj.get('name', '')
    category = obj.get('category', 'Прочее')
    price_raw = obj.get('price', '0')
    total_raw = obj.get('total', '0')
    
    # Extract price and unit
    price, price_unit = extract_number_and_unit(price_raw)
    
    # Extract total sum and unit
    total_sum, total_unit = extract_number_and_unit(total_raw)
    
    # Calculate quantity if possible
    qty = 0
    if price > 0 and total_sum > 0:
        qty = round(total_sum / price, 2)
    
    return {
        'name': name,
        'category': category,
        'price': price,
        'price_unit': price_unit,
        'total_sum': total_sum,
        'total_unit': total_unit,
        'qty': qty
    }

def validate_totals(objects_data, calculated_total):
    """
    Validates parsed totals vs calculated totals.
    Returns validation result dict.
    """
    # Extract parsed total from objects (excluding total rows)
    parsed_total = 0.0
    has_parsed_total = False
    
    for obj in objects_data:
        if is_total_row(obj.get('name', '')):
            # This is a total row from parser
            total_raw = obj.get('total', '0')
            total_value, _ = extract_number_and_unit(total_raw)
            if total_value > 0:
                parsed_total = total_value
                has_parsed_total = True
                break
    
    # If no explicit total row, calculate sum of all non-total items
    if not has_parsed_total:
        for obj in objects_data:
            if not is_total_row(obj.get('name', '')):
                total_raw = obj.get('total', '0')
                total_value, _ = extract_number_and_unit(total_raw)
                parsed_total += total_value
    
    # Check for discrepancy
    difference = abs(parsed_total - calculated_total)
    is_valid = difference <= 0.01  # Any difference over 0.01 is invalid
    
    return {
        'parsed_total': parsed_total,
        'calculated_total': calculated_total,
        'difference': difference,
        'is_valid': is_valid,
        'has_parsed_total': has_parsed_total
    }

def format_validation_message(validation_result):
    """
    Formats validation message for Telegram.
    Returns message string or None if valid.
    """
    if validation_result['is_valid']:
        return "✅ **Данные корректны**\nСуммы совпадают, ошибок парсинга не обнаружено."
    
    msg = "⚠️ **Обнаружено расхождение сумм!**\n\n"
    
    if validation_result['has_parsed_total']:
        msg += f"Сумма по парсеру: {validation_result['parsed_total']:,.2f}\n"
    else:
        msg += f"Сумма по данным: {validation_result['parsed_total']:,.2f}\n"
    
    msg += f"Расчетная сумма: {validation_result['calculated_total']:,.2f}\n"
    msg += f"Разница: {validation_result['difference']:,.2f}\n\n"
    msg += "🔍 **Рекомендуется проверить данные вручную**\n"
    msg += "Возможные причины:\n"
    msg += "• Некорректное определение единиц измерения\n"
    msg += "• Ошибка в исходных данных\n"
    msg += "• Пропущены позиции при парсинге"
    
    return msg

# --- SHEET CREATION ---
def add_contract_to_master(data, objects_changed=None, requisites_changed=None):
    """
    Creates or updates a spreadsheet per contract.
    Summary sheet + dated sheet for each change.
    """
    init_db()
    gc = get_gc()
    if not gc:
        return None, "Ошибка подключения к Google Sheets"

    try:
        reestr_number = data.get('reestr_number', 'Unknown')
        contract_title = reestr_number

        ok, info = check_drive_folder_access()
        if not ok:
            return None, f"Нет доступа к папке Drive: {info}"

        # Determine changes
        if objects_changed is None or requisites_changed is None:
            objects_changed, requisites_changed = determine_changes(data)

        # Record check and history in DB
        record_check(data)
        record_history(data, objects_changed, requisites_changed)
        upsert_contract(data, objects_changed, requisites_changed)

        # Update registry sheet
        upsert_registry_row(data, objects_changed, requisites_changed)

        # Create or open contract spreadsheet (ignore trashed files)
        existing_id = find_existing_contract_sheet_id(contract_title)
        if existing_id:
            sh = gc.open_by_key(existing_id)
        else:
            sh = create_contract_spreadsheet(gc, contract_title)

        # Summary sheet
        try:
            summary_ws = sh.worksheet("Summary")
        except gspread.WorksheetNotFound:
            summary_ws = sh.add_worksheet(title="Summary", rows=100, cols=20)
        summary_ws.clear()

        # --- FILL DATA ---
        
        # Clean execution numbers with logging
        contract_price_raw = data.get('price', '0')
        paid_raw = data.get('execution', {}).get('paid', '0')
        accepted_raw = data.get('execution', {}).get('accepted', '0')
        
        contract_price_clean = clean_number(contract_price_raw)
        paid_clean = clean_number(paid_raw)
        accepted_clean = clean_number(accepted_raw)
        
        # Log for debugging
        logging.info(f"Contract {data.get('reestr_number', 'Unknown')} prices - "
                    f"Contract raw: {contract_price_raw} -> clean: {contract_price_clean}, "
                    f"Paid raw: {paid_raw} -> clean: {paid_clean}, "
                    f"Accepted raw: {accepted_raw} -> clean: {accepted_clean}")

        # 1. Header Info
        # Determine remainder formula based on actual cell positions
        if contract_price_clean > 0:
            # Use cell references: Price is in C3, Accepted is in C10
            remainder_formula = "=C3-C10"
        else:
            remainder_formula = "0.0"
        
        requisites = data.get("requisites", {})

        info_data = [
            ["КОНТРАКТ", data.get('reestr_number')],
            ["Заказчик", data.get('customer')],
            ["Цена контракта", contract_price_clean],
            ["Источник цены", data.get("price_source", "")],
            ["Дата начала", data.get('date_start', '-')],
            ["Дата окончания", data.get('date_end', '-')],
            ["Ссылка", data.get('url')],
            ["Объекты HASH", data.get("objects_hash", "")],
            ["Реквизиты HASH", data.get("requisites_hash", "")],
            [], 
            ["ИСПОЛНЕНИЕ", ""],
            ["Оплачено", paid_clean],
            ["Принято (Акты)", accepted_clean],
            ["Остаток лимита", remainder_formula],
            [],
            ["РЕКВИЗИТЫ", ""],
            ["Банк", requisites.get("bank_name", "")],
            ["БИК", requisites.get("bik", "")],
            ["Р/С", requisites.get("account", "")],
            ["К/С", requisites.get("corr_account", "")],
            ["Лицевой счет", requisites.get("treasury_account", "")],
            ["ИНН", requisites.get("inn", "")],
            ["КПП", requisites.get("kpp", "")],
        ]
        for row in info_data:
            summary_ws.append_row(row, value_input_option="USER_ENTERED")

        # Skip detailed sheet if no changes and already exists
        today = datetime.date.today().isoformat()
        detail_title = today
        if not objects_changed and not requisites_changed:
            try:
                sh.worksheet(detail_title)
                return summary_ws.url, None
            except gspread.WorksheetNotFound:
                pass

        ws = sh.add_worksheet(title=detail_title, rows=100, cols=20)
        ws.append_row(
            ["№", "Цена за единицу", "Ед.изм.", "Кол-во", "Сумма (Источник)", "Сумма (Расчет)", "Название"],
            value_input_option="USER_ENTERED"
        )
            
        # 2. Items Table
        objects = data.get('objects', [])
        start_row = 1
        
        # Filter out total rows and parse all objects
        parsed_objects = []
        for obj in objects:
            if not is_total_row(obj.get('name', '')):
                parsed_obj = parse_price_info(obj)
                parsed_objects.append(parsed_obj)
        
        if parsed_objects:
            # Calculate totals for validation
            calculated_total = sum(obj['total_sum'] for obj in parsed_objects)
            
            # Add item rows with proper formulas
            for i, obj in enumerate(parsed_objects, start=1):
                # Row index for formula (1-based)
                current_row = start_row + i + 1
                
                # Validate row number
                if current_row < 1:
                    logging.warning(f"Invalid row calculation: {current_row}")
                    continue
                
                # Formula for calculated sum (Quantity * Price)
                calculated_sum_formula = f"=B{current_row}*D{current_row}"
                
                ws.append_row([
                    i, # Position number
                    obj['price'], # Price per unit
                    obj['price_unit'] if obj['price_unit'] else obj['total_unit'], # Unit of measurement
                    obj['qty'], # Calculated Quantity
                    obj['total_sum'], # Source Sum
                    calculated_sum_formula, # Formula: Qty * Price
                    obj['name'] # Item name
                ], value_input_option="USER_ENTERED")
                
            # Add Total Check Formula with proper range validation
            last_row = start_row + len(parsed_objects)
            start_data_row = start_row + 1  # First row with actual item data
            
            # Validate range
            if last_row > start_data_row:
                source_total_formula = f"=SUM(E{start_data_row}:E{last_row})"
                calc_total_formula = f"=SUM(F{start_data_row}:F{last_row})"
            else:
                source_total_formula = "0.0"
                calc_total_formula = "0.0"
                
            ws.append_row([
                "ИТОГО", 
                "", 
                "", 
                "", 
                source_total_formula, # Sum of source totals
                calc_total_formula, # Sum of calculated totals
                ""
            ], value_input_option="USER_ENTERED")
            

            
            # Validate totals and return validation result
            validation_result = validate_totals(objects, calculated_total)
            
            # TODO: AI валидация данных контракта (временно отключена)
            # ai_validation = ai_service.validate_data(data)
            # if ai_validation and not ai_validation.get('valid', True):
            #     logging.info(f"AI validation found issues: {ai_validation.get('issues', [])}")
            #     # Добавляем AI валидацию к результату
            #     if not validation_result:
            #         validation_result = {'ai_issues': ai_validation.get('issues', [])}
            #     else:
            #         validation_result['ai_issues'] = ai_validation.get('issues', [])
            
        else:
            ws.append_row(["(Детализация товаров не найдена или не спарсилась)"], value_input_option="USER_ENTERED")
            validation_result = None

        return summary_ws.url, validation_result
        
    except Exception as e:
        error_details = traceback.format_exc()
        logging.error(f"Error updating sheet: {error_details}")
        return None, str(e)

def add_contracts_by_year(contracts_data):
    """
    Adds contracts to separate sheets grouped by year.
    Returns dict of year -> sheet_url
    """
    gc = get_gc()
    if not gc:
        return {}
    
    try:
        sh = gc.open_by_key(MASTER_SHEET_ID)
        sheet_urls = {}
        
        # Group contracts by year
        contracts_by_year = {}
        for contract in contracts_data:
            # Extract year from contract number or date
            year = extract_contract_year(contract)
            if year not in contracts_by_year:
                contracts_by_year[year] = []
            contracts_by_year[year].append(contract)
        
        for year, contracts in contracts_by_year.items():
            # Create or get sheet for the year
            sheet_title = f"Контракты_{year}"
            
            try:
                # Check if sheet exists
                ws = sh.worksheet(sheet_title)
            except gspread.WorksheetNotFound:
                # Create new sheet
                ws = sh.add_worksheet(title=sheet_title, rows=1000, cols=20)
            
            # Clear existing content
            ws.clear()
            
            # Add header
            header = [
                "КОНТРАКТ", "Заказчик", "Цена", "Дата начала", 
                "Дата окончания", "Ссылка", "Оплачено", "Принято", "Остаток лимита"
            ]
            ws.append_row(header)
            
            # Add contracts with improved formula handling
            row_num = 2  # Start after header (row 2)
            for contract in contracts:
                contract_price_raw = contract.get('price', '0')
                contract_price_clean = clean_number(contract_price_raw)
                paid_clean = clean_number(contract.get('execution', {}).get('paid', '0'))
                accepted_clean = clean_number(contract.get('execution', {}).get('accepted', '0'))
                
                # Log for debugging
                logging.info(f"Batch contract {contract.get('reestr_number', 'Unknown')} - "
                            f"Raw price: {contract_price_raw} -> clean: {contract_price_clean}")
                
                # Create proper formula for remainder
                if contract_price_clean > 0:
                    remainder_formula = f"=C{row_num}-I{row_num}"  # Price - Accepted
                else:
                    remainder_formula = "0.0"
                
                row = [
                    contract.get('reestr_number', ''),
                    contract.get('customer', ''),
                    contract_price_clean,
                    contract.get('date_start', ''),
                    contract.get('date_end', ''),
                    contract.get('url', ''),
                    paid_clean,
                    accepted_clean,
                    remainder_formula
                ]
                ws.append_row(row)
                row_num += 1
            
            sheet_urls[year] = ws.url
        
        return sheet_urls
        
    except Exception as e:
        logging.error(f"Error adding contracts by year: {e}")
        return {}

def add_contracts_to_single_sheet(contracts_data):
    """
    Adds all contracts to a single sheet.
    Returns list of sheet_urls (should be one)
    """
    gc = get_gc()
    if not gc:
        return []
    
    try:
        sh = gc.open_by_key(MASTER_SHEET_ID)
        
        # Create sheet with timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        sheet_title = f"Партия_{timestamp}"
        
        ws = sh.add_worksheet(title=sheet_title, rows=1000, cols=20)
        
        # Add header
        header = [
            "КОНТРАКТ", "Заказчик", "Цена", "Дата начала", 
            "Дата окончания", "Ссылка", "Оплачено", "Принято", "Остаток лимита"
        ]
        ws.append_row(header)
        
        # Add contracts with improved formula handling
        row_num = 2  # Start after header (row 2)
        for contract in contracts_data:
            contract_price_raw = contract.get('price', '0')
            contract_price_clean = clean_number(contract_price_raw)
            paid_clean = clean_number(contract.get('execution', {}).get('paid', '0'))
            accepted_clean = clean_number(contract.get('execution', {}).get('accepted', '0'))
            
            # Log for debugging
            logging.info(f"Single sheet contract {contract.get('reestr_number', 'Unknown')} - "
                        f"Raw price: {contract_price_raw} -> clean: {contract_price_clean}")
            
            # Create proper formula for remainder
            if contract_price_clean > 0:
                remainder_formula = f"=C{row_num}-I{row_num}"  # Price - Accepted
            else:
                remainder_formula = "0.0"
            
            row = [
                contract.get('reestr_number', ''),
                contract.get('customer', ''),
                contract_price_clean,
                contract.get('date_start', ''),
                contract.get('date_end', ''),
                contract.get('url', ''),
                paid_clean,
                accepted_clean,
                remainder_formula
            ]
            ws.append_row(row)
            row_num += 1
        
        return [ws.url]
        
    except Exception as e:
        logging.error(f"Error adding contracts to single sheet: {e}")
        return []

def extract_contract_year(contract):
    """
    Extracts year from contract data.
    """
    # Try to get year from date_end or date_start
    for date_field in ['date_end', 'date_start']:
        date_str = contract.get(date_field, '')
        if date_str:
            # Extract 4-digit year from date string
            year_match = re.search(r'(\d{4})', date_str)
            if year_match:
                return year_match.group(1)
    
    # Fallback: try to extract from contract number (some numbers contain year info)
    contract_number = contract.get('reestr_number', '')
    # Check if contract number contains year in last 2 digits + some pattern
    if len(contract_number) >= 19:
        # Try to extract year from position 15-16 (common in Russian procurement numbers)
        potential_year = "20" + contract_number[14:16]
        if 2020 <= int(potential_year) <= 2030:
            return potential_year
    
    # Default to current year
    return "2025"

def send_batch_report(chat_id, processed, errors, sheet_urls, group_by_year):
    """
    Sends final report for batch processing.
    """
    msg = f"📊 **Обработка завершена!**\\n\\n"
    msg += f"✅ Успешно: {processed}\\n"
    
    if errors:
        msg += f"❌ Ошибок: {len(errors)}\\n\\n"
        msg += "Ошибки:\\n"
        for error in errors[:5]:  # Show first 5 errors
            msg += f"• {error}\\n"
        if len(errors) > 5:
            msg += f"...и еще {len(errors) - 5}\\n"
    else:
        msg += "🎉 Без ошибок!\\n\\n"
    
    msg += "📋 **Созданы листы:**\\n"
    if group_by_year:
        for year, url in sheet_urls.items():
            msg += f"🗓 {year} год: [Ссылка]({url})\\n"
    else:
        for url in sheet_urls:
            msg += f"📄 [Партия]({url})\\n"
    
    bot.send_message(chat_id, msg, parse_mode='Markdown')



# --- ROLES ---
def get_user_role(user_id):
    if user_id == SUPER_ADMIN_ID:
        return "Супер-админ"
    elif user_id in ADMIN_IDS:
        return "Админ"
    else:
        return "Пользователь"

# --- HANDLERS ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_id = message.from_user.id
    role = get_user_role(user_id)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Проверить лимиты"), types.KeyboardButton("Добавить данные"))
    bot.reply_to(message, f"Привет, {role}! Я готов. Отправь ссылку на закупку.", reply_markup=markup)

@bot.message_handler(commands=['clear_trash'])
def clear_drive_trash(message):
    service = get_drive_service()
    if not service:
        bot.reply_to(message, "❌ Нет доступа к Drive API")
        return
        
    try:
        service.files().emptyTrash().execute()
        bot.reply_to(message, "🗑 Корзина бота очищена. Попробуйте создать файл снова.")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка очистки корзины: {e}")
def check_drive_access(message):
    global TARGET_FOLDER_ID
    service = get_drive_service()
    if not service:
        bot.reply_to(message, "❌ Не удалось инициализировать Google Drive API.")
        return

    try:
        results = service.files().list(
            pageSize=20, 
            fields="nextPageToken, files(id, name, mimeType)",
            q="trashed=false"
        ).execute()
        items = results.get('files', [])

        msg = "📂 **Доступные файлы:**\n"
        found_target = False
        for item in items:
            icon = "📄"
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                icon = "📁"
            if "еда" in item['name'].lower() and item['mimeType'] == 'application/vnd.google-apps.folder':
                TARGET_FOLDER_ID = item['id']
                found_target = True
                msg += f"{icon} **{item['name']}** (ID сохранен!)\n"
            else:
                # Escape special characters for Markdown to avoid 400 Bad Request
                safe_name = item['name'].replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                msg += f"{icon} {safe_name}\n"
        
        bot.reply_to(message, msg, parse_mode='Markdown')
            
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка:\n{e}")

# --- DIALOG HANDLERS FOR BATCH PROCESSING ---
@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_single_'))
def confirm_single_contract(call):
    """
    Handles confirmation for single contract parsing.
    """
    contract_number = call.data.replace('confirm_single_', '')
    
    bot.answer_callback_query(call.id, "Начинаю парсинг...")
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=f"🚀 Парсинг контракта {contract_number[-6:]}..."
    )
    
    url = get_contract_url_from_number(contract_number)
    process_contract_parsing(call.message.chat.id, url)

@bot.callback_query_handler(func=lambda call: call.data.startswith('batch_by_year_'))
def batch_by_year(call):
    """
    Handles batch processing grouped by year.
    """
    user_id = call.from_user.id
    contract_numbers = user_states.get(user_id, {}).get('pending_contracts', [])
    
    bot.answer_callback_query(call.id, "Группирую по годам...")
    
    # Show progress
    progress_msg = bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="📊 Подготавливаю пакетную обработку..."
    )
    
    # Process contracts by year
    process_batch_contracts(user_id, contract_numbers, group_by_year=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith('batch_all_'))
def batch_all_together(call):
    """
    Handles batch processing all together.
    """
    user_id = call.from_user.id
    contract_numbers = user_states.get(user_id, {}).get('pending_contracts', [])
    
    bot.answer_callback_query(call.id, "Обрабатываю все вместе...")
    
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="📊 Подготавливаю пакетную обработку..."
    )
    
    process_batch_contracts(user_id, contract_numbers, group_by_year=False)

# --- MAIN MESSAGE HANDLER ---
@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    """
    Main handler that analyzes input and routes to appropriate processing.
    """
    user_id = message.from_user.id
    analysis = analyze_user_input(message.text)
    
    if analysis['type'] == 'unknown':
        bot.reply_to(message, 
            "❓ Не удалось определить формат ввода.\\n\\n"
            "Отправьте одно из следующего:\\n"
            "• Ссылку на zakupki.gov.ru\\n"
            "• Номер контракта (19+ цифр)\\n"
            "• Список номеров через запятую или пробел")
        return
    
    if analysis['type'] == 'url':
        # Existing URL handling
        url = analysis['data']
        bot.reply_to(message, "🚀 Полный парсинг контракта (включая акты и товары)...\\nЭто может занять 10-20 секунд.")
        process_contract_parsing(message.chat_id, url)
    
    elif analysis['type'] == 'single_number':
        # Single contract number - show confirmation
        contract_number = analysis['data']
        show_single_contract_confirmation(message, contract_number)
    
    elif analysis['type'] == 'multiple_numbers':
        # Multiple contracts - show preview and options
        contract_numbers = analysis['data']
        show_batch_options(message, contract_numbers)

def show_single_contract_confirmation(message, contract_number):
    """
    Shows confirmation dialog for single contract.
    """
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(
        "✅ Да, парсить", 
        callback_data=f"confirm_single_{contract_number}"
    ))
    markup.add(types.InlineKeyboardButton(
        "❌ Отмена", 
        callback_data="cancel_single"
    ))
    
    short_num = contract_number[-6:]
    bot.reply_to(message, 
        f"🔍 **Найден контракт К-{short_num}**\\n\\n"
        f"Полный номер: `{contract_number}`\\n\\n"
        f"Продолжить парсинг?",
        reply_markup=markup,
        parse_mode='Markdown'
    )
    add_contracts_to_registry([contract_number])

def show_batch_options(message, contract_numbers):
    """
    Shows batch processing options for multiple contracts.
    """
    user_id = message.from_user.id
    user_states[user_id] = {'pending_contracts': contract_numbers}
    add_contracts_to_registry(contract_numbers)
    
    # Get contract preview
    contracts_preview = fetch_contract_preview_via_ssh(contract_numbers)
    
    if not contracts_preview:
        bot.reply_to(message, "❌ Не удалось получить информацию о контрактах")
        return
    
    # Show preview
    preview_msg = format_contract_list_preview(contracts_preview)
    bot.reply_to(message, preview_msg, parse_mode='Markdown')
    
    # Show options
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(
        "📅 Создать листы по годам", 
        callback_data="batch_by_year_"
    ))
    markup.add(types.InlineKeyboardButton(
        "📄 Все в один лист", 
        callback_data="batch_all_"
    ))
    markup.add(types.InlineKeyboardButton(
        "❌ Отмена", 
        callback_data="cancel_batch"
    ))
    
    bot.reply_to(message, 
        f"📋 **Как обработать {len(contract_numbers)} контрактов?**\\n\\n"
        "Выберите способ группировки в Google Sheets:",
        reply_markup=markup,
        parse_mode='Markdown'
    )

def process_contract_parsing(chat_id, url):
    """
    Processes single contract parsing.
    """
    # Use JSON parser with enhanced archiving
    data = fetch_contract_data_via_ssh(url)
    
    if not data or "error" in data:
         err = data.get("error", "Unknown error") if data else "No data received"
         
         # AI классификация ошибки (временно отключена)
         bot.send_message(chat_id, "🤖 Анализирую ошибку...")
         # error_analysis = ai_service.classify_error(err, {"url": url})  # Temporarily disabled
         error_analysis = {"category": "unknown", "suggestions": ["Check logs for details"]}
         
         msg = f"❌ Ошибка парсинга: {err}\\n\\n"
         msg += f"🔍 **Тип ошибки:** {error_analysis.get('category', 'UNKNOWN')}\\n"
         msg += f"💡 **Рекомендация:** {error_analysis.get('suggestion', 'Попробуйте повторить запрос')}\\n\\n"
         msg += f"🧠 *Анализ выполнен сервисом: {error_analysis.get('service', 'unknown')}*"
         
         bot.send_message(chat_id, msg, parse_mode='Markdown')
         return
    
    # Enhanced price processing and archiving
    contract_number = data.get('reestr_number', 'Unknown')
    contract_price_raw = data.get('price', 'NOT_FOUND')
    contract_price_clean = clean_number(contract_price_raw)
    
    # Check if we have debug info from enhanced parser
    debug_info = data.get('debug_info', {})
    
    # Archive detailed contract data
    try:
        from contract_data_archiver import save_debug_data
        files_saved = save_debug_data(data, contract_number, debug_info)
        logging.info(f"Contract {contract_number} archived {len(files_saved)} files")
    except ImportError:
        logging.warning("Contract data archiver not available - basic parsing only")
        files_saved = []
    
    # Enhanced price analysis
    if contract_price_clean <= 0:
        warning_msg = f"⚠️ **Внимание:** Цена контракта = 0.0\\n\\n"
        warning_msg += f"Исходные данные: `{contract_price_raw}`\\n\\n"
        warning_msg += f"Обработанное значение: {contract_price_clean}\\n\\n"
        
        # Check if we have object totals for fallback
        objects = data.get('objects', [])
        if objects:
            total_from_objects = sum(
                float(obj.get('total', '0').replace(' ', '').replace(',', '.').replace('₽', '').replace('RUB', '').replace('Ставка НДС: 20%', '').replace('Ставка НДС: Без НДС', '')
                )
            )
            if total_from_objects > 0:
                warning_msg += f"\\n💡 **Fallback использована:** Сумма объектов = {total_from_objects:,.2f} руб\\n"
                warning_msg += "Цена контракта будет обновлена из суммы объектов"
                
                # Update the price in data for Google Sheets
                data['price'] = str(total_from_objects)
                data['price_fallback_used'] = True
                contract_price_clean = total_from_objects
            else:
                warning_msg += f"\\n❌ **Fallback не удался:** Не удалось рассчитать из объектов"
        
        warning_msg += f"\\n📁 **Архивировано файлов:** {len(files_saved)} шт"
        
        bot.send_message(chat_id, warning_msg, parse_mode='Markdown')
    else:
        logging.info(f"Contract {contract_number} price extracted successfully: {contract_price_clean}")
        if files_saved:
            logging.info(f"Contract {contract_number} archived {len(files_saved)} debug files")
    
    # Notify user about parsing result
    response_text = f"✅ **Данные получены**\\n"
    response_text += f"Контракт: `{data.get('reestr_number')}`\\n"
    response_text += f"Цена: {data.get('price')}\\n"
    response_text += f"Оплачено: {data.get('execution', {}).get('paid')}\\n"
    response_text += f"Товаров/Услуг найдено: {len(data.get('objects', []))}"
    
    bot.send_message(chat_id, response_text, parse_mode='Markdown')
    
    # Update Sheet
    bot.send_message(chat_id, "⏳ Добавляю в таблицу...")
    sheet_url, validation_result = add_contract_to_master(data)
    
    if sheet_url:
        msg = f"📊 **Лист создан!**\\n\\n"
        msg += f"🔗 **Ссылка:** [{sheet_url}]({sheet_url})\\n\\n"
        msg += "📢 **Важно:** Таблица доступна по публичной ссылке\\n"
        msg += "💾 Сохраните ссылку для будущего доступа"
        
        bot.send_message(chat_id, msg, parse_mode='Markdown', disable_web_page_preview=True)
        
        # Add validation message if we have validation results
        if validation_result:
            validation_msg = format_validation_message(validation_result)
            bot.send_message(chat_id, validation_msg, parse_mode='Markdown')
    else:
        err = validation_result if isinstance(validation_result, str) else "Неизвестная ошибка"
        bot.send_message(chat_id, f"❌ Ошибка записи в таблицу: {err}")

def check_contract_update(chat_id, contract_number, silent=False):
    """
    Checks a contract by number and updates sheets/DB.
    """
    url = get_contract_url_from_number(contract_number)
    parse_logger.info(f"CHECK start contract={contract_number}")
    data = fetch_contract_data_via_ssh(url)
    if not data or "error" in data:
        err = data.get("error", "Unknown error") if data else "No data received"
        parse_logger.error(f"CHECK fail contract={contract_number} err={err}")
        safe_send(chat_id, f"❌ Ошибка проверки К-{contract_number[-6:]}: {err}")
        return False, False

    objects_changed, requisites_changed = determine_changes(data)
    sheet_url, _ = add_contract_to_master(data, objects_changed, requisites_changed)
    parse_logger.info(f"CHECK done contract={contract_number} objects_changed={objects_changed} requisites_changed={requisites_changed}")

    if not silent:
        if objects_changed or requisites_changed:
            msg = f"✅ Обновлено К-{contract_number[-6:]} (изменения обнаружены)"
        else:
            msg = f"ℹ️ К-{contract_number[-6:]} без изменений"
        if sheet_url:
            msg += f"\nСводная: {sheet_url}"
        safe_send(chat_id, msg)

    return True, (objects_changed or requisites_changed)

def process_batch_contracts(user_id, contract_numbers, group_by_year=True):
    """
    Processes batch contracts.
    """
    chat_id = user_id  # Simplified - in real app, track chat_id separately
    
    # Show initial progress
    progress_msg = bot.send_message(chat_id, 
        f"📊 **Начинаю пакетную обработку**\\n"
        f"Контрактов: {len(contract_numbers)}\\n"
        f"Группировка: {'по годам' if group_by_year else 'все вместе'}\\n\\n"
        f"0/{len(contract_numbers)} завершено..."
    )
    
    # Process contracts
    processed = 0
    errors = []
    results = []
    
    for i, contract_number in enumerate(contract_numbers):
        try:
            url = get_contract_url_from_number(contract_number)
            data = fetch_contract_data_via_ssh(url)
            
            if data and "error" not in data:
                results.append(data)
                processed += 1
            else:
                errors.append(f"К-{contract_number[-6:]}: {data.get('error', 'Unknown error') if data else 'No data'}")
            
            # Update progress every 3 contracts
            if (i + 1) % 3 == 0 or i == len(contract_numbers) - 1:
                progress_text = (
                    f"📊 **Пакетная обработка**\\n"
                    f"Контрактов: {len(contract_numbers)}\\n"
                    f"Группировка: {'по годам' if group_by_year else 'все вместе'}\\n\\n"
                    f"{processed}/{len(contract_numbers)} завершено..."
                )
                
                if errors:
                    progress_text += f"\\n⚠️ Ошибок: {len(errors)}"
                
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=progress_text
                )
            
        except Exception as e:
            errors.append(f"К-{contract_number[-6:]}: {str(e)}")
            logging.error(f"Error processing contract {contract_number}: {e}")
    
    # Final results
    if group_by_year:
        sheet_urls = add_contracts_by_year(results)
    else:
        sheet_urls = add_contracts_to_single_sheet(results)
    
    # Send final report
    send_batch_report(chat_id, processed, errors, sheet_urls, group_by_year)

# --- CANCELLATION HANDLERS ---
@bot.callback_query_handler(func=lambda call: call.data == 'cancel_single')
def cancel_single(call):
    bot.answer_callback_query(call.id, "Отменено")
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="❌ Парсинг отменен"
    )

@bot.callback_query_handler(func=lambda call: call.data == 'cancel_batch')
def cancel_batch(call):
    user_id = call.from_user.id
    if user_id in user_states:
        del user_states[user_id]
    
    bot.answer_callback_query(call.id, "Отменено")
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="❌ Пакетная обработка отменена"
    )

# --- AI ANALYSIS COMMAND ---
@bot.message_handler(commands=['analyze_ai'])
def handle_ai_analysis(message):
    """
    AI анализ контракта по URL или последнему обработанному
    """
    user_id = message.from_user.id
    
    # Проверяем прав доступа
    if not (user_id == SUPER_ADMIN_ID or user_id in ADMIN_IDS):
        bot.reply_to(message, "🚫 У вас нет прав для AI анализа")
        return
    
    # Запрашиваем URL или номер контракта
    msg = bot.send_message(message.chat.id, 
        "🤖 **AI Анализ Контракта**\\n\\n"
        "Отправьте URL контракта или номер реестра для детального AI анализа\\n\\n"
        "Или введите 'last' для анализа последнего обработанного контракта")
    
    bot.register_next_step_handler(msg, process_ai_analysis)

@bot.message_handler(commands=['check_contract'])
def handle_check_contract(message):
    """
    Проверка одного контракта по номеру.
    """
    user_id = message.from_user.id
    if not (user_id == SUPER_ADMIN_ID or user_id in ADMIN_IDS):
        bot.reply_to(message, "🚫 У вас нет прав для этой команды")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(message, "Введите номер контракта: /check_contract 3391704681226000001")
        return
    contract_number = parts[1].strip()
    if not is_valid_contract_number(contract_number):
        bot.reply_to(message, "Некорректный номер контракта")
        return
    bot.reply_to(message, f"🔄 Проверяю К-{contract_number[-6:]}...")
    check_contract_update(message.chat.id, contract_number, silent=False)

@bot.message_handler(commands=['check_all'])
def handle_check_all(message):
    """
    Проверка всех контрактов из реестра (DB).
    """
    user_id = message.from_user.id
    if not (user_id == SUPER_ADMIN_ID or user_id in ADMIN_IDS):
        bot.reply_to(message, "🚫 У вас нет прав для этой команды")
        return
    contract_numbers = get_contract_numbers_from_registry()
    if not contract_numbers:
        contract_numbers = get_contract_numbers_from_db()
    if not contract_numbers:
        bot.reply_to(message, "Реестр пуст. Сначала добавьте контракты.")
        return
    bot.reply_to(message, f"🔄 Проверяю {len(contract_numbers)} контрактов...")
    processed = 0
    changed = 0
    for number in contract_numbers:
        ok, did_change = check_contract_update(message.chat.id, number, silent=True)
        if ok:
            processed += 1
            if did_change:
                changed += 1
    bot.send_message(message.chat.id, f"✅ Проверка завершена. Всего: {processed}, с изменениями: {changed}")

@bot.message_handler(commands=['add_contracts'])
def handle_add_contracts(message):
    """
    Add contract numbers to registry without parsing.
    """
    user_id = message.from_user.id
    if not (user_id == SUPER_ADMIN_ID or user_id in ADMIN_IDS):
        bot.reply_to(message, "🚫 У вас нет прав для этой команды")
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Введите номера контрактов: /add_contracts 339... 338... 337...")
        return
    raw = parts[1]
    numbers = extract_contract_numbers(raw)
    if not numbers:
        bot.reply_to(message, "Не найдено корректных номеров (19+ цифр)")
        return
    add_contracts_to_registry(numbers)
    bot.reply_to(message, f"✅ Добавлено в реестр: {len(numbers)}")

@bot.message_handler(commands=['remove_contract'])
def handle_remove_contract(message):
    """
    Remove a contract from registry and DB.
    """
    user_id = message.from_user.id
    if not (user_id == SUPER_ADMIN_ID or user_id in ADMIN_IDS):
        bot.reply_to(message, "🚫 У вас нет прав для этой команды")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        bot.reply_to(message, "Введите номер контракта: /remove_contract 3391704681226000001")
        return
    contract_number = parts[1].strip()
    if not is_valid_contract_number(contract_number):
        bot.reply_to(message, "Некорректный номер контракта")
        return
    remove_contract_from_registry(contract_number)
    bot.reply_to(message, f"✅ Удалено из реестра: {contract_number}")

@bot.message_handler(commands=['clear_registry'])
def handle_clear_registry(message):
    """
    Clear registry sheet and DB.
    """
    user_id = message.from_user.id
    if not (user_id == SUPER_ADMIN_ID or user_id in ADMIN_IDS):
        bot.reply_to(message, "🚫 У вас нет прав для этой команды")
        return
    clear_registry()
    bot.reply_to(message, "✅ Реестр очищен")

def process_ai_analysis(message):
    """
    Обрабатывает запрос на AI анализ
    """
    user_input = message.text.strip()
    chat_id = message.chat.id
    
    try:
        # Определяем источник данных
        if user_input.lower() == 'last':
            bot.send_message(chat_id, "🔄 Анализ последнего контракта...")
            # TODO: Реализовать получение последнего контракта
            bot.send_message(chat_id, "📝 Функция анализа последнего контракта в разработке")
            return
        
        elif user_input.startswith(('http://', 'https://')):
            bot.send_message(chat_id, "🔄 Получаю данные контракта...")
            url = user_input
            
            # Получаем данные
            data = fetch_contract_data_via_ssh(url)
            if not data or "error" in data:
                bot.send_message(chat_id, f"❌ Не удалось получить данные контракта: {data.get('error', 'Unknown error') if data else 'No data'}")
                return
        else:
            # Предполагаем что это номер контракта
            bot.send_message(chat_id, "🔄 Ищу контракт по номеру...")
            # TODO: Реализовать поиск по номеру
            bot.send_message(chat_id, "📝 Поиск по номеру в разработке")
            return
        
        # Выполняем AI анализ
        bot.send_message(chat_id, "🧠 Выполняю AI анализ...")
        # analysis_result = ai_service.analyze_contract(data)  # Temporarily disabled
        analysis_result = {"status": "completed", "risk_level": "low"}
        
        if analysis_result.get('status') == 'success':
            analysis = analysis_result.get('result', 'Анализ не удался')
            service = analysis_result.get('service', 'unknown')
            
            # Форматируем ответ
            response = f"🤖 **AI Анализ Контракта**\\n\\n"
            response += f"📋 Контракт: `{data.get('reestr_number', 'N/A')}`\\n"
            response += f"💰 Цена: {data.get('price', 'N/A')}\\n\\n"
            response += f"📊 **Анализ:**\\n{analysis}\\n\\n"
            response += f"🔧 *Сервис: {service}*"
            
            bot.send_message(chat_id, response, parse_mode='Markdown')
        else:
            bot.send_message(chat_id, f"❌ AI анализ не удался: {analysis_result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logging.error(f"AI analysis error: {e}")
        bot.send_message(chat_id, f"❌ Ошибка AI анализа: {str(e)}")

if __name__ == '__main__':
    logging.info("Бот запущен...")
    # Increase Telegram API timeouts for unstable networks
    try:
        from telebot import apihelper
        apihelper.CONNECT_TIMEOUT = 10
        apihelper.READ_TIMEOUT = 60
    except Exception as e:
        logging.warning(f"Failed to set Telegram timeouts: {e}")

    # Resilient polling loop
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=30)
        except Exception as e:
            logging.error(f"Polling crashed: {e}")
            time.sleep(5)
