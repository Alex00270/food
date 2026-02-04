import os
import logging
import re
import traceback
import subprocess
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import telebot
from telebot import types
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

token = os.getenv('TELEGRAM_TOKEN')
super_admin_id = os.getenv('SUPER_ADMIN_ID')
admin_ids_str = os.getenv('ADMIN_IDS')
creds_path = os.getenv('GOOGLE_API_CREDENTIALS_PATH', 'credentials.json')

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
    try:
        return Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    except Exception as e:
        logging.error(f"Failed to load credentials: {e}")
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

# --- REMOTE PARSER ---
def fetch_contract_data_via_ssh(url):
    """
    Executes a remote script on 'ussr' to fetch FULL contract data as JSON.
    """
    try:
        ssh_command = [
            "ssh", "ussr",
            f"~/zakupki-parser/venv/bin/python ~/zakupki-parser/fetch_contract_data.py '{url}'"
        ]
        logging.info(f"Executing remote fetch (JSON) for: {url}")
        result = subprocess.run(ssh_command, capture_output=True, text=False)
        
        if result.returncode != 0:
            error_msg = result.stderr.decode('utf-8')
            logging.error(f"Remote fetch failed: {error_msg}")
            return None
            
        json_output = result.stdout.decode('utf-8')
        return json.loads(json_output)
        
    except Exception as e:
        logging.error(f"SSH execution error: {e}")
        return None

def fetch_contract_preview_via_ssh(contract_numbers):
    """
    Fetches preview information for multiple contracts via SSH.
    Returns list of contracts with basic info.
    """
    try:
        numbers_str = ','.join(contract_numbers)
        ssh_command = [
            "ssh", "ussr",
            f"~/zakupki-parser/venv/bin/python ~/zakupki-parser/fetch_contracts_preview.py '{numbers_str}'"
        ]
        logging.info(f"Executing remote preview for {len(contract_numbers)} contracts")
        result = subprocess.run(ssh_command, capture_output=True, text=False)
        
        if result.returncode != 0:
            error_msg = result.stderr.decode('utf-8')
            logging.error(f"Remote preview failed: {error_msg}")
            return None
            
        json_output = result.stdout.decode('utf-8')
        return json.loads(json_output)
        
    except Exception as e:
        logging.error(f"SSH preview execution error: {e}")
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
    """
    if not value_str:
        return 0.0
    
    # Remove common suffixes/prefixes
    clean = value_str.replace('₽', '').replace('RUB', '').replace('ДЕТ ДН', '').replace('УСЛ ЕД', '')
    # Remove text like "Ставка НДС..." (take first line if multiline)
    clean = clean.split('\n')[0]
    
    # Remove spaces (thousands separator)
    clean = clean.replace(' ', '').replace('\xa0', '') # \xa0 is non-breaking space
    
    # Replace comma with dot
    clean = clean.replace(',', '.')
    
    try:
        # Extract first valid number using regex (handles "Price: 100")
        match = re.search(r'(\d+(\.\d+)?)', clean)
        if match:
            return float(match.group(1))
        return 0.0
    except:
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
def add_contract_to_master(data):
    """
    Adds a new worksheet to the MASTER_SHEET_ID with FULL contract data.
    """
    gc = get_gc()
    if not gc:
        return None, "Ошибка подключения к Google Sheets"

    try:
        # Open master sheet
        sh = gc.open_by_key(MASTER_SHEET_ID)
        
        # Sheet title: Contract Number
        base_title = f"К-{data.get('reestr_number', 'Unknown')[-6:]}"
        title = base_title
        
        counter = 1
        while True:
            try:
                sh.worksheet(title)
                title = f"{base_title}_{counter}"
                counter += 1
            except gspread.WorksheetNotFound:
                break
            
        ws = sh.add_worksheet(title=title, rows=100, cols=20)
            
        # --- FILL DATA ---
        
        # Clean execution numbers
        paid_clean = clean_number(data.get('execution', {}).get('paid', '0'))
        accepted_clean = clean_number(data.get('execution', {}).get('accepted', '0'))
        contract_price_clean = clean_number(data.get('price', '0'))

        # 1. Header Info
        info_data = [
            ["КОНТРАКТ", data.get('reestr_number')],
            ["Заказчик", data.get('customer')],
            ["Цена контракта", contract_price_clean],
            ["Дата начала", data.get('date_start', '-')],
            ["Дата окончания", data.get('date_end', '-')],
            ["Ссылка", data.get('url')],
            [], 
            ["ИСПОЛНЕНИЕ", ""],
            ["Оплачено", paid_clean],
            ["Принято (Акты)", accepted_clean],
            ["Остаток лимита", f"={contract_price_clean}-{accepted_clean}"], # Formula
            [],
            ["ОБЪЕКТЫ ЗАКУПКИ", "Кол-во", "Ед.изм.", "Цена", "Сумма (Источник)", "Сумма (Расчет)", "Название"] 
        ]
        
        for row in info_data:
            ws.append_row(row)
            
        # 2. Items Table
        objects = data.get('objects', [])
        start_row = len(info_data) + 1
        
        # Filter out total rows and parse all objects
        parsed_objects = []
        for obj in objects:
            if not is_total_row(obj.get('name', '')):
                parsed_obj = parse_price_info(obj)
                parsed_objects.append(parsed_obj)
        
        if parsed_objects:
            # Calculate totals for validation
            calculated_total = sum(obj['total_sum'] for obj in parsed_objects)
            
            for i, obj in enumerate(parsed_objects):
                # Row index for formula (1-based)
                current_row = start_row + i + 1
                
                ws.append_row([
                    "-", # Date
                    obj['qty'], # Calculated Quantity
                    obj['price_unit'] if obj['price_unit'] else obj['total_unit'], # Unit of measurement
                    obj['price'], # Price per unit
                    obj['total_sum'], # Source Sum
                    f"=B{current_row}*D{current_row}", # Formula: Qty * Price
                    obj['name'] # Item name
                ])
                
            # Add Total Check Formula
            last_row = start_row + len(parsed_objects)
            ws.append_row([
                "ИТОГО", 
                "", 
                "", 
                "", 
                f"=SUM(E{start_row+1}:E{last_row})", # Sum of source totals
                f"=SUM(F{start_row+1}:F{last_row})", # Sum of calculated totals
                ""
            ])
            
            # Validate totals and return validation result
            validation_result = validate_totals(objects, calculated_total)
            
        else:
            ws.append_row(["(Детализация товаров не найдена или не спарсилась)"])
            validation_result = None

        # CRITICAL: Share the sheet with public access
        try:
            sh.share('', perm_type='anyone', role='reader')
            logging.info(f"Sheet {ws.title} made publicly accessible")
        except Exception as e:
            logging.error(f"Failed to make sheet public: {e}")
            # Still return URL even if sharing fails
            # User might have access through other means

        return ws.url, validation_result
        
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
            
            # Add contracts
            for contract in contracts:
                contract_price_clean = clean_number(contract.get('price', '0'))
                paid_clean = clean_number(contract.get('execution', {}).get('paid', '0'))
                accepted_clean = clean_number(contract.get('execution', {}).get('accepted', '0'))
                
                row = [
                    contract.get('reestr_number', ''),
                    contract.get('customer', ''),
                    contract_price_clean,
                    contract.get('date_start', ''),
                    contract.get('date_end', ''),
                    contract.get('url', ''),
                    paid_clean,
                    accepted_clean,
                    f"={contract_price_clean}-{accepted_clean}"
                ]
                ws.append_row(row)
            
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
        
        # Add contracts
        for contract in contracts_data:
            contract_price_clean = clean_number(contract.get('price', '0'))
            paid_clean = clean_number(contract.get('execution', {}).get('paid', '0'))
            accepted_clean = clean_number(contract.get('execution', {}).get('accepted', '0'))
            
            row = [
                contract.get('reestr_number', ''),
                contract.get('customer', ''),
                contract_price_clean,
                contract.get('date_start', ''),
                contract.get('date_end', ''),
                contract.get('url', ''),
                paid_clean,
                accepted_clean,
                f"={contract_price_clean}-{accepted_clean}"
            ]
            ws.append_row(row)
        
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

def show_batch_options(message, contract_numbers):
    """
    Shows batch processing options for multiple contracts.
    """
    user_id = message.from_user.id
    user_states[user_id] = {'pending_contracts': contract_numbers}
    
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
    # Use JSON parser
    data = fetch_contract_data_via_ssh(url)
    
    if not data or "error" in data:
         err = data.get("error", "Unknown error") if data else "No data received"
         bot.send_message(chat_id, f"❌ Ошибка парсинга: {err}")
         return
         
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
        bot.send_message(chat_id, "❌ Ошибка записи в таблицу")

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

if __name__ == '__main__':
    logging.info("Бот запущен...")
    bot.infinity_polling()
