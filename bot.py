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
            ["ОБЪЕКТЫ ЗАКУПКИ", "Цена", "Всего", "Сумма (Расчет)", "Название"] 
        ]
        
        for row in info_data:
            ws.append_row(row)
            
        # 2. Items Table
        objects = data.get('objects', [])
        start_row = len(info_data) + 1
        
        if objects:
            for i, obj in enumerate(objects):
                name = obj.get('name', '')
                
                # Skip "Total" rows from source if parser caught them
                if "итого" in name.lower():
                    continue
                    
                price = clean_number(obj.get('price'))
                total_sum_source = clean_number(obj.get('total')) # This is usually "Sum", not "Quantity" in zakupki table?
                # Actually, in zakupki table: Price per unit | Quantity | Total Sum
                # Our parser returns: name, price, total (which is likely Total Sum from column 6)
                # We need to deduce Quantity = Total / Price if possible, or parse Quantity explicitly
                
                # Let's assume 'total' from parser is the 'Total Sum' column.
                # If Price > 0, we can calc Qty, or leave it 0
                qty = 0
                if price > 0 and total_sum_source > 0:
                    qty = round(total_sum_source / price, 2)
                
                # Row index for formula (1-based)
                current_row = start_row + i + 1
                
                ws.append_row([
                    "-", # Date
                    obj.get('category', 'Прочее'), 
                    qty, # Calculated Qty or 0
                    price, 
                    total_sum_source, # Source Sum
                    f"=C{current_row}*D{current_row}", # Formula check
                    name 
                ])
                
            # Add Total Check Formula
            last_row = start_row + len(objects)
            ws.append_row(["ИТОГО", "", "", "", f"=SUM(E{start_row+1}:E{last_row})", f"=SUM(F{start_row+1}:F{last_row})"])
            
        else:
            ws.append_row(["(Детализация товаров не найдена или не спарсилась)"])

        return ws.url, None
        
    except Exception as e:
        error_details = traceback.format_exc()
        logging.error(f"Error updating sheet: {error_details}")
        return None, str(e)



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

@bot.message_handler(func=lambda message: 'zakupki.gov.ru' in message.text)
def handle_zakupki_link(message):
    url = message.text.strip()
    
    bot.reply_to(message, "🚀 Полный парсинг контракта (включая акты и товары)...\nЭто может занять 10-20 секунд.")
    
    # Use JSON parser
    data = fetch_contract_data_via_ssh(url)
    
    if not data or "error" in data:
         err = data.get("error", "Unknown error") if data else "No data received"
         bot.reply_to(message, f"❌ Ошибка парсинга: {err}")
         return
         
    # Notify user about parsing result
    response_text = f"✅ **Данные получены**\n"
    response_text += f"Контракт: `{data.get('reestr_number')}`\n"
    response_text += f"Цена: {data.get('price')}\n"
    response_text += f"Оплачено: {data.get('execution', {}).get('paid')}\n"
    response_text += f"Товаров/Услуг найдено: {len(data.get('objects', []))}"
    
    bot.reply_to(message, response_text, parse_mode='Markdown')
    
    # Update Sheet
    bot.reply_to(message, "⏳ Добавляю в таблицу...")
    sheet_url, warning = add_contract_to_master(data)
    
    if sheet_url:
        msg = f"📊 **Лист создан!**\n\nСсылка: {sheet_url}"
        if warning:
            msg += f"\n(Примечание: {warning})"
        bot.reply_to(message, msg)
    else:
        bot.reply_to(message, f"❌ Ошибка записи в таблицу: {warning}")

if __name__ == '__main__':
    logging.info("Бот запущен...")
    bot.infinity_polling()
