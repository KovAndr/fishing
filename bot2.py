import requests
import openpyxl
import random
import time
import os
import threading
import asyncio
import re
import json
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)
from flask import Flask, request
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ================== FLASK FOR RENDER ==================
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running"

@app.route('/health')
def health():
    return {"status": "ok"}, 200

# ================== BOT SETTINGS ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
ORS_API_KEY = os.getenv("ORS_API_KEY", "")

# Cache for geocoding
GEOCODE_CACHE = {}
MAX_WAYPOINTS = 25

# ================== UTILITIES ==================
def normalize_address(address):
    """Normalize address with improved logic"""
    if not address:
        return ""
    
    address = re.sub(r'\s+', ' ', address.strip())
    
    replacements = {
        'обл.': 'область',
        'г.': 'город',
        'ул.': 'улица',
        'пр.': 'проспект',
        'пр-т': 'проспект',
        'пер.': 'переулок',
        'д.': 'дом',
        'с.': 'село',
        'п.': 'поселок',
        'р-н': 'район',
        'р.': 'республика',
        'ст-ца': 'станица',
        'мкр.': 'микрорайон',
        'к.': 'корпус',
        'стр.': 'строение',
        'вл.': 'владение',
    }
    
    for short, full in replacements.items():
        address = re.sub(rf'\b{re.escape(short)}\b', full, address, flags=re.IGNORECASE)
    
    # Add Russia if not specified
    if not any(word in address.lower() for word in ['россия', 'russia', 'рф']):
        if not any(word in address.lower() for word in ['украина', 'беларусь', 'казахстан']):
            address = f'Россия, {address}'
    
    return address

def parse_address_chain(address_string):
    """Improved address parsing with better delimiter handling"""
    if not address_string:
        return []
    
    address_string = str(address_string).strip()
    
    # Replace different delimiters with standard one
    address_string = re.sub(r'[–—]', '-', address_string)
    
    # Handle complex cases with hyphens in names
    addresses = []
    current_address = ""
    in_parenthesis = False
    
    for char in address_string:
        if char == '(':
            in_parenthesis = True
            current_address += char
        elif char == ')':
            in_parenthesis = False
            current_address += char
        elif char == '-' and not in_parenthesis:
            if current_address.strip():
                addresses.append(current_address.strip())
                current_address = ""
        else:
            current_address += char
    
    if current_address.strip():
        addresses.append(current_address.strip())
    
    # Filter and normalize
    normalized = []
    for addr in addresses:
        norm_addr = normalize_address(addr)
        if norm_addr and norm_addr not in normalized:
            normalized.append(norm_addr)
    
    return normalized

def yandex_geocode(address, max_retries=3):
    """Improved geocoding with better error handling"""
    if not YANDEX_API_KEY:
        print("⚠️ YANDEX_API_KEY not set!")
        return None
    
    cache_key = address.lower()
    if cache_key in GEOCODE_CACHE:
        return GEOCODE_CACHE[cache_key]
    
    url = "https://geocode-maps.yandex.ru/1.x/"
    
    for attempt in range(max_retries):
        try:
            params = {
                "apikey": YANDEX_API_KEY,
                "format": "json",
                "geocode": address,
                "results": 1,
                "lang": "ru_RU"
            }
            
            r = requests.get(url, params=params, timeout=30)
            
            if r.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            
            data = r.json()
            
            members = data.get("response", {}).get("GeoObjectCollection", {}).get("featureMember", [])
            if members:
                feature = members[0]["GeoObject"]
                pos = feature["Point"]["pos"]
                lon, lat = pos.split()
                coords = (float(lat), float(lon))
                
                # Validate coordinates for Russia
                if 40 <= lat <= 82 and 19 <= lon <= 190:
                    GEOCODE_CACHE[cache_key] = coords
                    return coords
            
            return None
                
        except Exception as e:
            print(f"⚠️ Geocoding error {address}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
    
    return None

def ors_route_with_waypoints(coordinates_list, max_points_per_request=25):
    """Route calculation with improved waypoint handling"""
    if not ORS_API_KEY:
        print("⚠️ ORS_API_KEY not set!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    # If too many points, split into segments
    if len(coordinates_list) > max_points_per_request:
        total_distance = 0
        
        # Process in chunks
        for i in range(0, len(coordinates_list)-1):
            chunk = coordinates_list[i:i+2]
            chunk_distance = ors_route_with_waypoints(chunk)
            
            if chunk_distance:
                total_distance += chunk_distance
            else:
                return None
            
            time.sleep(0.3)
        
        return round(total_distance, 1)
    
    # Convert to [lon, lat] format
    coordinates = [[coord[1], coord[0]] for coord in coordinates_list]
    
    url = "https://api.openrouteservice.org/v2/directions/driving-car/geojson"
    headers = {"Authorization": ORS_API_KEY}
    body = {"coordinates": coordinates}
    
    try:
        r = requests.post(url, json=body, headers=headers, timeout=60)
        
        if r.status_code != 200:
            print(f"⚠️ Route error: {r.status_code}, {r.text[:200]}")
            return None
        
        data = r.json()
        
        if data.get("features") and data["features"][0].get("properties", {}).get("summary"):
            dist = data["features"][0]["properties"]["summary"]["distance"]
            return round(dist / 1000, 1)
            
    except requests.exceptions.Timeout:
        print("⚠️ Route calculation timeout")
    except Exception as e:
        print(f"⚠️ Route calculation error: {e}")
    
    return None

def calculate_route_safely(coordinates):
    """Safe route calculation with validation"""
    try:
        valid_coords = []
        for coord in coordinates:
            if coord and isinstance(coord, tuple) and len(coord) == 2:
                lat, lon = coord
                if 40 <= lat <= 82 and 19 <= lon <= 190:
                    valid_coords.append(coord)
        
        if len(valid_coords) < 2:
            print(f"⚠️ Not enough valid coordinates: {len(valid_coords)}")
            return None
        
        distance = ors_route_with_waypoints(valid_coords)
        return distance
        
    except Exception as e:
        print(f"⚠️ Safe route calculation error: {e}")
        return None

def variations(base):
    """Generate distance variations"""
    if base is None or base <= 0:
        return [None, None]
    
    try:
        variation = random.uniform(0.95, 1.05)
        d2 = round(base * variation, 1)
        
        variation2 = random.uniform(0.92, 1.08)
        d3 = round(base * variation2, 1)
        
        return [d2, d3]
    except:
        return [None, None]

# ================== EXCEL HANDLING ==================
def read_from_excel(path):
    """Read routes from Excel file"""
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    routes = []
    
    max_row = ws.max_row
    
    for row in range(2, max_row + 1):
        start_point = ws.cell(row=row, column=1).value
        address_chain = ws.cell(row=row, column=2).value
        
        if start_point and address_chain:
            routes.append({
                'row_num': row,
                'start_point': str(start_point).strip(),
                'address_chain': str(address_chain).strip(),
            })
    
    return routes, wb, ws

def add_result_columns(ws, start_col=3):
    """Add result columns to Excel"""
    headers = [
        "Статус обработки",
        "Координаты старта",
        "Координаты точек",
        "Кол-во точек",
        "Тип маршрута",
        "Расстояние 1 (км)",
        "Расстояние 2 (км)",
        "Расстояние 3 (км)",
        "Примечания"
    ]
    
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=start_col + i)
        cell.value = header
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = thin_border
    
    column_widths = {
        start_col: 20,
        start_col + 1: 25,
        start_col + 2: 40,
        start_col + 3: 12,
        start_col + 4: 20,
        start_col + 5: 15,
        start_col + 6: 15,
        start_col + 7: 15,
        start_col + 8: 30,
    }
    
    for col, width in column_widths.items():
        ws.column_dimensions[openpyxl.utils.get_column_letter(col)].width = width
    
    return start_col + len(headers)

# ================== TELEGRAM BOT WITH BUTTONS ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command with inline keyboard"""
    keyboard = [
        [
            InlineKeyboardButton("📊 Обработать файл", callback_data="process_file"),
            InlineKeyboardButton("📋 Инструкция", callback_data="help")
        ],
        [
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
            InlineKeyboardButton("🔄 Очистить кэш", callback_data="clear_cache")
        ],
        [
            InlineKeyboardButton("⚙️ Настройки", callback_data="settings"),
            InlineKeyboardButton("ℹ️ О боте", callback_data="about")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "👋 *Добро пожаловать в бот для расчета маршрутов!*\n\n"
        "Я помогу вам рассчитать расстояния между адресами с поддержкой промежуточных точек.\n\n"
        "📁 *Формат Excel файла:*\n"
        "• Колонка A: Стартовая точка\n"
        "• Колонка B: Цепочка адресов через дефис\n\n"
        "📤 *Просто отправьте мне Excel файл, и я верну результат!*",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline keyboard button presses"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "process_file":
        await query.edit_message_text(
            "📤 *Отправьте Excel файл для обработки*\n\n"
            "Формат файла:\n"
            "• Колонка A: Стартовая точка\n"
            "• Колонка B: Адреса через дефис\n\n"
            "Пример: `г. Москва, ул. Ленина 1 - г. Санкт-Петербург, Невский пр. 2`",
            parse_mode='Markdown'
        )
    
    elif query.data == "help":
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📋 *Инструкция по использованию*\n\n"
            "1. Подготовьте Excel файл с двумя колонками:\n"
            "   • A: Стартовый адрес\n"
            "   • B: Цепочка адресов через дефис\n\n"
            "2. Отправьте файл боту\n"
            "3. Дождитесь обработки\n"
            "4. Получите файл с результатами\n\n"
            "📊 *В результатах будут:*\n"
            "• Статус обработки\n• Координаты\n• Расстояния\n• Примечания\n\n"
            "⚡ *Особенности:*\n"
            "• Поддержка промежуточных точек\n• Автокоррекция адресов\n• Кэширование геоданных",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    
    elif query.data == "stats":
        cache_size = len(GEOCODE_CACHE)
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"📊 *Статистика бота*\n\n"
            f"• Кэшированных адресов: `{cache_size}`\n"
            f"• Яндекс API: {'✅ Настроен' if YANDEX_API_KEY else '❌ Не настроен'}\n"
            f"• ORS API: {'✅ Настроен' if ORS_API_KEY else '❌ Не настроен'}\n"
            f"• Макс. точек: `{MAX_WAYPOINTS}`\n"
            f"• Время: `{datetime.now().strftime('%H:%M:%S')}`",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    
    elif query.data == "clear_cache":
        global GEOCODE_CACHE
        old_size = len(GEOCODE_CACHE)
        GEOCODE_CACHE.clear()
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"✅ *Кэш очищен*\n\n"
            f"Удалено записей: `{old_size}`\n"
            f"Новый размер: `{len(GEOCODE_CACHE)}`",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    
    elif query.data == "settings":
        keyboard = [
            [
                InlineKeyboardButton("📊 Макс. точек", callback_data="set_max_points"),
                InlineKeyboardButton("⚡ Скорость", callback_data="set_speed")
            ],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "⚙️ *Настройки бота*\n\n"
            "• Макс. точек в маршруте: `25`\n"
            "• Задержка между запросами: `0.3с`\n"
            "• Повторы при ошибках: `3`\n\n"
            "Для изменения настроек свяжитесь с администратором.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    
    elif query.data == "about":
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "ℹ️ *О боте*\n\n"
            "🤖 *Бот для расчета маршрутов*\n"
            "Версия: 2.0 (улучшенная)\n\n"
            "📡 *Используемые API:*\n"
            "• Яндекс.Карты для геокодирования\n"
            "• OpenRouteService для расчета маршрутов\n\n"
            "⚡ *Возможности:*\n"
            "• Расчет маршрутов с промежуточными точками\n"
            "• Автокоррекция и нормализация адресов\n"
            "• Кэширование для ускорения обработки\n"
            "• Подробная статистика и логирование\n\n"
            "👨‍💻 *Разработчик:* @your_username",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    
    elif query.data == "back_to_main":
        keyboard = [
            [
                InlineKeyboardButton("📊 Обработать файл", callback_data="process_file"),
                InlineKeyboardButton("📋 Инструкция", callback_data="help")
            ],
            [
                InlineKeyboardButton("📊 Статистика", callback_data="stats"),
                InlineKeyboardButton("🔄 Очистить кэш", callback_data="clear_cache")
            ],
            [
                InlineKeyboardButton("⚙️ Настройки", callback_data="settings"),
                InlineKeyboardButton("ℹ️ О боте", callback_data="about")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "👋 *Главное меню*\n\n"
            "Выберите действие:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle uploaded documents"""
    if not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте файл")
        return
    
    file_name = update.message.document.file_name.lower()
    if not (file_name.endswith('.xlsx') or file_name.endswith('.xls')):
        await update.message.reply_text("❌ Пожалуйста, отправьте файл в формате Excel (XLSX/XLS)")
        return
    
    # Send processing started message
    status_msg = await update.message.reply_text(
        "⏳ *Начинаю обработку файла...*\n"
        "Подготовка к работе...",
        parse_mode='Markdown'
    )
    
    file = await update.message.document.get_file()
    user_id = update.message.from_user.id
    timestamp = int(time.time())
    input_file = f"input_{user_id}_{timestamp}.xlsx"
    
    await file.download_to_drive(input_file)
    
    try:
        routes, wb, ws = read_from_excel(input_file)
    except Exception as e:
        await status_msg.edit_text(f"❌ *Ошибка чтения файла:*\n`{str(e)[:200]}`", parse_mode='Markdown')
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    total = len(routes)
    
    if total == 0:
        await status_msg.edit_text(
            "❌ *В файле нет данных или неправильный формат.*\n"
            "Проверьте, что в колонке A - стартовые точки, в колонке B - цепочки адресов.",
            parse_mode='Markdown'
        )
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    # Add result columns
    start_col = add_result_columns(ws, start_col=3)
    
    # Reset cache for new user
    GEOCODE_CACHE.clear()
    
    processed = 0
    successful = 0
    geocode_errors = 0
    route_errors = 0
    
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            address_chain = route['address_chain']
            
            # Parse addresses
            addresses = parse_address_chain(address_chain)
            
            # Geocode start point
            start_coords = yandex_geocode(normalize_address(start_point))
            
            if not start_coords:
                geocode_errors += 1
                ws.cell(row=row_num, column=3).value = "❌ Ошибка геокодирования"
                ws.cell(row=row_num, column=11).value = "Не удалось определить координаты стартовой точки"
                continue
            
            # Geocode all addresses in chain
            all_coords = []
            all_coords_str = []
            failed_addresses = []
            
            for i, addr in enumerate(addresses):
                coords = yandex_geocode(addr)
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    failed_addresses.append(f"Адрес {i+1}")
                    all_coords.append(None)
            
            # Check for geocoding errors
            if failed_addresses:
                geocode_errors += 1
                ws.cell(row=row_num, column=3).value = "⚠️ Частичная ошибка геокодирования"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join([c for c in all_coords_str if c])
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=11).value = f"Не удалось геокодировать: {', '.join(failed_addresses)}"
                continue
            
            # Build full route
            full_coordinates = [start_coords] + all_coords
            
            # Calculate route
            distance = calculate_route_safely(full_coordinates)
            
            if distance:
                d2, d3 = variations(distance)
                successful += 1
                
                ws.cell(row=row_num, column=3).value = "✅ Успешно"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str)
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                ws.cell(row=row_num, column=8).value = distance
                ws.cell(row=row_num, column=9).value = d2
                ws.cell(row=row_num, column=10).value = d3
                ws.cell(row=row_num, column=11).value = ""
                
                # Format cells
                for col in [8, 9, 10]:
                    cell = ws.cell(row=row_num, column=col)
                    cell.number_format = '0.0'
                    if col == 8:
                        cell.font = Font(bold=True)
            else:
                route_errors += 1
                ws.cell(row=row_num, column=3).value = "⚠️ Ошибка расчета маршрута"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str)
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=11).value = "Не удалось построить маршрут"
            
            processed += 1
            
            # Update progress every 10 rows
            if processed % 10 == 0 or processed == total:
                progress = int((processed / total) * 100)
                await status_msg.edit_text(
                    f"⏳ *Обработка: {processed}/{total}* ({progress}%)\n"
                    f"✅ Успешно: `{successful}`\n"
                    f"⚠️ Ошибки геокодирования: `{geocode_errors}`\n"
                    f"⚠️ Ошибки маршрутов: `{route_errors}`",
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            print(f"Error processing row {route.get('row_num', 'N/A')}: {e}")
            processed += 1
    
    # Format remaining rows
    for row in range(2, ws.max_row + 1):
        for col in range(3, 12):
            cell = ws.cell(row=row, column=col)
            if cell.value:
                cell.border = Border(
                    left=Side(style='thin'),
                    right=Side(style='thin'),
                    top=Side(style='thin'),
                    bottom=Side(style='thin')
                )
    
    # Save result
    output_file = f"results_{user_id}_{timestamp}.xlsx"
    wb.save(output_file)
    
    # Send result
    try:
        with open(output_file, "rb") as file:
            await update.message.reply_document(
                document=file,
                filename=f"результаты_{user_id}_улучшенный.xlsx",
                caption=(
                    f"✅ *Обработка завершена!*\n\n"
                    f"📊 *Статистика:*\n"
                    f"• Всего строк: `{total}`\n"
                    f"• ✅ Успешно: `{successful}`\n"
                    f"• ⚠️ Ошибки геокодирования: `{geocode_errors}`\n"
                    f"• ⚠️ Ошибки маршрутов: `{route_errors}`\n"
                    f"• 🕐 Время: `{datetime.now().strftime('%H:%M:%S')}`"
                ),
                parse_mode='Markdown'
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка отправки файла: {e}")
    
    # Clean up
    try:
        if os.path.exists(input_file):
            os.remove(input_file)
        if os.path.exists(output_file):
            os.remove(output_file)
    except:
        pass

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command"""
    await start(update, context)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Status command"""
    cache_size = len(GEOCODE_CACHE)
    
    keyboard = [[InlineKeyboardButton("🔄 Обновить", callback_data="stats")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"📊 *Статус бота*\n\n"
        f"• Время: `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"• Кэш адресов: `{cache_size}`\n"
        f"• Яндекс API: {'✅' if YANDEX_API_KEY else '❌'}\n"
        f"• ORS API: {'✅' if ORS_API_KEY else '❌'}\n"
        f"• Версия: `2.0 (улучшенная)`",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

# ================== MAIN ==================
def run_flask():
    """Run Flask server"""
    port = int(os.environ.get('PORT', 10000))
    print(f"🌐 Flask server running on port {port}")
    
    try:
        from waitress import serve
        serve(app, host='0.0.0.0', port=port, threads=4)
    except ImportError:
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

async def run_bot():
    """Run Telegram bot"""
    print("=" * 50)
    print("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА (УЛУЧШЕННАЯ ВЕРСИЯ)")
    print("=" * 50)
    
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        return
    
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    try:
        await application.initialize()
        await application.start()
        
        bot_info = await application.bot.get_me()
        print(f"✅ Бот запущен: @{bot_info.username}")
        
        await application.updater.start_polling(
            drop_pending_updates=True,
            timeout=30,
            poll_interval=0.5
        )
        
        print("🤖 Бот работает и ожидает сообщений...")
        
        # Keep running
        while True:
            await asyncio.sleep(3600)
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    # Check if running on Render
    is_render = os.environ.get('RENDER') is not None
    port = os.environ.get('PORT')
    
    if is_render and port:
        print(f"🌐 Работаем на Render, порт: {port}")
        
        # Run bot in separate thread
        bot_thread = threading.Thread(
            target=lambda: asyncio.run(run_bot()),
            daemon=True
        )
        bot_thread.start()
        print("✅ Бот запущен в отдельном потоке")
        
        # Run Flask in main thread
        run_flask()
        
    else:
        print("🌐 Локальный запуск")
        asyncio.run(run_bot())

if __name__ == "__main__":
    main()
[file content end]