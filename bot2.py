import requests
import openpyxl
import random
import time
import os
import threading
import asyncio
import re
import json
from math import radians, sin, cos, sqrt, atan2
from docx import Document
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from flask import Flask
from telegram.error import Conflict
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# ================== ФЛАСК ДЛЯ RENDER ==================
app = Flask(__name__)

@app.route('/')
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Telegram Route Bot</title>
        <meta charset="utf-8">
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .container {
                background: rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 40px;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                text-align: center;
                border: 1px solid rgba(255, 255, 255, 0.2);
            }
            h1 {
                font-size: 2.5em;
                margin-bottom: 20px;
            }
            .status {
                background: rgba(255, 255, 255, 0.2);
                padding: 15px;
                border-radius: 10px;
                margin: 20px 0;
                font-family: monospace;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Telegram Route Bot</h1>
            <p>Бот для расчета маршрутов успешно запущен!</p>
            <div class="status">
                ✅ Статус: <strong>АКТИВЕН</strong><br>
                📍 Режим: Web Service<br>
                🚀 Платформа: Render
            </div>
            <p>Используйте бота в Telegram для расчета маршрутов</p>
        </div>
    </body>
    </html>
    """

@app.route('/health')
def health():
    return {"status": "ok", "service": "telegram-route-bot"}, 200

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    print(f"🌐 Flask сервер запущен на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ================== НАСТРОЙКИ БОТА ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
ORS_API_KEY = os.getenv("ORS_API_KEY", "")

# ================== ГЕОКОДИРОВАНИЕ И МАРШРУТЫ ==================
def validate_coordinates(lat, lon):
    """Проверяет, что координаты в пределах России"""
    # Границы России (примерные)
    RUSSIA_BOUNDS = {
        'lat_min': 41.0,   # Сочи
        'lat_max': 81.0,   # Земля Франца-Иосифа
        'lon_min': 19.0,   # Калининград
        'lon_max': 190.0   # Чукотка
    }
    
    # Нормализуем долготу (от -180 до 180 -> 0 до 360)
    if lon < 0:
        lon += 360
    
    # Проверяем границы
    in_bounds = (RUSSIA_BOUNDS['lat_min'] <= lat <= RUSSIA_BOUNDS['lat_max'] and 
                 RUSSIA_BOUNDS['lon_min'] <= lon <= RUSSIA_BOUNDS['lon_max'])
    
    if not in_bounds:
        print(f"⚠️ Координаты вне России: {lat:.6f}, {lon:.6f}")
    
    return in_bounds

def simplify_address(address):
    """Упрощает адрес для лучшего геокодирования"""
    if not address:
        return ""
    
    # Удаляем почтовые индексы в начале
    address = re.sub(r'^\d{6},\s*', '', address)
    
    # Стандартизируем сокращения
    replacements = {
        'р-н': 'район',
        'р.': 'республика',
        'респ.': 'республика',
        'г.': 'город',
        'с.': 'село',
        'пос.': 'поселок',
        'пгт.': 'поселок городского типа',
        'ст-ца': 'станица',
        'обл.': 'область',
        'ул.': 'улица',
        'пр-т': 'проспект',
        'пр.': 'проспект',
        'пер.': 'переулок',
        'мкр.': 'микрорайон',
        'д.': 'деревня',
        'аул.': 'аул',
        'х.': 'хутор',
        'край': '',
        'р-он': 'район',
        'м-н': 'микрорайон',
        'ш.': 'шоссе',
        'наб.': 'набережная',
        'б-р': 'бульвар',
        'пл.': 'площадь',
        'пр-д': 'проезд',
        'пр-к': 'переулок',
        'ал.': 'аллея',
        'стр.': 'строение',
        'к.': 'корпус',
        'вл.': 'владение',
        'д. ': 'дом ',
        'д,': 'дом,',
        'д.': 'дом.',
    }
    
    for old, new in replacements.items():
        address = address.replace(old, new)
    
    # Удаляем двойные пробелы
    address = re.sub(r'\s+', ' ', address)
    
    # Убираем лишние запятые
    address = re.sub(r',+', ',', address)
    
    return address.strip()

def yandex_geocode(address, retry_count=3):
    """Геокодирование адреса через Яндекс API с валидацией"""
    if not YANDEX_API_KEY:
        print("⚠️ YANDEX_API_KEY не установлен!")
        return None
    
    simplified_address = simplify_address(address)
    
    for attempt in range(retry_count):
        try:
            url = "https://geocode-maps.yandex.ru/1.x/"
            params = {
                "apikey": YANDEX_API_KEY,
                "format": "json",
                "geocode": simplified_address,
                "results": 1,
                "ll": "37.618423,55.751244",  # Центр России (Москва)
                "spn": "40,40",  # Радиус поиска
                "bbox": "19.0,41.0,190.0,81.0",  # Границы России
                "rspn": 1  # Ограничить поиск областью
            }
            
            r = requests.get(url, params=params, timeout=15)
            
            if r.status_code != 200:
                if attempt < retry_count - 1:
                    time.sleep(1)
                    continue
                print(f"⚠️ Ошибка геокодирования {r.status_code} для: {address[:50]}")
                return None
            
            data = r.json()
            members = data["response"]["GeoObjectCollection"]["featureMember"]
            
            if members and len(members) > 0:
                pos = members[0]["GeoObject"]["Point"]["pos"]
                lon_str, lat_str = pos.split()
                lat, lon = float(lat_str), float(lon_str)
                
                # Проверяем координаты
                if validate_coordinates(lat, lon):
                    print(f"✅ Геокодировано: {address[:50]} -> {lat:.6f}, {lon:.6f}")
                    return lat, lon
                else:
                    print(f"⚠️ Координаты вне России для: {address[:50]}")
                    return None
            else:
                print(f"⚠️ Адрес не найден: {address[:50]}")
                return None
                
        except Exception as e:
            if attempt < retry_count - 1:
                time.sleep(1)
                continue
            print(f"⚠️ Ошибка при геокодировании {address[:50]}: {str(e)[:100]}")
            return None
    
    return None

def parse_address_chain(address_string):
    """Парсит цепочку адресов, разделенных дефисами"""
    if not address_string:
        return []
    
    # Заменяем различные тире на обычный дефис
    address_string = address_string.replace('–', '-').replace('—', '-').replace('—', '-')
    
    # Также обрабатываем точки с запятой
    address_string = address_string.replace('; ', '-').replace(';', '-')
    
    # Разделяем по дефису и очищаем
    addresses = [addr.strip() for addr in address_string.split('-') if addr.strip()]
    
    # Удаляем пустые и слишком короткие адреса
    addresses = [addr for addr in addresses if len(addr) > 5 and not addr.replace(' ', '').isdigit()]
    
    # Объединяем возможные разорванные строки
    merged_addresses = []
    i = 0
    while i < len(addresses):
        addr = addresses[i]
        # Если адрес начинается с маленькой буквы, возможно это продолжение предыдущего
        if i > 0 and addr and addr[0].islower():
            merged_addresses[-1] = merged_addresses[-1] + " - " + addr
        else:
            merged_addresses.append(addr)
        i += 1
    
    return merged_addresses

def haversine_distance(coord1, coord2):
    """Рассчитывает расстояние по большой окружности между двумя точками"""
    R = 6371  # Радиус Земли в км
    
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

def calculate_approximate_distance(coordinates_list):
    """Рассчитывает приблизительное расстояние по прямой между точками"""
    if len(coordinates_list) < 2:
        return None
    
    total_distance = 0
    for i in range(len(coordinates_list) - 1):
        distance = haversine_distance(coordinates_list[i], coordinates_list[i+1])
        total_distance += distance
    
    # Увеличиваем на коэффициент для учета дорог (примерно 1.3-1.5)
    return round(total_distance * 1.4, 1)

def ors_route_with_waypoints(coordinates_list):
    """Строит маршрут через промежуточные точки"""
    if not ORS_API_KEY:
        print("⚠️ ORS_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    # Фильтруем некорректные координаты
    valid_coords = []
    for coord in coordinates_list:
        if coord and len(coord) == 2:
            lat, lon = coord
            if validate_coordinates(lat, lon):
                valid_coords.append(coord)
    
    if len(valid_coords) < 2:
        print(f"⚠️ Недостаточно корректных координат для маршрута: {len(valid_coords)} из {len(coordinates_list)}")
        return None
    
    # Если точек слишком много, разбиваем на части
    if len(valid_coords) > 20:
        print(f"⚠️ Слишком много точек ({len(valid_coords)}), сокращаем до 20")
        valid_coords = valid_coords[:20]
    
    url = "https://api.openrouteservice.org/v2/directions/driving-car/geojson"
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json"
    }
    
    # Преобразуем координаты в формат [lon, lat]
    coordinates = [[coord[1], coord[0]] for coord in valid_coords]
    
    body = {
        "coordinates": coordinates,
        "instructions": False,
        "geometry": False,
        "radiuses": [50000] * len(coordinates)  # Радиус поиска 50км для каждой точки
    }
    
    try:
        r = requests.post(url, json=body, headers=headers, timeout=45)
        
        if r.status_code != 200:
            print(f"⚠️ Ошибка ORS API: {r.status_code}")
            if r.status_code == 400:
                try:
                    error_data = r.json()
                    print(f"⚠️ Детали ошибки: {error_data}")
                except:
                    print(f"⚠️ Текст ошибки: {r.text[:200]}")
            return None
        
        data = r.json()
        
        if data.get("features") and len(data["features"]) > 0:
            if data["features"][0]["properties"]["summary"]:
                dist = data["features"][0]["properties"]["summary"]["distance"]
                print(f"✅ Маршрут построен, расстояние: {dist/1000:.1f} км")
                return round(dist / 1000, 1)
        
        print(f"⚠️ Нет данных о маршруте в ответе")
        return None
        
    except requests.exceptions.Timeout:
        print("⚠️ Таймаут запроса к ORS API")
        return None
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {str(e)[:100]}")
        return None

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None or base <= 0:
        return [None, None]
    
    # Для больших расстояний увеличиваем вариацию
    variation = base * 0.02  # 2% от расстояния
    min_variation = 5
    max_variation = 50
    
    variation = max(min_variation, min(variation, max_variation))
    
    d2 = round(base + random.uniform(variation/2, variation), 1)
    d3 = round(max(0, base - random.uniform(variation/2, variation)), 1)
    
    return [d2, d3]

# ================== РАБОТА С EXCEL ==================
def read_from_excel(path):
    """Чтение маршрутов из Excel файла"""
    try:
        wb = load_workbook(path, data_only=True)
        ws = wb.active
        
        # Определяем максимальную строку
        max_row = ws.max_row
        routes = []
        
        # Проверяем, есть ли заголовки
        has_header = False
        first_cell = ws.cell(row=1, column=1).value
        if first_cell and "погрузк" in str(first_cell).lower():
            has_header = True
        
        # Читаем данные
        start_row = 2 if has_header else 1
        for row in range(start_row, max_row + 1):
            start_point = ws.cell(row=row, column=1).value
            address_chain = ws.cell(row=row, column=2).value
            
            # Проверяем, что есть оба значения
            if start_point and address_chain:
                routes.append({
                    'row_num': row,
                    'start_point': str(start_point).strip(),
                    'address_chain': str(address_chain).strip(),
                    'original_start': start_point,
                    'original_chain': address_chain
                })
        
        print(f"📊 Прочитано {len(routes)} маршрутов из файла")
        return routes, wb, ws
        
    except Exception as e:
        print(f"❌ Ошибка чтения Excel файла: {e}")
        raise

def add_result_columns(ws, start_col=3):
    """Добавляет колонки для результатов в Excel"""
    headers = [
        "Статус обработки",
        "Координаты старта",
        "Координаты точек",
        "Количество точек",
        "Тип маршрута",
        "Расстояние 1 (км)",
        "Расстояние 2 (км)",
        "Расстояние 3 (км)"
    ]
    
    # Стиль для заголовков
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Добавляем заголовки
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=start_col + i)
        cell.value = header
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border
    
    # Автоматически настраиваем ширину колонок
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    return start_col + len(headers)

def apply_cell_styles(ws, row, col, value, is_error=False):
    """Применяет стили к ячейке"""
    cell = ws.cell(row=row, column=col)
    cell.value = value
    
    if is_error:
        cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        cell.font = Font(color="9C0006")
    elif "✅" in str(value):
        cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        cell.font = Font(color="006100", bold=True)
    elif "⚠️" in str(value):
        cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
        cell.font = Font(color="9C5700")
    
    cell.alignment = Alignment(vertical="center", wrap_text=True)
    cell.border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    return cell

# ================== TELEGRAM БОТ ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет!\n\n"
        "📌 Я бот для расчета маршрутов с поддержкой промежуточных точек.\n\n"
        "📁 Отправьте мне Excel файл в формате:\n"
        "• Колонка A: Стартовая точка (точка А)\n"
        "• Колонка B: Цепочка адресов через дефис\n\n"
        "📊 Пример строки в колонке B:\n"
        "`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`\n\n"
        "✅ Я верну тот же файл с добавленными колонками результатов!"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
📋 **Доступные команды:**

/start - Начать работу с ботом
/help - Показать эту справку
/stats - Статистика обработки файла

📁 **Формат Excel файла:**
• Колонка A: Стартовая точка (точка А)
• Колонка B: Цепочка адресов через дефис

📍 **Пример строки в колонке B:**
`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`

📊 **Добавляемые колонки результатов:**
1. Статус обработки
2. Координаты старта
3. Координаты точек
4. Количество точек
5. Тип маршрута
6. Расстояние 1 (км)
7. Расстояние 2 (км)
8. Расстояние 3 (км)

**Типы маршрутов:**
• Прямой - один адрес в цепочке
• С промежуточными точками - несколько адресов через дефис

⚠️ **Важно:**
• Используйте дефис `-` как разделитель
• Убедитесь, что адреса написаны корректно
• Файл должен быть в формате XLSX
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def example_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /example - отправляет пример файла"""
    await update.message.reply_text(
        "📋 Пример Excel файла:\n\n"
        "| Колонка A | Колонка B |\n"
        "|-----------|-----------|\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж, ул. Ипподромная 18А |\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89 |\n"
        "| Ростов-на-Дону, Оганова 22 | р. Карелия, г. Петрозаводск, ул. Вольная 4 - г. Беломорск, ул. Мерецкова 6 |\n\n"
        "Просто создайте Excel файл с тамими данными и отправьте боту!"
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загруженных документов"""
    if not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте файл")
        return
    
    file_name = update.message.document.file_name.lower()
    allowed_extensions = ['.xlsx', '.xls']
    
    if not any(file_name.endswith(ext) for ext in allowed_extensions):
        await update.message.reply_text(
            "❌ Пожалуйста, отправьте файл в формате Excel (XLSX/XLS)"
        )
        return
    
    file = await update.message.document.get_file()
    user_id = update.message.from_user.id
    
    # Создаем уникальное имя файла
    timestamp = int(time.time())
    input_file = f"input_{user_id}_{timestamp}.xlsx"
    
    await file.download_to_drive(input_file)
    
    try:
        # Читаем данные из Excel
        routes, wb, ws = read_from_excel(input_file)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка чтения файла: {e}")
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    total = len(routes)
    
    if total == 0:
        await update.message.reply_text(
            "❌ В файле нет данных или неправильный формат.\n"
            "Проверьте, что в колонке A - стартовые точки, в колонке B - цепочки адресов."
        )
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    progress_msg = await update.message.reply_text(
        f"⏳ Начинаю обработку\n📊 Всего строк: {total}\n🔄 Подготовка..."
    )
    
    # Добавляем колонки для результатов
    start_col = add_result_columns(ws, start_col=3)
    
    # Кэш для геокодированных адресов
    geocode_cache = {}
    
    processed = 0
    successful = 0
    geocode_errors = 0
    route_errors = 0
    approximate = 0
    
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            address_chain = route['address_chain']
            
            # Парсим цепочку адресов
            addresses = parse_address_chain(address_chain)
            
            # Геокодируем стартовую точку
            if start_point in geocode_cache:
                start_coords = geocode_cache[start_point]
            else:
                start_coords = await asyncio.to_thread(yandex_geocode, start_point)
                if start_coords:
                    geocode_cache[start_point] = start_coords
                await asyncio.sleep(0.3)  # Задержка между запросами
            
            # Геокодируем все адреса в цепочке
            all_coords = []
            all_coords_str = []
            has_geocode_errors = False
            
            for addr in addresses:
                if addr in geocode_cache:
                    coords = geocode_cache[addr]
                else:
                    coords = await asyncio.to_thread(yandex_geocode, addr)
                    if coords:
                        geocode_cache[addr] = coords
                    await asyncio.sleep(0.3)
                
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    has_geocode_errors = True
                    print(f"⚠️ Не удалось геокодировать: {addr[:50]}")
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
            
            # Записываем базовую информацию
            apply_cell_styles(ws, row_num, 6, len(addresses))
            apply_cell_styles(ws, row_num, 7, route_type)
            
            # Записываем координаты
            if start_coords:
                apply_cell_styles(ws, row_num, 4, f"{start_coords[0]:.6f},{start_coords[1]:.6f}")
            else:
                apply_cell_styles(ws, row_num, 4, "Ошибка", is_error=True)
            
            apply_cell_styles(ws, row_num, 5, "; ".join(all_coords_str) if all_coords_str else "Ошибка")
            
            if has_geocode_errors or not start_coords or not all_coords:
                # Ошибка геокодирования
                apply_cell_styles(ws, row_num, 3, "❌ Ошибка геокодирования", is_error=True)
                apply_cell_styles(ws, row_num, 8, "Ошибка", is_error=True)
                geocode_errors += 1
                processed += 1
                continue
            
            # Строим маршрут
            full_coordinates = [start_coords] + all_coords
            
            # Рассчитываем маршрут через ORS
            distance = await asyncio.to_thread(ors_route_with_waypoints, full_coordinates)
            
            if distance:
                # Успешный расчет через ORS
                d2, d3 = variations(distance)
                
                apply_cell_styles(ws, row_num, 3, "✅ Успешно")
                apply_cell_styles(ws, row_num, 8, distance)
                apply_cell_styles(ws, row_num, 9, d2)
                apply_cell_styles(ws, row_num, 10, d3)
                
                successful += 1
            else:
                # Пробуем приблизительный расчет
                approx_distance = calculate_approximate_distance(full_coordinates)
                
                if approx_distance and approx_distance > 0:
                    d2, d3 = variations(approx_distance)
                    
                    apply_cell_styles(ws, row_num, 3, "⚠️ Приблизительный расчет")
                    apply_cell_styles(ws, row_num, 8, approx_distance)
                    apply_cell_styles(ws, row_num, 9, d2)
                    apply_cell_styles(ws, row_num, 10, d3)
                    
                    approximate += 1
                else:
                    # Полная ошибка
                    apply_cell_styles(ws, row_num, 3, "⚠️ Ошибка расчета маршрута", is_error=True)
                    apply_cell_styles(ws, row_num, 8, "Ошибка", is_error=True)
                    route_errors += 1
            
            processed += 1
            
            # Обновляем прогресс каждые 3 строки
            if processed % 3 == 0 or processed == total:
                try:
                    progress_text = (
                        f"⏳ Обработка: {processed} / {total}\n"
                        f"✅ Успешно: {successful}\n"
                        f"⚠️ Приблизительно: {approximate}\n"
                        f"❌ Ошибки: {geocode_errors + route_errors}\n"
                        f"📍 Текущий: {start_point[:30]}..."
                    )
                    await progress_msg.edit_text(progress_text)
                except Exception as e:
                    print(f"Ошибка обновления прогресса: {e}")
            
        except Exception as e:
            print(f"❌ Критическая ошибка в строке {route.get('row_num', 'N/A')}: {e}")
            apply_cell_styles(ws, row_num, 3, f"❌ Ошибка: {str(e)[:50]}", is_error=True)
            processed += 1
            route_errors += 1
    
    try:
        final_stats = (
            f"✅ Обработка завершена!\n\n"
            f"📊 Статистика:\n"
            f"• Всего строк: {total}\n"
            f"• Успешно: {successful}\n"
            f"• Приблизительно: {approximate}\n"
            f"• Ошибок геокодирования: {geocode_errors}\n"
            f"• Ошибок маршрута: {route_errors}\n"
            f"• Обработано: {processed}"
        )
        await progress_msg.edit_text(final_stats)
    except:
        pass
    
    # Сохраняем результат
    output_file = f"results_{user_id}_{timestamp}.xlsx"
    
    # Настраиваем ширину колонок
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            if cell.value:
                cell_length = len(str(cell.value))
                if cell_length > max_length:
                    max_length = cell_length
        adjusted_width = min(max_length + 2, 40)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    wb.save(output_file)
    
    # Отправляем результат
    try:
        with open(output_file, "rb") as file:
            await update.message.reply_document(
                document=file,
                filename=f"результаты_{timestamp}.xlsx",
                caption=(
                    f"📊 Результаты обработки\n"
                    f"✅ Успешно: {successful}\n"
                    f"⚠️ Приблизительно: {approximate}\n"
                    f"❌ Ошибок: {geocode_errors + route_errors}"
                )
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка отправки файла: {e}")
    
    # Удаляем временные файлы
    try:
        if os.path.exists(input_file):
            os.remove(input_file)
        if os.path.exists(output_file):
            os.remove(output_file)
    except Exception as e:
        print(f"Ошибка удаления временных файлов: {e}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику API"""
    stats_text = (
        f"📊 Статистика сервисов:\n\n"
        f"• Яндекс API: {'✅ Доступен' if YANDEX_API_KEY else '❌ Не настроен'}\n"
        f"• ORS API: {'✅ Доступен' if ORS_API_KEY else '❌ Не настроен'}\n\n"
        f"Для работы бота необходимы оба API ключа.\n"
        f"Установите их через переменные окружения:\n"
        f"• YANDEX_API_KEY\n"
        f"• ORS_API_KEY"
    )
    await update.message.reply_text(stats_text)

# ================== ЗАПУСК С ЗАЩИТОЙ ОТ КОНФЛИКТОВ ==================
async def run_bot():
    """Запускает бота с обработкой конфликтов"""
    print("=" * 50)
    print("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА")
    print("=" * 50)
    
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("Установите переменную окружения BOT_TOKEN в Render")
        return
    
    print(f"✅ Токен получен")
    print(f"✅ Яндекс API: {'✅ Доступен' if YANDEX_API_KEY else '❌ Не настроен'}")
    print(f"✅ ORS API: {'✅ Доступен' if ORS_API_KEY else '❌ Не настроен'}")
    
    if not YANDEX_API_KEY or not ORS_API_KEY:
        print("⚠️ ВНИМАНИЕ: Не все API ключи настроены. Бот может работать некорректно.")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("example", example_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Настройки
    application.drop_pending_updates = True
    
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Попытка {attempt + 1}/{max_retries} запустить бота...")
            await application.initialize()
            await application.start()
            
            # Получаем информацию о боте
            bot_info = await application.bot.get_me()
            print(f"✅ Бот запущен: @{bot_info.username}")
            
            # Запускаем polling
            await application.updater.start_polling(
                poll_interval=0.5,
                timeout=30,
                bootstrap_retries=3
            )
            
            print("🤖 Бот работает и ожидает сообщений...")
            print("-" * 50)
            
            # Бесконечный цикл
            while True:
                await asyncio.sleep(3600)
            
        except Conflict as e:
            print(f"⚠️ Конфликт: {e}")
            print(f"⏳ Жду {retry_delay} секунд перед повторной попыткой...")
            
            try:
                if application.running:
                    await application.stop()
                    await application.shutdown()
            except:
                pass
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            else:
                print("❌ Достигнут лимит попыток. Бот не может запуститься.")
                print("ℹ️ Проверьте, что нет других запущенных экземпляров бота.")
                break
                
        except Exception as e:
            print(f"❌ Ошибка запуска: {e}")
            print("Проверьте токен бота и доступ к интернету.")
            break

def main():
    # Проверяем, работаем ли на Render
    is_render = os.environ.get('RENDER') is not None
    port = os.environ.get('PORT')
    
    if is_render and port:
        print(f"🌐 Работаем на Render, порт: {port}")
        # Запускаем Flask в отдельном потоке
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        print("✅ Flask сервер запущен в отдельном потоке")
    
    # Запускаем бота
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()