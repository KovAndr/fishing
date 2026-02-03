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

# Кэш для геокодирования
GEOCODE_CACHE = {}
# Максимальное количество точек в маршруте для ORS
MAX_WAYPOINTS = 25

# ================== УТИЛИТЫ ==================
def normalize_address(address):
    """Нормализация адреса"""
    if not address:
        return ""
    
    # Удаляем лишние пробелы
    address = re.sub(r'\s+', ' ', address.strip())
    
    # Стандартизируем обозначения
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
    
    # Добавляем Россию, если не указано
    if not any(word in address.lower() for word in ['россия', 'russia', 'рф']):
        # Проверяем, не является ли адрес зарубежным
        if not any(word in address.lower() for word in ['украина', 'беларусь', 'казахстан']):
            address = f'Россия, {address}'
    
    return address

def parse_address_chain(address_string):
    """Парсит цепочку адресов с улучшенной логикой"""
    if not address_string:
        return []
    
    # Нормализуем строку
    address_string = str(address_string).strip()
    
    # Заменяем различные разделители
    address_string = re.sub(r'[–—]', '-', address_string)
    
    # Обрабатываем сложные случаи с дефисами в названиях
    # Разделяем по дефису, который стоит после пробела или в начале строки
    parts = []
    current_part = ""
    
    # Простой алгоритм: делим по дефисам, но объединяем части, которые выглядят как продолжение адреса
    temp_parts = address_string.split('-')
    
    for i, part in enumerate(temp_parts):
        part = part.strip()
        if not part:
            continue
            
        # Если часть начинается с маленькой буквы или это номер дома, присоединяем к предыдущей
        if i > 0 and (part[0].islower() or re.match(r'^\d+[а-яА-Я]?$', part)):
            parts[-1] = f"{parts[-1]}-{part}"
        else:
            parts.append(part)
    
    # Фильтруем и нормализуем
    addresses = [normalize_address(addr) for addr in parts if addr]
    
    # Удаляем дубликаты
    unique_addresses = []
    seen = set()
    for addr in addresses:
        if addr not in seen:
            unique_addresses.append(addr)
            seen.add(addr)
    
    return unique_addresses

# ================== ЛОГИКА БОТА ==================
def read_from_excel(path):
    """Чтение маршрутов из Excel файла"""
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    routes = []
    
    # Определяем максимальную строку
    max_row = ws.max_row
    
    # Читаем данные, пропуская заголовки если они есть
    start_row = 1
    if ws.cell(row=1, column=1).value and isinstance(ws.cell(row=1, column=1).value, str):
        # Проверяем, является ли первая строка заголовком
        header1 = str(ws.cell(row=1, column=1).value).lower()
        if any(word in header1 for word in ['пункт', 'адрес', 'грузоотправитель']):
            start_row = 2
    
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
    
    return routes, wb, ws

def yandex_geocode(address, max_retries=3):
    """Улучшенное геокодирование с проверкой координат"""
    if not YANDEX_API_KEY:
        print("⚠️ YANDEX_API_KEY не установлен!")
        return None
    
    # Проверяем кэш
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
            
            r = requests.get(url, params=params, timeout=20)
            
            if r.status_code != 200:
                print(f"⚠️ Ошибка геокодирования {address}: {r.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None
            
            data = r.json()
            
            if (data["response"]["GeoObjectCollection"]["featureMember"] and 
                len(data["response"]["GeoObjectCollection"]["featureMember"]) > 0):
                
                feature = data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]
                pos = feature["Point"]["pos"]
                lon, lat = pos.split()
                coords = (float(lat), float(lon))
                
                # Проверяем, что координаты в разумных пределах для России
                if is_valid_russian_coords(coords):
                    GEOCODE_CACHE[cache_key] = coords
                    return coords
                else:
                    print(f"⚠️ Координаты вне России для адреса: {address}")
                    # Пробуем альтернативный вариант
                    alternative_address = try_alternative_address(address)
                    if alternative_address and alternative_address != address:
                        return yandex_geocode(alternative_address, max_retries=1)
                    return None
            else:
                print(f"⚠️ Адрес не найден: {address}")
                # Пробуем альтернативный вариант
                alternative_address = try_alternative_address(address)
                if alternative_address and alternative_address != address:
                    return yandex_geocode(alternative_address, max_retries=1)
                return None
                
        except Exception as e:
            print(f"⚠️ Ошибка при геокодировании {address}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
    
    return None

def is_valid_russian_coords(coords):
    """Проверка, что координаты находятся в пределах России"""
    if not coords:
        return False
    
    lat, lon = coords
    
    # Примерные границы России (включая Крым)
    min_lat, max_lat = 41.0, 82.0  # Широта
    min_lon, max_lon = 19.0, 190.0  # Долгота (включая Чукотку)
    
    # Проверяем основные границы
    if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
        return False
    
    # Дополнительные проверки для исключения очевидных ошибок
    # Координаты в Швейцарии и т.п.
    suspicious_coords = [
        (47.427551, 9.377873),  # Швейцария
        (31.474271, 74.402927),  # Пакистан
        (-12.057917, -77.106686),  # Перу
        (4.612851, -74.096036),  # Колумбия
    ]
    
    for sus_lat, sus_lon in suspicious_coords:
        if abs(lat - sus_lat) < 0.1 and abs(lon - sus_lon) < 0.1:
            return False
    
    return True

def try_alternative_address(address):
    """Попытка исправить адрес"""
    # Удаляем лишние части
    address = address.strip()
    
    # Удаляем индекс в начале
    address = re.sub(r'^\d{6},\s*', '', address)
    
    # Исправляем опечатки в названиях регионов
    corrections = {
        'Кверля': 'Карелия',
        'Бедгородская': 'Белгородская',
        'Нижегородкская': 'Нижегородская',
        'Крамский': 'Краснодарский',
        'Московкская': 'Московская',
        'Вологдаская': 'Вологодская',
        'Тамбовска': 'Тамбовская',
        'Воронежска': 'Воронежская',
    }
    
    for wrong, correct in corrections.items():
        address = re.sub(rf'\b{wrong}\b', correct, address, flags=re.IGNORECASE)
    
    # Добавляем "Россия" если нет
    if 'россия' not in address.lower():
        address = f'Россия, {address}'
    
    return address

def ors_route_with_waypoints(coordinates_list, max_points_per_request=25):
    """Построение маршрута с ограничением на количество точек"""
    if not ORS_API_KEY:
        print("⚠️ ORS_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    url = "https://api.openrouteservice.org/v2/directions/driving-car/geojson"
    headers = {"Authorization": ORS_API_KEY}
    
    # Если точек слишком много, разбиваем на части
    if len(coordinates_list) > max_points_per_request:
        print(f"⚠️ Слишком много точек ({len(coordinates_list)}), разбиваю на части...")
        
        total_distance = 0
        for i in range(0, len(coordinates_list) - 1):
            segment_coords = [coordinates_list[i], coordinates_list[i + 1]]
            segment_dist = ors_route_with_waypoints(segment_coords)
            time.sleep(0.5)  # Задержка между запросами
            
            if segment_dist:
                total_distance += segment_dist
            else:
                return None
        
        return round(total_distance, 1)
    
    # Преобразуем координаты в формат [lon, lat]
    coordinates = [[coord[1], coord[0]] for coord in coordinates_list]
    
    body = {"coordinates": coordinates}
    
    try:
        r = requests.post(url, json=body, headers=headers, timeout=45)
        
        if r.status_code != 200:
            print(f"⚠️ Ошибка маршрута: {r.status_code}")
            print(f"Ответ: {r.text[:500]}")
            return None
        
        data = r.json()
        
        if data["features"] and data["features"][0]["properties"]["summary"]:
            dist = data["features"][0]["properties"]["summary"]["distance"]
            return round(dist / 1000, 1)
        else:
            print(f"⚠️ Нет данных о маршруте в ответе")
            return None
            
    except requests.exceptions.Timeout:
        print(f"⚠️ Таймаут при построении маршрута")
        return None
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {e}")
        return None

def calculate_route_safely(coordinates):
    """Безопасный расчет маршрута с обработкой ошибок"""
    try:
        # Проверяем координаты
        valid_coords = []
        for coord in coordinates:
            if coord and is_valid_russian_coords(coord):
                valid_coords.append(coord)
            else:
                print(f"⚠️ Пропускаю невалидные координаты: {coord}")
        
        if len(valid_coords) < 2:
            print(f"⚠️ Недостаточно валидных координат: {len(valid_coords)}")
            return None
        
        # Рассчитываем маршрут
        distance = ors_route_with_waypoints(valid_coords)
        return distance
        
    except Exception as e:
        print(f"⚠️ Ошибка при безопасном расчете маршрута: {e}")
        return None

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None or base <= 0:
        return [None, None]
    
    try:
        # Более реалистичные вариации
        variation_percent = random.uniform(1.02, 1.08)  # 2-8% вариация
        
        d2 = round(base * variation_percent, 1)
        d3 = round(base * (2 - variation_percent), 1)  # Симметричная вариация вниз
        
        # Гарантируем, что расстояния не отрицательные
        d3 = max(0, d3)
        
        return [d2, d3]
    except:
        return [None, None]

def add_result_columns(ws, start_col=3):
    """Добавляет колонки для результатов в Excel с улучшенным форматированием"""
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
    
    # Стили
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    thin_border = Border(
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
        cell.border = thin_border
    
    # Настраиваем ширину колонок
    column_widths = {
        start_col: 20,    # Статус
        start_col + 1: 25, # Коорд. старта
        start_col + 2: 40, # Коорд. точек
        start_col + 3: 12, # Кол-во
        start_col + 4: 20, # Тип
        start_col + 5: 15, # Расст. 1
        start_col + 6: 15, # Расст. 2
        start_col + 7: 15, # Расст. 3
        start_col + 8: 30, # Примечания
    }
    
    for col, width in column_widths.items():
        ws.column_dimensions[openpyxl.utils.get_column_letter(col)].width = width
    
    return start_col + len(headers)

# ================== TELEGRAM БОТ ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет!\n\n"
        "📌 Я бот для расчета маршрутов с поддержкой промежуточных точек.\n\n"
        "📁 Отправьте мне Excel файл в формате:\n"
        "• Колонка A: Стартовая точка\n"
        "• Колонка B: Цепочка адресов через дефис\n\n"
        "📊 Пример строки в колонке B:\n"
        "`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`\n\n"
        "✅ Я верну тот же файл с добавленными колонками результатов!\n\n"
        "⚙️ Улучшенная версия: исправлены ошибки геокодирования и расчета маршрутов."
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
        f"⏳ Начинаю обработку\nВсего строк: {total}\n"
        f"📊 Версия: улучшенная с исправлением ошибок\n"
        f"⏱️ Начало: {datetime.now().strftime('%H:%M:%S')}"
    )
    
    # Добавляем колонки для результатов
    start_col = add_result_columns(ws, start_col=3)
    
    # Сбрасываем кэш для нового пользователя
    GEOCODE_CACHE.clear()
    
    processed = 0
    successful = 0
    geocode_errors = 0
    route_errors = 0
    
    # Статистика
    stats = {
        'total': total,
        'successful': 0,
        'geocode_errors': 0,
        'route_errors': 0,
        'processing_times': []
    }
    
    for route in routes:
        start_time = time.time()
        
        try:
            row_num = route['row_num']
            original_start = route['start_point']
            original_chain = route['address_chain']
            
            # Нормализуем адреса
            normalized_start = normalize_address(original_start)
            addresses = parse_address_chain(original_chain)
            
            # Геокодируем стартовую точку
            start_coords = yandex_geocode(normalized_start)
            if not start_coords:
                geocode_errors += 1
                stats['geocode_errors'] += 1
                
                # Записываем ошибку
                ws.cell(row=row_num, column=3).value = "❌ Ошибка геокодирования старта"
                ws.cell(row=row_num, column=4).value = "Не найден"
                ws.cell(row=row_num, column=5).value = ""
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=9).value = ""
                ws.cell(row=row_num, column=10).value = ""
                ws.cell(row=row_num, column=11).value = "Не удалось определить координаты стартовой точки"
                
                continue
            
            # Геокодируем все адреса в цепочке
            all_coords = []
            all_coords_str = []
            failed_addresses = []
            
            for i, addr in enumerate(addresses):
                normalized_addr = normalize_address(addr)
                coords = yandex_geocode(normalized_addr)
                
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    failed_addresses.append(f"Адрес {i+1}: {addr[:50]}...")
                    all_coords.append(None)  # Помечаем как невалидный
            
            # Если есть ошибки геокодирования
            if failed_addresses:
                geocode_errors += 1
                stats['geocode_errors'] += 1
                
                notes = "; ".join(failed_addresses)
                ws.cell(row=row_num, column=3).value = "⚠️ Частичная ошибка геокодирования"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join([c for c in all_coords_str if c])
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=9).value = ""
                ws.cell(row=row_num, column=10).value = ""
                ws.cell(row=row_num, column=11).value = f"Не удалось геокодировать: {notes}"
                
                continue
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
            
            # Строим маршрут: стартовая точка + все точки из цепочки
            full_coordinates = [start_coords] + all_coords
            
            # Рассчитываем маршрут с обработкой ошибок
            distance = calculate_route_safely(full_coordinates)
            
            if distance:
                d2, d3 = variations(distance)
                successful += 1
                stats['successful'] += 1
                
                # Записываем успешный результат
                ws.cell(row=row_num, column=3).value = "✅ Успешно"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str)
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = route_type
                ws.cell(row=row_num, column=8).value = distance
                ws.cell(row=row_num, column=9).value = d2
                ws.cell(row=row_num, column=10).value = d3
                ws.cell(row=row_num, column=11).value = ""
                
                # Форматируем ячейки с расстояниями
                for col in [8, 9, 10]:
                    cell = ws.cell(row=row_num, column=col)
                    cell.number_format = '0.0'
                    if col == 8:  # Основное расстояние
                        cell.font = Font(bold=True)
            else:
                route_errors += 1
                stats['route_errors'] += 1
                
                # Записываем ошибку расчета маршрута
                ws.cell(row=row_num, column=3).value = "⚠️ Ошибка расчета маршрута"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str)
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = route_type
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=9).value = ""
                ws.cell(row=row_num, column=10).value = ""
                ws.cell(row=row_num, column=11).value = "Не удалось построить маршрут между точками"
            
            processed += 1
            
            # Обновляем прогресс каждые 5 строк или в конце
            if processed % 5 == 0 or processed == total:
                try:
                    elapsed = time.time() - start_time
                    stats['processing_times'].append(elapsed)
                    avg_time = sum(stats['processing_times']) / len(stats['processing_times'])
                    
                    await progress_msg.edit_text(
                        f"⏳ Обработка: {processed} / {total}\n"
                        f"✅ Успешно: {successful}\n"
                        f"⚠️ Ошибки геокодирования: {geocode_errors}\n"
                        f"⚠️ Ошибки маршрутов: {route_errors}\n"
                        f"⏱️ Среднее время: {avg_time:.1f}с\n"
                        f"📍 Текущий: {original_start[:30]}..."
                    )
                except:
                    pass
                
        except Exception as e:
            print(f"❌ Критическая ошибка обработки строки {route.get('row_num', 'N/A')}: {e}")
            processed += 1
    
    # Форматируем оставшиеся строки
    for row in range(2, ws.max_row + 1):
        for col in range(3, 12):  # Колонки с результатами
            cell = ws.cell(row=row, column=col)
            if cell.value:
                cell.border = Border(
                    left=Side(style='thin'),
                    right=Side(style='thin'),
                    top=Side(style='thin'),
                    bottom=Side(style='thin')
                )
    
    try:
        total_time = sum(stats['processing_times'])
        await progress_msg.edit_text(
            f"✅ Обработка завершена!\n"
            f"📊 Статистика:\n"
            f"• Всего строк: {total}\n"
            f"• Успешно: {successful}\n"
            f"• Ошибки геокодирования: {geocode_errors}\n"
            f"• Ошибки маршрутов: {route_errors}\n"
            f"• Общее время: {total_time:.1f}с\n"
            f"📄 Формирую отчет..."
        )
    except:
        pass
    
    # Сохраняем результат
    output_file = f"results_{user_id}_{timestamp}.xlsx"
    wb.save(output_file)
    
    # Отправляем результат
    try:
        with open(output_file, "rb") as file:
            await update.message.reply_document(
                document=file,
                filename=f"результаты_{user_id}_улучшенный.xlsx",
                caption=(
                    f"✅ Готово! Обработка завершена.\n"
                    f"📊 Статистика:\n"
                    f"• Всего строк: {total}\n"
                    f"• ✅ Успешно: {successful}\n"
                    f"• ⚠️ Ошибки геокодирования: {geocode_errors}\n"
                    f"• ⚠️ Ошибки маршрутов: {route_errors}\n"
                    f"• 🕐 Время: {datetime.now().strftime('%H:%M:%S')}"
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

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
📋 **Улучшенный бот для расчета маршрутов**

**Что исправлено:**
✅ Исправлено геокодирование адресов
✅ Исправлены ошибки расчета маршрутов
✅ Добавлена проверка координат
✅ Улучшена обработка ошибок

**Доступные команды:**
/start - Начать работу с ботом
/help - Показать эту справку
/status - Проверить статус бота

**📁 Формат Excel файла:**
• Колонка A: Стартовая точка (точка А)
• Колонка B: Цепочка адресов через дефис

**📍 Пример строки в колонке B:**
`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`

**📊 Добавляемые колонки результатов:**
1. Статус обработки
2. Координаты старта
3. Координаты точек
4. Кол-во точек
5. Тип маршрута
6. Расстояние 1 (км)
7. Расстояние 2 (км)
8. Расстояние 3 (км)
9. Примечания

**🚀 Особенности:**
• Автоматическое исправление опечаток в адресах
• Проверка координат на принадлежность к России
• Обработка маршрутов с большим количеством точек
• Подробная статистика обработки
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status"""
    status_text = f"""
🤖 **Статус бота**

**Версия:** Улучшенная с исправлением ошибок
**Дата:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}

**API статус:**
• Яндекс.Карты: {'✅ Доступен' if YANDEX_API_KEY else '❌ Не настроен'}
• OpenRouteService: {'✅ Доступен' if ORS_API_KEY else '❌ Не настроен'}

**Статистика кэша:**
• Геокодированных адресов: {len(GEOCODE_CACHE)}

**📊 Последняя обработка:**
• Очистите кэш командой /clearcache при необходимости
"""
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def clearcache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистка кэша геокодирования"""
    global GEOCODE_CACHE
    old_size = len(GEOCODE_CACHE)
    GEOCODE_CACHE.clear()
    
    await update.message.reply_text(
        f"✅ Кэш очищен\n"
        f"🗑️ Удалено записей: {old_size}"
    )

# ================== ЗАПУСК С ЗАЩИТОЙ ОТ КОНФЛИКТОВ ==================
async def run_bot():
    """Запускает бота с обработкой конфликтов"""
    print("=" * 50)
    print("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА (УЛУЧШЕННАЯ ВЕРСИЯ)")
    print("=" * 50)
    
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("Установите переменную окружения BOT_TOKEN в Render")
        return
    
    print(f"✅ Токен получен")
    print(f"✅ Яндекс API: {'установлен' if YANDEX_API_KEY else 'не установлен'}")
    print(f"✅ ORS API: {'установлен' if ORS_API_KEY else 'не установлен'}")
    print(f"✅ Макс. точек в маршруте: {MAX_WAYPOINTS}")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("clearcache", clearcache_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Пытаемся запустить бота с обработкой конфликтов
    max_retries = 5
    retry_delay = 10  # секунд
    
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
                drop_pending_updates=True,
                timeout=30,
                poll_interval=0.5
            )
            
            print("🤖 Бот работает и ожидает сообщений...")
            
            # Бесконечный цикл (пока не будет остановлен)
            while True:
                await asyncio.sleep(3600)  # Спим час
            
        except Conflict as e:
            print(f"⚠️ Конфликт: {e}")
            print(f"⏳ Жду {retry_delay} секунд перед повторной попыткой...")
            
            # Останавливаем бота если он запущен
            try:
                await application.stop()
                await application.shutdown()
            except:
                pass
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Экспоненциальная задержка
            else:
                print("❌ Достигнут лимит попыток. Бот не может запуститься.")
                print("ℹ️ Проверьте, что нет других запущенных экземпляров бота.")
                break
                
        except Exception as e:
            print(f"❌ Ошибка: {e}")
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
    asyncio.run(run_bot())

if __name__ == "__main__":
    main()