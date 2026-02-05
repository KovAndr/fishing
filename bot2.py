import requests
import openpyxl
import random
import time
import os
import threading
import asyncio
from math import radians, cos, sin, sqrt, atan2
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
from openpyxl.styles import Font, PatternFill, Alignment
from urllib.parse import quote

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
GRAPHHOPPER_API_KEY = "2c8e643a-360f-47ab-855d-7e884ce217ad"

# ================== ЛОГИКА БОТА ==================

# Границы Крыма
CRIMEA_BOUNDS = {
    'min_lat': 44.0,
    'max_lat': 46.5,
    'min_lon': 32.0,
    'max_lon': 37.0
}

def is_in_crimea(lat, lon):
    """Проверяет, находится ли точка в Крыму"""
    return (CRIMEA_BOUNDS['min_lat'] <= lat <= CRIMEA_BOUNDS['max_lat'] and
            CRIMEA_BOUNDS['min_lon'] <= lon <= CRIMEA_BOUNDS['max_lon'])

def haversine_distance(lat1, lon1, lat2, lon2):
    """Рассчитывает расстояние между двумя точками по прямой (в км)"""
    R = 6371.0
    
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = sin(dlat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

def read_from_excel(path):
    """Чтение маршрутов из Excel файла"""
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    routes = []
    
    max_row = ws.max_row
    
    for row in range(1, max_row + 1):
        start_point = ws.cell(row=row, column=1).value
        address_chain = ws.cell(row=row, column=2).value
        
        if start_point and address_chain:
            routes.append({
                'row_num': row,
                'start_point': str(start_point).strip(),
                'address_chain': str(address_chain).strip(),
            })
    
    return routes, wb, ws

def parse_address_chain(address_string):
    """Парсит цепочку адресов с разными разделителями"""
    if not address_string:
        return []
    
    # Нормализуем разделители - заменяем все типы дефисов и тире на стандартный разделитель |
    import re
    
    # Убираем лишние пробелы
    address_string = re.sub(r'\s+', ' ', address_string.strip())
    
    # Заменяем разные варианты разделителей на стандартный |
    separators = [' - ', ' – ', ' — ', ' -', '- ', ';', ',']
    normalized = address_string
    for sep in separators:
        normalized = normalized.replace(sep, '|')
    
    # Также обрабатываем случаи, где дефис без пробелов, но между словами
    # Разделяем по | и фильтруем пустые строки
    addresses = [addr.strip() for addr in normalized.split('|') if addr.strip()]
    
    # Убираем дубликаты, сохраняя порядок
    seen = set()
    unique_addresses = []
    for addr in addresses:
        if addr not in seen:
            seen.add(addr)
            unique_addresses.append(addr)
    
    return unique_addresses

def yandex_geocode(address):
    """Геокодирование адреса через Яндекс API"""
    if not YANDEX_API_KEY:
        print("⚠️ YANDEX_API_KEY не установлен!")
        return None
    
    # Кодируем адрес для URL
    encoded_address = quote(address)
    
    url = "https://geocode-maps.yandex.ru/1.x/"
    params = {
        "apikey": YANDEX_API_KEY,
        "format": "json",
        "geocode": encoded_address,
        "results": 1,
        "lang": "ru_RU"
    }
    
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            print(f"⚠️ Ошибка геокодирования {r.status_code} для: {address[:50]}...")
            return None
        
        data = r.json()
        if (data["response"]["GeoObjectCollection"]["featureMember"] and 
            len(data["response"]["GeoObjectCollection"]["featureMember"]) > 0):
            pos = data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["Point"]["pos"]
            lon, lat = pos.split()
            return float(lat), float(lon)
        else:
            print(f"⚠️ Адрес не найден: {address[:50]}...")
            return None
    except Exception as e:
        print(f"⚠️ Ошибка при геокодировании {address[:50]}: {e}")
        return None

def graphhopper_route(start_coord, end_coord):
    """Рассчитывает маршрут между двумя точками через GraphHopper"""
    if not GRAPHHOPPER_API_KEY:
        print("⚠️ GRAPHHOPPER_API_KEY не установлен!")
        return None
    
    # Проверяем координаты
    if not start_coord or not end_coord:
        print("⚠️ Пустые координаты для GraphHopper")
        return None
    
    try:
        start_lat, start_lon = start_coord
        end_lat, end_lon = end_coord
        
        url = "https://graphhopper.com/api/1/route"
        params = {
            "point": [f"{start_lat},{start_lon}", f"{end_lat},{end_lon}"],
            "vehicle": "car",
            "locale": "ru",
            "instructions": "false",
            "calc_points": "false",
            "key": GRAPHHOPPER_API_KEY
        }
        
        # Формируем URL с параметрами
        request_url = f"{url}?point={start_lat},{start_lon}&point={end_lat},{end_lon}&vehicle=car&locale=ru&instructions=false&calc_points=false&key={GRAPHHOPPER_API_KEY}"
        
        response = requests.get(request_url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if "paths" in data and len(data["paths"]) > 0:
                distance_meters = data["paths"][0]["distance"]
                distance_km = round(distance_meters / 1000, 1)
                print(f"✅ GraphHopper: {distance_km} км от {start_coord} до {end_coord}")
                return distance_km
            else:
                print(f"⚠️ GraphHopper: нет данных о маршруте")
                return None
        else:
            print(f"⚠️ GraphHopper ошибка {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"⚠️ Ошибка GraphHopper: {e}")
        return None

def calculate_route_distance(start_coord, waypoint_coords):
    """Рассчитывает общее расстояние от точки А через все промежуточные точки"""
    if not start_coord or not waypoint_coords:
        return None
    
    total_distance = 0
    
    # Начинаем от точки А
    current_point = start_coord
    
    # Если есть только одна точка назначения (прямой маршрут)
    if len(waypoint_coords) == 1:
        distance = graphhopper_route(start_coord, waypoint_coords[0])
        if distance:
            return distance
        else:
            # Если GraphHopper не сработал, используем гаверсинус
            return round(haversine_distance(
                start_coord[0], start_coord[1],
                waypoint_coords[0][0], waypoint_coords[0][1]
            ), 1)
    
    # Для нескольких точек: A -> 1, 1 -> 2, 2 -> 3, ...
    for i, next_point in enumerate(waypoint_coords):
        print(f"📍 Рассчитываю отрезок {i+1}: {current_point} -> {next_point}")
        
        distance = graphhopper_route(current_point, next_point)
        
        if distance is None:
            print(f"⚠️ Не удалось рассчитать отрезок {i+1}, использую гаверсинус")
            # Используем гаверсинус как fallback
            distance = haversine_distance(
                current_point[0], current_point[1],
                next_point[0], next_point[1]
            )
            distance = round(distance, 1)
        
        print(f"📏 Отрезок {i+1}: {distance} км")
        total_distance += distance
        current_point = next_point
    
    return round(total_distance, 1)

def calculate_crimea_route(start_coord, crimea_coords):
    """Специальная функция для расчета маршрутов в/из Крыма"""
    try:
        total_distance = 0
        
        # Определяем, находится ли старт в Крыму
        start_in_crimea = is_in_crimea(start_coord[0], start_coord[1])
        
        # Координаты Крымского моста
        bridge_start = (45.3005, 36.5125)  # материковая сторона
        bridge_end = (45.2779, 36.5611)    # крымская сторона
        bridge_length = 19  # км
        
        current_point = start_coord
        
        # Для каждой точки в Крыму
        for i, next_point in enumerate(crimea_coords):
            if not is_in_crimea(next_point[0], next_point[1]):
                print(f"⚠️ Точка {next_point} не в Крыму, но вызвана функция для Крыма")
                continue
            
            # Если текущая точка не в Крыму, а следующая в Крыму
            if not is_in_crimea(current_point[0], current_point[1]):
                # 1. От текущей точки до начала моста
                dist_to_bridge = graphhopper_route(current_point, bridge_start)
                if dist_to_bridge is None:
                    dist_to_bridge = haversine_distance(
                        current_point[0], current_point[1],
                        bridge_start[0], bridge_start[1]
                    )
                
                # 2. Мост
                dist_bridge = bridge_length
                
                # 3. От конца моста до точки в Крыму
                dist_from_bridge = graphhopper_route(bridge_end, next_point)
                if dist_from_bridge is None:
                    dist_from_bridge = haversine_distance(
                        bridge_end[0], bridge_end[1],
                        next_point[0], next_point[1]
                    )
                
                segment_distance = dist_to_bridge + dist_bridge + dist_from_bridge
            
            # Если обе точки в Крыму
            else:
                # Пытаемся использовать GraphHopper для маршрута внутри Крыма
                segment_distance = graphhopper_route(current_point, next_point)
                if segment_distance is None:
                    segment_distance = haversine_distance(
                        current_point[0], current_point[1],
                        next_point[0], next_point[1]
                    )
            
            total_distance += segment_distance
            current_point = next_point
        
        return round(total_distance, 1)
    except Exception as e:
        print(f"⚠️ Ошибка расчета маршрута Крыма: {e}")
        return None

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None:
        return [None, None]
    
    # Уменьшаем разброс
    variation = base * random.uniform(0.01, 0.03)  # 1-3%
    return [
        round(base + variation, 1),
        round(max(0, base - variation), 1)
    ]

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
    
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=start_col + i)
        cell.value = header
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="FFE4B5", end_color="FFE4B5", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center")
    
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    return start_col + len(headers)

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
        "✅ Я верну тот же файл с добавленными колонками результатов!\n\n"
        "🌉 Особенность: автоматический учет Крымского моста при маршрутах в Крым."
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
    
    timestamp = int(time.time())
    input_file = f"input_{user_id}_{timestamp}.xlsx"
    
    await file.download_to_drive(input_file)
    
    try:
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
        f"⏳ Начинаю обработку\nВсего строк: {total}\nОбработка..."
    )
    
    start_col = add_result_columns(ws, start_col=3)
    
    geocode_cache = {}
    
    processed = 0
    errors = 0
    
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            address_chain = route['address_chain']
            
            print(f"\n{'='*50}")
            print(f"Обработка строки {row_num}:")
            print(f"Старт: {start_point}")
            print(f"Маршрут: {address_chain}")
            
            # Геокодируем стартовую точку
            if start_point in geocode_cache:
                start_coords = geocode_cache[start_point]
            else:
                start_coords = yandex_geocode(start_point)
                time.sleep(0.5)
                if start_coords:
                    geocode_cache[start_point] = start_coords
                else:
                    print(f"⚠️ Не удалось геокодировать стартовую точку: {start_point}")
            
            # Парсим цепочку адресов
            addresses = parse_address_chain(address_chain)
            print(f"Распарсено адресов: {len(addresses)}")
            for i, addr in enumerate(addresses):
                print(f"  {i+1}. {addr}")
            
            # Геокодируем все адреса в цепочке
            waypoint_coords = []
            waypoint_coords_str = []
            geocode_errors = False
            
            for addr in addresses:
                if addr in geocode_cache:
                    coords = geocode_cache[addr]
                else:
                    coords = yandex_geocode(addr)
                    time.sleep(0.5)
                    if coords:
                        geocode_cache[addr] = coords
                    else:
                        print(f"⚠️ Не удалось геокодировать адрес: {addr}")
                
                if coords:
                    waypoint_coords.append(coords)
                    waypoint_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    geocode_errors = True
                    break
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
            
            if geocode_errors or not start_coords or not waypoint_coords:
                # Записываем ошибку
                ws.cell(row=row_num, column=3).value = "❌ Ошибка геокодирования"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}" if start_coords else "Ошибка"
                ws.cell(row=row_num, column=5).value = "; ".join(waypoint_coords_str) if waypoint_coords_str else "Ошибка"
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = route_type
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=9).value = ""
                ws.cell(row=row_num, column=10).value = ""
                errors += 1
                print(f"❌ Ошибка геокодирования в строке {row_num}")
            else:
                # Проверяем, есть ли точки в Крыму
                has_crimea = any(is_in_crimea(coord[0], coord[1]) for coord in waypoint_coords)
                start_in_crimea = is_in_crimea(start_coords[0], start_coords[1])
                
                # Рассчитываем расстояние
                if has_crimea:
                    print(f"📍 Обнаружены точки в Крыму. Использую специальный расчет.")
                    
                    # Разделяем точки на крымские и не крымские
                    crimea_points = [coord for coord in waypoint_coords if is_in_crimea(coord[0], coord[1])]
                    non_crimea_points = [coord for coord in waypoint_coords if not is_in_crimea(coord[0], coord[1])]
                    
                    total_distance = 0
                    current_point = start_coords
                    
                    # Обрабатываем все точки по порядку
                    for next_point in waypoint_coords:
                        next_in_crimea = is_in_crimea(next_point[0], next_point[1])
                        current_in_crimea = is_in_crimea(current_point[0], current_point[1])
                        
                        # Если переход между Крымом и не-Крымом
                        if current_in_crimea != next_in_crimea:
                            print(f"📍 Переход между регионами: {'Крым' if current_in_crimea else 'не Крым'} -> {'Крым' if next_in_crimea else 'не Крым'}")
                            
                            # Координаты моста
                            bridge_start = (45.3005, 36.5125)
                            bridge_end = (45.2779, 36.5611)
                            bridge_length = 19
                            
                            if not current_in_crimea:  # Из не-Крыма в Крым
                                # До моста
                                dist1 = graphhopper_route(current_point, bridge_start)
                                if dist1 is None:
                                    dist1 = haversine_distance(
                                        current_point[0], current_point[1],
                                        bridge_start[0], bridge_start[1]
                                    )
                                
                                # Мост
                                dist2 = bridge_length
                                
                                # От моста до точки
                                dist3 = graphhopper_route(bridge_end, next_point)
                                if dist3 is None:
                                    dist3 = haversine_distance(
                                        bridge_end[0], bridge_end[1],
                                        next_point[0], next_point[1]
                                    )
                                
                                segment_distance = dist1 + dist2 + dist3
                            else:  # Из Крыма в не-Крым
                                # До моста
                                dist1 = graphhopper_route(current_point, bridge_end)
                                if dist1 is None:
                                    dist1 = haversine_distance(
                                        current_point[0], current_point[1],
                                        bridge_end[0], bridge_end[1]
                                    )
                                
                                # Мост
                                dist2 = bridge_length
                                
                                # От моста до точки
                                dist3 = graphhopper_route(bridge_start, next_point)
                                if dist3 is None:
                                    dist3 = haversine_distance(
                                        bridge_start[0], bridge_start[1],
                                        next_point[0], next_point[1]
                                    )
                                
                                segment_distance = dist1 + dist2 + dist3
                        else:
                            # Обе точки в одном регионе
                            segment_distance = graphhopper_route(current_point, next_point)
                            if segment_distance is None:
                                segment_distance = haversine_distance(
                                    current_point[0], current_point[1],
                                    next_point[0], next_point[1]
                                )
                        
                        total_distance += segment_distance
                        current_point = next_point
                    
                    distance = round(total_distance, 1)
                else:
                    # Все точки вне Крыма - обычный расчет
                    distance = calculate_route_distance(start_coords, waypoint_coords)
                
                if distance:
                    d2, d3 = variations(distance)
                    
                    # Определяем статус
                    status = "✅ Успешно"
                    if has_crimea:
                        status += " (с учетом Крымского моста)"
                    
                    # Записываем результаты
                    ws.cell(row=row_num, column=3).value = status
                    ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=5).value = "; ".join(waypoint_coords_str)
                    ws.cell(row=row_num, column=6).value = len(addresses)
                    ws.cell(row=row_num, column=7).value = route_type
                    ws.cell(row=row_num, column=8).value = distance
                    ws.cell(row=row_num, column=9).value = d2
                    ws.cell(row=row_num, column=10).value = d3
                    
                    for col in [8, 9, 10]:
                        cell = ws.cell(row=row_num, column=col)
                        cell.number_format = '0.0'
                    
                    print(f"✅ Успешно: {distance} км")
                else:
                    ws.cell(row=row_num, column=3).value = "⚠️ Ошибка расчета маршрута"
                    ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=5).value = "; ".join(waypoint_coords_str)
                    ws.cell(row=row_num, column=6).value = len(addresses)
                    ws.cell(row=row_num, column=7).value = route_type
                    ws.cell(row=row_num, column=8).value = "Ошибка"
                    ws.cell(row=row_num, column=9).value = ""
                    ws.cell(row=row_num, column=10).value = ""
                    errors += 1
                    print(f"⚠️ Ошибка расчета маршрута в строке {row_num}")
            
            processed += 1
            
            # Обновляем прогресс
            if processed % 2 == 0 or processed == total:
                try:
                    success_count = processed - errors
                    await progress_msg.edit_text(
                        f"⏳ Обработка: {processed} / {total}\n"
                        f"✅ Успешно: {success_count}\n"
                        f"❌ Ошибок: {errors}\n"
                        f"📍 Текущий: {start_point[:30]}..."
                    )
                except:
                    pass
                
        except Exception as e:
            print(f"❌ Ошибка обработки строки {route.get('row_num', 'N/A')}: {e}")
            errors += 1
    
    try:
        await progress_msg.edit_text(
            f"✅ Обработка завершена!\n"
            f"Успешно: {processed - errors}\n"
            f"Ошибок: {errors}\n"
            f"Формирую отчет..."
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
                filename=f"результаты_{user_id}.xlsx",
                caption=f"✅ Готово!\nУспешно обработано: {processed - errors} строк\nОшибок: {errors}"
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
📋 **Доступные команды:**

/start - Начать работу с ботом
/help - Показать эту справку

📁 **Формат Excel файла:**
• Колонка A: Стартовая точка (точка А)
• Колонка B: Цепочка адресов через дефис или тире

📍 **Пример строки в колонке B:**
`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`

📊 **Добавляемые колонки результатов:**
1. Статус обработки
2. Координаты старта
3. Координаты точек
4. Количество точек
5. Тип маршрута
6. Расстояние 1 (км) - основное
7. Расстояние 2 (км) - +1-3%
8. Расстояние 3 (км) - -1-3%

🌉 **Особенность работы с Крымом:**
• Автоматическое определение точек в Крыму
• Учет Крымского моста (19 км)
• Разделение маршрута при переходе между Крымом и материком

**Типы маршрутов:**
• Прямой - один адрес в цепочке
• С промежуточными точками - несколько адресов
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def example_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /example"""
    await update.message.reply_text(
        "📋 Пример Excel файла:\n\n"
        "| Колонка A | Колонка B |\n"
        "|-----------|-----------|\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж, ул. Ипподромная 18А |\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж - г. Сергиев Посад - г. Москва |\n"
        "| Ростов-на-Дону, Оганова 22 | р. Крым, г. Симферополь |\n\n"
        "Просто создайте Excel файл с такими данными и отправьте боту!"
    )

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
    print(f"✅ Яндекс API: {'установлен' if YANDEX_API_KEY else 'не установлен'}")
    print(f"✅ GraphHopper API: установлен")
    
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("example", example_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Попытка {attempt + 1}/{max_retries} запустить бота...")
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
            
            while True:
                await asyncio.sleep(3600)
            
        except Conflict as e:
            print(f"⚠️ Конфликт: {e}")
            print(f"⏳ Жду {retry_delay} секунд перед повторной попыткой...")
            
            try:
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
            print(f"❌ Ошибка: {e}")
            break

def main():
    is_render = os.environ.get('RENDER') is not None
    port = os.environ.get('PORT')
    
    if is_render and port:
        print(f"🌐 Работаем на Render, порт: {port}")
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        print("✅ Flask сервер запущен в отдельном потоке")
    
    asyncio.run(run_bot())

if __name__ == "__main__":
    main()