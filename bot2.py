import requests
import openpyxl
import random
import time
import os
import threading
import asyncio
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
import math
from math import radians, sin, cos, sqrt, atan2
import json

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
                🚀 Платформа: Render<br>
                🗺️ Используется: GraphHopper API
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
GRAPH_HOPPER_API_KEY = os.getenv("GRAPH_HOPPER_API_KEY", "2c8e643a-360f-47ab-855d-7e884ce217ad")

# ================== ГРАФХОППЕР ФУНКЦИИ ==================
def graphhopper_geocode(address, retries=3):
    """Геокодирование через GraphHopper с повторными попытками"""
    if not GRAPH_HOPPER_API_KEY:
        print("⚠️ GRAPH_HOPPER_API_KEY не установлен!")
        return None
    
    for attempt in range(retries):
        try:
            url = "https://graphhopper.com/api/1/geocode"
            params = {
                "q": address,
                "locale": "ru",
                "limit": 1,
                "key": GRAPH_HOPPER_API_KEY,
                "provider": "default"
            }
            
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("hits") and len(data["hits"]) > 0:
                    lat = data["hits"][0]["point"]["lat"]
                    lon = data["hits"][0]["point"]["lng"]
                    print(f"✅ Геокодирование успешно: {address} -> {lat}, {lon}")
                    return float(lat), float(lon)
                else:
                    print(f"⚠️ Адрес не найден GraphHopper: {address}")
            else:
                print(f"⚠️ Ошибка геокодирования GraphHopper {response.status_code}: {response.text[:100]}")
            
            # Задержка перед повторной попыткой
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
                
        except requests.exceptions.Timeout:
            print(f"⚠️ Таймаут при геокодировании: {address}")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
        except Exception as e:
            print(f"⚠️ Ошибка при геокодировании GraphHopper {address}: {e}")
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
    
    return None

def graphhopper_route_with_waypoints(coordinates_list, profile="car", retries=3):
    """Расчет маршрута через GraphHopper с промежуточными точками"""
    if not GRAPH_HOPPER_API_KEY:
        print("⚠️ GRAPH_HOPPER_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        print("⚠️ Слишком мало точек для маршрута")
        return None
    
    # Проверяем координаты на валидность
    valid_coords = []
    for lat, lon in coordinates_list:
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            valid_coords.append((lat, lon))
        else:
            print(f"⚠️ Невалидные координаты пропущены: {lat}, {lon}")
    
    if len(valid_coords) < 2:
        print("⚠️ Недостаточно валидных координат для маршрута")
        return None
    
    for attempt in range(retries):
        try:
            url = f"https://graphhopper.com/api/1/route"
            
            # Строим параметры запроса
            params = {
                "key": GRAPH_HOPPER_API_KEY,
                "vehicle": profile,
                "locale": "ru",
                "instructions": "false",
                "calc_points": "false",
                "points_encoded": "false",
                "optimize": "false"  # Не оптимизировать порядок точек
            }
            
            # Формируем строку точек для запроса
            points = []
            for lat, lon in valid_coords:
                points.append(f"point={lat},{lon}")
            
            # Добавляем точки к URL
            url_with_points = f"{url}?{'&'.join(points)}"
            
            # Добавляем остальные параметры
            for key, value in params.items():
                url_with_points += f"&{key}={value}"
            
            print(f"🔗 Запрос маршрута: {len(valid_coords)} точек")
            
            response = requests.get(url_with_points, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Проверяем наличие пути
                if "paths" in data and len(data["paths"]) > 0:
                    distance_m = data["paths"][0]["distance"]  # Расстояние в метрах
                    distance_km = round(distance_m / 1000, 1)
                    
                    # Получаем время в пути (опционально)
                    time_ms = data["paths"][0]["time"]  # Время в миллисекундах
                    time_h = round(time_ms / 3600000, 1)  # Время в часах
                    
                    print(f"✅ Маршрут рассчитан: {distance_km} км, {time_h} часов")
                    return distance_km
                else:
                    print(f"⚠️ Не удалось построить маршрут")
                    
            elif response.status_code == 429:
                print(f"⚠️ Превышен лимит запросов, попытка {attempt + 1}/{retries}")
                wait_time = 5 * (attempt + 1)
                print(f"⏳ Жду {wait_time} секунд...")
                time.sleep(wait_time)
                
            else:
                print(f"⚠️ Ошибка маршрута {response.status_code}: {response.text[:200]}")
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                    
        except requests.exceptions.Timeout:
            print(f"⚠️ Таймаут при расчете маршрута")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
        except Exception as e:
            print(f"⚠️ Ошибка при расчете маршрута GraphHopper: {e}")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    
    return None

def calculate_haversine_distance(start_coords, waypoints_coords):
    """Расчет примерного расстояния по формуле гаверсинусов (запасной вариант)"""
    def haversine(coord1, coord2):
        """Расчет расстояния между двумя точками по гаверсинусу"""
        R = 6371  # Радиус Земли в км
        
        lat1, lon1 = radians(coord1[0]), radians(coord1[1])
        lat2, lon2 = radians(coord2[0]), radians(coord2[1])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    try:
        # Общее расстояние: старт -> точка1 -> точка2 -> ... -> конечная точка
        total_distance = 0
        current_point = start_coords
        
        # Проходим по всем точкам
        for next_point in waypoints_coords:
            total_distance += haversine(current_point, next_point)
            current_point = next_point
        
        # Увеличиваем на 15-20% для учета дорог (вместо прямой линии)
        total_distance = total_distance * 1.18
        
        return round(total_distance, 1)
        
    except Exception as e:
        print(f"⚠️ Ошибка расчета по гаверсинусу: {e}")
        return None

# ================== ОСНОВНЫЕ ФУНКЦИИ ОБРАБОТКИ ==================
def read_from_docx(path):
    """Чтение адресов из DOCX файла"""
    doc = Document(path)
    lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    return [l for l in lines if len(l) > 10 and not l.replace(' ', '').isdigit()]

def read_from_excel(path):
    """Чтение маршрутов из Excel файла с двумя колонками: стартовая точка и цепочка адресов"""
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    routes = []
    
    # Определяем максимальную строку
    max_row = ws.max_row
    
    # Читаем данные, пропуская заголовки если они есть
    for row in range(1, max_row + 1):
        start_point = ws.cell(row=row, column=1).value  # Колонка A
        address_chain = ws.cell(row=row, column=2).value  # Колонка B
        
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

def parse_address_chain(address_string):
    """Парсит цепочку адресов, разделенных дефисами"""
    if not address_string:
        return []
    
    # Нормализуем различные тире и форматирование
    address_string = address_string.replace('–', '-').replace('—', '-').replace(' - ', '-').replace('\n', '-')
    
    # Разделяем по дефису и очищаем
    addresses = []
    for addr in address_string.split('-'):
        cleaned = addr.strip()
        if cleaned:
            addresses.append(cleaned)
    
    return addresses

def validate_coordinates(coord):
    """Проверяет валидность координат"""
    lat, lon = coord
    return -90 <= lat <= 90 and -180 <= lon <= 180

def variations(base):
    """Генерирует варианты расстояний ±5-15%"""
    if base is None:
        return [None, None]
    
    # Вариант 1: +5-15%
    d2 = round(base * (1 + random.uniform(0.05, 0.15)), 1)
    # Вариант 2: -5-15%
    d3 = round(base * (1 - random.uniform(0.05, 0.15)), 1)
    
    return [d2, d3]

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
    
    # Добавляем заголовки
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=start_col + i)
        cell.value = header
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="FFE4B5", end_color="FFE4B5", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center")
    
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

def process_route_row(route, ws, geocode_cache):
    """Обработка одной строки маршрута"""
    try:
        row_num = route['row_num']
        start_point = route['start_point']
        address_chain = route['address_chain']
        
        # 1. Геокодируем стартовую точку
        if start_point in geocode_cache:
            start_coords = geocode_cache[start_point]
        else:
            start_coords = graphhopper_geocode(start_point)
            if start_coords:
                geocode_cache[start_point] = start_coords
                time.sleep(0.5)  # Задержка между запросами
        
        if not start_coords:
            ws.cell(row=row_num, column=3).value = "❌ Ошибка геокодирования старта"
            ws.cell(row=row_num, column=4).value = "Ошибка"
            return {"status": "error"}
        
        # 2. Парсим цепочку адресов
        addresses = parse_address_chain(address_chain)
        if not addresses:
            ws.cell(row=row_num, column=3).value = "❌ Нет адресов в цепочке"
            ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
            return {"status": "error"}
        
        # 3. Геокодируем все адреса в цепочке
        waypoints_coords = []
        waypoints_str = []
        
        for i, addr in enumerate(addresses):
            if addr in geocode_cache:
                coords = geocode_cache[addr]
            else:
                coords = graphhopper_geocode(addr)
                if coords:
                    geocode_cache[addr] = coords
                time.sleep(0.5)  # Задержка между запросами
            
            if not coords:
                ws.cell(row=row_num, column=3).value = f"❌ Ошибка геокодирования точки {i+1}"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str) if waypoints_str else "Ошибка"
                return {"status": "error"}
            
            if not validate_coordinates(coords):
                ws.cell(row=row_num, column=3).value = f"❌ Невалидные координаты точки {i+1}"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str) if waypoints_str else "Ошибка"
                return {"status": "error"}
            
            waypoints_coords.append(coords)
            waypoints_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
        
        # 4. Определяем тип маршрута
        route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
        
        # 5. Рассчитываем расстояние через GraphHopper
        # Формируем полный список координат: старт + все промежуточные точки
        all_coords = [start_coords] + waypoints_coords
        distance = graphhopper_route_with_waypoints(all_coords)
        
        # 6. Если GraphHopper не сработал, используем гаверсинус
        if distance is None:
            print(f"⚠️ GraphHopper не сработал, используем гаверсинус")
            distance = calculate_haversine_distance(start_coords, waypoints_coords)
        
        if distance is None:
            ws.cell(row=row_num, column=3).value = "⚠️ Ошибка расчета маршрута"
            ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
            ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str)
            ws.cell(row=row_num, column=6).value = len(addresses)
            ws.cell(row=row_num, column=7).value = route_type
            ws.cell(row=row_num, column=8).value = "Ошибка"
            return {"status": "error"}
        
        # 7. Генерируем варианты расстояний
        d2, d3 = variations(distance)
        
        # 8. Записываем результаты
        ws.cell(row=row_num, column=3).value = "✅ Успешно"
        ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
        ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str)
        ws.cell(row=row_num, column=6).value = len(addresses)
        ws.cell(row=row_num, column=7).value = route_type
        ws.cell(row=row_num, column=8).value = distance
        ws.cell(row=row_num, column=9).value = d2
        ws.cell(row=row_num, column=10).value = d3
        
        # Форматируем ячейки с расстояниями
        for col in [8, 9, 10]:
            cell = ws.cell(row=row_num, column=col)
            cell.number_format = '0.0'
        
        print(f"✅ Строка {row_num} обработана: {distance} км")
        return {"status": "success", "distance": distance}
        
    except Exception as e:
        print(f"❌ Ошибка обработки строки {route.get('row_num', 'N/A')}: {e}")
        return {"status": "error"}

# ================== TELEGRAM БОТ ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для расчета маршрутов с использованием GraphHopper API.\n\n"
        "📁 Отправьте мне Excel файл в формате:\n"
        "• Колонка A: Стартовая точка (точка А)\n"
        "• Колонка B: Цепочка адресов через дефис\n\n"
        "📊 Пример строки в колонке B:\n"
        "`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`\n\n"
        "✅ Я верну тот же файл с добавленными колонками результатов!"
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
        f"⏳ Начинаю обработку\nВсего строк: {total}\nОбработка..."
    )
    
    # Добавляем колонки для результатов
    start_col = add_result_columns(ws, start_col=3)
    
    # Кэш для геокодированных адресов
    geocode_cache = {}
    
    processed = 0
    errors = 0
    successes = 0
    
    for route in routes:
        result = process_route_row(route, ws, geocode_cache)
        processed += 1
        
        if result["status"] == "success":
            successes += 1
        else:
            errors += 1
        
        # Обновляем прогресс каждые 5 строк или в конце
        if processed % 5 == 0 or processed == total:
            try:
                await progress_msg.edit_text(
                    f"⏳ Обработка: {processed} / {total}\n"
                    f"✅ Успешно: {successes}\n"
                    f"❌ Ошибок: {errors}"
                )
            except:
                pass
    
    try:
        await progress_msg.edit_text(
            f"✅ Обработка завершена!\n"
            f"Успешно: {successes}\n"
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
                filename=f"результаты_{file_name}",
                caption=f"✅ Готово!\nУспешно обработано: {successes} строк\nОшибок: {errors}"
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
/status - Статус API сервисов

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

**Используемые API:**
• GraphHopper для геокодирования и маршрутизации
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка статуса API сервисов"""
    status_message = "🔍 **Проверка статуса API сервисов:**\n\n"
    
    # Проверяем GraphHopper
    if GRAPH_HOPPER_API_KEY:
        try:
            # Пробуем сделать простой запрос к GraphHopper
            url = "https://graphhopper.com/api/1/geocode"
            params = {
                "q": "Москва",
                "locale": "ru",
                "limit": 1,
                "key": GRAPH_HOPPER_API_KEY
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                status_message += "✅ GraphHopper API: **РАБОТАЕТ**\n"
            else:
                status_message += f"⚠️ GraphHopper API: **ОШИБКА {response.status_code}**\n"
        except Exception as e:
            status_message += f"❌ GraphHopper API: **НЕ ДОСТУПЕН** ({str(e)[:50]})\n"
    else:
        status_message += "❌ GraphHopper API: **КЛЮЧ НЕ УСТАНОВЛЕН**\n"
    
    # Общая информация
    status_message += f"\n📊 **Информация:**\n"
    status_message += f"• Бот использует GraphHopper API для всех операций\n"
    status_message += f"• Лимит GraphHopper: 500 запросов/день (бесплатный тариф)\n"
    status_message += f"• Задержка между запросами: 0.5 секунд\n"
    
    await update.message.reply_text(status_message, parse_mode='Markdown')

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
    print(f"✅ GraphHopper API: {'установлен' if GRAPH_HOPPER_API_KEY else 'не установлен'}")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
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