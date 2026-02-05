import requests
import pandas as pd
import openpyxl
import random
import time
import os
import threading
import asyncio
import re
import tempfile
import json
from pathlib import Path
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
from openpyxl.utils import get_column_letter
import warnings
warnings.filterwarnings('ignore')

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
GRAPHHOPPER_API_KEY = os.getenv("GRAPHHOPPER_API_KEY", "2c8e643a-360f-47ab-855d-7e884ce217ad")

# ================== ФУНКЦИИ ОБРАБОТКИ АДРЕСОВ ==================
def clean_text(text):
    """Очистка текста от лишних символов"""
    if not text:
        return ""
    # Заменяем различные типы тире на обычный дефис
    text = str(text).replace('–', '-').replace('—', '-').replace('−', '-')
    # Убираем лишние пробелы
    text = ' '.join(text.split())
    # Заменяем двойные дефисы на одинарные
    while '--' in text:
        text = text.replace('--', '-')
    return text.strip()

def extract_region_from_address(address):
    """Извлекает регион (область, край, республику) из адреса"""
    if not address:
        return None
    
    address = clean_text(address)
    
    # Паттерны для регионов
    region_patterns = [
        r'^(.*?)\s+(?:обл\.|область|край|респ\.|республика|АО|авт\.\s+округ|р-н|район)',
        r'^(р\.\s+[А-Яа-я]+)',  # р. Карелия, р. Коми
        r'^(?:КЧР|КБР|РСО-Алания|р-н\s+[А-Яа-я]+)',  # Сокращенные названия
    ]
    
    for pattern in region_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            region = match.group(1).strip()
            # Если регион начинается с "р.", убираем точку
            if region.startswith('р.'):
                region = region.replace('р.', 'Республика')
            return region
    
    return None

def extract_settlement_from_address(address):
    """Извлекает населенный пункт из адреса"""
    if not address:
        return None
    
    address = clean_text(address)
    
    # Удаляем регион из начала
    region = extract_region_from_address(address)
    if region:
        # Удаляем регион и следующий за ним разделитель
        pattern = re.escape(region) + r'[,\s\-]*'
        address = re.sub(pattern, '', address, 1, re.IGNORECASE)
    
    # Паттерны для населенных пунктов с разными типами
    settlement_patterns = [
        # г. Москва, г.Санкт-Петербург
        r'(?:г\.|город\s+)([^,\-]+)',
        # с. Ивановка, п. Горный
        r'(?:с\.|село\s+|п\.|посёлок\s+|пос\.|поселок\s+)([^,\-]+)',
        # ст-ца Каневская, ст.Ленинградская
        r'(?:ст-ца\s+|ст\.|станица\s+)([^,\-]+)',
        # д. Петрово, д.Новое
        r'(?:д\.|деревня\s+)([^,\-]+)',
        # х. Согласный
        r'(?:х\.|хутор\s+)([^,\-]+)',
        # р.п. Мухтолово
        r'(?:р\.п\.|рабочий посёлок\s+)([^,\-]+)',
        # пгт. Черноморское
        r'(?:пгт\.|посёлок городского типа\s+)([^,\-]+)',
        # аул Кошехабль
        r'(?:аул\s+)([^,\-]+)',
        # Если есть запятая, берем первое слово до запятой
        r'^([^,]+)(?=,)',
        # Просто первое слово
        r'^([^\s\-]+)'
    ]
    
    for pattern in settlement_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            settlement = match.group(1).strip()
            # Убираем возможные точки в конце
            if settlement.endswith('.'):
                settlement = settlement[:-1]
            return settlement
    
    return None

def parse_address_chain(address_string, default_region=None):
    """Парсит цепочку адресов с учетом региона из первого адреса"""
    if not address_string:
        return []
    
    address_string = clean_text(address_string)
    
    # Разделяем по дефису
    addresses = [addr.strip() for addr in re.split(r'\s*-\s*', address_string) if addr.strip()]
    
    if not addresses:
        return []
    
    # Извлекаем регион из первого адреса
    first_region = extract_region_from_address(addresses[0])
    region_to_use = first_region if first_region else default_region
    
    parsed_addresses = []
    
    for i, addr in enumerate(addresses):
        # Извлекаем регион для текущего адреса
        current_region = extract_region_from_address(addr)
        settlement = extract_settlement_from_address(addr)
        
        if not settlement:
            # Если не удалось извлечь населенный пункт, используем весь адрес
            settlement = addr
        
        # Если у текущего адреса нет региона, используем регион из первого адреса
        if not current_region and region_to_use and i > 0:
            # Формируем полный адрес с регионом
            full_address = f"{region_to_use}, {settlement}"
        elif current_region:
            full_address = f"{current_region}, {settlement}"
        else:
            full_address = settlement
        
        parsed_addresses.append(full_address)
    
    return parsed_addresses

def simplify_address_for_geocoding(address):
    """Упрощает адрес для геокодирования в GraphHopper"""
    if not address:
        return address
    
    address = clean_text(address)
    
    # Извлекаем регион и населенный пункт
    region = extract_region_from_address(address)
    settlement = extract_settlement_from_address(address)
    
    if not settlement:
        # Если не удалось извлечь, возвращаем оригинальный адрес
        return address
    
    # Формируем простой адрес для GraphHopper
    # GraphHopper лучше работает с простыми названиями городов
    if settlement:
        # Для GraphHopper часто достаточно только названия города
        simple_address = settlement
        
        # Добавляем "Russia" для лучшего геокодирования
        if 'россия' not in simple_address.lower() and 'russia' not in simple_address.lower():
            simple_address = f"{simple_address}, Russia"
        
        return simple_address
    
    return address

# ================== GRAPHHOPPER API ФУНКЦИИ ==================
def graphhopper_geocode(address):
    """Геокодирование адреса через GraphHopper API"""
    if not GRAPHHOPPER_API_KEY:
        print("⚠️ GRAPHHOPPER_API_KEY не установлен!")
        return None
    
    # Упрощаем адрес
    simplified_address = simplify_address_for_geocoding(address)
    
    print(f"📍 GraphHopper геокодирует: {address[:50]}... -> {simplified_address}")
    
    url = "https://graphhopper.com/api/1/geocode"
    params = {
        "q": simplified_address,
        "key": GRAPHHOPPER_API_KEY,
        "locale": "ru",
        "limit": 3,
        "provider": "default"
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        
        if r.status_code != 200:
            print(f"⚠️ Ошибка геокодирования {r.status_code} для: {simplified_address}")
            print(f"⚠️ Ответ: {r.text[:200]}")
            return None
        
        data = r.json()
        
        if data.get("hits") and len(data["hits"]) > 0:
            # Берем первый результат
            hit = data["hits"][0]
            location = hit.get("point", {})
            
            lat = location.get("lat")
            lng = location.get("lng")
            
            if lat is not None and lng is not None:
                coords = (float(lat), float(lng))
                print(f"✅ Найдены координаты: {coords} для '{hit.get('name', 'N/A')}'")
                return coords
        
        # Если не нашли, пробуем без "Russia"
        if simplified_address.endswith(", Russia"):
            simplified_address_ru = simplified_address[:-7].strip()
            print(f"🔄 Пробую без 'Russia': {simplified_address_ru}")
            params["q"] = simplified_address_ru
            
            r = requests.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                data = r.json()
                if data.get("hits") and len(data["hits"]) > 0:
                    hit = data["hits"][0]
                    location = hit.get("point", {})
                    lat = location.get("lat")
                    lng = location.get("lng")
                    if lat is not None and lng is not None:
                        coords = (float(lat), float(lng))
                        print(f"✅ Найдены координаты (без Russia): {coords}")
                        return coords
        
        print(f"⚠️ Адрес не найден: {simplified_address}")
        return None
        
    except Exception as e:
        print(f"⚠️ Ошибка при геокодировании {address}: {e}")
        return None

def graphhopper_route_with_waypoints(coordinates_list):
    """Строит маршрут через промежуточные точки через GraphHopper API"""
    if not GRAPHHOPPER_API_KEY:
        print("⚠️ GRAPHHOPPER_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    url = "https://graphhopper.com/api/1/route"
    
    # Подготавливаем параметры для запроса
    params = {
        "key": GRAPHHOPPER_API_KEY,
        "vehicle": "car",
        "locale": "ru",
        "instructions": "false",
        "calc_points": "false",
        "points_encoded": "false",
        "elevation": "false",
        "optimize": "false"
    }
    
    # Добавляем точки маршрута в формате "lat,lng"
    points = []
    for i, coord in enumerate(coordinates_list):
        points.append(f"point={coord[0]},{coord[1]}")
    
    # Формируем URL с параметрами
    query_string = "&".join(points) + "&" + "&".join([f"{k}={v}" for k, v in params.items()])
    
    try:
        print(f"📍 GraphHopper строит маршрут через {len(coordinates_list)} точек...")
        
        full_url = f"{url}?{query_string}"
        r = requests.get(full_url, timeout=60)
        
        if r.status_code != 200:
            print(f"⚠️ Ошибка маршрута {r.status_code}")
            # Попробуем получить детали ошибки
            try:
                error_details = r.json()
                print(f"⚠️ Детали ошибки: {error_details}")
            except:
                print(f"⚠️ Текст ошибки: {r.text[:200]}")
            return None
        
        data = r.json()
        
        if data.get("paths") and len(data["paths"]) > 0:
            path = data["paths"][0]
            distance_meters = path.get("distance", 0)
            
            if distance_meters > 0:
                distance_km = round(distance_meters / 1000, 1)
                print(f"✅ Маршрут построен: {distance_km} км")
                return distance_km
            else:
                print(f"⚠️ Нулевое расстояние в маршруте")
                return None
        else:
            print(f"⚠️ Некорректный ответ от GraphHopper")
            return None
            
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {e}")
        return None

# ================== ЧТЕНИЕ И ЗАПИСЬ EXCEL ==================
def read_excel_with_fallback(file_path):
    """Читает Excel файл с несколькими попытками и разными методами"""
    try:
        # Сначала пробуем openpyxl
        print(f"📖 Пытаюсь прочитать файл с openpyxl...")
        wb = load_workbook(file_path, data_only=True, read_only=False)
        ws = wb.active
        
        # Собираем данные
        data = []
        max_row = ws.max_row
        max_col = ws.max_column
        
        # Определяем, есть ли заголовки
        has_headers = False
        if max_row > 0:
            # Проверяем первую строку на наличие текста
            first_row = []
            for col in range(1, min(max_col, 10) + 1):  # Проверяем первые 10 колонок
                cell_value = ws.cell(row=1, column=col).value
                first_row.append(str(cell_value) if cell_value else "")
            
            # Если в первой строке есть слова "пункт", "назначение", "груз" и т.д., то это заголовки
            header_keywords = ['пункт', 'назначение', 'груз', 'адрес', 'point', 'address', 'destination']
            first_row_text = ' '.join(first_row).lower()
            has_headers = any(keyword in first_row_text for keyword in header_keywords)
        
        start_row = 2 if has_headers else 1
        
        for row in range(start_row, max_row + 1):
            col1 = ws.cell(row=row, column=1).value
            col2 = ws.cell(row=row, column=2).value
            
            if col1 and col2:
                data.append({
                    'row_num': row,
                    'start_point': clean_text(str(col1)),
                    'address_chain': clean_text(str(col2)),
                    'original_start': col1,
                    'original_chain': col2
                })
        
        print(f"✅ Успешно прочитано {len(data)} строк с openpyxl")
        return data, wb, ws
        
    except Exception as e1:
        print(f"⚠️ Ошибка openpyxl: {e1}")
        
        try:
            # Пробуем pandas как запасной вариант
            print(f"📖 Пытаюсь прочитать файл с pandas...")
            
            # Определяем расширение файла
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext in ['.xls', '.xlsx', '.xlsm', '.xlsb']:
                # Читаем с pandas
                df = pd.read_excel(file_path, header=None, engine='openpyxl' if file_ext == '.xlsx' else None)
            else:
                # Пробуем все движки
                df = pd.read_excel(file_path, header=None)
            
            # Создаем новый workbook с openpyxl
            wb = openpyxl.Workbook()
            ws = wb.active
            
            # Копируем данные из DataFrame
            for r_idx, row in df.iterrows():
                for c_idx, value in enumerate(row):
                    ws.cell(row=r_idx+1, column=c_idx+1, value=value)
            
            # Собираем данные
            data = []
            for idx, row in df.iterrows():
                if pd.notna(row[0]) and pd.notna(row[1]):
                    data.append({
                        'row_num': idx + 1,
                        'start_point': clean_text(str(row[0])),
                        'address_chain': clean_text(str(row[1])),
                        'original_start': row[0],
                        'original_chain': row[1]
                    })
            
            print(f"✅ Успешно прочитано {len(data)} строк с pandas")
            return data, wb, ws
            
        except Exception as e2:
            print(f"❌ Ошибка pandas: {e2}")
            raise Exception(f"Не удалось прочитать файл. Убедитесь, что это корректный Excel файл. Ошибки: {e1}, {e2}")

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
    
    # Определяем текущее количество колонок
    current_max_col = ws.max_column
    
    # Если уже есть колонки, начинаем после последней
    if current_max_col >= start_col:
        start_col = current_max_col + 1
    
    # Добавляем заголовки
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=start_col + i)
        cell.value = header
        cell.font = Font(bold=True, size=11)
        cell.fill = PatternFill(start_color="FFE4B5", end_color="FFE4B5", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    
    # Настраиваем ширину колонок
    for i in range(len(headers)):
        col_letter = get_column_letter(start_col + i)
        ws.column_dimensions[col_letter].width = 20
    
    return start_col

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None or base <= 0:
        return [None, None]
    
    # Вариации в пределах 2-5%
    variation_percent = random.uniform(0.02, 0.05)
    variation = base * variation_percent
    
    var1 = round(base + random.uniform(variation/2, variation), 1)
    var2 = round(max(10, base - random.uniform(variation/2, variation)), 1)  # минимум 10 км
    
    return [var1, var2]

# ================== TELEGRAM БОТ ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для расчета маршрутов.\n\n"
        "📁 **Отправьте мне Excel файл в формате:**\n"
        "• Колонка A: Стартовая точка\n"
        "• Колонка B: Цепочка адресов через дефис\n\n"
        "**Пример:**\n"
        "A1: Ростов-на-Дону, ул. Оганова 22\n"
        "B1: Ярославская обл., г. Ростов Великий - г. Ярославль\n\n"
        "✅ Я верну тот же файл с результатами расчетов!\n\n"
        "⚡ Используется GraphHopper API\n"
        "📍 Геокодируются только населенные пункты\n"
        "🛣️ Расчет автомобильных маршрутов"
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загруженных документов"""
    if not update.message or not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте файл")
        return
    
    file_name = update.message.document.file_name or "file.xlsx"
    file_name_lower = file_name.lower()
    
    # Проверяем расширение файла
    allowed_extensions = ['.xlsx', '.xls', '.xlsm', '.xlsb', '.ods']
    
    if not any(file_name_lower.endswith(ext) for ext in allowed_extensions):
        await update.message.reply_text(
            "❌ Пожалуйста, отправьте файл в формате Excel:\n"
            "• .xlsx (рекомендуется)\n"
            "• .xls\n"
            "• .xlsm\n"
            "• .xlsb\n"
            "• .ods"
        )
        return
    
    try:
        # Скачиваем файл
        file = await update.message.document.get_file()
        user_id = update.message.from_user.id
        timestamp = int(time.time())
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            input_file = tmp_file.name
        
        await file.download_to_drive(input_file)
        
        # Проверяем размер файла
        file_size = os.path.getsize(input_file)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            await update.message.reply_text("❌ Файл слишком большой (максимум 10 МБ)")
            os.remove(input_file)
            return
        
        await update.message.reply_text(f"📥 Файл получен: {file_name}")
        
        # Читаем данные из Excel
        try:
            routes, wb, ws = read_excel_with_fallback(input_file)
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка чтения файла: {str(e)[:200]}\n\n"
                                           "Убедитесь, что файл не поврежден и является корректным Excel файлом.")
            if os.path.exists(input_file):
                os.remove(input_file)
            return
        
        total = len(routes)
        
        if total == 0:
            await update.message.reply_text(
                "❌ В файле нет данных или неправильный формат.\n\n"
                "Проверьте, что:\n"
                "1. В колонке A есть стартовые точки\n"
                "2. В колонке B есть цепочки адресов\n"
                "3. Данные начинаются с первой строки (или со второй, если есть заголовки)"
            )
            if os.path.exists(input_file):
                os.remove(input_file)
            return
        
        # Отправляем начальное сообщение
        progress_msg = await update.message.reply_text(
            f"⏳ Начинаю обработку...\n"
            f"📊 Всего строк: {total}\n"
            f"🔑 API: GraphHopper\n"
            f"⏱️ Ориентировочное время: {total * 3} секунд"
        )
        
        # Добавляем колонки для результатов
        start_col = add_result_columns(ws, start_col=3)
        
        # Настройки для обработки
        geocode_cache = {}
        processed = 0
        errors = 0
        geocode_errors = 0
        route_errors = 0
        successful = 0
        
        # Обрабатываем каждую строку
        for route in routes:
            try:
                row_num = route['row_num']
                start_point = route['start_point']
                address_chain = route['address_chain']
                
                print(f"\n{'='*60}")
                print(f"📝 Строка {row_num}/{total}")
                print(f"🏁 Старт: {start_point[:50]}...")
                print(f"🛣️ Маршрут: {address_chain[:50]}...")
                
                # ===== ГЕОКОДИРОВАНИЕ СТАРТОВОЙ ТОЧКИ =====
                start_simplified = simplify_address_for_geocoding(start_point)
                cache_key_start = f"start_{start_simplified}"
                
                if cache_key_start in geocode_cache:
                    start_coords = geocode_cache[cache_key_start]
                    print(f"✅ Старт из кэша: {start_coords}")
                else:
                    start_coords = graphhopper_geocode(start_point)
                    time.sleep(0.3)  # Пауза между запросами
                    if start_coords:
                        geocode_cache[cache_key_start] = start_coords
                    else:
                        print(f"❌ Ошибка геокодирования старта: {start_point}")
                        geocode_errors += 1
                        errors += 1
                        
                        # Записываем ошибку
                        ws.cell(row=row_num, column=start_col).value = "❌ Ошибка геокодирования старта"
                        ws.cell(row=row_num, column=start_col+1).value = "Ошибка"
                        ws.cell(row=row_num, column=start_col+2).value = "Ошибка"
                        ws.cell(row=row_num, column=start_col+3).value = 0
                        ws.cell(row=row_num, column=start_col+4).value = "Ошибка"
                        ws.cell(row=row_num, column=start_col+5).value = "Ошибка"
                        
                        processed += 1
                        continue
                
                # ===== ПАРСИНГ ЦЕПОЧКИ АДРЕСОВ =====
                # Извлекаем регион из первого адреса цепочки
                first_address_region = None
                if address_chain and '-' in address_chain:
                    first_part = address_chain.split('-')[0].strip()
                    first_address_region = extract_region_from_address(first_part)
                
                addresses = parse_address_chain(address_chain, first_address_region)
                
                if not addresses:
                    print(f"⚠️ Не удалось распарсить цепочку адресов")
                    errors += 1
                    
                    ws.cell(row=row_num, column=start_col).value = "❌ Ошибка парсинга адресов"
                    ws.cell(row=row_num, column=start_col+1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=start_col+2).value = "Ошибка"
                    ws.cell(row=row_num, column=start_col+3).value = 0
                    ws.cell(row=row_num, column=start_col+4).value = "Ошибка"
                    ws.cell(row=row_num, column=start_col+5).value = "Ошибка"
                    
                    processed += 1
                    continue
                
                # ===== ГЕОКОДИРОВАНИЕ ТОЧЕК МАРШРУТА =====
                all_coords = []
                all_coords_str = []
                has_geocode_error = False
                
                for i, addr in enumerate(addresses):
                    addr_simplified = simplify_address_for_geocoding(addr)
                    cache_key_addr = f"addr_{addr_simplified}"
                    
                    if cache_key_addr in geocode_cache:
                        coords = geocode_cache[cache_key_addr]
                        print(f"✅ Точка {i+1} из кэша: {coords}")
                    else:
                        coords = graphhopper_geocode(addr)
                        time.sleep(0.3)  # Пауза между запросами
                        if coords:
                            geocode_cache[cache_key_addr] = coords
                        else:
                            print(f"❌ Ошибка геокодирования точки {i+1}: {addr}")
                            has_geocode_error = True
                            geocode_errors += 1
                            break
                    
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                
                if has_geocode_error or not all_coords:
                    errors += 1
                    
                    ws.cell(row=row_num, column=start_col).value = "❌ Ошибка геокодирования точек"
                    ws.cell(row=row_num, column=start_col+1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=start_col+2).value = "Ошибка" if not all_coords_str else "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=start_col+3).value = len(addresses)
                    ws.cell(row=row_num, column=start_col+4).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                    ws.cell(row=row_num, column=start_col+5).value = "Ошибка"
                    
                    processed += 1
                    continue
                
                # ===== РАСЧЕТ МАРШРУТА =====
                route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                full_coordinates = [start_coords] + all_coords
                
                print(f"📍 Строю маршрут через {len(full_coordinates)} точек...")
                
                distance = graphhopper_route_with_waypoints(full_coordinates)
                time.sleep(0.5)  # Пауза для API
                
                if distance and distance > 0:
                    d2, d3 = variations(distance)
                    
                    # Записываем успешный результат
                    ws.cell(row=row_num, column=start_col).value = "✅ Успешно"
                    ws.cell(row=row_num, column=start_col+1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=start_col+2).value = "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=start_col+3).value = len(addresses)
                    ws.cell(row=row_num, column=start_col+4).value = route_type
                    ws.cell(row=row_num, column=start_col+5).value = distance
                    ws.cell(row=row_num, column=start_col+6).value = d2 if d2 else ""
                    ws.cell(row=row_num, column=start_col+7).value = d3 if d3 else ""
                    
                    successful += 1
                    print(f"✅ Успешно: {distance} км")
                else:
                    route_errors += 1
                    errors += 1
                    
                    ws.cell(row=row_num, column=start_col).value = "⚠️ Ошибка расчета маршрута"
                    ws.cell(row=row_num, column=start_col+1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=start_col+2).value = "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=start_col+3).value = len(addresses)
                    ws.cell(row=row_num, column=start_col+4).value = route_type
                    ws.cell(row=row_num, column=start_col+5).value = "Ошибка"
                    
                    print(f"⚠️ Ошибка расчета маршрута")
                
                processed += 1
                
                # ===== ОБНОВЛЕНИЕ ПРОГРЕССА =====
                if processed % 2 == 0 or processed == total:
                    try:
                        progress_percent = int((processed / total) * 100)
                        
                        progress_text = (
                            f"⏳ Обработка: {processed}/{total} ({progress_percent}%)\n"
                            f"✅ Успешно: {successful}\n"
                            f"❌ Ошибки: {errors}\n"
                        )
                        
                        if geocode_errors > 0:
                            progress_text += f"📍 Геокодирование: {geocode_errors}\n"
                        
                        if route_errors > 0:
                            progress_text += f"🛣️ Маршруты: {route_errors}\n"
                        
                        settlement = extract_settlement_from_address(start_point)
                        if settlement:
                            progress_text += f"📍 Текущий: {settlement[:30]}..."
                        
                        await progress_msg.edit_text(progress_text)
                    except Exception as e:
                        print(f"⚠️ Ошибка обновления прогресса: {e}")
                        
            except Exception as e:
                print(f"❌ Критическая ошибка в строке {row_num}: {e}")
                errors += 1
                processed += 1
        
        # ===== СОХРАНЕНИЕ И ОТПРАВКА РЕЗУЛЬТАТА =====
        try:
            await progress_msg.edit_text(
                f"✅ Обработка завершена!\n"
                f"📊 Итоги:\n"
                f"• Всего строк: {total}\n"
                f"• Успешно: {successful}\n"
                f"• Ошибок: {errors}\n"
                f"  └ Геокодирование: {geocode_errors}\n"
                f"  └ Расчет маршрутов: {route_errors}\n\n"
                f"💾 Сохраняю результаты..."
            )
        except:
            pass
        
        # Сохраняем результат
        output_file = f"results_{user_id}_{timestamp}.xlsx"
        wb.save(output_file)
        
        # Отправляем результат
        try:
            with open(output_file, "rb") as file:
                caption = (
                    f"✅ Обработка завершена!\n\n"
                    f"📊 **Статистика:**\n"
                    f"• Всего строк: {total}\n"
                    f"• Успешно: {successful}\n"
                    f"• Ошибок: {errors}\n\n"
                    f"⚡ **Использовано:**\n"
                    f"• GraphHopper API\n"
                    f"• Геокодирование по населенным пунктам\n"
                    f"• Расчет автомобильных маршрутов\n\n"
                    f"📎 Файл: {file_name}"
                )
                
                await update.message.reply_document(
                    document=file,
                    filename=f"результаты_{file_name}",
                    caption=caption,
                    parse_mode='Markdown'
                )
            
            print(f"✅ Файл отправлен пользователю {user_id}")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка отправки файла: {str(e)[:200]}")
        
        # ===== ОЧИСТКА =====
        try:
            if os.path.exists(input_file):
                os.remove(input_file)
            if os.path.exists(output_file):
                os.remove(output_file)
        except Exception as e:
            print(f"⚠️ Ошибка очистки файлов: {e}")
        
    except Exception as e:
        error_msg = str(e)[:500]
        await update.message.reply_text(f"❌ Критическая ошибка: {error_msg}\n\n"
                                       "Пожалуйста, попробуйте:\n"
                                       "1. Сохранить файл как .xlsx\n"
                                       "2. Проверить, что файл не поврежден\n"
                                       "3. Отправить файл заново")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
📋 **Доступные команды:**

/start - Начало работы
/help - Эта справка

📁 **Формат Excel файла:**

| Колонка A | Колонка B |
|-----------|-----------|
| Стартовая точка | Цепочка адресов через дефис |

📍 **Пример данных:**
A1: Ростов-на-Дону, Оганова 22
B1: Ярославская обл., г. Ростов Великий - г. Ярославль

📊 **Добавляемые колонки:**
1. Статус обработки
2. Координаты старта
3. Координаты точек
4. Количество точек
5. Тип маршрута
6. Расстояние 1 (км)
7. Расстояние 2 (км)
8. Расстояние 3 (км)

⚡ **Особенности:**
• Используется GraphHopper API
• Геокодируются только города/населенные пункты
• Улицы и номера домов игнорируются
• Автоматическое применение регионов
"""
    await update.message.reply_text(help_text)

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовая команда для проверки работы бота"""
    await update.message.reply_text(
        "🤖 Бот работает!\n\n"
        "Отправьте Excel файл для расчета маршрутов.\n\n"
        "GraphHopper API: " + ("✅ Доступен" if GRAPHHOPPER_API_KEY else "❌ Не настроен")
    )

# ================== ЗАПУСК БОТА ==================
async def run_bot():
    """Запускает бота с обработкой конфликтов"""
    print("=" * 60)
    print("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА")
    print("=" * 60)
    
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("Установите переменную окружения BOT_TOKEN в Render")
        return
    
    print(f"✅ Токен получен")
    print(f"✅ GraphHopper API ключ: {'✅ Настроен' if GRAPHHOPPER_API_KEY else '❌ Не настроен'}")
    
    if not GRAPHHOPPER_API_KEY:
        print("⚠️ ВНИМАНИЕ: GraphHopper API ключ не установлен!")
        print("Добавьте переменную GRAPHHOPPER_API_KEY в Render")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("test", test_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Пытаемся запустить бота
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
                drop_pending_updates=True,
                timeout=30,
                poll_interval=0.5
            )
            
            print("🤖 Бот работает и ожидает сообщений...")
            print("ℹ️ Для остановки нажмите Ctrl+C")
            
            # Бесконечный цикл
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
                break
                
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            break

def main():
    """Основная функция запуска"""
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