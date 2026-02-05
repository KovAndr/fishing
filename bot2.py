import requests
import openpyxl
import random
import time
import os
import threading
import asyncio
import re
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
from datetime import datetime, timedelta

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
                🔧 Версия: GraphHopper API
            </div>
            <p>Используйте бота в Telegram для расчета маршрутов</p>
        </div>
    </body>
    </html>
    """

@app.route('/health')
def health():
    return {"status": "ok", "service": "telegram-route-bot", "api": "graphhopper"}, 200

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    print(f"🌐 Flask сервер запущен на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ================== НАСТРОЙКИ БОТА ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
GRAPHOPPER_API_KEY = os.getenv("GRAPHOPPER_API_KEY", "2c8e643a-360f-47ab-855d-7e884ce217ad")

# ================== УЛУЧШЕННАЯ ЛОГИКА БОТА ==================
def clean_address_enhanced(address):
    """Улучшенная очистка адреса"""
    if not address:
        return ""
    
    # Удаляем почтовый индекс в начале
    address = re.sub(r'^\d{6},\s*', '', address)
    
    # Нормализуем пробелы
    address = ' '.join(address.split())
    
    # Заменяем все виды тире на обычный дефис
    address = re.sub(r'[–—−]', '-', address)
    
    # Удаляем лишние символы, но сохраняем буквы, цифры, пробелы, запятые, точки, дефисы
    address = re.sub(r'[^\w\s\.,\-]', '', address)
    
    # Удаляем мусорные слова
    address = re.sub(r'\b(?:ул\.|ул\b|пер\.|пр\.|пр-т|пр-кт|б-р|ш\.|г\.|г\b|обл\.|р-н|р\b|с\.|ст-ца|х\.|п\.|пос\.|мкр\.|кв\.|д\.|корп\.|стр\.|лит\.)\b\.?', '', address, flags=re.IGNORECASE)
    
    # Нормализуем запятые
    address = re.sub(r'\s*,\s*', ', ', address)
    
    # Удаляем двойные пробелы
    address = re.sub(r'\s+', ' ', address)
    
    return address.strip()

def parse_address_chain_enhanced(address_string):
    """Улучшенный парсинг цепочки адресов"""
    if not address_string:
        return []
    
    # Нормализуем разделители
    address_string = re.sub(r'[–—−]', '-', address_string)
    
    # Заменяем " - " на разделитель
    address_string = re.sub(r'\s*-\s*', '|SEP|', address_string)
    
    # Разделяем
    parts = address_string.split('|SEP|')
    
    # Очищаем и фильтруем
    addresses = []
    for part in parts:
        cleaned = clean_address_enhanced(part)
        if cleaned and len(cleaned) > 5:
            addresses.append(cleaned)
    
    return addresses

def validate_coordinates(lat, lon):
    """Проверка координат на валидность (в пределах России и близлежащих стран)"""
    try:
        # Российские координаты и ближнее зарубежье
        if 40 <= lat <= 80 and 19 <= lon <= 180:
            return True
        return False
    except:
        return False

def graphhopper_geocode_enhanced(address, max_retries=3):
    """Улучшенное геокодирование через GraphHopper с повторными попытками"""
    if not GRAPHOPPER_API_KEY:
        print("⚠️ GRAPHOPPER_API_KEY не установлен!")
        return None
    
    cleaned_address = clean_address_enhanced(address)
    
    for attempt in range(max_retries):
        try:
            url = "https://graphhopper.com/api/1/geocode"
            params = {
                "q": f"{cleaned_address}, Россия",
                "locale": "ru",
                "limit": 1,
                "key": GRAPHOPPER_API_KEY,
                "provider": "default"
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("hits") and len(data["hits"]) > 0:
                    lat = data["hits"][0]["point"]["lat"]
                    lon = data["hits"][0]["point"]["lng"]
                    
                    if validate_coordinates(lat, lon):
                        return (lat, lon)
                    else:
                        print(f"⚠️ Невалидные координаты для {address}: {lat}, {lon}")
                        return None
                else:
                    print(f"⚠️ Адрес не найден: {address}")
                    
            elif response.status_code == 429:
                wait_time = 2 ** attempt
                print(f"⚠️ Rate limit, жду {wait_time} секунд...")
                time.sleep(wait_time)
                continue
                
            else:
                print(f"⚠️ Ошибка геокодирования {address}: {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ Ошибка при геокодировании {address}: {e}")
        
        time.sleep(1)  # Пауза между попытками
    
    return None

def graphhopper_route_simple(points, profile="car"):
    """Простой расчет маршрута через GraphHopper (до 10 точек)"""
    if not GRAPHOPPER_API_KEY:
        print("⚠️ GRAPHOPPER_API_KEY не установлен!")
        return None
    
    if len(points) < 2:
        return None
    
    try:
        # Формируем строку точек
        points_param = []
        for lat, lon in points:
            points_param.append(f"point={lat},{lon}")
        
        points_str = "&".join(points_param)
        
        url = f"https://graphhopper.com/api/1/route?{points_str}&profile={profile}&locale=ru&instructions=false&calc_points=false&key={GRAPHOPPER_API_KEY}"
        
        response = requests.get(url, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if "paths" in data and len(data["paths"]) > 0:
                distance_meters = data["paths"][0]["distance"]
                distance_km = distance_meters / 1000
                
                # Базовые проверки на валидность расстояния
                if 1 <= distance_km <= 20000:
                    return round(distance_km, 1)
                else:
                    print(f"⚠️ Нереалистичное расстояние: {distance_km} км")
                    return None
            else:
                print(f"⚠️ Нет данных о маршруте")
                return None
        else:
            print(f"⚠️ Ошибка API: {response.status_code}, текст: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {e}")
        return None

def calculate_route_optimized(points):
    """Оптимизированный расчет маршрута"""
    if len(points) < 2:
        return None
    
    # Ограничиваем количество точек для одного запроса
    if len(points) > 15:
        print(f"⚠️ Слишком много точек ({len(points)}), ограничиваю до 10")
        points = points[:10]  # Берем только первые 10 точек
    
    # Пробуем разные профили транспорта
    profiles = ["car", "small_truck", "truck"]
    
    for profile in profiles:
        distance = graphhopper_route_simple(points, profile)
        if distance:
            print(f"✅ Успешно рассчитано с профилем {profile}: {distance} км")
            return distance
        
        time.sleep(1)  # Пауза между попытками
    
    return None

def variations_enhanced(base_distance):
    """Генерация вариаций расстояния"""
    if base_distance is None or base_distance <= 0:
        return [None, None]
    
    try:
        # Рассчитываем отклонение в зависимости от расстояния
        if base_distance < 100:
            # Для коротких расстояний - небольшой процент
            deviation_percent = random.uniform(2, 5)
        elif base_distance < 500:
            deviation_percent = random.uniform(3, 7)
        elif base_distance < 1000:
            deviation_percent = random.uniform(4, 8)
        else:
            # Для длинных расстояний - фиксированный процент
            deviation_percent = random.uniform(5, 10)
        
        deviation = base_distance * deviation_percent / 100
        
        return [
            round(base_distance + deviation, 1),
            round(max(1, base_distance - deviation), 1)
        ]
    except:
        return [
            round(base_distance * 1.05, 1),
            round(base_distance * 0.95, 1)
        ]

def read_from_excel_enhanced(path):
    """Улучшенное чтение Excel файла"""
    try:
        wb = load_workbook(path, data_only=True)
        ws = wb.active
        
        routes = []
        
        # Ищем колонки с данными
        start_col = None
        chain_col = None
        
        # Пробуем найти заголовки
        for col in range(1, min(10, ws.max_column + 1)):
            cell_value = ws.cell(row=1, column=col).value
            if cell_value:
                cell_lower = str(cell_value).lower()
                if any(keyword in cell_lower for keyword in ['пункт', 'отправ', 'старт', 'начало']):
                    start_col = col
                elif any(keyword in cell_lower for keyword in ['пункт', 'назнач', 'цель', 'адрес', 'маршрут', 'точк']):
                    chain_col = col
        
        # Если не нашли заголовки, используем первые две колонки
        if start_col is None:
            start_col = 1
        if chain_col is None:
            chain_col = 2
        
        # Читаем данные
        start_row = 2 if ws.cell(row=1, column=start_col).value else 1
        
        for row in range(start_row, ws.max_row + 1):
            start_point = ws.cell(row=row, column=start_col).value
            address_chain = ws.cell(row=row, column=chain_col).value
            
            if start_point and address_chain:
                routes.append({
                    'row_num': row,
                    'start_point': str(start_point).strip(),
                    'address_chain': str(address_chain).strip()
                })
        
        print(f"📊 Прочитано {len(routes)} маршрутов из файла")
        return routes, wb, ws
        
    except Exception as e:
        print(f"❌ Ошибка чтения Excel: {e}")
        return [], None, None

def add_result_columns_enhanced(ws):
    """Добавление колонок для результатов"""
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
    
    # Определяем первую пустую колонку
    result_start_col = ws.max_column + 1
    
    # Добавляем заголовки
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=result_start_col + i)
        cell.value = header
        cell.font = Font(bold=True, color="000000")
        cell.fill = PatternFill(start_color="FFE4B5", end_color="FFE4B5", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    
    # Настраиваем ширину колонок
    for col in range(ws.max_column + 1, ws.max_column + len(headers) + 1):
        col_letter = openpyxl.utils.get_column_letter(col)
        if col - ws.max_column - 1 == 0:  # Статус
            ws.column_dimensions[col_letter].width = 20
        elif col - ws.max_column - 1 == 1:  # Координаты старта
            ws.column_dimensions[col_letter].width = 25
        elif col - ws.max_column - 1 == 2:  # Координаты точек
            ws.column_dimensions[col_letter].width = 40
        elif col - ws.max_column - 1 == 3:  # Количество точек
            ws.column_dimensions[col_letter].width = 15
        elif col - ws.max_column - 1 == 4:  # Тип маршрута
            ws.column_dimensions[col_letter].width = 25
        else:  # Расстояния
            ws.column_dimensions[col_letter].width = 18
    
    return result_start_col

# ================== TELEGRAM БОТ ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для расчета маршрутов.\n\n"
        "📁 **Отправьте мне Excel файл в формате:**\n"
        "• Колонка 1: Стартовая точка\n"
        "• Колонка 2: Цепочка адресов через дефис\n\n"
        "📋 **Пример строки:**\n"
        "`Ростов-на-Дону, Оганова 22`\n"
        "`Воронеж, ул. Ипподромная 18А - Сергиев Посад, ул. Кирова 89`\n\n"
        "✅ Я верну файл с рассчитанными расстояниями!"
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загруженных документов"""
    if not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте файл")
        return
    
    file_name = update.message.document.file_name.lower()
    if not (file_name.endswith('.xlsx') or file_name.endswith('.xls')):
        await update.message.reply_text("❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)")
        return
    
    # Скачиваем файл
    file = await update.message.document.get_file()
    user_id = update.message.from_user.id
    timestamp = int(time.time())
    input_file = f"input_{user_id}_{timestamp}.xlsx"
    
    try:
        await file.download_to_drive(input_file)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка загрузки файла: {e}")
        return
    
    # Читаем данные
    routes, wb, ws = read_from_excel_enhanced(input_file)
    if not routes:
        await update.message.reply_text(
            "❌ Не удалось прочитать данные из файла.\n"
            "Убедитесь, что файл содержит минимум 2 колонки с адресами."
        )
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    # Добавляем колонки для результатов
    result_start_col = add_result_columns_enhanced(ws)
    
    # Отправляем сообщение о начале обработки
    total_routes = len(routes)
    progress_msg = await update.message.reply_text(
        f"⏳ Начинаю обработку...\n"
        f"Всего маршрутов: {total_routes}\n"
        f"Статус: готовится..."
    )
    
    # Кэш для геокодирования
    geocode_cache = {}
    
    # Статистика
    stats = {
        'processed': 0,
        'success': 0,
        'geocode_errors': 0,
        'route_errors': 0
    }
    
    # Обрабатываем каждый маршрут
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            address_chain = route['address_chain']
            
            # Проверяем, не обрабатывалась ли уже эта строка
            existing_status = ws.cell(row=row_num, column=result_start_col).value
            if existing_status and ("✅" in str(existing_status) or "⚠️" in str(existing_status)):
                stats['processed'] += 1
                continue
            
            # Геокодируем стартовую точку
            if start_point in geocode_cache:
                start_coords = geocode_cache[start_point]
            else:
                start_coords = graphhopper_geocode_enhanced(start_point)
                geocode_cache[start_point] = start_coords
                time.sleep(0.5)  # Пауза между запросами геокодирования
            
            if not start_coords:
                # Записываем ошибку геокодирования
                ws.cell(row=row_num, column=result_start_col).value = "⚠️ Ошибка геокодирования"
                ws.cell(row=row_num, column=result_start_col + 5).value = "Ошибка"
                stats['geocode_errors'] += 1
                stats['processed'] += 1
                continue
            
            # Парсим цепочку адресов
            addresses = parse_address_chain_enhanced(address_chain)
            if not addresses:
                ws.cell(row=row_num, column=result_start_col).value = "⚠️ Нет адресов в цепочке"
                ws.cell(row=row_num, column=result_start_col + 5).value = "Ошибка"
                stats['geocode_errors'] += 1
                stats['processed'] += 1
                continue
            
            # Геокодируем адреса из цепочки
            all_coords = []
            all_coords_str = []
            geocode_failed = False
            
            for addr in addresses:
                if addr in geocode_cache:
                    coords = geocode_cache[addr]
                else:
                    coords = graphhopper_geocode_enhanced(addr)
                    geocode_cache[addr] = coords
                    time.sleep(0.5)  # Пауза между запросами геокодирования
                
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    geocode_failed = True
                    print(f"❌ Не удалось геокодировать адрес: {addr}")
                    break
            
            if geocode_failed or not all_coords:
                ws.cell(row=row_num, column=result_start_col).value = "⚠️ Ошибка геокодирования точек"
                ws.cell(row=row_num, column=result_start_col + 1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=result_start_col + 2).value = "; ".join(all_coords_str) if all_coords_str else "Ошибка"
                ws.cell(row=row_num, column=result_start_col + 3).value = len(addresses)
                ws.cell(row=row_num, column=result_start_col + 4).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                ws.cell(row=row_num, column=result_start_col + 5).value = "Ошибка"
                stats['geocode_errors'] += 1
                stats['processed'] += 1
                continue
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
            
            # Рассчитываем маршрут
            full_coordinates = [start_coords] + all_coords
            distance = calculate_route_optimized(full_coordinates)
            time.sleep(1)  # Пауза между запросами расчета маршрута
            
            if distance:
                # Генерируем вариации
                d2, d3 = variations_enhanced(distance)
                
                # Записываем результаты
                ws.cell(row=row_num, column=result_start_col).value = "✅ Успешно"
                ws.cell(row=row_num, column=result_start_col + 1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=result_start_col + 2).value = "; ".join(all_coords_str)
                ws.cell(row=row_num, column=result_start_col + 3).value = len(addresses)
                ws.cell(row=row_num, column=result_start_col + 4).value = route_type
                ws.cell(row=row_num, column=result_start_col + 5).value = distance
                ws.cell(row=row_num, column=result_start_col + 6).value = d2
                ws.cell(row=row_num, column=result_start_col + 7).value = d3
                
                stats['success'] += 1
            else:
                ws.cell(row=row_num, column=result_start_col).value = "⚠️ Ошибка расчета маршрута"
                ws.cell(row=row_num, column=result_start_col + 1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                ws.cell(row=row_num, column=result_start_col + 2).value = "; ".join(all_coords_str)
                ws.cell(row=row_num, column=result_start_col + 3).value = len(addresses)
                ws.cell(row=row_num, column=result_start_col + 4).value = route_type
                ws.cell(row=row_num, column=result_start_col + 5).value = "Ошибка"
                ws.cell(row=row_num, column=result_start_col + 6).value = ""
                ws.cell(row=row_num, column=result_start_col + 7).value = ""
                
                stats['route_errors'] += 1
            
            stats['processed'] += 1
            
            # Обновляем прогресс каждые 5 маршрутов
            if stats['processed'] % 5 == 0 or stats['processed'] == total_routes:
                try:
                    progress_text = (
                        f"⏳ Обработка: {stats['processed']} / {total_routes}\n"
                        f"✅ Успешно: {stats['success']}\n"
                        f"📍 Ошибки геокодирования: {stats['geocode_errors']}\n"
                        f"🛣️ Ошибки расчета: {stats['route_errors']}\n"
                        f"⏱️ API: GraphHopper"
                    )
                    await progress_msg.edit_text(progress_text)
                except:
                    pass
                
        except Exception as e:
            print(f"❌ Критическая ошибка при обработке строки {route.get('row_num', 'N/A')}: {e}")
            stats['processed'] += 1
            stats['route_errors'] += 1
    
    # Сохраняем результат
    output_file = f"results_{user_id}_{timestamp}.xlsx"
    wb.save(output_file)
    
    # Отправляем результат
    try:
        final_text = (
            f"✅ Обработка завершена!\n\n"
            f"📊 **Статистика:**\n"
            f"• Всего маршрутов: {total_routes}\n"
            f"• ✅ Успешно: {stats['success']}\n"
            f"• 📍 Ошибки геокодирования: {stats['geocode_errors']}\n"
            f"• 🛣️ Ошибки расчета: {stats['route_errors']}\n\n"
            f"🔧 Использованный API: GraphHopper"
        )
        
        await update.message.reply_text(final_text)
        
        with open(output_file, "rb") as file:
            await update.message.reply_document(
                document=file,
                filename=f"результаты_{user_id}.xlsx",
                caption="📊 Результаты расчета маршрутов"
            )
            
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка отправки файла: {e}")
    
    # Очищаем временные файлы
    try:
        if os.path.exists(input_file):
            os.remove(input_file)
        if os.path.exists(output_file):
            os.remove(output_file)
    except:
        pass

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
📋 **Доступные команды:**

/start - Начать работу с ботом
/help - Показать эту справку
/test - Проверить работу API

📁 **Формат Excel файла:**
• Колонка 1: Стартовая точка (точка А)
• Колонка 2: Цепочка адресов через дефис

📍 **Пример строки:**
`Ростов-на-Дону, Оганова 22`
`Воронеж, ул. Ипподромная 18А - Сергиев Посад, ул. Кирова 89`

📊 **Добавляемые колонки:**
1. Статус обработки
2. Координаты старта
3. Координаты точек
4. Количество точек
5. Тип маршрута
6. Расстояние 1 (км) - основное
7. Расстояние 2 (км) - + вариант
8. Расстояние 3 (км) - - вариант

⚠️ **Ограничения:**
• Максимум 10 точек в маршруте
• API: GraphHopper (бесплатный тариф)
• Геокодирование: только Россия и ближнее зарубежье
"""
    await update.message.reply_text(help_text)

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка работы API"""
    await update.message.reply_text("🧪 Проверяю работу API...")
    
    test_address = "Москва, Красная площадь"
    
    # Проверяем геокодирование
    coords = graphhopper_geocode_enhanced(test_address)
    
    if coords:
        await update.message.reply_text(
            f"📍 **Геокодирование:** ✅ Работает\n"
            f"Адрес: {test_address}\n"
            f"Координаты: {coords[0]:.6f}, {coords[1]:.6f}"
        )
        
        # Проверяем расчет маршрута
        spb_coords = graphhopper_geocode_enhanced("Санкт-Петербург, Дворцовая площадь")
        if spb_coords:
            distance = calculate_route_optimized([coords, spb_coords])
            if distance:
                await update.message.reply_text(
                    f"🛣️ **Расчет маршрута:** ✅ Работает\n"
                    f"Москва → Санкт-Петербург: {distance} км"
                )
            else:
                await update.message.reply_text("🛣️ **Расчет маршрута:** ❌ Ошибка")
    else:
        await update.message.reply_text("📍 **Геокодирование:** ❌ Ошибка")

async def example_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет пример файла"""
    example_text = """
📋 **Пример Excel файла:**

| Стартовая точка | Цепочка адресов |
|-----------------|-----------------|
| Ростов-на-Дону, ул. Оганова 22 | Воронеж, ул. Ипподромная 18А |
| Ростов-на-Дону, ул. Оганова 22 | Воронеж, ул. Ипподромная 18А - Сергиев Посад, ул. Кирова 89 |
| Ростов-на-Дону, ул. Оганова 22 | Ярославль, ул. Магистральная 1 - Ростов Великий, ул. Покровская 42 |

**Советы:**
1. Используйте дефис `-` для разделения адресов в цепочке
2. Указывайте адреса как можно полнее
3. Избегайте специальных символов кроме запятых и дефисов
"""
    await update.message.reply_text(example_text)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статус бота"""
    status_text = """
🤖 **Статус бота:**

✅ **API ключи:**
• GraphHopper: {"установлен" if GRAPHOPPER_API_KEY else "❌ отсутствует"}
• Яндекс: {"установлен" if YANDEX_API_KEY else "не установлен"}

🔧 **Возможности:**
• Геокодирование адресов
• Расчет маршрутов с промежуточными точками
• Обработка Excel файлов
• Поддержка до 10 точек в маршруте

📡 **Используемые сервисы:**
• Основной: GraphHopper API
• Резервный: Яндекс Геокодер
"""
    await update.message.reply_text(status_text)

# ================== ЗАПУСК БОТА ==================
async def run_bot():
    """Запускает Telegram бота"""
    print("=" * 50)
    print("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА")
    print("=" * 50)
    
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("Установите переменную окружения BOT_TOKEN")
        return
    
    print("✅ Инициализация бота...")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("example", example_command))
    application.add_handler(CommandHandler("test", test_command))
    application.add_handler(CommandHandler("status", status_command))
    
    # Добавляем обработчик документов
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Настройка polling
    await application.initialize()
    await application.start()
    
    # Получаем информацию о боте
    bot_info = await application.bot.get_me()
    print(f"✅ Бот запущен: @{bot_info.username}")
    print(f"   ID: {bot_info.id}")
    print(f"   Имя: {bot_info.first_name}")
    
    # Запускаем polling
    await application.updater.start_polling(
        drop_pending_updates=True,
        timeout=30,
        poll_interval=0.5,
        allowed_updates=Update.ALL_TYPES
    )
    
    print("🤖 Бот готов к работе!")
    print("📡 Ожидание сообщений...")
    
    # Бесконечный цикл
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("\n⏹️ Остановка бота...")
        await application.stop()

def main():
    """Основная функция"""
    # Проверяем переменные окружения
    print("🔍 Проверка настроек...")
    
    required_vars = ["BOT_TOKEN", "GRAPHOPPER_API_KEY"]
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("ℹ️ Установите их в настройках Render")
        return
    
    print("✅ Все настройки проверены")
    
    # Проверяем работу на Render
    is_render = os.environ.get('RENDER') is not None
    port = os.environ.get('PORT')
    
    if is_render and port:
        print(f"🌐 Запуск на Render, порт: {port}")
        # Запускаем Flask в отдельном потоке
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        print("✅ Flask сервер запущен")
    
    # Запускаем бота
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("\n👋 Завершение работы")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
[file content end]