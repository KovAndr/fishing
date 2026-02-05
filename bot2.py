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

# ================== ФУНКЦИИ ОБРАБОТКИ АДРЕСОВ ==================
def extract_region_from_address(address):
    """Извлекает регион (область, край, республику) из адреса"""
    if not address:
        return None
    
    # Паттерны для регионов
    region_patterns = [
        r'(?:[А-Яа-я]+(?:\s+[А-Яа-я]+)*\s+(?:обл\.|область|край|респ\.|республика|АО|р-н))',
        r'(?:р\.\s+[А-Яа-я]+)',  # р. Карелия, р. Коми и т.д.
        r'(?:КЧР|КБР|РСО-Алания|р-н)',  # Сокращенные названия
    ]
    
    for pattern in region_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            region = match.group(0)
            # Убираем точку в конце, если есть
            if region.endswith('.'):
                region = region[:-1]
            return region.strip()
    
    return None

def extract_settlement_from_address(address):
    """Извлекает населенный пункт из адреса"""
    if not address:
        return None
    
    # Удаляем регион из начала адреса, если он есть
    address_clean = address
    region = extract_region_from_address(address)
    if region:
        # Удаляем регион и следующие за ним разделители
        address_clean = re.sub(f'^{re.escape(region)}[,\s-]*', '', address_clean)
    
    # Паттерны для населенных пунктов
    settlement_patterns = [
        # г. Москва, г.Санкт-Петербург
        r'(?:г\.|город\s+)([^,]+)',
        # с. Ивановка, п. Горный
        r'(?:с\.|село\s+|п\.|посёлок\s+|пос\.|поселок\s+)([^,]+)',
        # ст-ца Каневская, ст.Ленинградская
        r'(?:ст-ца\s+|ст\.|станица\s+)([^,]+)',
        # д. Петрово, д.Новое
        r'(?:д\.|деревня\s+)([^,]+)',
        # х. Согласный
        r'(?:х\.|хутор\s+)([^,]+)',
        # р.п. Мухтолово
        r'(?:р\.п\.|рабочий посёлок\s+)([^,]+)',
        # пгт. Черноморское
        r'(?:пгт\.|посёлок городского типа\s+)([^,]+)',
        # аул Кошехабль
        r'(?:аул\s+)([^,]+)',
        # с. Александровское
        r'^([А-Яа-я]+(?:\s+[А-Яа-я]+)*)(?=,)',
    ]
    
    for pattern in settlement_patterns:
        match = re.search(pattern, address_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    # Если не нашли по паттернам, берем первое слово после регионов
    words = address_clean.split()
    if words:
        # Пропускаем служебные слова
        for word in words:
            word_lower = word.lower()
            if word_lower not in ['ул.', 'улица', 'пр.', 'проспект', 'пер.', 'переулок', 'ш.', 'шоссе', 'мкр.', 'микрорайон']:
                return word
    
    return None

def parse_address_chain(address_string, default_region=None):
    """Парсит цепочку адресов с учетом региона из первого адреса"""
    if not address_string:
        return []
    
    # Заменяем различные тире на обычный дефис
    address_string = address_string.replace('–', '-').replace('—', '-')
    
    # Разделяем по дефису
    addresses = [addr.strip() for addr in address_string.split('-') if addr.strip()]
    
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
            continue
            
        # Если у текущего адреса нет региона, используем регион из первого адреса
        if not current_region and region_to_use and i > 0:
            # Добавляем регион к населенному пункту
            parsed_address = f"{region_to_use}, {settlement}"
        else:
            # Оставляем как есть (с регионом или без)
            if current_region:
                parsed_address = f"{current_region}, {settlement}"
            else:
                parsed_address = settlement
        
        parsed_addresses.append(parsed_address)
    
    return parsed_addresses

def simplify_address_for_geocoding(address):
    """Упрощает адрес для геокодирования"""
    if not address:
        return address
    
    # Извлекаем регион и населенный пункт
    region = extract_region_from_address(address)
    settlement = extract_settlement_from_address(address)
    
    if not settlement:
        return address
    
    # Формируем упрощенный адрес
    if region:
        simplified = f"{settlement}, {region}, Россия"
    else:
        simplified = f"{settlement}, Россия"
    
    # Для особых случаев
    if "Крым" in address or "Севастополь" in address or "Симферополь" in address:
        simplified = f"{settlement}, Республика Крым, Россия"
    elif "ДНР" in address or "Донецк" in address:
        simplified = f"{settlement}, ДНР"
    elif "Херсон" in address or "Запорож" in address:
        simplified = f"{settlement}, Россия"
    
    return simplified

# ================== ЛОГИКА БОТА ==================
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

def yandex_geocode(address):
    """Геокодирование адреса через Яндекс API"""
    if not YANDEX_API_KEY:
        print("⚠️ YANDEX_API_KEY не установлен!")
        return None
    
    # Упрощаем адрес
    simplified_address = simplify_address_for_geocoding(address)
    
    print(f"📍 Геокодируем: {address[:50]}... -> {simplified_address}")
    
    url = "https://geocode-maps.yandex.ru/1.x/"
    params = {
        "apikey": YANDEX_API_KEY,
        "format": "json",
        "geocode": simplified_address,
        "results": 1,
        "lang": "ru_RU"
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"⚠️ Ошибка геокодирования {r.status_code} для: {simplified_address}")
            return None
        
        data = r.json()
        if (data["response"]["GeoObjectCollection"]["featureMember"] and 
            len(data["response"]["GeoObjectCollection"]["featureMember"]) > 0):
            pos = data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["Point"]["pos"]
            lon, lat = pos.split()
            coords = (float(lat), float(lon))
            print(f"✅ Найдены координаты: {coords}")
            return coords
        else:
            print(f"⚠️ Адрес не найден: {simplified_address}")
            return None
    except Exception as e:
        print(f"⚠️ Ошибка при геокодировании {address}: {e}")
        return None

def ors_route_with_waypoints(coordinates_list):
    """Строит маршрут через промежуточные точки"""
    if not ORS_API_KEY:
        print("⚠️ ORS_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    url = "https://api.openrouteservice.org/v2/directions/driving-car/geojson"
    headers = {"Authorization": ORS_API_KEY}
    
    # Преобразуем координаты в формат [lon, lat]
    coordinates = [[coord[1], coord[0]] for coord in coordinates_list]
    
    body = {"coordinates": coordinates}
    
    try:
        print(f"📍 Строим маршрут через {len(coordinates)} точек...")
        r = requests.post(url, json=body, headers=headers, timeout=60)
        
        if r.status_code != 200:
            print(f"⚠️ Ошибка маршрута: {r.status_code}")
            # Пробуем получить детали ошибки
            try:
                error_details = r.json()
                print(f"⚠️ Детали ошибки: {error_details}")
            except:
                pass
            return None
        
        data = r.json()
        if data.get("features") and data["features"][0].get("properties", {}).get("summary"):
            dist = data["features"][0]["properties"]["summary"]["distance"]
            distance_km = round(dist / 1000, 1)
            print(f"✅ Маршрут построен: {distance_km} км")
            return distance_km
        else:
            print(f"⚠️ Некорректный ответ от ORS")
            return None
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {e}")
        return None

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None:
        return [None, None]
    
    # Генерируем вариации в пределах 5%
    variation_percent = 0.05
    variation = base * variation_percent
    
    return [
        round(base + random.uniform(variation/2, variation), 1),
        round(max(0, base - random.uniform(variation/2, variation)), 1)
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
        "ℹ️ Примечание: Для геокодирования используются только населенные пункты и регионы."
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
    geocode_errors = 0
    route_errors = 0
    
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            address_chain = route['address_chain']
            
            print(f"\n{'='*50}")
            print(f"📝 Обработка строки {row_num}:")
            print(f"Старт: {start_point}")
            print(f"Маршрут: {address_chain}")
            
            # Геокодируем стартовую точку
            start_simplified = simplify_address_for_geocoding(start_point)
            cache_key = f"start_{start_simplified}"
            
            if cache_key in geocode_cache:
                start_coords = geocode_cache[cache_key]
            else:
                start_coords = yandex_geocode(start_point)
                time.sleep(1.5)  # Увеличиваем задержку для соблюдения лимитов API
                if start_coords:
                    geocode_cache[cache_key] = start_coords
            
            # Парсим цепочку адресов с учетом региона из первого адреса
            # Извлекаем регион из первого адреса цепочки
            first_address_region = None
            if address_chain and '-' in address_chain:
                first_part = address_chain.split('-')[0].strip()
                first_address_region = extract_region_from_address(first_part)
            
            addresses = parse_address_chain(address_chain, first_address_region)
            
            # Геокодируем все адреса в цепочке
            all_coords = []
            all_coords_str = []
            has_geocode_error = False
            
            for i, addr in enumerate(addresses):
                addr_simplified = simplify_address_for_geocoding(addr)
                cache_key = f"addr_{addr_simplified}"
                
                if cache_key in geocode_cache:
                    coords = geocode_cache[cache_key]
                else:
                    coords = yandex_geocode(addr)
                    time.sleep(1.5)  # Увеличиваем задержку для соблюдения лимитов API
                    if coords:
                        geocode_cache[cache_key] = coords
                
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                    print(f"✅ Геокодирован [{i+1}]: {addr[:40]}...")
                else:
                    print(f"❌ Ошибка геокодирования [{i+1}]: {addr}")
                    has_geocode_error = True
                    geocode_errors += 1
                    break
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
            
            if has_geocode_error or not start_coords or not all_coords:
                # Записываем ошибку геокодирования
                status = "❌ Ошибка геокодирования"
                start_coords_str = f"{start_coords[0]:.6f},{start_coords[1]:.6f}" if start_coords else "Ошибка"
                coords_str = "; ".join(all_coords_str) if all_coords_str else "Ошибка"
                print(f"❌ Ошибка в строке {row_num}: не удалось геокодировать все адреса")
                errors += 1
            else:
                # Строим маршрут: стартовая точка + все точки из цепочки
                full_coordinates = [start_coords] + all_coords
                
                print(f"📍 Построение маршрута через {len(full_coordinates)} точек...")
                
                # Рассчитываем маршрут
                distance = ors_route_with_waypoints(full_coordinates)
                time.sleep(3)  # Увеличиваем задержку для соблюдения лимитов ORS API
                
                if distance:
                    d2, d3 = variations(distance)
                    status = "✅ Успешно"
                    start_coords_str = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    coords_str = "; ".join(all_coords_str)
                    print(f"✅ Маршрут построен: {distance} км (варианты: {d2}, {d3})")
                else:
                    status = "⚠️ Ошибка расчета маршрута"
                    start_coords_str = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    coords_str = "; ".join(all_coords_str)
                    print(f"⚠️ Ошибка расчета маршрута для строки {row_num}")
                    route_errors += 1
                    errors += 1
            
            # Записываем результаты
            ws.cell(row=row_num, column=3).value = status
            ws.cell(row=row_num, column=4).value = start_coords_str
            ws.cell(row=row_num, column=5).value = coords_str
            ws.cell(row=row_num, column=6).value = len(addresses)
            ws.cell(row=row_num, column=7).value = route_type
            
            if status == "✅ Успешно":
                ws.cell(row=row_num, column=8).value = distance
                ws.cell(row=row_num, column=9).value = d2
                ws.cell(row=row_num, column=10).value = d3
                
                # Форматируем ячейки с расстояниями
                for col in [8, 9, 10]:
                    cell = ws.cell(row=row_num, column=col)
                    cell.number_format = '0.0'
            else:
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=9).value = ""
                ws.cell(row=row_num, column=10).value = ""
            
            processed += 1
            
            # Обновляем прогресс каждые 2 строки или в конце
            if processed % 2 == 0 or processed == total:
                try:
                    success_count = processed - errors
                    progress_percent = int((processed / total) * 100)
                    
                    progress_text = (
                        f"⏳ Обработка: {processed}/{total} ({progress_percent}%)\n"
                        f"✅ Успешно: {success_count}\n"
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
            print(f"❌ Критическая ошибка обработки строки {route.get('row_num', 'N/A')}: {e}")
            errors += 1
    
    try:
        success_count = processed - errors
        await progress_msg.edit_text(
            f"✅ Обработка завершена!\n"
            f"📊 Статистика:\n"
            f"• Всего строк: {total}\n"
            f"• Успешно: {success_count}\n"
            f"• Ошибок: {errors}\n"
            f"  └ Геокодирование: {geocode_errors}\n"
            f"  └ Расчет маршрутов: {route_errors}\n\n"
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
            success_count = processed - errors
            await update.message.reply_document(
                document=file,
                filename=f"результаты_{timestamp}.xlsx",
                caption=(
                    f"✅ Обработка завершена!\n"
                    f"📊 Статистика:\n"
                    f"• Всего строк: {total}\n"
                    f"• Успешно: {success_count}\n"
                    f"• Ошибок: {errors}\n"
                    f"  └ Геокодирование: {geocode_errors}\n"
                    f"  └ Расчет маршрутов: {route_errors}\n\n"
                    f"ℹ️ Примечания:\n"
                    f"• Для геокодирования используются населенные пункты\n"
                    f"• Регион из первого адреса применяется к последующим\n"
                    f"• Улицы и номера домов игнорируются"
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
📋 **Доступные команды:**

/start - Начать работу с ботом
/help - Показать эту справку

📁 **Формат Excel файла:**
• Колонка A: Стартовая точка (точка А)
• Колонка B: Цепочка адресов через дефис

📍 **Пример строки в колонке B:**
`Ярославская обл., г. Ростов Великий, ул. Покровская 42/19 - г. Ярославль, ул. Магистральная 1`

📊 **Добавляемые колонки результатов:**
1. Статус обработки
2. Координаты старта
3. Координаты точек
4. Количество точек
5. Тип маршрута
6. Расстояние 1 (км)
7. Расстояние 2 (км)
8. Расстояние 3 (км)

**🔥 Особенности обработки:**
• Используются только населенные пункты (города, села, поселки)
• Улицы и номера домов игнорируются
• Регион из первого адреса применяется к последующим адресам в цепочке
• Для геокодирования добавляется "Россия"

**⏱️ Время обработки:**
• ~3-5 секунд на строку
• Для больших файлов может потребоваться время
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def example_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /example - отправляет пример файла"""
    await update.message.reply_text(
        "📋 Пример Excel файла:\n\n"
        "| Колонка A | Колонка B |\n"
        "|-----------|-----------|\n"
        "| Ростов-на-Дону, Оганова 22 | Ярославская обл., г. Ростов Великий |\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж - г. Сергиев Посад |\n"
        "| Ростов-на-Дону, Оганова 22 | р. Карелия, г. Петрозаводск - г. Беломорск |\n\n"
        "Просто создайте Excel файл с такими данными и отправьте боту!\n\n"
        "ℹ️ Регион из первого адреса в цепочке будет применен к последующим адресам."
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
    print(f"✅ ORS API: {'установлен' if ORS_API_KEY else 'не установлен'}")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("example", example_command))
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
            print("ℹ️ Для остановки нажмите Ctrl+C")
            
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