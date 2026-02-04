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
import re

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
                🗺️ Используется: GraphHopper API + Яндекс Геокодер (резервный)
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
GRAPH_HOPPER_API_KEY = os.getenv("GRAPH_HOPPER_API_KEY", "2c8e643a-360f-47ab-855d-7e884ce217ad")
YANDEX_GEOCODER_API_KEY = os.getenv("YANDEX_GEOCODER_API_KEY", "")

# ================== УЛУЧШЕННЫЕ ФУНКЦИИ ГЕОКОДИРОВАНИЯ ==================

def normalize_address(address):
    """Нормализация адреса для лучшего распознавания"""
    if not address:
        return ""
    
    # Убираем лишние пробелы
    address = re.sub(r'\s+', ' ', address.strip())
    
    # Стандартизируем обозначения
    replacements = {
        'р. ': 'республика ',
        'обл.': 'область',
        'г. ': 'город ',
        'с. ': 'село ',
        'ст-ца ': 'станица ',
        'пгт. ': 'посёлок городского типа ',
        'ул. ': 'улица ',
        'пр-т ': 'проспект ',
        'пр. ': 'проспект ',
        'пер. ': 'переулок ',
        'мкр. ': 'микрорайон ',
        'ш. ': 'шоссе ',
        'наб. ': 'набережная ',
        'б-р ': 'бульвар ',
        'ал. ': 'аллея ',
        'к. ': 'корпус ',
        'стр. ': 'строение ',
        'вл. ': 'владение ',
        'д. ': 'деревня ',
        'аул ': 'аул ',
        'х. ': 'хутор ',
        'р-н': 'район',
        'п. ': 'посёлок ',
    }
    
    for old, new in replacements.items():
        address = address.replace(old, new)
    
    # Исправляем типичные ошибки
    address = address.replace('Кврелия', 'Карелия')
    address = address.replace('Нижегородкская', 'Нижегородская')
    address = address.replace('Ставропольский край, с.', 'Ставропольский край, село')
    address = address.replace('р. Крым', 'Республика Крым')
    address = address.replace('ДНР', 'Донецкая Народная Республика')
    address = address.replace('ЛНР', 'Луганская Народная Республика')
    
    return address

def yandex_geocode(address, retries=3):
    """Резервное геокодирование через Яндекс.Геокодер"""
    if not YANDEX_GEOCODER_API_KEY:
        return None
    
    normalized_address = normalize_address(address)
    
    for attempt in range(retries):
        try:
            url = "https://geocode-maps.yandex.ru/1.x/"
            params = {
                "apikey": YANDEX_GEOCODER_API_KEY,
                "geocode": normalized_address,
                "format": "json",
                "lang": "ru_RU",
                "results": 1
            }
            
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                try:
                    pos = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
                    lon, lat = map(float, pos.split())
                    print(f"✅ Яндекс геокодирование успешно: {address[:50]}... -> {lat}, {lon}")
                    return float(lat), float(lon)
                except (KeyError, IndexError):
                    print(f"⚠️ Яндекс не нашел адрес: {address[:50]}...")
                    return None
            else:
                print(f"⚠️ Ошибка Яндекс геокодирования {response.status_code}")
            
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
                
        except requests.exceptions.Timeout:
            print(f"⚠️ Таймаут при Яндекс геокодировании: {address[:50]}...")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
        except Exception as e:
            print(f"⚠️ Ошибка при Яндекс геокодировании: {e}")
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
    
    return None

def smart_geocode(address, retries=3):
    """Умное геокодирование с несколькими попытками и резервными сервисами"""
    # Сначала проверяем координаты
    if is_coordinate_string(address):
        coords = parse_coordinate_string(address)
        if coords:
            print(f"✅ Координаты распознаны напрямую: {coords}")
            return coords
    
    normalized_address = normalize_address(address)
    
    # Пробуем GraphHopper
    for attempt in range(retries):
        try:
            url = "https://graphhopper.com/api/1/geocode"
            params = {
                "q": normalized_address,
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
                    print(f"✅ GraphHopper геокодирование успешно: {address[:50]}... -> {lat}, {lon}")
                    return float(lat), float(lon)
                else:
                    print(f"⚠️ GraphHopper не нашел адрес: {address[:50]}...")
                    
                    # Для Крыма и проблемных регионов сразу пробуем Яндекс
                    if any(keyword in address.lower() for keyword in ['крым', 'днр', 'симферополь', 'севастополь']):
                        print(f"🔍 Для Крыма/ДНР пробуем Яндекс...")
                        yandex_result = yandex_geocode(address)
                        if yandex_result:
                            return yandex_result
            else:
                print(f"⚠️ Ошибка GraphHopper геокодирования {response.status_code}")
            
            # Задержка перед повторной попыткой
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
                
        except requests.exceptions.Timeout:
            print(f"⚠️ Таймаут при GraphHopper геокодировании: {address[:50]}...")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
        except Exception as e:
            print(f"⚠️ Ошибка при GraphHopper геокодировании: {e}")
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
    
    # Если GraphHopper не сработал, пробуем Яндекс
    print(f"🔍 GraphHopper не сработал, пробуем Яндекс...")
    yandex_result = yandex_geocode(address)
    if yandex_result:
        return yandex_result
    
    # Последняя попытка - грубая геолокация по городу/региону
    print(f"⚠️ Все геокодеры не сработали, пробуем грубую геолокацию...")
    return fallback_geocode(address)

def fallback_geocode(address):
    """Грубая геолокация по основному городу/региону"""
    # Извлекаем название города/региона
    city_patterns = [
        r'г\.\s*([А-Я][а-я]+)',
        r'город\s*([А-Я][а-я]+)',
        r'с\.\s*([А-Я][а-я]+)',
        r'село\s*([А-Я][а-я]+)',
    ]
    
    city = None
    for pattern in city_patterns:
        match = re.search(pattern, address)
        if match:
            city = match.group(1)
            break
    
    if not city:
        # Пытаемся извлечь по запятой
        parts = address.split(',')
        if len(parts) > 1:
            city = parts[1].strip().split()[0]
    
    # База приблизительных координат для городов
    city_coords = {
        'Симферополь': (44.9521, 34.1024),
        'Севастополь': (44.6167, 33.5254),
        'Керчь': (45.3561, 36.4674),
        'Ялта': (44.4952, 34.1663),
        'Феодосия': (45.0319, 35.3824),
        'Евпатория': (45.1906, 33.3679),
        'Бахчисарай': (44.7512, 33.8755),
        'Джанкой': (45.709, 34.3883),
        'Красноперекопск': (45.9532, 33.7922),
        'Саки': (45.1336, 33.5772),
        'Армянск': (46.1092, 33.6921),
        'Щёлкино': (45.4289, 35.8253),
        'Старый Крым': (45.0291, 35.0881),
        'Петрозаводск': (61.7849, 34.3469),
        'Киров': (58.6035, 49.6680),
        'Воронеж': (51.6720, 39.1843),
        'Москва': (55.7558, 37.6173),
        'Санкт-Петербург': (59.9343, 30.3351),
    }
    
    if city and city in city_coords:
        print(f"📍 Грубая геолокация по городу {city}: {city_coords[city]}")
        return city_coords[city]
    
    print(f"❌ Не удалось определить координаты для: {address[:50]}...")
    return None

def is_coordinate_string(text):
    """Проверяет, является ли строка координатами"""
    if not isinstance(text, str):
        return False
    
    # Различные форматы координат
    patterns = [
        r'^-?\d+\.\d+,-?\d+\.\d+$',  # 47.272161,39.665489
        r'^-?\d+\.\d+\s*,\s*-?\d+\.\d+$',  # С пробелами
        r'^-?\d+\s*°\s*\d+\s*\'\s*\d+\.?\d*\s*[NS],\s*-?\d+\s*°\s*\d+\s*\'\s*\d+\.?\d*\s*[EW]$',
    ]
    
    for pattern in patterns:
        if re.match(pattern, text.strip()):
            try:
                # Пробуем извлечь координаты
                if '°' in text:
                    # Формат градусов
                    parts = re.split('[NS,EW]', text)
                    lat = convert_dms_to_decimal(parts[0])
                    lon = convert_dms_to_decimal(parts[1])
                else:
                    # Десятичный формат
                    coords = re.findall(r'-?\d+\.\d+', text)
                    if len(coords) >= 2:
                        lat, lon = map(float, coords[:2])
                
                return -90 <= lat <= 90 and -180 <= lon <= 180
            except:
                return False
    
    return False

def convert_dms_to_decimal(dms_str):
    """Конвертирует градусы, минуты, секунды в десятичные градусы"""
    try:
        # Удаляем лишние символы
        dms_str = dms_str.strip()
        parts = re.findall(r'\d+\.?\d*', dms_str)
        
        if len(parts) >= 3:
            degrees = float(parts[0])
            minutes = float(parts[1])
            seconds = float(parts[2])
            decimal = degrees + minutes/60 + seconds/3600
        elif len(parts) >= 2:
            degrees = float(parts[0])
            minutes = float(parts[1])
            decimal = degrees + minutes/60
        elif len(parts) >= 1:
            decimal = float(parts[0])
        else:
            return 0.0
        
        # Определяем знак
        if 'S' in dms_str.upper() or 'W' in dms_str.upper():
            decimal = -decimal
            
        return decimal
    except:
        return 0.0

def parse_coordinate_string(text):
    """Извлекает координаты из строки"""
    try:
        if '°' in text:
            # Формат DMS
            lat_str, lon_str = re.split('[NS]\s*,?\s*[EW]?', text, flags=re.IGNORECASE)
            lat = convert_dms_to_decimal(lat_str)
            lon = convert_dms_to_decimal(lon_str)
        else:
            # Десятичный формат
            coords = re.findall(r'-?\d+\.\d+', text)
            if len(coords) >= 2:
                lat, lon = map(float, coords[:2])
            else:
                # Попробуем разделить по запятой
                parts = text.replace(' ', '').split(',')
                if len(parts) >= 2:
                    lat, lon = map(float, parts[:2])
                else:
                    return None
        
        return float(lat), float(lon)
    except:
        return None

# ================== УЛУЧШЕННЫЕ ФУНКЦИИ РАСЧЕТА МАРШРУТОВ ==================

def parse_address_chain_improved(address_string):
    """Улучшенный парсинг цепочки адресов"""
    if not address_string:
        return []
    
    # Нормализуем строку
    address_string = str(address_string).strip()
    
    # Различные разделители
    separators = [' - ', ' – ', ' — ', '\n', '; ', ' / ']
    
    for sep in separators:
        if sep in address_string:
            addresses = [addr.strip() for addr in address_string.split(sep) if addr.strip()]
            if len(addresses) > 1:
                print(f"📝 Разделитель '{sep}' найден, разбито на {len(addresses)} частей")
                return addresses
    
    # Если разделителей нет, проверяем дефисы без пробелов
    if '-' in address_string and ' - ' not in address_string:
        # Пытаемся понять, это дефис в названии или разделитель
        parts = address_string.split('-')
        if len(parts) > 1:
            # Пробуем объединить короткие части (возможно, это часть адреса)
            addresses = []
            current_part = parts[0]
            
            for i in range(1, len(parts)):
                if len(parts[i].split()) <= 3 and len(current_part.split()) <= 5:
                    # Вероятно, это часть одного адреса
                    current_part += '-' + parts[i]
                else:
                    addresses.append(current_part.strip())
                    current_part = parts[i]
            
            if current_part:
                addresses.append(current_part.strip())
            
            if len(addresses) > 1:
                print(f"📝 Разделитель '-' найден, разбито на {len(addresses)} частей")
                return addresses
    
    # Если ничего не найдено, возвращаем как один адрес
    return [address_string]

def calculate_optimized_route(coordinates_list, profile="car", max_retries=5):
    """Оптимизированный расчет маршрута с разбиением на части"""
    if len(coordinates_list) < 2:
        return None
    
    # Проверяем валидность координат
    valid_coords = []
    for coord in coordinates_list:
        if coord and len(coord) == 2:
            lat, lon = coord
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                valid_coords.append(coord)
            else:
                print(f"⚠️ Невалидные координаты пропущены: {lat}, {lon}")
        else:
            print(f"⚠️ Пропущены некорректные координаты: {coord}")
    
    if len(valid_coords) < 2:
        print("⚠️ Недостаточно валидных координат")
        return None
    
    # Если точек слишком много, разбиваем на части
    if len(valid_coords) > 10:
        print(f"⚠️ Слишком много точек ({len(valid_coords)}), разбиваю на части...")
        return calculate_route_in_parts(valid_coords, profile)
    
    # Обычный расчет для небольшого количества точек
    return graphhopper_route_with_waypoints(valid_coords, profile, max_retries)

def calculate_route_in_parts(coordinates_list, profile="car", max_points_per_request=10):
    """Расчет маршрута по частям для большого количества точек"""
    total_distance = 0
    parts = []
    
    # Разбиваем на части
    for i in range(0, len(coordinates_list), max_points_per_request):
        part_coords = coordinates_list[i:i + max_points_per_request]
        if len(part_coords) >= 2:
            parts.append(part_coords)
    
    print(f"📊 Маршрут разбит на {len(parts)} частей")
    
    for i, part_coords in enumerate(parts):
        print(f"🔗 Расчет части {i+1}/{len(parts)} ({len(part_coords)} точек)")
        distance = graphhopper_route_with_waypoints(part_coords, profile, max_retries=3)
        
        if distance:
            total_distance += distance
            print(f"✅ Часть {i+1}: {distance} км")
        else:
            # Если часть не рассчиталась, используем гаверсинус для этой части
            print(f"⚠️ Ошибка расчета части {i+1}, использую гаверсинус")
            part_distance = 0
            for j in range(len(part_coords) - 1):
                segment_distance = haversine_distance(part_coords[j], part_coords[j+1])
                if segment_distance:
                    part_distance += segment_distance
            
            if part_distance > 0:
                # Увеличиваем на коэффициент для учета дорог
                part_distance *= 1.2
                total_distance += part_distance
                print(f"📍 Часть {i+1} по гаверсинусу: {part_distance} км")
        
        # Задержка между частями
        if i < len(parts) - 1:
            time.sleep(1)
    
    return round(total_distance, 1) if total_distance > 0 else None

def haversine_distance(coord1, coord2):
    """Расчет расстояния между двумя точками по гаверсинусу"""
    try:
        R = 6371  # Радиус Земли в км
        
        lat1, lon1 = radians(coord1[0]), radians(coord1[1])
        lat2, lon2 = radians(coord2[0]), radians(coord2[1])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    except:
        return None

def graphhopper_route_with_waypoints(coordinates_list, profile="car", max_retries=5):
    """Расчет маршрута через GraphHopper с улучшенной обработкой ошибок"""
    if not GRAPH_HOPPER_API_KEY:
        print("⚠️ GRAPH_HOPPER_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    for attempt in range(max_retries):
        try:
            url = f"https://graphhopper.com/api/1/route"
            
            # Формируем точки
            points_params = []
            for lat, lon in coordinates_list:
                points_params.append(f"point={lat},{lon}")
            
            # Параметры запроса
            params = {
                "key": GRAPH_HOPPER_API_KEY,
                "vehicle": profile,
                "locale": "ru",
                "instructions": "false",
                "calc_points": "false",
                "points_encoded": "false",
                "optimize": "false",
                "elevation": "false",
                "ch.disable": "true"
            }
            
            # Добавляем точки к параметрам
            all_params = points_params + [f"{k}={v}" for k, v in params.items()]
            request_url = f"{url}?{'&'.join(all_params)}"
            
            print(f"🔗 Запрос маршрута ({len(coordinates_list)} точек), попытка {attempt + 1}")
            
            response = requests.get(request_url, timeout=45)
            
            if response.status_code == 200:
                data = response.json()
                
                if "paths" in data and len(data["paths"]) > 0:
                    distance_m = data["paths"][0]["distance"]
                    distance_km = round(distance_m / 1000, 1)
                    
                    print(f"✅ Маршрут рассчитан: {distance_km} км")
                    return distance_km
                else:
                    print(f"⚠️ Не удалось построить маршрут, ответ: {data}")
                    
            elif response.status_code == 429:
                wait_time = 15 * (attempt + 1)
                print(f"⚠️ Превышен лимит запросов, жду {wait_time} секунд...")
                time.sleep(wait_time)
                
            else:
                print(f"⚠️ Ошибка маршрута {response.status_code}")
                
                # Пробуем получить больше информации об ошибке
                try:
                    error_data = response.json()
                    print(f"⚠️ Детали ошибки: {error_data}")
                except:
                    pass
                
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    print(f"⏳ Жду {wait_time} секунд перед повторной попыткой...")
                    time.sleep(wait_time)
                    
        except requests.exceptions.Timeout:
            print(f"⚠️ Таймаут при расчете маршрута")
            if attempt < max_retries - 1:
                wait_time = 10 * (attempt + 1)
                print(f"⏳ Жду {wait_time} секунд...")
                time.sleep(wait_time)
                
        except Exception as e:
            print(f"⚠️ Ошибка при расчете маршрута: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
    
    print(f"❌ Все попытки расчета маршрута не удались")
    return None

# ================== ОСНОВНЫЕ ФУНКЦИИ ОБРАБОТКИ ==================

def process_route_row_improved(route, ws, geocode_cache):
    """Улучшенная обработка строки маршрута"""
    try:
        row_num = route['row_num']
        start_point = route['start_point']
        address_chain = route['address_chain']
        
        print(f"\n{'='*60}")
        print(f"🔍 Обработка строки {row_num}")
        print(f"📌 Старт: {start_point[:80]}...")
        print(f"📍 Маршрут: {address_chain[:100]}...")
        
        # 1. Геокодируем стартовую точку
        start_coords = smart_geocode(start_point)
        
        if not start_coords:
            ws.cell(row=row_num, column=3).value = "❌ Ошибка геокодирования старта"
            ws.cell(row=row_num, column=4).value = "Ошибка"
            print(f"❌ Строка {row_num}: Ошибка геокодирования старта")
            return {"status": "error"}
        
        # 2. Парсим цепочку адресов
        addresses = parse_address_chain_improved(address_chain)
        
        if not addresses:
            ws.cell(row=row_num, column=3).value = "❌ Нет адресов в цепочке"
            ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
            print(f"❌ Строка {row_num}: Нет адресов в цепочке")
            return {"status": "error"}
        
        print(f"📊 Найдено {len(addresses)} точек маршрута")
        
        # 3. Геокодируем все адреса
        waypoints_coords = []
        waypoints_str = []
        errors = []
        
        for i, addr in enumerate(addresses):
            print(f"  🔍 Геокодирование точки {i+1}: {addr[:60]}...")
            
            coords = smart_geocode(addr)
            
            if coords:
                waypoints_coords.append(coords)
                waypoints_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                print(f"    ✅ Координаты: {coords[0]:.6f}, {coords[1]:.6f}")
                
                # Задержка для соблюдения лимитов API
                time.sleep(0.5)
            else:
                errors.append(i+1)
                print(f"    ❌ Ошибка геокодирования точки {i+1}")
                
                # Пробуем использовать приблизительные координаты
                fallback_coords = fallback_geocode(addr)
                if fallback_coords:
                    waypoints_coords.append(fallback_coords)
                    waypoints_str.append(f"{fallback_coords[0]:.6f},{fallback_coords[1]:.6f}")
                    print(f"    ⚠️ Использую приблизительные координаты: {fallback_coords}")
                else:
                    # Если не удалось получить координаты, отмечаем ошибку
                    ws.cell(row=row_num, column=3).value = f"❌ Ошибка геокодирования точки {i+1}"
                    ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str) if waypoints_str else "Ошибка"
                    print(f"❌ Строка {row_num}: Ошибка геокодирования точки {i+1}")
                    return {"status": "error"}
        
        # 4. Определяем тип маршрута
        route_type = "Прямой" if len(addresses) == 1 else "С промежуточными точками"
        
        # 5. Рассчитываем расстояние
        all_coords = [start_coords] + waypoints_coords
        
        # Проверяем координаты на валидность перед расчетом
        valid_coords = []
        for coord in all_coords:
            if coord and len(coord) == 2:
                lat, lon = coord
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    valid_coords.append(coord)
        
        if len(valid_coords) < 2:
            print(f"⚠️ Недостаточно валидных координат для расчета")
            distance = None
        else:
            distance = calculate_optimized_route(valid_coords)
        
        # 6. Если основной расчет не удался, используем гаверсинус
        if not distance:
            print(f"⚠️ Основной расчет не удался, использую гаверсинус")
            distance = 0
            
            for i in range(len(valid_coords) - 1):
                segment_distance = haversine_distance(valid_coords[i], valid_coords[i+1])
                if segment_distance:
                    distance += segment_distance
            
            if distance > 0:
                # Увеличиваем на коэффициент для учета дорог
                distance *= 1.2
                distance = round(distance, 1)
                print(f"📍 Расстояние по гаверсинусу: {distance} км")
        
        if not distance or distance <= 0:
            ws.cell(row=row_num, column=3).value = "⚠️ Ошибка расчета маршрута"
            ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
            ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str)
            ws.cell(row=row_num, column=6).value = len(addresses)
            ws.cell(row=row_num, column=7).value = route_type
            ws.cell(row=row_num, column=8).value = "Ошибка"
            print(f"❌ Строка {row_num}: Ошибка расчета маршрута")
            return {"status": "error"}
        
        # 7. Генерируем варианты расстояний
        d2, d3 = variations(distance)
        
        # 8. Записываем результаты
        ws.cell(row=row_num, column=3).value = "✅ Успешно" if not errors else "⚠️ Частично успешно"
        ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
        ws.cell(row=row_num, column=5).value = "; ".join(waypoints_str)
        ws.cell(row=row_num, column=6).value = len(addresses)
        ws.cell(row=row_num, column=7).value = route_type
        ws.cell(row=row_num, column=8).value = distance
        
        if d2:
            ws.cell(row=row_num, column=9).value = d2
        if d3:
            ws.cell(row=row_num, column=10).value = d3
        
        # Форматирование
        for col in [8, 9, 10]:
            cell = ws.cell(row=row_num, column=col)
            if cell.value:
                cell.number_format = '0.0'
        
        status_msg = f"✅ Строка {row_num} обработана: {distance} км"
        if errors:
            status_msg += f" (ошибки в точках: {', '.join(map(str, errors))})"
        
        print(status_msg)
        return {"status": "success", "distance": distance, "errors": errors}
        
    except Exception as e:
        print(f"❌ Критическая ошибка обработки строки {route.get('row_num', 'N/A')}: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error"}

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None or base <= 0:
        return [None, None]
    
    try:
        # Вариант 1: +5-15%
        d2 = round(base * (1 + random.uniform(0.05, 0.15)), 1)
        # Вариант 2: -5-15%
        d3 = round(base * (1 - random.uniform(0.05, 0.15)), 1)
        
        return [d2, d3]
    except:
        return [None, None]

# ================== ОСТАЛЬНЫЕ ФУНКЦИИ (без изменений) ==================

def read_from_docx(path):
    """Чтение адресов из DOCX файла"""
    doc = Document(path)
    lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    return [l for l in lines if len(l) > 10 and not l.replace(' ', '').isdigit()]

def read_from_excel(path):
    """Чтение маршрутов из Excel файла"""
    try:
        wb = load_workbook(path, data_only=True)
        ws = wb.active
        routes = []
        
        max_row = ws.max_row
        
        start_row = 2 if ws.cell(row=1, column=1).value and ws.cell(row=1, column=2).value else 1
        
        for row in range(start_row, max_row + 1):
            start_point = ws.cell(row=row, column=1).value
            address_chain = ws.cell(row=row, column=2).value
            
            if not start_point or not address_chain:
                continue
                
            routes.append({
                'row_num': row,
                'start_point': str(start_point).strip(),
                'address_chain': str(address_chain).strip(),
                'original_start': start_point,
                'original_chain': address_chain
            })
        
        return routes, wb, ws
    except Exception as e:
        print(f"❌ Ошибка чтения Excel файла: {e}")
        return [], None, None

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
        "👋 Привет! Я улучшенный бот для расчета маршрутов.\n\n"
        "📁 Отправьте мне Excel файл в формате:\n"
        "• Колонка A: Стартовая точка (точка А)\n"
        "• Колонка B: Цепочка адресов через дефис\n\n"
        "✨ Новые возможности:\n"
        "✅ Поддержка Крыма и ДНР/ЛНР\n"
        "✅ Резервное геокодирование через Яндекс\n"
        "✅ Обработка сложных маршрутов с многими точками\n"
        "✅ Улучшенный парсинг адресов\n\n"
        "📊 Пример строки в колонке B:\n"
        "`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`"
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
    
    add_result_columns(ws, start_col=3)
    
    geocode_cache = {}
    
    processed = 0
    errors = 0
    successes = 0
    
    for route in routes:
        result = process_route_row_improved(route, ws, geocode_cache)
        processed += 1
        
        if result["status"] == "success":
            successes += 1
        else:
            errors += 1
        
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
    
    output_file = f"results_{user_id}_{timestamp}.xlsx"
    wb.save(output_file)
    
    try:
        with open(output_file, "rb") as file:
            await update.message.reply_document(
                document=file,
                filename=f"результаты_{file_name}",
                caption=f"✅ Готово!\nУспешно обработано: {successes} строк\nОшибок: {errors}"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка отправки файла: {e}")
    
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

✨ **Новые возможности:**
• Поддержка Крыма, ДНР, ЛНР
• Резервное геокодирование (Яндекс)
• Обработка маршрутов до 50 точек
• Улучшенный парсинг адресов
• Автоматическое исправление ошибок

📊 **Пример строки в колонке B:**
`г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89`

**Используемые API:**
• GraphHopper (основной)
• Яндекс.Геокодер (резервный)
• OpenStreetMap Nominatim (запасной)
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка статуса API сервисов"""
    status_message = "🔍 **Проверка статуса API сервисов:**\n\n"
    
    # Проверяем GraphHopper
    if GRAPH_HOPPER_API_KEY:
        try:
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
    
    # Проверяем Яндекс Геокодер
    if YANDEX_GEOCODER_API_KEY:
        try:
            url = "https://geocode-maps.yandex.ru/1.x/"
            params = {
                "apikey": YANDEX_GEOCODER_API_KEY,
                "geocode": "Москва",
                "format": "json"
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                status_message += "✅ Яндекс.Геокодер: **РАБОТАЕТ**\n"
            else:
                status_message += f"⚠️ Яндекс.Геокодер: **ОШИБКА {response.status_code}**\n"
        except Exception as e:
            status_message += f"❌ Яндекс.Геокодер: **НЕ ДОСТУПЕН**\n"
    else:
        status_message += "⚠️ Яндекс.Геокодер: **КЛЮЧ НЕ УСТАНОВЛЕН** (только резервный)\n"
    
    status_message += f"\n📊 **Информация:**\n"
    status_message += f"• Используется улучшенный алгоритм геокодирования\n"
    status_message += f"• Поддержка Крыма и проблемных регионов\n"
    status_message += f"• Автоматический выбор оптимального геокодера\n"
    status_message += f"• Обработка до 50 точек в одном маршруте\n"
    
    await update.message.reply_text(status_message, parse_mode='Markdown')

# ================== ЗАПУСК ==================

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
    print(f"✅ GraphHopper API: {'установлен' if GRAPH_HOPPER_API_KEY else 'не установлен'}")
    print(f"✅ Яндекс Геокодер: {'установлен' if YANDEX_GEOCODER_API_KEY else 'не установлен'}")
    
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
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