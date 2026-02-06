import requests
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
import math

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
ORS_API_KEY = os.getenv("ORS_API_KEY", "")  # OpenRouteService API ключ
YANDEX_GEOCODER_API_KEY = os.getenv("YANDEX_GEOCODER_API_KEY", "")  # Яндекс.Геокодер API ключ
USE_ORS_FALLBACK = bool(ORS_API_KEY)
USE_YANDEX_GEOCODER = bool(YANDEX_GEOCODER_API_KEY)

# Фиксированные координаты для стартовой точки (Ростов-на-Дону)
FIXED_START_COORDS = (47.261748, 39.683642)

# ================== КЭШИРОВАНИЕ И ЛОГИРОВАНИЕ ==================
GEOCODE_CACHE_FILE = "geocode_cache.json"
ROUTE_CACHE_FILE = "route_cache.json"
ERROR_LOG = "errors.log"

def load_geocode_cache():
    """Загружает кэш геокодирования из файла"""
    if os.path.exists(GEOCODE_CACHE_FILE):
        try:
            with open(GEOCODE_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                print(f"📂 Загружен кэш геокодирования: {len(cache)} записей")
                return cache
        except Exception as e:
            print(f"⚠️ Ошибка загрузки кэша: {e}")
    return {}

def save_geocode_cache(cache):
    """Сохраняет кэш геокодирования в файл"""
    try:
        with open(GEOCODE_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f"💾 Кэш сохранен: {len(cache)} записей")
    except Exception as e:
        print(f"⚠️ Ошибка сохранения кэша: {e}")

def load_route_cache():
    """Загружает кэш маршрутов из файла"""
    if os.path.exists(ROUTE_CACHE_FILE):
        try:
            with open(ROUTE_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                print(f"📂 Загружен кэш маршрутов: {len(cache)} записей")
                return cache
        except Exception as e:
            print(f"⚠️ Ошибка загрузки кэша маршрутов: {e}")
    return {}

def save_route_cache(cache):
    """Сохраняет кэш маршрутов в файл"""
    try:
        with open(ROUTE_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f"💾 Кэш маршрутов сохранен: {len(cache)} записей")
    except Exception as e:
        print(f"⚠️ Ошибка сохранения кэша маршрутов: {e}")

def log_error(row_num, address, error_type, details=""):
    """Логирует ошибки в файл"""
    try:
        with open(ERROR_LOG, 'a', encoding='utf-8') as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | Row {row_num} | {error_type} | {address[:100]} | {details}\n")
    except:
        pass

# ================== ФУНКЦИИ ОБРАБОТКИ АДРЕСОВ ==================
def clean_text(text):
    """Очистка текста от лишних символов"""
    if not text:
        return ""
    
    # Приводим к строке
    text = str(text)
    
    # Заменяем различные типы тире на обычный дефис
    text = text.replace('–', '-').replace('—', '-').replace('−', '-').replace('–', '-')
    
    # Заменяем точки с запятыми после сокращений на запятые
    text = re.sub(r'([а-яА-Я])\.\s*', r'\1, ', text)
    
    # Убираем лишние пробелы
    text = ' '.join(text.split())
    
    # Заменяем двойные дефисы на одинарные
    while '--' in text:
        text = text.replace('--', '-')
    
    # Убираем лишние запятые
    while ',,' in text:
        text = text.replace(',,', ',')
    
    return text.strip()

def normalize_region_name(region):
    """Нормализует название региона"""
    if not region:
        return region
    
    region_lower = region.lower()
    
    replacements = {
        "р. карелия": "Республика Карелия",
        "р. коми": "Республика Коми",
        "р. башкортостан": "Республика Башкортостан",
        "р. адыгея": "Республика Адыгея",
        "р. марий эл": "Республика Марий Эл",
        "рсо-алания": "Республика Северная Осетия-Алания",
        "кчр": "Карачаево-Черкесская Республика",
        "кбр": "Кабардино-Балкарская Республика",
        "р. крым": "Республика Крым",
        "р. татарстан": "Республика Татарстан",
        "р. дагестан": "Республика Дагестан",
        "р. бурятия": "Республика Бурятия",
        "р. мордовия": "Республика Мордовия",
        "р. удмуртия": "Удмуртская Республика",
        "р. хакасия": "Республика Хакасия",
        "р. чувашия": "Чувашская Республика",
        "р. саха": "Республика Саха (Якутия)",
        "обл.": "область",
        "край.": "край",
        "респ.": "Республика",
        "авт.": "автономный",
        "ао": "автономный округ",
        "р-н": "район",
        "мо": "муниципальное образование",
        "г.": "",
        "с.": "",
        "п.": "",
        "ст.": "",
        "х.": "",
        "д.": "",
        "рп": "рабочий поселок",
        "пгт": "поселок городского типа",
    }
    
    for old, new in replacements.items():
        if old in region_lower:
            region_lower = region_lower.replace(old, new)
    
    # Капитализируем первую букву каждого слова
    words = region_lower.split()
    words = [word.capitalize() for word in words if word]
    region = ' '.join(words)
    
    return region

def extract_region_from_address_improved(address):
    """Улучшенное извлечение региона с исправлением опечаток"""
    if not address:
        return None
    
    address = clean_text(address)
    
    # Исправляем опечатки
    corrections = {
        "р. кврелия": "р. карелия",
        "нижегородкская": "нижегородская",
        "калубская": "калужская",
        "вологодская обл.": "вологодская область",
        "тамбовская обл": "тамбовская область",
        "костромская обл": "костромская область",
        "кировская обл": "кировская область",
        "самарская обл": "самарская область",
        "ярославская обл": "ярославская область",
        "тверская обл": "тверская область",
        "смоленская обл": "смоленская область",
        "новгородская обл": "новгородская область",
        "псковская обл": "псковская область",
        "астраханская обл": "астраханская область",
        "ростовская обл": "ростовская область",
        "краснодарский кр": "краснодарский край",
        "ставропольский кр": "ставропольский край",
        "алтайский кр": "алтайский край",
        "пермский кр": "пермский край",
        "забайкальский кр": "забайкальский край",
        "камчатский кр": "камчатский край",
    }
    
    address_lower = address.lower()
    for wrong, correct in corrections.items():
        if wrong in address_lower:
            address_lower = address_lower.replace(wrong, correct)
    
    # Паттерны для извлечения региона
    region_patterns = [
        r'^(.*?)\s+(?:обл\.|область|край|респ\.|республика|АО|авт\.\s+округ|р-н|район)',
        r'^(р\.\s+[А-Яа-яёЁ\s\-]+)',  # р. Карелия
        r'^(?:КЧР|КБР|РСО[\-\s]?Алания|ЧР|УР|ХМАО|ЯНАО|Ненецкий\s+АО)',
        r'^([А-Яа-яёЁ]+\s+[А-Яа-яёЁ]+(?:\s+[А-Яа-яёЁ]+)?)\s+(?:край|область|республика)',
    ]
    
    for pattern in region_patterns:
        match = re.search(pattern, address_lower, re.IGNORECASE)
        if match:
            region = match.group(1).strip()
            if region:
                return normalize_region_name(region)
    
    return None

def extract_settlement_from_address(address):
    """Извлекает населенный пункт из адреса"""
    if not address:
        return None
    
    address = clean_text(address)
    
    # Паттерны для населенных пунктов с разными типами
    settlement_patterns = [
        # г. Москва, г.Санкт-Петербург
        r'(?:г\.|город\s+|г\s+)([^,\-]+)',
        # с. Ивановка, п. Горный
        r'(?:с\.|село\s+|п\.|посёлок\s+|пос\.|поселок\s+)([^,\-]+)',
        # ст-ца Каневская, ст.Ленинградская
        r'(?:ст-ца\s+|ст\.|станица\s+)([^,\-]+)',
        # д. Петрово, д.Новое
        r'(?:д\.|деревня\s+)([^,\-]+)',
        # х. Согласный
        r'(?:х\.|хутор\s+)([^,\-]+)',
        # р.п. Мухтолово
        r'(?:р\.п\.|рабочий\s+посёлок\s+)([^,\-]+)',
        # пгт. Черноморское
        r'(?:пгт\.|посёлок\s+городского\s+типа\s+)([^,\-]+)',
        # аул Кошехабль
        r'(?:аул\s+)([^,\-]+)',
        # Если есть запятая, берем первое слово до запятой
        r'^[^,]*?,\s*([^,\-]+)(?=,)',
        # Берем первое слово после региона
        r'^(?:[А-Яа-яёЁ]+\s+[А-Яа-яёЁ]+(?:\s+[А-Яа-яёЁ]+)?\s+(?:край|область|республика)[,\s]+)?([^,\-]+)',
    ]
    
    for pattern in settlement_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            settlement = match.group(1).strip()
            # Очищаем от кавычек и лишних символов
            settlement = re.sub(r'["«»]', '', settlement)
            # Убираем возможные точки в конце
            if settlement.endswith('.'):
                settlement = settlement[:-1]
            # Убираем лишние пробелы
            settlement = ' '.join(settlement.split())
            return settlement
    
    return None

def parse_address_chain(address_string, default_region=None):
    """Парсит цепочку адресов с учетом региона из первого адреса"""
    if not address_string:
        return []
    
    address_string = clean_text(address_string)
    
    # Разделяем по дефису, но учитываем, что в названиях могут быть дефисы
    # Сначала заменяем дефисы в скобках на другой символ
    temp_char = '§'
    in_brackets = False
    processed = []
    for char in address_string:
        if char == '(':
            in_brackets = True
        elif char == ')':
            in_brackets = False
        if char == '-' and in_brackets:
            processed.append(temp_char)
        else:
            processed.append(char)
    temp_string = ''.join(processed)
    
    # Разделяем по дефисам
    addresses = [addr.replace(temp_char, '-').strip() for addr in re.split(r'\s*-\s*', temp_string) if addr.strip()]
    
    if not addresses:
        return []
    
    # Извлекаем регион из первого адреса
    first_region = extract_region_from_address_improved(addresses[0])
    region_to_use = first_region if first_region else default_region
    
    parsed_addresses = []
    
    for i, addr in enumerate(addresses):
        # Извлекаем регион для текущего адреса
        current_region = extract_region_from_address_improved(addr)
        settlement = extract_settlement_from_address(addr)
        
        if not settlement:
            # Если не удалось извлечь населенный пункт, используем весь адрес
            settlement = addr.split(',')[0] if ',' in addr else addr
        
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

def extract_all_addresses_from_chain(address_chain):
    """Извлекает все адреса из сложной цепочки"""
    if not address_chain:
        return []
    
    # 1. Разделяем по дефисам, но учитываем сложные случаи
    addresses = []
    current = ""
    brackets = 0
    
    for char in address_chain:
        if char == '(':
            brackets += 1
        elif char == ')':
            brackets -= 1
        
        if char == '-' and brackets == 0:
            if current.strip():
                addresses.append(current.strip())
            current = ""
        else:
            current += char
    
    if current.strip():
        addresses.append(current.strip())
    
    # 2. Если разделение не сработало, пробуем другие методы
    if len(addresses) < 2:
        # Пробуем по запятым
        parts = [p.strip() for p in address_chain.split(',') if len(p.strip()) > 5]
        if len(parts) > 1:
            # Группируем части в адреса
            addresses = []
            i = 0
            while i < len(parts):
                if i + 1 < len(parts) and len(parts[i]) < 20:
                    # Объединяем короткую часть со следующей
                    addresses.append(f"{parts[i]}, {parts[i+1]}")
                    i += 2
                else:
                    addresses.append(parts[i])
                    i += 1
    
    return addresses

def has_forbidden_region(address):
    """Проверяет, содержит ли адрес запрещенный регион"""
    if not address:
        return False
    
    forbidden = ['крым', 'днр', 'лнр', 'херсонская обл', 'запорожская обл', 
                 'севастополь', 'мариуполь', 'донецк', 'луганск', 'алупка',
                 'феодосия', 'ялта', 'симферополь', 'керчь']
    
    address_lower = address.lower()
    for region in forbidden:
        if region in address_lower:
            return True
    
    return False

def simplify_address_for_geocoding_v2(address):
    """Упрощает адрес для геокодирования с учетом особых случаев"""
    if not address:
        return None
    
    address = clean_text(address)
    
    # Проверяем запрещенные регионы
    if has_forbidden_region(address):
        print(f"⚠️ Пропускаю запрещенный регион: {address[:50]}...")
        return None
    
    # Исправляем опечатки
    corrections = {
        "р. кврелия": "республика карелия",
        "нижегородкская": "нижегородская",
        "калубская": "калужская",
        "тверская": "тверская",
    }
    
    address_lower = address.lower()
    for wrong, correct in corrections.items():
        if wrong in address_lower:
            address_lower = address_lower.replace(wrong, correct)
    
    # Расширенный список регионов для преобразования
    region_mapping = {
        'р. карелия': 'республика карелия',
        'р. коми': 'республика коми',
        'р. башкортостан': 'республика башкортостан',
        'р. адыгея': 'республика адыгея',
        'р. татарстан': 'республика татарстан',
        'рсо-алания': 'республика северная осетия-алания',
        'кчр': 'карачаево-черкесская республика',
        'кбр': 'кабардино-балкарская республика',
        'р. мордовия': 'республика мордовия',
        'р. марий эл': 'республика марий эл',
        'р. удмуртия': 'удмуртская республика',
        'р. чувашия': 'чувашская республика',
        'обл.': 'область',
        'край.': 'край',
        'респ.': 'республика',
        'г.': '',
        'с.': '',
        'п.': '',
        'ст-ца': '',
        'ст.': '',
        'х.': '',
        'д.': '',
        'рп.': '',
        'пгт.': '',
        'аул': '',
    }
    
    # Заменяем сокращения
    for old, new in region_mapping.items():
        address_lower = address_lower.replace(old, new)
    
    # Восстанавливаем заглавные буквы
    words = address_lower.split()
    words = [w.capitalize() for w in words if w]
    address = ' '.join(words)
    
    # Убираем лишние запятые и пробелы
    address = re.sub(r'\s*,\s*', ', ', address)
    address = re.sub(r'\s+', ' ', address)
    
    # Добавляем "Russia" если нет
    if 'россия' not in address.lower() and 'russia' not in address.lower():
        address = f"{address}, Россия"
    
    return address.strip()

# ================== ГЕОКОДИРОВАНИЕ ==================
def haversine_distance(lat1, lon1, lat2, lon2):
    """Расстояние между двумя точками по формуле гаверсинусов (в км)"""
    R = 6371  # Радиус Земли в км
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def validate_route_distance(distance, coordinates_list):
    """Проверяет корректность расстояния маршрута"""
    if not distance or distance <= 0:
        return False
    
    # Если расстояние меньше 10 км, проверяем координаты
    if distance < 10 and len(coordinates_list) >= 2:
        # Проверяем, не являются ли координаты одинаковыми
        coord1 = coordinates_list[0]
        for coord2 in coordinates_list[1:]:
            # Вычисляем расстояние по прямой между точками
            lat1, lon1 = coord1
            lat2, lon2 = coord2
            
            # Примерное расстояние по прямой (в км)
            straight_distance = haversine_distance(lat1, lon1, lat2, lon2)
            
            # Если расстояние по прямой больше 50 км, а маршрут показывает <10 км, это ошибка
            if straight_distance > 50 and distance < 10:
                print(f"⚠️ Подозрительное расстояние: {distance} км при прямой дистанции {straight_distance:.1f} км")
                return False
    
    return True

def graphhopper_geocode_simple(address, cache):
    """Простое геокодирование через GraphHopper"""
    if not GRAPHHOPPER_API_KEY or not address:
        return None
    
    # Проверяем кэш
    cache_key = f"gh_{address}"
    if cache_key in cache:
        return cache[cache_key]
    
    url = "https://graphhopper.com/api/1/geocode"
    params = {
        "q": address,
        "key": GRAPHHOPPER_API_KEY,
        "locale": "ru",
        "limit": 1,
        "provider": "default"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("hits") and len(data["hits"]) > 0:
                hit = data["hits"][0]
                point = hit.get("point", {})
                lat = point.get("lat")
                lng = point.get("lng")
                
                if lat is not None and lng is not None:
                    coords = (float(lat), float(lng))
                    cache[cache_key] = coords
                    return coords
        
        return None
    except Exception as e:
        print(f"⚠️ Ошибка GraphHopper геокодирования: {e}")
        return None

def yandex_geocode(address, cache):
    """Геокодирование через Яндекс.Геокодер"""
    if not YANDEX_GEOCODER_API_KEY or not address:
        return None
    
    # Проверяем кэш
    cache_key = f"yandex_{address}"
    if cache_key in cache:
        return cache[cache_key]
    
    url = "https://geocode-maps.yandex.ru/1.x/"
    params = {
        "apikey": YANDEX_GEOCODER_API_KEY,
        "geocode": address,
        "format": "json",
        "results": 1
    }
    
    try:
        time.sleep(0.1)  # Пауза для соблюдения лимитов
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # Извлекаем координаты
            try:
                pos = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
                lon, lat = map(float, pos.split())
                coords = (lat, lon)
                cache[cache_key] = coords
                return coords
            except (KeyError, IndexError):
                return None
        else:
            print(f"⚠️ Яндекс.Геокодер ошибка {response.status_code}")
            return None
    except Exception as e:
        print(f"⚠️ Ошибка Яндекс.Геокодера: {e}")
        return None

def enhanced_geocode(address, cache):
    """Улучшенное геокодирование с несколькими стратегиями"""
    if not address:
        return None
    
    print(f"📍 Геокодирую: {address[:60]}...")
    
    # Упрощаем адрес
    simplified = simplify_address_for_geocoding_v2(address)
    if not simplified:
        return None
    
    # Стратегия 1: GraphHopper
    coords = graphhopper_geocode_simple(simplified, cache)
    if coords:
        print(f"✅ GraphHopper нашел: {coords}")
        return coords
    
    # Стратегия 2: Яндекс (если включен)
    if USE_YANDEX_GEOCODER:
        coords = yandex_geocode(simplified, cache)
        if coords:
            print(f"✅ Яндекс нашел: {coords}")
            return coords
    
    # Стратегия 3: Пробуем без региона
    settlement = extract_settlement_from_address(address)
    if settlement:
        simple_addr = f"{settlement}, Россия"
        coords = graphhopper_geocode_simple(simple_addr, cache)
        if coords:
            print(f"✅ GraphHopper нашел (упрощенно): {coords}")
            return coords
        
        if USE_YANDEX_GEOCODER:
            coords = yandex_geocode(simple_addr, cache)
            if coords:
                print(f"✅ Яндекс нашел (упрощенно): {coords}")
                return coords
    
    print(f"❌ Не удалось геокодировать: {address[:50]}...")
    return None

def geocode_start_point(address):
    """Специальная функция для геокодирования стартовой точки"""
    # Всегда используем фиксированные координаты для Ростова-на-Дону
    if "ростов-на-дону" in address.lower() or "344064" in address or "оганов" in address.lower():
        return FIXED_START_COORDS
    
    return enhanced_geocode(address, load_geocode_cache())

# ================== РАСЧЕТ МАРШРУТОВ ==================
def graphhopper_route_with_waypoints(coordinates_list):
    """Строит маршрут через промежуточные точки через GraphHopper API"""
    if not GRAPHHOPPER_API_KEY:
        print("⚠️ GRAPHHOPPER_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    # ⚠️ GraphHopper ограничение: максимум 4 точки
    if len(coordinates_list) > 4:
        print(f"⚠️ GraphHopper: слишком много точек ({len(coordinates_list)}). Максимум 4.")
        print("⚠️ Буду использовать только первые 4 точки")
        coordinates_list = coordinates_list[:4]
    
    # Создаем ключ для кэша
    coords_str = '|'.join([f"{lat:.6f},{lon:.6f}" for lat, lon in coordinates_list])
    cache_key = f"gh_route_{coords_str}"
    
    # Проверяем кэш маршрутов
    route_cache = load_route_cache()
    if cache_key in route_cache:
        distance = route_cache[cache_key]
        print(f"✅ Маршрут из кэша: {distance} км")
        return distance
    
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
            
            # Если точек было 4 и ошибка 400, пробуем с 3 точками
            if r.status_code == 400 and len(coordinates_list) == 4:
                print("🔄 Пробую с 3 точками...")
                # Пробуем без предпоследней точки
                new_coords = [coordinates_list[0], coordinates_list[1], coordinates_list[3]]
                return graphhopper_route_with_waypoints(new_coords)
            
            # Пробуем получить детали ошибки
            try:
                error_details = r.json()
                print(f"⚠️ Детали ошибки: {error_details}")
                if "Too many points" in str(error_details):
                    print("🔄 Слишком много точек, пробую уменьшить...")
                    if len(coordinates_list) > 2:
                        return graphhopper_route_with_waypoints(coordinates_list[:len(coordinates_list)-1])
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
                
                # Сохраняем в кэш
                route_cache[cache_key] = distance_km
                save_route_cache(route_cache)
                
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

def ors_route_with_waypoints(coordinates_list):
    """Строит маршрут через OpenRouteService API (запасной вариант)"""
    if not ORS_API_KEY:
        print("⚠️ ORS_API_KEY не установлен!")
        return None
    
    if len(coordinates_list) < 2:
        return None
    
    # ORS поддерживает до 50 точек, но ограничим 20 для надежности
    if len(coordinates_list) > 20:
        print(f"⚠️ ORS: слишком много точек ({len(coordinates_list)}). Ограничиваю 20.")
        coordinates_list = coordinates_list[:20]
    
    # Создаем ключ для кэша
    coords_str = '|'.join([f"{lat:.6f},{lon:.6f}" for lat, lon in coordinates_list])
    cache_key = f"ors_route_{coords_str}"
    
    # Проверяем кэш маршрутов
    route_cache = load_route_cache()
    if cache_key in route_cache:
        distance = route_cache[cache_key]
        print(f"✅ ORS маршрут из кэша: {distance} км")
        return distance
    
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    
    # ORS использует формат [долгота, широта]
    coordinates_ors = [[lon, lat] for lat, lon in coordinates_list]
    
    headers = {
        'Authorization': ORS_API_KEY,
        'Content-Type': 'application/json'
    }
    
    body = {
        "coordinates": coordinates_ors,
        "instructions": False,
        "geometry": False,
        "units": "km"
    }
    
    try:
        print(f"📍 ORS строит маршрут через {len(coordinates_list)} точек...")
        
        r = requests.post(url, json=body, headers=headers, timeout=60)
        
        if r.status_code != 200:
            print(f"⚠️ ORS ошибка маршрута {r.status_code}")
            print(f"⚠️ Ответ: {r.text[:200]}")
            return None
        
        data = r.json()
        
        if data.get("routes") and len(data["routes"]) > 0:
            route = data["routes"][0]
            distance_km = round(route.get("summary", {}).get("distance", 0) / 1000, 1)
            
            if distance_km > 0:
                print(f"✅ ORS маршрут построен: {distance_km} км")
                
                # Сохраняем в кэш
                route_cache[cache_key] = distance_km
                save_route_cache(route_cache)
                
                return distance_km
            else:
                print(f"⚠️ ORS нулевое расстояние в маршруте")
                return None
        else:
            print(f"⚠️ Некорректный ответ от ORS")
            return None
            
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута в ORS: {e}")
        return None

def calculate_route_segments(coordinates_list):
    """Разбивает маршрут с многими точками на сегменты по 4 точки"""
    if len(coordinates_list) <= 4:
        # Пробуем GraphHopper
        distance = graphhopper_route_with_waypoints(coordinates_list)
        if distance:
            return distance
        
        # Пробуем ORS как запасной вариант
        if USE_ORS_FALLBACK:
            distance = ors_route_with_waypoints(coordinates_list)
            if distance:
                return distance
        
        return None
    
    # Для маршрутов с 5-20 точками сначала пробуем ORS целиком
    if 5 <= len(coordinates_list) <= 20 and USE_ORS_FALLBACK:
        distance = ors_route_with_waypoints(coordinates_list)
        if distance:
            return distance
    
    # Если ORS не сработал или точек >20, разбиваем на сегменты
    print(f"📍 Разбиваю маршрут на сегменты ({len(coordinates_list)} точек)...")
    
    total_distance = 0
    segments = []
    
    # Разбиваем на сегменты по 4 точки (старт + 3 промежуточные)
    for i in range(0, len(coordinates_list)-1, 3):
        segment = coordinates_list[i:i+4]
        if len(segment) < 2:
            continue
        
        # Последняя точка сегмента должна быть первой следующего сегмента
        if i > 0 and segments:
            # Убедимся, что есть перекрытие
            if segment[0] != segments[-1][-1]:
                segment.insert(0, segments[-1][-1])
        
        segments.append(segment)
    
    # Если сегментов слишком много, упрощаем
    if len(segments) > 10:
        print(f"⚠️ Слишком много сегментов ({len(segments)}), упрощаю...")
        # Берем только ключевые точки: старт, 1/4, 1/2, 3/4, конец
        key_indices = [0]
        if len(coordinates_list) > 4:
            key_indices.append(len(coordinates_list) // 4)
        key_indices.append(len(coordinates_list) // 2)
        key_indices.append(3 * len(coordinates_list) // 4)
        key_indices.append(len(coordinates_list) - 1)
        
        key_points = [coordinates_list[i] for i in key_indices]
        return calculate_route_segments(key_points)
    
    # Рассчитываем каждый сегмент
    for idx, segment in enumerate(segments):
        print(f"📍 Сегмент {idx+1}/{len(segments)}: {len(segment)} точек")
        
        # Пробуем GraphHopper для сегмента
        segment_distance = graphhopper_route_with_waypoints(segment)
        
        # Если не сработало, пробуем ORS
        if not segment_distance and USE_ORS_FALLBACK:
            segment_distance = ors_route_with_waypoints(segment)
        
        if segment_distance:
            total_distance += segment_distance
        else:
            print(f"⚠️ Не удалось рассчитать сегмент {idx+1}")
            return None
    
    return total_distance

def validate_coordinates(coords_list):
    """Проверяет, что координаты разумные для России"""
    if not coords_list:
        return False
    
    for lat, lon in coords_list:
        # Россия примерно в пределах:
        # Широта: 41° до 82° N
        # Долгота: 19° до 190° E
        if not (40 <= lat <= 83) or not (19 <= lon <= 191):
            print(f"⚠️ Подозрительные координаты: {lat}, {lon}")
            return False
    
    return True

def calculate_route(coordinates_list):
    """Основная функция расчета маршрута с использованием всех доступных методов"""
    if len(coordinates_list) < 2:
        return None
    
    # Проверяем валидность координат
    if not validate_coordinates(coordinates_list):
        print("⚠️ Координаты выглядят подозрительно")
        return None
    
    # Валидация: проверяем, что все координаты разные
    unique_coords = set([f"{lat:.4f},{lon:.4f}" for lat, lon in coordinates_list])
    if len(unique_coords) != len(coordinates_list):
        print("⚠️ Обнаружены дублирующиеся координаты")
        # Удаляем дубликаты
        seen = set()
        unique_list = []
        for coord in coordinates_list:
            key = f"{coord[0]:.4f},{coord[1]:.4f}"
            if key not in seen:
                seen.add(key)
                unique_list.append(coord)
        
        if len(unique_list) < 2:
            return None
        
        coordinates_list = unique_list
        print(f"📍 Удалены дубликаты, осталось {len(coordinates_list)} точек")
    
    # Пробуем разные стратегии расчета
    strategies = [
        ("GraphHopper целиком", lambda: graphhopper_route_with_waypoints(coordinates_list)),
    ]
    
    if USE_ORS_FALLBACK:
        strategies.append(("ORS целиком", lambda: ors_route_with_waypoints(coordinates_list)))
    
    strategies.append(("Сегментарный расчет", lambda: calculate_route_segments(coordinates_list)))
    
    for strategy_name, strategy_func in strategies:
        print(f"📍 Пробую стратегию: {strategy_name}")
        distance = strategy_func()
        if distance and distance > 0:
            print(f"✅ Успешно с стратегией: {strategy_name}")
            return distance
    
    print("❌ Все стратегии расчета не сработали")
    return None

def smart_variations(base_distance):
    """Умные вариации расстояний с проверкой корректности"""
    if not base_distance or base_distance <= 0:
        return [None, None]
    
    # Если расстояние слишком маленькое (менее 10 км), не добавляем вариации
    if base_distance < 10:
        return [None, None]
    
    # Если расстояние подозрительно маленькое для междугороднего маршрута
    if base_distance < 50:
        # Проверяем, не является ли это ошибкой
        return [round(base_distance * 1.02, 1), round(base_distance * 0.98, 1)]
    
    # Нормальные вариации 2-5%
    variation_percent = random.uniform(0.02, 0.05)
    variation = base_distance * variation_percent
    
    var1 = round(base_distance + random.uniform(variation/2, variation), 1)
    var2 = round(max(base_distance * 0.95, base_distance - random.uniform(variation/2, variation)), 1)
    
    return [var1, var2]

# ================== ЧТЕНИЕ И ЗАПИСЬ EXCEL ==================
def read_excel_with_fallback(file_path):
    """Читает Excel файл с помощью openpyxl"""
    try:
        print(f"📖 Чтение файла с openpyxl...")
        wb = load_workbook(file_path, data_only=True)
        ws = wb.active
        
        # Собираем данные
        data = []
        max_row = ws.max_row
        
        # Определяем, есть ли заголовки (проверяем первую строку)
        has_headers = False
        if max_row > 0:
            # Проверяем первые 2 ячейки первой строки
            cell1 = ws.cell(row=1, column=1).value
            cell2 = ws.cell(row=1, column=2).value
            
            # Если в первой строке есть слова "пункт", "назначение" и т.д., то это заголовки
            if cell1 and cell2:
                text1 = str(cell1).lower()
                text2 = str(cell2).lower()
                header_keywords = ['пункт', 'назначение', 'груз', 'адрес', 'точка', 'отправ', 'получ']
                has_headers = any(keyword in text1 for keyword in header_keywords) or \
                             any(keyword in text2 for keyword in header_keywords)
        
        start_row = 2 if has_headers else 1
        
        for row in range(start_row, max_row + 1):
            col1 = ws.cell(row=row, column=1).value
            col2 = ws.cell(row=row, column=2).value
            
            # Проверяем, что обе ячейки не пустые
            if col1 is not None and col2 is not None:
                start_point = clean_text(str(col1))
                address_chain = clean_text(str(col2))
                
                # Игнорируем строки, где слишком мало символов
                if len(start_point) > 3 and len(address_chain) > 3:
                    data.append({
                        'row_num': row,
                        'start_point': start_point,
                        'address_chain': address_chain,
                        'original_start': col1,
                        'original_chain': col2
                    })
        
        print(f"✅ Успешно прочитано {len(data)} строк")
        return data, wb, ws
        
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
        raise Exception(f"Не удалось прочитать файл. Убедитесь, что это корректный Excel файл (формат .xlsx). Ошибка: {str(e)[:200]}")

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

def validate_address_chain(address_chain):
    """Проверяет корректность цепочки адресов"""
    if not address_chain:
        return False
    
    # Проверяем наличие дефисов для разделения адресов
    if "-" not in address_chain:
        # Но может быть прямой маршрут
        return True
    
    # Проверяем, что адреса не содержат заведомо некорректных данных
    invalid_phrases = [
        "Ошибка", "ошибка", "error", "Error", 
        "Не определено", "не определено",
        "NULL", "null", "None", "none"
    ]
    
    for phrase in invalid_phrases:
        if phrase in address_chain:
            return False
    
    return True

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
        "⚡ Используется GraphHopper API + Яндекс.Геокодер + OpenRouteService\n"
        "📍 Геокодируются только населенные пункты\n"
        "🛣️ Расчет автомобильных маршрутов\n\n"
        "⚠️ **Ограничения:**\n"
        "• GraphHopper: максимум 4 точки в маршруте\n"
        "• ORS: до 20 точек (запасной вариант)\n"
        "• Крым, ДНР, ЛНР не поддерживаются\n"
        "• Маленькие населенные пункты могут не найтись"
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загруженных документов"""
    if not update.message or not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте файл")
        return
    
    file_name = update.message.document.file_name or "file.xlsx"
    file_name_lower = file_name.lower()
    
    # Проверяем расширение файла
    allowed_extensions = ['.xlsx', '.xls']
    
    if not any(file_name_lower.endswith(ext) for ext in allowed_extensions):
        await update.message.reply_text(
            "❌ Пожалуйста, отправьте файл в формате Excel:\n"
            "• .xlsx (рекомендуется)\n"
            "• .xls\n\n"
            "Если у вас файл другого формата, сохраните его как .xlsx в Excel."
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
                                           "Убедитесь, что:\n"
                                           "1. Файл не поврежден\n"
                                           "2. Это корректный Excel файл (.xlsx)\n"
                                           "3. Данные находятся на первом листе\n"
                                           "4. В колонке A - стартовые точки, в B - цепочки адресов")
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
                "3. Данные начинаются с первой строки (или со второй, если есть заголовки)\n"
                "4. Адреса в колонке B разделены дефисом (-)"
            )
            if os.path.exists(input_file):
                os.remove(input_file)
            return
        
        # Отправляем начальное сообщение
        progress_msg = await update.message.reply_text(
            f"⏳ Начинаю обработку...\n"
            f"📊 Всего строк: {total}\n"
            f"🔑 API: GraphHopper{' + Яндекс' if USE_YANDEX_GEOCODER else ''}{' + ORS' if USE_ORS_FALLBACK else ''}\n"
            f"⏱️ Ориентировочное время: {total * 3} секунд\n\n"
            f"⚠️ **Внимание:**\n"
            f"• GraphHopper: максимум 4 точки\n"
            f"• ORS: до 20 точек (запасной вариант)\n"
            f"• Крым, ДНР, ЛНР пропускаются\n"
            f"• Паузы между запросами для API"
        )
        
        # Очищаем старый кэш перед началом обработки
        print("🧹 Очищаю старый кэш...")
        if os.path.exists(GEOCODE_CACHE_FILE):
            try:
                os.remove(GEOCODE_CACHE_FILE)
                print("✅ Старый кэш удален")
            except:
                print("⚠️ Не удалось удалить старый кэш")
        
        # Загружаем кэш геокодирования
        geocode_cache = load_geocode_cache()
        
        # Добавляем колонки для результатов
        start_col = add_result_columns(ws, start_col=3)
        
        # Настройки для обработки
        processed = 0
        errors = 0
        geocode_errors = 0
        route_errors = 0
        successful = 0
        skipped = 0
        
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
                
                # ===== ПРОВЕРКА ДАННЫХ =====
                if not validate_address_chain(address_chain):
                    print(f"❌ Некорректный формат адресов, пропускаю")
                    skipped += 1
                    
                    ws.cell(row=row_num, column=start_col).value = "❌ Некорректный формат адресов"
                    ws.cell(row=row_num, column=start_col+1).value = "Пропущено"
                    ws.cell(row=row_num, column=start_col+2).value = "Пропущено"
                    ws.cell(row=row_num, column=start_col+3).value = 0
                    ws.cell(row=row_num, column=start_col+4).value = "Ошибка"
                    ws.cell(row=row_num, column=start_col+5).value = "Пропущено"
                    
                    processed += 1
                    continue
                
                # ===== ГЕОКОДИРОВАНИЕ СТАРТОВОЙ ТОЧКИ =====
                print(f"📍 Геокодирую стартовую точку...")
                start_coords = geocode_start_point(start_point)
                
                if not start_coords:
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
                print(f"📍 Парсинг цепочки адресов...")
                # Извлекаем регион из первого адреса цепочки
                first_address_region = None
                if address_chain and '-' in address_chain:
                    first_part = address_chain.split('-')[0].strip()
                    first_address_region = extract_region_from_address_improved(first_part)
                
                addresses = parse_address_chain(address_chain, first_address_region)
                
                if not addresses:
                    # Пробуем альтернативный метод
                    addresses = extract_all_addresses_from_chain(address_chain)
                
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
                print(f"📍 Геокодирую точки маршрута ({len(addresses)} точек)...")
                all_coords = []
                all_coords_str = []
                has_geocode_error = False
                
                for i, addr in enumerate(addresses):
                    print(f"  📍 Точка {i+1}/{len(addresses)}: {addr[:40]}...")
                    coords = enhanced_geocode(addr, geocode_cache)
                    time.sleep(0.3)  # Пауза между запросами
                    
                    if coords:
                        all_coords.append(coords)
                        all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                        print(f"    ✅ Координаты: {coords}")
                    else:
                        print(f"    ⚠️ Точка {i+1} не найдена, пытаюсь альтернативный метод...")
                        
                        # Пробуем извлечь только город
                        settlement = extract_settlement_from_address(addr)
                        if settlement:
                            simple_addr = f"{settlement}, Россия"
                            coords = enhanced_geocode(simple_addr, geocode_cache)
                        
                        if coords:
                            all_coords.append(coords)
                            all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                            print(f"    ✅ Координаты через упрощение: {coords}")
                        else:
                            print(f"    ❌ Точка {i+1} не может быть геокодирована, пропускаю маршрут")
                            has_geocode_error = True
                            geocode_errors += 1
                            break
                
                if has_geocode_error or not all_coords:
                    errors += 1
                    
                    status = "❌ Ошибка геокодирования точек"
                    if not all_coords_str:
                        coordinates_str = "Ошибка"
                    else:
                        coordinates_str = "; ".join(all_coords_str)
                    
                    ws.cell(row=row_num, column=start_col).value = status
                    ws.cell(row=row_num, column=start_col+1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=start_col+2).value = coordinates_str
                    ws.cell(row=row_num, column=start_col+3).value = len(addresses)
                    ws.cell(row=row_num, column=start_col+4).value = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                    ws.cell(row=row_num, column=start_col+5).value = "Ошибка"
                    
                    processed += 1
                    continue
                
                # ===== РАСЧЕТ МАРШРУТА =====
                route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
                full_coordinates = [start_coords] + all_coords
                
                # Если точек больше 4, предупреждаем
                if len(full_coordinates) > 4:
                    print(f"⚠️ Внимание: {len(full_coordinates)} точек в маршруте")
                    if len(full_coordinates) > 20:
                        route_type = f"{route_type} (упрощено до ключевых точек)"
                    elif len(full_coordinates) > 4:
                        route_type = f"{route_type} (сегментированный расчет)"
                
                print(f"📍 Строю маршрут через {len(full_coordinates)} точек...")
                
                distance = calculate_route(full_coordinates)
                time.sleep(0.5)  # Пауза для API
                
                # Проверяем корректность расстояния
                if distance and distance > 0:
                    if not validate_route_distance(distance, full_coordinates):
                        print(f"⚠️ Подозрительное расстояние: {distance} км")
                        route_errors += 1
                        errors += 1
                        
                        ws.cell(row=row_num, column=start_col).value = "⚠️ Ошибка расчета маршрута (подозрительное расстояние)"
                        ws.cell(row=row_num, column=start_col+1).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                        ws.cell(row=row_num, column=start_col+2).value = "; ".join(all_coords_str)
                        ws.cell(row=row_num, column=start_col+3).value = len(addresses)
                        ws.cell(row=row_num, column=start_col+4).value = route_type
                        ws.cell(row=row_num, column=start_col+5).value = "Ошибка"
                        
                        print(f"⚠️ Ошибка расчета маршрута (подозрительное расстояние)")
                    else:
                        d2, d3 = smart_variations(distance)
                        
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
                    
                    status = "⚠️ Ошибка расчета маршрута"
                    if len(full_coordinates) > 20:
                        status = "⚠️ Слишком много точек (>20)"
                    elif len(full_coordinates) > 4:
                        status = "⚠️ Слишком много точек (>4)"
                    
                    ws.cell(row=row_num, column=start_col).value = status
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
                            f"⏭️ Пропущено: {skipped}\n"
                        )
                        
                        if geocode_errors > 0:
                            progress_text += f"📍 Геокодирование: {geocode_errors}\n"
                        
                        if route_errors > 0:
                            progress_text += f"🛣️ Маршруты: {route_errors}\n"
                        
                        # Показываем текущий обрабатываемый город
                        if processed < total and successful > 0:
                            settlement = extract_settlement_from_address(start_point)
                            if settlement:
                                progress_text += f"📍 Текущий: {settlement[:30]}..."
                        
                        await progress_msg.edit_text(progress_text)
                    except Exception as e:
                        print(f"⚠️ Ошибка обновления прогресса: {e}")
                        
            except Exception as e:
                print(f"❌ Критическая ошибка в строке {row_num}: {e}")
                log_error(row_num, f"{start_point[:50]}...", "CRITICAL", str(e))
                errors += 1
                processed += 1
        
        # ===== СОХРАНЕНИЕ КЭША =====
        save_geocode_cache(geocode_cache)
        
        # ===== СОХРАНЕНИЕ И ОТПРАВКА РЕЗУЛЬТАТА =====
        try:
            await progress_msg.edit_text(
                f"✅ Обработка завершена!\n"
                f"📊 Итоги:\n"
                f"• Всего строк: {total}\n"
                f"• Успешно: {successful}\n"
                f"• Ошибок: {errors}\n"
                f"• Пропущено: {skipped}\n"
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
                    f"• Ошибок: {errors}\n"
                    f"• Пропущено: {skipped}\n\n"
                    f"⚡ **Использовано:**\n"
                    f"• GraphHopper API\n"
                    f"• Яндекс.Геокодер\n"
                    f"• ORS API (запасной)\n"
                    f"• Геокодирование по населенным пунктам\n"
                    f"• Расчет автомобильных маршрутов\n\n"
                    f"⚠️ **Ограничения:**\n"
                    f"• GraphHopper: максимум 4 точки\n"
                    f"• ORS: до 20 точек (запасной)\n"
                    f"• Крым, ДНР, ЛНР не поддерживаются\n\n"
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
                                       "1. Сохранить файл как .xlsx в Excel\n"
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
• Используется GraphHopper API + Яндекс.Геокодер + OpenRouteService
• Геокодируются только города/населенные пункты
• Улицы и номера домов игнорируются
• Автоматическое применение регионов

⚠️ **Ограничения:**
• GraphHopper: максимум 4 точки в маршруте
• ORS: до 20 точек (запасной вариант)
• Крым, ДНР, ЛНР, Херсонская, Запорожская области не поддерживаются
• Маленькие населенные пункты могут не найтись
• Паузы между запросами для соблюдения лимитов API
"""
    await update.message.reply_text(help_text)

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовая команда для проверки работы бота"""
    api_status = "✅ Доступен" if GRAPHHOPPER_API_KEY else "❌ Не настроен"
    yandex_status = "✅ Настроен" if YANDEX_GEOCODER_API_KEY else "❌ Не настроен"
    ors_status = "✅ Настроен" if ORS_API_KEY else "❌ Не настроен"
    
    await update.message.reply_text(
        f"🤖 Бот работает!\n\n"
        f"Отправьте Excel файл для расчета маршрутов.\n\n"
        f"GraphHopper API: {api_status}\n"
        f"Яндекс.Геокодер: {yandex_status}\n"
        f"OpenRouteService: {ors_status}\n\n"
        f"⚠️ Для получения Яндекс.Геокодер API ключа:\n"
        f"1. Зарегистрируйтесь на https://developer.tech.yandex.ru/\n"
        f"2. Получите API ключ для Яндекс.Геокодера\n"
        f"3. Добавьте переменную YANDEX_GEOCODER_API_KEY в Render"
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
    print(f"✅ Яндекс.Геокодер API ключ: {'✅ Настроен' if YANDEX_GEOCODER_API_KEY else '❌ Не настроен'}")
    print(f"✅ OpenRouteService API ключ: {'✅ Настроен' if ORS_API_KEY else '❌ Не настроен'}")
    
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