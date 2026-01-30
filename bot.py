import requests
import openpyxl
import random
import time
import os
from docx import Document
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

# ================== НАСТРОЙКИ ==================

# ВАЖНО: os.getenv() принимает имя переменной окружения, а не её значение!
# Удали эти значения и установи их в Render как переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")  # ⚠️ Исправлено: только имя переменной
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")  # ⚠️ Исправлено
ORS_API_KEY = os.getenv("ORS_API_KEY")  # ⚠️ Исправлено

DEFAULT_START_COORDS = (47.2357, 39.7011)  # Ростов-на-Дону
USER_START_POINTS = {}  # user_id -> (lat, lon)

# ================== ЛОГИКА ==================

def read_and_merge_addresses(path):
    doc = Document(path)
    lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    return [l for l in lines if len(l) > 10 and not l.replace(' ', '').isdigit()]

def yandex_geocode(address):
    if not YANDEX_API_KEY:
        print("⚠️ YANDEX_API_KEY не установлен!")
        return None
    
    url = "https://geocode-maps.yandex.ru/1.x/"
    params = {
        "apikey": YANDEX_API_KEY,
        "format": "json",
        "geocode": address,
        "results": 1
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            print(f"⚠️ Ошибка геокодирования: {r.status_code}")
            return None
        pos = r.json()["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["Point"]["pos"]
        lon, lat = pos.split()
        return float(lat), float(lon)
    except Exception as e:
        print(f"⚠️ Ошибка при геокодировании: {e}")
        return None

def ors_route(start, end):
    if not ORS_API_KEY:
        print("⚠️ ORS_API_KEY не установлен!")
        return None
    
    url = "https://api.openrouteservice.org/v2/directions/driving-car/geojson"
    headers = {"Authorization": ORS_API_KEY}
    body = {"coordinates": [[start[1], start[0]], [end[1], end[0]]]}
    try:
        r = requests.post(url, json=body, headers=headers, timeout=20)
        if r.status_code != 200:
            print(f"⚠️ Ошибка маршрута: {r.status_code}")
            return None
        dist = r.json()["features"][0]["properties"]["summary"]["distance"]
        return round(dist / 1000, 1)
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {e}")
        return None

def variations(base):
    return [
        round(base + random.uniform(5, 20), 1),
        round(max(0, base - random.uniform(5, 20)), 1)
    ]

# ================== TELEGRAM ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет!\n\n"
        "1️⃣ Укажи стартовую точку:\n"
        "/startpoint Город, улица, дом\n\n"
        "2️⃣ Пришли DOCX с адресами\n\n"
        "📊 Я верну Excel с маршрутами"
    )

async def set_start_point(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "❗ Пример:\n/startpoint Ростов-на-Дону, Оганова 22"
        )
        return

    address = " ".join(context.args)
    coords = yandex_geocode(address)

    if not coords:
        await update.message.reply_text("❌ Не смог найти этот адрес")
        return

    USER_START_POINTS[update.message.from_user.id] = coords

    await update.message.reply_text(
        f"✅ Стартовая точка сохранена:\n{address}\n"
        f"📍 {coords[0]}, {coords[1]}"
    )

async def handle_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = await update.message.document.get_file()
    user_id = update.message.from_user.id

    docx_path = f"{user_id}.docx"
    await file.download_to_drive(docx_path)

    addresses = read_and_merge_addresses(docx_path)
    total = len(addresses)

    if total == 0:
        await update.message.reply_text("❌ В файле нет адресов")
        os.remove(docx_path)
        return

    progress_msg = await update.message.reply_text(
        f"⏳ Начинаю обработку\nВсего адресов: {total}"
    )

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Маршруты"
    ws.append([
        "№", "Адрес", "Широта", "Долгота",
        "Маршрут 1 (км)", "Маршрут 2 (км)", "Маршрут 3 (км)"
    ])

    start_coords = USER_START_POINTS.get(user_id, DEFAULT_START_COORDS)

    for i, addr in enumerate(addresses, 1):
        coords = yandex_geocode(addr)

        if coords:
            d1 = ors_route(start_coords, coords)
            time.sleep(1)  # Уменьшил задержку для скорости

            if d1:
                d2, d3 = variations(d1)
            else:
                d2 = d3 = None

            ws.append([i, addr, coords[0], coords[1], d1, d2, d3])
        else:
            ws.append([i, addr, None, None, None, None, None])

        if i % 2 == 0 or i == total:
            try:
                await progress_msg.edit_text(
                    f"⏳ Обработка: {i} / {total}\n"
                    f"📍 {addr[:60]}"
                )
            except:
                pass  # Игнорируем ошибки редактирования

    try:
        await progress_msg.edit_text("✅ Готово! Отправляю файл…")
    except:
        pass

    out_file = f"routes_{user_id}.xlsx"
    wb.save(out_file)

    with open(out_file, "rb") as file:
        await update.message.reply_document(document=file)

    # Очистка временных файлов
    if os.path.exists(docx_path):
        os.remove(docx_path)
    if os.path.exists(out_file):
        os.remove(out_file)

# ================== ЗАПУСК ==================

def main():
    # Проверка токена перед запуском
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("Установи переменную окружения BOT_TOKEN в Render")
        exit(1)
    
    print(f"✅ Токен получен (длина: {len(BOT_TOKEN)})")
    
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("startpoint", set_start_point))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_doc))

    print("🤖 Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()