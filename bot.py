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

DEFAULT_START_COORDS = (47.2357, 39.7011)
USER_START_POINTS = {}

# ================== ЛОГИКА БОТА ==================
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

# ================== TELEGRAM БОТ ==================
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
    if not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте файл DOCX")
        return
    
    if not update.message.document.file_name.endswith('.docx'):
        await update.message.reply_text("❌ Пожалуйста, отправьте файл в формате DOCX")
        return
    
    file = await update.message.document.get_file()
    user_id = update.message.from_user.id

    docx_path = f"temp_{user_id}_{int(time.time())}.docx"
    await file.download_to_drive(docx_path)

    try:
        addresses = read_and_merge_addresses(docx_path)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка чтения файла: {e}")
        if os.path.exists(docx_path):
            os.remove(docx_path)
        return
    
    total = len(addresses)

    if total == 0:
        await update.message.reply_text("❌ В файле нет адресов")
        if os.path.exists(docx_path):
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
            time.sleep(1)

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
                pass

    try:
        await progress_msg.edit_text("✅ Готово! Отправляю файл…")
    except:
        pass

    out_file = f"routes_{user_id}_{int(time.time())}.xlsx"
    wb.save(out_file)

    try:
        with open(out_file, "rb") as file:
            await update.message.reply_document(
                document=file,
                filename=f"маршруты_{user_id}.xlsx"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка отправки файла: {e}")

    try:
        if os.path.exists(docx_path):
            os.remove(docx_path)
        if os.path.exists(out_file):
            os.remove(out_file)
    except:
        pass

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
    application.add_handler(CommandHandler("startpoint", set_start_point))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_doc))
    
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