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

def parse_address_chain(address_string):
    """Парсит цепочку адресов, разделенных дефисами"""
    if not address_string:
        return []
    
    # Заменяем различные тире на обычный дефис
    address_string = address_string.replace('–', '-').replace('—', '-')
    
    # Разделяем по дефису и очищаем
    addresses = [addr.strip() for addr in address_string.split('-') if addr.strip()]
    return addresses

def yandex_geocode(address):
    """Геокодирование адреса через Яндекс API"""
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
            print(f"⚠️ Ошибка геокодирования: {r.status_code} для адреса: {address}")
            return None
        
        data = r.json()
        if (data["response"]["GeoObjectCollection"]["featureMember"] and 
            len(data["response"]["GeoObjectCollection"]["featureMember"]) > 0):
            pos = data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["Point"]["pos"]
            lon, lat = pos.split()
            return float(lat), float(lon)
        else:
            print(f"⚠️ Адрес не найден: {address}")
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
        r = requests.post(url, json=body, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"⚠️ Ошибка маршрута: {r.status_code}")
            return None
        
        data = r.json()
        if data["features"] and data["features"][0]["properties"]["summary"]:
            dist = data["features"][0]["properties"]["summary"]["distance"]
            return round(dist / 1000, 1)
        else:
            return None
    except Exception as e:
        print(f"⚠️ Ошибка при построении маршрута: {e}")
        return None

def variations(base):
    """Генерирует варианты расстояний"""
    if base is None:
        return [None, None]
    
    return [
        round(base + random.uniform(5, 20), 1),
        round(max(0, base - random.uniform(5, 20)), 1)
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
    
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            address_chain = route['address_chain']
            
            # Геокодируем стартовую точку
            if start_point in geocode_cache:
                start_coords = geocode_cache[start_point]
            else:
                start_coords = yandex_geocode(start_point)
                time.sleep(0.5)  # Задержка между запросами
                if start_coords:
                    geocode_cache[start_point] = start_coords
            
            # Парсим цепочку адресов
            addresses = parse_address_chain(address_chain)
            
            # Геокодируем все адреса в цепочке
            all_coords = []
            all_coords_str = []
            geocode_errors = False
            
            for addr in addresses:
                if addr in geocode_cache:
                    coords = geocode_cache[addr]
                else:
                    coords = yandex_geocode(addr)
                    time.sleep(0.5)  # Задержка между запросами
                    if coords:
                        geocode_cache[addr] = coords
                
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    geocode_errors = True
                    break
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(addresses) > 1 else "Прямой"
            
            if geocode_errors or not start_coords or not all_coords:
                # Записываем ошибку
                ws.cell(row=row_num, column=3).value = "❌ Ошибка геокодирования"
                ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}" if start_coords else "Ошибка"
                ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str) if all_coords_str else "Ошибка"
                ws.cell(row=row_num, column=6).value = len(addresses)
                ws.cell(row=row_num, column=7).value = route_type
                ws.cell(row=row_num, column=8).value = "Ошибка"
                ws.cell(row=row_num, column=9).value = ""
                ws.cell(row=row_num, column=10).value = ""
                errors += 1
            else:
                # Строим маршрут: стартовая точка + все точки из цепочки
                full_coordinates = [start_coords] + all_coords
                
                # Рассчитываем маршрут
                distance = ors_route_with_waypoints(full_coordinates)
                time.sleep(1)  # Задержка между запросами к ORS
                
                if distance:
                    d2, d3 = variations(distance)
                    
                    # Записываем результаты
                    ws.cell(row=row_num, column=3).value = "✅ Успешно"
                    ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=6).value = len(addresses)
                    ws.cell(row=row_num, column=7).value = route_type
                    ws.cell(row=row_num, column=8).value = distance
                    ws.cell(row=row_num, column=9).value = d2
                    ws.cell(row=row_num, column=10).value = d3
                    
                    # Форматируем ячейки с расстояниями
                    for col in [8, 9, 10]:
                        cell = ws.cell(row=row_num, column=col)
                        cell.number_format = '0.0'
                else:
                    ws.cell(row=row_num, column=3).value = "⚠️ Ошибка расчета маршрута"
                    ws.cell(row=row_num, column=4).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=5).value = "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=6).value = len(addresses)
                    ws.cell(row=row_num, column=7).value = route_type
                    ws.cell(row=row_num, column=8).value = "Ошибка"
                    ws.cell(row=row_num, column=9).value = ""
                    ws.cell(row=row_num, column=10).value = ""
                    errors += 1
            
            processed += 1
            
            # Обновляем прогресс каждые 5 строк или в конце
            if processed % 5 == 0 or processed == total:
                try:
                    status = f"✅ {processed - errors}" if processed - errors > 0 else ""
                    error_status = f"❌ {errors}" if errors > 0 else ""
                    
                    await progress_msg.edit_text(
                        f"⏳ Обработка: {processed} / {total}\n"
                        f"{status} {error_status}\n"
                        f"📍 Текущий: {start_point[:30]}..."
                    )
                except:
                    pass
                
        except Exception as e:
            print(f"Ошибка обработки строки {route.get('row_num', 'N/A')}: {e}")
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
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def example_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /example - отправляет пример файла"""
    await update.message.reply_text(
        "📋 Пример Excel файла:\n\n"
        "| Колонка A | Колонка B |\n"
        "|-----------|-----------|\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж, ул. Ипподромная 18А |\n"
        "| Ростов-на-Дону, Оганова 22 | г. Воронеж, ул. Ипподромная 18А - г. Сергиев Посад, ул. Кирова 89 |\n"
        "| Ростов-на-Дону, Оганова 22 | р. Карелия, г. Петрозаводск, ул. Вольная 4 - г. Беломорск, ул. Мерецкова 6 |\n\n"
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