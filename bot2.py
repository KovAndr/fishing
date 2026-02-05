import requests
import openpyxl
import os
import asyncio
import threading
import time
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
# Используем бесплатный OSRM вместо ORS (не требует ключа)
OSRM_BASE_URL = "http://router.project-osrm.org/route/v1/driving/"

# ================== УЛУЧШЕННАЯ ЛОГИКА БОТА ==================
def read_from_excel_new_format(path):
    """Чтение Excel файла с двумя колонками: точка А и точка Б"""
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    routes = []
    
    # Начинаем с первой строки (в вашем файле есть заголовки)
    # Пропускаем заголовки
    for row in range(2, ws.max_row + 1):
        point_a = ws.cell(row=row, column=1).value  # Колонка A
        point_b = ws.cell(row=row, column=2).value  # Колонка B
        
        if point_a and point_b:
            # Очищаем адреса
            point_a_clean = str(point_a).strip()
            point_b_clean = str(point_b).strip()
            
            # Проверяем, есть ли промежуточные точки через тире
            if '-' in point_b_clean:
                # Разбиваем на цепочку адресов
                addresses = [addr.strip() for addr in point_b_clean.split('-') if addr.strip()]
                # Первый адрес в цепочке - точка A, остальные - промежуточные
                start_point = point_a_clean
                chain_addresses = addresses
            else:
                # Простой маршрут А -> Б
                start_point = point_a_clean
                chain_addresses = [point_b_clean]
            
            routes.append({
                'row_num': row,
                'start_point': start_point,
                'chain_addresses': chain_addresses,
                'original_a': point_a,
                'original_b': point_b
            })
    
    return routes, wb, ws

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
        "results": 1,
        "lang": "ru_RU"
    }
    
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            print(f"⚠️ Ошибка геокодирования: {r.status_code} для адреса: {address}")
            return None
        
        data = r.json()
        if (data.get("response", {}).get("GeoObjectCollection", {}).get("featureMember") and 
            len(data["response"]["GeoObjectCollection"]["featureMember"]) > 0):
            pos = data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"]["Point"]["pos"]
            lon, lat = pos.split()
            return float(lon), float(lat)  # OSRM использует формат lon,lat
        else:
            print(f"⚠️ Адрес не найден: {address}")
            return None
    except Exception as e:
        print(f"⚠️ Ошибка при геокодировании {address}: {e}")
        return None

def get_coordinates_from_cache(address, geocode_cache):
    """Получение координат из кэша или геокодирование"""
    if address in geocode_cache:
        return geocode_cache[address]
    
    coords = yandex_geocode(address)
    time.sleep(0.3)  # Задержка для соблюдения лимитов API
    if coords:
        geocode_cache[address] = coords
    return coords

def osrm_calculate_route(coordinates):
    """Расчет расстояния через OSRM"""
    if len(coordinates) < 2:
        return None
    
    # Формируем строку координат для OSRM
    coords_str = ";".join([f"{lon},{lat}" for lon, lat in coordinates])
    url = f"{OSRM_BASE_URL}{coords_str}"
    
    params = {
        "overview": "false",
        "geometries": "geojson",
        "steps": "false"
    }
    
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"⚠️ Ошибка OSRM: {r.status_code}")
            return None
        
        data = r.json()
        if data.get("code") == "Ok" and data.get("routes"):
            distance = data["routes"][0]["distance"]  # в метрах
            return round(distance / 1000, 1)  # конвертируем в км
        else:
            print(f"⚠️ Ошибка в ответе OSRM: {data.get('code')}")
            return None
    except Exception as e:
        print(f"⚠️ Ошибка при расчете маршрута: {e}")
        return None

def add_result_columns_new(ws, start_col=3):
    """Добавляет колонки для результатов в Excel"""
    headers = [
        "Статус обработки",
        "Координаты точки А",
        "Координаты точек Б",
        "Количество точек в маршруте",
        "Тип маршрута",
        "Общее расстояние (км)",
        "Расстояние А-1 (км)",
        "Детализация расстояний"
    ]
    
    # Добавляем заголовки
    for i, header in enumerate(headers):
        cell = ws.cell(row=1, column=start_col + i)
        cell.value = header
        cell.font = Font(bold=True, size=11)
        cell.fill = PatternFill(start_color="FFE4B5", end_color="FFE4B5", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    
    # Настраиваем ширину колонок
    column_widths = [20, 25, 30, 20, 20, 15, 15, 40]
    for i, width in enumerate(column_widths):
        column_letter = openpyxl.utils.get_column_letter(start_col + i)
        ws.column_dimensions[column_letter].width = width
    
    return start_col + len(headers)

# ================== TELEGRAM БОТ ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для расчета расстояний между точками.\n\n"
        "📁 **Формат файла:**\n"
        "• Колонка A: Пункт погрузки, грузоотправитель (Точка А)\n"
        "• Колонка B: Пункт назначения, грузополучатель (Точка Б или цепочка)\n\n"
        "📍 **Примеры данных:**\n"
        "• Для прямого маршрута: `г. Ростов-на-Дону, ул. Оганова 22`\n"
        "• Для маршрута с промежуточными точками:\n"
        "  `г. Москва, ул. Тверская - г. Санкт-Петербург, Невский пр. - г. Выборг`\n\n"
        "📊 **Я верну файл с результатами:**\n"
        "• Общее расстояние маршрута\n"
        "• Детализация по отрезкам\n"
        "• Статус обработки\n\n"
        "Просто отправьте мне Excel файл!"
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
        routes, wb, ws = read_from_excel_new_format(input_file)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка чтения файла: {e}")
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    total = len(routes)
    
    if total == 0:
        await update.message.reply_text(
            "❌ В файле нет данных или неправильный формат.\n"
            "Проверьте, что в колонке A и B есть адреса."
        )
        if os.path.exists(input_file):
            os.remove(input_file)
        return
    
    progress_msg = await update.message.reply_text(
        f"⏳ Начинаю обработку\nВсего строк: {total}\nОбработка..."
    )
    
    # Добавляем колонки для результатов
    start_col = add_result_columns_new(ws, start_col=3)
    
    # Определяем индексы колонок для записи результатов
    status_col = 3
    coords_a_col = 4
    coords_b_col = 5
    num_points_col = 6
    route_type_col = 7
    total_distance_col = 8
    segment_distance_col = 9
    details_col = 10
    
    # Кэш для геокодированных адресов
    geocode_cache = {}
    
    processed = 0
    errors = 0
    
    for route in routes:
        try:
            row_num = route['row_num']
            start_point = route['start_point']
            chain_addresses = route['chain_addresses']
            
            # Геокодируем стартовую точку
            start_coords = get_coordinates_from_cache(start_point, geocode_cache)
            
            # Геокодируем все адреса в цепочке
            all_coords = []
            all_coords_str = []
            geocode_errors = False
            
            for addr in chain_addresses:
                coords = get_coordinates_from_cache(addr, geocode_cache)
                if coords:
                    all_coords.append(coords)
                    all_coords_str.append(f"{coords[0]:.6f},{coords[1]:.6f}")
                else:
                    geocode_errors = True
                    break
            
            # Определяем тип маршрута
            route_type = "С промежуточными точками" if len(chain_addresses) > 1 else "Прямой"
            
            if geocode_errors or not start_coords:
                # Записываем ошибку
                ws.cell(row=row_num, column=status_col).value = "❌ Ошибка геокодирования"
                ws.cell(row=row_num, column=coords_a_col).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}" if start_coords else "Ошибка"
                ws.cell(row=row_num, column=coords_b_col).value = "; ".join(all_coords_str) if all_coords_str else "Ошибка"
                ws.cell(row=row_num, column=num_points_col).value = len(chain_addresses)
                ws.cell(row=row_num, column=route_type_col).value = route_type
                ws.cell(row=row_num, column=total_distance_col).value = "Ошибка"
                ws.cell(row=row_num, column=segment_distance_col).value = ""
                ws.cell(row=row_num, column=details_col).value = ""
                errors += 1
            else:
                # Строим полный маршрут: стартовая точка + все точки цепочки
                full_route_coords = [start_coords] + all_coords
                
                # Рассчитываем общий маршрут
                total_distance = osrm_calculate_route(full_route_coords)
                time.sleep(0.5)  # Задержка для OSRM
                
                # Рассчитываем расстояния по отрезкам (если есть промежуточные точки)
                segment_distances = []
                segment_details = []
                
                if len(full_route_coords) >= 2:
                    for i in range(len(full_route_coords) - 1):
                        segment_coords = [full_route_coords[i], full_route_coords[i + 1]]
                        segment_dist = osrm_calculate_route(segment_coords)
                        time.sleep(0.3)
                        
                        if segment_dist:
                            segment_distances.append(segment_dist)
                            from_point = start_point if i == 0 else chain_addresses[i-1]
                            to_point = chain_addresses[i] if i < len(chain_addresses) else chain_addresses[-1]
                            segment_details.append(f"{from_point[:30]}... → {to_point[:30]}...: {segment_dist} км")
                
                if total_distance and segment_distances:
                    # Суммируем отрезки для проверки
                    sum_segments = round(sum(segment_distances), 1)
                    
                    # Записываем результаты
                    ws.cell(row=row_num, column=status_col).value = "✅ Успешно"
                    ws.cell(row=row_num, column=coords_a_col).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=coords_b_col).value = "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=num_points_col).value = len(chain_addresses)
                    ws.cell(row=row_num, column=route_type_col).value = route_type
                    ws.cell(row=row_num, column=total_distance_col).value = total_distance
                    ws.cell(row=row_num, column=segment_distance_col).value = sum_segments if segment_distances else ""
                    ws.cell(row=row_num, column=details_col).value = "\n".join(segment_details)
                    
                    # Форматируем ячейки с расстояниями
                    for col in [total_distance_col, segment_distance_col]:
                        cell = ws.cell(row=row_num, column=col)
                        cell.number_format = '0.0'
                else:
                    ws.cell(row=row_num, column=status_col).value = "⚠️ Ошибка расчета маршрута"
                    ws.cell(row=row_num, column=coords_a_col).value = f"{start_coords[0]:.6f},{start_coords[1]:.6f}"
                    ws.cell(row=row_num, column=coords_b_col).value = "; ".join(all_coords_str)
                    ws.cell(row=row_num, column=num_points_col).value = len(chain_addresses)
                    ws.cell(row=row_num, column=route_type_col).value = route_type
                    ws.cell(row=row_num, column=total_distance_col).value = "Ошибка"
                    ws.cell(row=row_num, column=segment_distance_col).value = ""
                    ws.cell(row=row_num, column=details_col).value = ""
                    errors += 1
            
            processed += 1
            
            # Обновляем прогресс каждые 5 строк
            if processed % 5 == 0 or processed == total:
                try:
                    success_count = processed - errors
                    await progress_msg.edit_text(
                        f"⏳ Обработка: {processed}/{total}\n"
                        f"✅ Успешно: {success_count}\n"
                        f"❌ Ошибок: {errors}\n"
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
                filename=f"результаты_{file_name}",
                caption=(
                    f"✅ Готово!\n"
                    f"Успешно обработано: {processed - errors} строк\n"
                    f"Ошибок: {errors}\n"
                    f"\n"
                    f"📊 Колонки результатов:\n"
                    f"1. Статус обработки\n"
                    f"2. Координаты точки А\n"
                    f"3. Координаты точек Б\n"
                    f"4. Количество точек в маршруте\n"
                    f"5. Тип маршрута\n"
                    f"6. Общее расстояние (км)\n"
                    f"7. Расстояние А-1 (км)\n"
                    f"8. Детализация расстояний"
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
📋 **Бот для расчета расстояний между точками**

📍 **Как использовать:**
1. Подготовьте Excel файл с двумя колонками:
   • Колонка A: Пункт погрузки (Точка А)
   • Колонка B: Пункт назначения (Точка Б или цепочка адресов через дефис)

2. Отправьте файл боту

3. Получите обработанный файл с результатами:

📊 **Колонки результатов:**
• Статус обработки
• Координаты точки А
• Координаты точек Б
• Количество точек в маршруте
• Тип маршрута
• Общее расстояние (км)
• Расстояние А-1 (км)
• Детализация расстояний

📍 **Формат цепочки адресов:**
• Для одного адреса: `г. Москва, ул. Тверская`
• Для нескольких: `г. Москва - г. Санкт-Петербург - г. Выборг`

🚗 **Расчет расстояний:**
• Используется OSRM (Open Source Routing Machine)
• Учитываются промежуточные точки
• Суммируются все отрезки маршрута

⚡ **Команды:**
/start - Начать работу
/help - Эта справка
/example - Пример файла
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def example_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет пример файла"""
    await update.message.reply_text(
        "📋 **Пример Excel файла:**\n\n"
        "| Пункт погрузки (А) | Пункт назначения (Б) |\n"
        "|-------------------|---------------------|\n"
        "| Ростов-на-Дону, Оганова 22 | Москва, Тверская ул. |\n"
        "| Ростов-на-Дону, Оганова 22 | Воронеж - Курск - Белгород |\n"
        "| Ростов-на-Дону, Оганова 22 | Краснодар - Сочи - Анапа |\n\n"
        "📍 **Важно:**\n"
        "• Адреса в колонке B разделяются дефисом `-`\n"
        "• Можно использовать тире `–` или `—`\n"
        "• Для прямого маршрута указывайте один адрес\n\n"
        "Просто создайте Excel файл и отправьте боту!"
    )

# ================== ЗАПУСК С ЗАЩИТОЙ ОТ КОНФЛИКТОВ ==================
async def run_bot():
    """Запускает бота с обработкой конфликтов"""
    print("=" * 50)
    print("🚀 ЗАПУСК ТЕЛЕГРАМ БОТА ДЛЯ РАСЧЕТА РАССТОЯНИЙ")
    print("=" * 50)
    
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("Установите переменную окружения BOT_TOKEN в Render")
        return
    
    print(f"✅ Токен получен")
    print(f"✅ Яндекс API: {'установлен' if YANDEX_API_KEY else 'не установлен'}")
    print(f"✅ OSRM: будет использоваться бесплатный сервис")
    
    # Создаем приложение
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("example", example_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Пытаемся запустить бота с обработкой конфликтов
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