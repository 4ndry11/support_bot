import re
import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Updater, MessageHandler, Filters, CallbackContext,
    CommandHandler, ConversationHandler
)
from collections import Counter
from openpyxl import Workbook
from io import BytesIO

# ==========================================
# НАСТРОЙКИ
# ==========================================
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

# Вебхуки Bitrix24
BITRIX_CONTACT_URL = os.environ["BITRIX_CONTACT_URL"]  # crm.contact.list
BITRIX_TASK_URL = os.environ["BITRIX_TASK_URL"]        # task.item.add

# Админ (только для управления сотрудниками/категориями)
ADMIN_TELEGRAM_ID = 727013047

# Чат поддержки (бот работает только в этом чате)
SUPPORT_CHAT_ID = int(os.environ.get("SUPPORT_CHAT_ID", 0))

# Дефолтный ответственный для новых сотрудников
RESPONSIBLE_ID = 596

# Состояния для ConversationHandler
(
    ADD_EMPLOYEE_TG_ID,
    ADD_EMPLOYEE_BITRIX_ID,
    ADD_EMPLOYEE_NAME,
    ADD_CATEGORY_CODE,
    ADD_CATEGORY_NAME,
    CONFIRM_DUPLICATE
) = range(6)

# ==========================================
# POSTGRESQL CONNECTION POOL
# ==========================================
pool = None
categories_cache = None
categories_cache_time = None

def init_pool():
    global pool
    if pool is None:
        pool = SimpleConnectionPool(1, 10, DATABASE_URL)
    return pool

def get_conn():
    """Получить соединение из пула"""
    if pool is None:
        init_pool()
    return pool.getconn()

def release_conn(conn):
    """Вернуть соединение в пул"""
    if pool:
        pool.putconn(conn)

# ==========================================
# DATABASE FUNCTIONS - EMPLOYEES
# ==========================================

def get_employee_by_telegram_id(telegram_id):
    """Получить сотрудника по Telegram ID"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM support_employees WHERE telegram_id = %s",
                (telegram_id,)
            )
            return cur.fetchone()
    finally:
        release_conn(conn)

def add_employee(telegram_id, name, bitrix_id):
    """Добавить сотрудника"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO support_employees (telegram_id, name, bitrix_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (telegram_id) DO UPDATE
                SET name = EXCLUDED.name, bitrix_id = EXCLUDED.bitrix_id
                """,
                (telegram_id, name, bitrix_id)
            )
            conn.commit()
            return True
    except Exception as e:
        conn.rollback()
        print(f"❌ add_employee error: {e}")
        return False
    finally:
        release_conn(conn)

def delete_employee(telegram_id):
    """Удалить сотрудника"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM support_employees WHERE telegram_id = %s",
                (telegram_id,)
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        print(f"❌ delete_employee error: {e}")
        return False
    finally:
        release_conn(conn)

def get_all_employees():
    """Получить всех сотрудников"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM support_employees ORDER BY name"
            )
            return cur.fetchall()
    finally:
        release_conn(conn)

# ==========================================
# DATABASE FUNCTIONS - CATEGORIES
# ==========================================

def get_category_by_code(code):
    """Получить категорию по коду"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM support_categories WHERE code = %s",
                (code.upper(),)
            )
            return cur.fetchone()
    finally:
        release_conn(conn)

def add_category(code, name):
    """Добавить категорию"""
    global categories_cache, categories_cache_time
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO support_categories (code, name)
                VALUES (%s, %s)
                ON CONFLICT (code) DO UPDATE
                SET name = EXCLUDED.name
                """,
                (code.upper(), name)
            )
            conn.commit()
            # Сбрасываем кэш
            categories_cache = None
            categories_cache_time = None
            return True
    except Exception as e:
        conn.rollback()
        print(f"❌ add_category error: {e}")
        return False
    finally:
        release_conn(conn)

def delete_category(code):
    """Удалить категорию"""
    global categories_cache, categories_cache_time
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM support_categories WHERE code = %s",
                (code.upper(),)
            )
            conn.commit()
            # Сбрасываем кэш
            categories_cache = None
            categories_cache_time = None
            return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        print(f"❌ delete_category error: {e}")
        return False
    finally:
        release_conn(conn)

def get_all_categories(use_cache=True):
    """Получить все категории (с кэшированием на 60 секунд)"""
    global categories_cache, categories_cache_time

    # Проверяем кэш
    if use_cache and categories_cache is not None and categories_cache_time is not None:
        if (datetime.now() - categories_cache_time).total_seconds() < 60:
            return categories_cache

    # Загружаем из БД
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM support_categories ORDER BY code"
            )
            result = cur.fetchall()

            # Обновляем кэш
            if use_cache:
                categories_cache = result
                categories_cache_time = datetime.now()

            return result
    finally:
        release_conn(conn)

# ==========================================
# DATABASE FUNCTIONS - RECORDS
# ==========================================

def add_record(employee_telegram_id, category_code, phone, comment):
    """Добавить запись"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO support_records
                (employee_telegram_id, category_code, phone, comment)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (employee_telegram_id, category_code.upper(), phone, comment)
            )
            conn.commit()
            record_id = cur.fetchone()[0]
            return record_id
    except Exception as e:
        conn.rollback()
        print(f"❌ add_record error: {e}")
        return None
    finally:
        release_conn(conn)

def check_duplicate_record(employee_telegram_id, category_code, phone, minutes=5):
    """Проверить наличие дубликата за последние N минут"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM support_records
                WHERE employee_telegram_id = %s
                AND category_code = %s
                AND phone = %s
                AND timestamp > NOW() - make_interval(mins => %s)
                """,
                (employee_telegram_id, category_code.upper(), phone, minutes)
            )
            count = cur.fetchone()[0]
            return count > 0
    finally:
        release_conn(conn)

def get_records_by_phone(phone, days):
    """Получить записи по телефону за последние N дней"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    r.timestamp,
                    e.name as employee_name,
                    c.name as category_name,
                    r.category_code,
                    r.phone,
                    r.comment
                FROM support_records r
                LEFT JOIN support_employees e ON r.employee_telegram_id = e.telegram_id
                LEFT JOIN support_categories c ON r.category_code = c.code
                WHERE r.phone = %s
                AND r.timestamp > NOW() - make_interval(days => %s)
                ORDER BY r.timestamp DESC
                """,
                (phone, days)
            )
            return cur.fetchall()
    finally:
        release_conn(conn)

def get_team_stats(days):
    """Получить статистику по команде за последние N дней"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Общая статистика
            cur.execute(
                """
                SELECT COUNT(*) as total_records
                FROM support_records
                WHERE timestamp > NOW() - make_interval(days => %s)
                """,
                (days,)
            )
            total = cur.fetchone()['total_records']

            # По сотрудникам
            cur.execute(
                """
                SELECT
                    e.name,
                    COUNT(*) as count
                FROM support_records r
                LEFT JOIN support_employees e ON r.employee_telegram_id = e.telegram_id
                WHERE r.timestamp > NOW() - make_interval(days => %s)
                GROUP BY e.name
                ORDER BY count DESC
                """,
                (days,)
            )
            by_employee = cur.fetchall()

            # По категориям
            cur.execute(
                """
                SELECT
                    c.name,
                    c.code,
                    COUNT(*) as count
                FROM support_records r
                LEFT JOIN support_categories c ON r.category_code = c.code
                WHERE r.timestamp > NOW() - make_interval(days => %s)
                GROUP BY c.name, c.code
                ORDER BY count DESC
                """,
                (days,)
            )
            by_category = cur.fetchall()

            return {
                'total': total,
                'by_employee': by_employee,
                'by_category': by_category
            }
    finally:
        release_conn(conn)

def get_all_records(days):
    """Получить все записи за последние N дней (для экспорта)"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    r.timestamp,
                    e.name as employee_name,
                    c.name as category_name,
                    r.category_code,
                    r.phone,
                    r.comment
                FROM support_records r
                LEFT JOIN support_employees e ON r.employee_telegram_id = e.telegram_id
                LEFT JOIN support_categories c ON r.category_code = c.code
                WHERE r.timestamp > NOW() - make_interval(days => %s)
                ORDER BY r.timestamp DESC
                """,
                (days,)
            )
            return cur.fetchall()
    finally:
        release_conn(conn)

# ==========================================
# УТИЛИТЫ
# ==========================================

def clean_phone(p: str) -> str:
    """Убрать все символы кроме цифр"""
    return re.sub(r"\D", "", p)

def normalize_phone(phone: str) -> str:
    """Нормализовать телефон в формат +380XXXXXXXXX"""
    digits = clean_phone(phone)
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+" + digits

def is_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь админом"""
    return user_id == ADMIN_TELEGRAM_ID

# ==========================================
# ПАРСИНГ СООБЩЕНИЙ
# ==========================================

def parse_message(text: str):
    """
    Парсинг рабочего сообщения формата:
    CODE +380XXXXXXXXX | Комментарий
    """
    # Получаем все доступные коды из БД
    categories = get_all_categories()
    if not categories:
        print("❌ Нет категорий в базе данных")
        return None

    # Создаём список кодов для regex
    codes = [cat['code'] for cat in categories]
    codes_pattern = '|'.join(codes)

    # Динамический regex на основе кодов из БД
    pattern = rf"^({codes_pattern})\s+(\+?[0-9]+)\s*\|\s*(.+)"
    match = re.match(pattern, text.strip(), re.IGNORECASE | re.S)

    if not match:
        print(f"❌ Сообщение не соответствует формату: {text}")
        return None
    code, phone, comment = match.groups()
    phone = normalize_phone(phone)
    print(f"✅ Распознано: code={code}, phone={phone}, comment={comment[:50]}")
    return code.upper(), phone, comment.strip()

# ==========================================
# BITRIX24 ИНТЕГРАЦИЯ
# ==========================================

def find_contact_by_phone(phone):
    """Поиск контакта в Bitrix24 по телефону"""
    norm_phone_full = normalize_phone(phone)
    try:
        r = requests.get(
            BITRIX_CONTACT_URL,
            params={
                "filter[PHONE]": norm_phone_full,
                "select[]": ["ID", "NAME", "LAST_NAME", "PHONE"]
            }
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Bitrix24 error: {e}")
        return None

    result = data.get("result", [])
    if not result:
        return None

    for c in result:
        for ph in c.get("PHONE", []):
            if clean_phone(ph.get("VALUE", "")) == clean_phone(norm_phone_full):
                return c
    return None

def create_task(contact_id, category, comment, responsible_id):
    """Создание задачи в Bitrix24"""
    now = datetime.now()
    deadline = now + timedelta(days=1)
    deadline_str = deadline.strftime("%Y-%m-%dT%H:%M:%S+03:00")

    payload = {
        "fields": {
            "TITLE": f"Запис: {category}",
            "DESCRIPTION": comment,
            "RESPONSIBLE_ID": responsible_id,
            "DEADLINE": deadline_str,
            "UF_CRM_TASK": [f"C_{contact_id}"],
        },
        "notify": True
    }

    task_res = requests.post(BITRIX_TASK_URL, json=payload)
    if task_res.status_code != 200:
        print(f"❌ create_task: {task_res.text}")
        return

    task_id = task_res.json().get("result")
    if not task_id:
        print("❌ create_task: no task id")
        return

    # Добавить комментарий в таймлайн
    comment_url = BITRIX_CONTACT_URL.replace("crm.contact.list", "crm.timeline.comment.add")
    timeline_payload = {
        "fields": {
            "ENTITY_ID": contact_id,
            "ENTITY_TYPE": "contact",
            "COMMENT": f"📌 {category}: {comment}",
            "AUTHOR_ID": responsible_id
        }
    }
    requests.post(comment_url, json=timeline_payload)

    # Завершить задачу
    complete_url = BITRIX_TASK_URL.replace("task.item.add", "task.complete")
    requests.post(complete_url, json={"id": task_id})

# ==========================================
# КОМАНДА: /info
# ==========================================

def handle_info_command(update: Update, context: CallbackContext):
    """
    Команда: /info +380XXXXXXXXX, N
    Показывает информацию по клиенту за последние N дней
    """
    text = update.message.text.strip()
    m = re.match(r"^/info\s+([+\d()\-\s]+)\s*,\s*(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /info +380XXXXXXXXX, N\nНапр.: /info +380631234567, 7")
        return

    phone_raw, days_str = m.groups()
    phone = normalize_phone(phone_raw)
    days = int(days_str)

    # Получаем данные из БД
    records = get_records_by_phone(phone, days)

    # ФИО клиента из CRM
    contact = find_contact_by_phone(phone)
    client_name = None
    if contact:
        client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip()
        if not client_name:
            client_name = None

    total = len(records)
    since_dt = datetime.now() - timedelta(days=days)

    # Агрегация по сотрудникам
    by_emp = Counter(r['employee_name'] for r in records if r['employee_name'])

    # Агрегация по категориям
    by_cat = Counter((r['category_code'], r['category_name']) for r in records if r['category_code'])

    # Последние 5 записей
    latest = records[:5]

    # Формирование ответа
    header_name = client_name if client_name else "Не знайдений у CRM"
    header = (
        f"ℹ️ Інформація по клієнту: {header_name}\n"
        f"📞 Телефон: {phone}\n"
        f"Період: останні {days} дн. (з {since_dt.strftime('%Y-%m-%d')})"
    )
    total_line = f"• Звернень: {total}"

    # За співробітниками
    if by_emp:
        emp_lines = "\n".join([f"   — {emp}: {cnt}" for emp, cnt in by_emp.most_common()])
        emp_block = f"👤 За співробітниками:\n{emp_lines}"
    else:
        emp_block = "👤 За співробітниками: —"

    # По категоріях
    if by_cat:
        cat_lines = []
        for (code, name), cnt in by_cat.most_common():
            cat_lines.append(f"   — {name} ({code}): {cnt}")
        cat_block = "🧩 По категоріях:\n" + "\n".join(cat_lines)
    else:
        cat_block = "🧩 По категоріях: —"

    # Останні записи
    if latest:
        last_lines = []
        for r in latest:
            ts = r['timestamp'].strftime("%Y-%m-%d %H:%M")
            category = r['category_name'] or r['category_code']
            employee = r['employee_name'] or "—"
            comment = r['comment'] or ""
            if len(comment) > 120:
                comment = comment[:117] + "..."
            last_lines.append(f"   • {ts} — {category} — {employee} — {comment}")
        latest_block = "🗒 Останні записи:\n" + "\n".join(last_lines)
    else:
        latest_block = "🗒 Останні записи: —"

    reply = "\n".join([header, total_line, emp_block, cat_block, latest_block])
    update.message.reply_text(reply)

# ==========================================
# КОМАНДА: /team_stats
# ==========================================

def handle_team_stats_command(update: Update, context: CallbackContext):
    """
    Команда: /team_stats N
    Показывает общую статистику по команде за последние N дней
    """
    text = update.message.text.strip()
    m = re.match(r"^/team_stats\s+(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /team_stats N\nНапр.: /team_stats 30")
        return

    days = int(m.group(1))
    stats = get_team_stats(days)

    since_dt = datetime.now() - timedelta(days=days)
    header = (
        f"👥 Командна статистика за {days} дн.\n"
        f"📅 Період: з {since_dt.strftime('%Y-%m-%d')}\n"
        f"• Загалом звернень: {stats['total']}"
    )

    # За співробітниками
    if stats['by_employee']:
        emp_lines = []
        for idx, emp in enumerate(stats['by_employee'], 1):
            name = emp['name'] or "—"
            count = emp['count']
            emp_lines.append(f"{idx}. {name}: {count} звернень")
        emp_block = "\n\n🏆 Топ співробітників:\n" + "\n".join(emp_lines)
    else:
        emp_block = "\n\n🏆 Топ співробітників: —"

    # По категоріях
    if stats['by_category']:
        cat_lines = []
        for cat in stats['by_category']:
            name = cat['name'] or cat['code']
            code = cat['code']
            count = cat['count']
            cat_lines.append(f"   — {name} ({code}): {count}")
        cat_block = "\n\n🧩 По категоріях:\n" + "\n".join(cat_lines)
    else:
        cat_block = "\n\n🧩 По категоріях: —"

    reply = header + emp_block + cat_block
    update.message.reply_text(reply)

# ==========================================
# КОМАНДА: /export
# ==========================================

def handle_export_command(update: Update, context: CallbackContext):
    """
    Команда: /export N
    Экспорт всех записей за последние N дней в Excel
    """
    text = update.message.text.strip()
    m = re.match(r"^/export\s+(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /export N\nНапр.: /export 30")
        return

    days = int(m.group(1))
    records = get_all_records(days)

    if not records:
        update.message.reply_text("❌ Немає записів за цей період")
        return

    # Создаем Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "Звернення"

    # Заголовки
    ws.append(["Дата/час", "Співробітник", "Категорія", "Телефон клієнта", "Коментар"])

    # Данные
    for r in records:
        ws.append([
            r['timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
            r['employee_name'] or "—",
            f"{r['category_name']} ({r['category_code']})" if r['category_name'] else r['category_code'],
            r['phone'],
            r['comment'] or ""
        ])

    # Автоширина колонок
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column].width = adjusted_width

    # Сохраняем в BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    update.message.reply_document(
        document=buffer,
        filename=filename,
        caption=f"📊 Експорт за останні {days} дн. ({len(records)} записів)"
    )

# ==========================================
# КОМАНДА: /list_employees
# ==========================================

def handle_list_employees_command(update: Update, context: CallbackContext):
    """Список всех сотрудников"""
    employees = get_all_employees()

    if not employees:
        update.message.reply_text("❌ Немає співробітників у базі")
        return

    lines = ["👥 Список співробітників:\n"]
    for emp in employees:
        lines.append(
            f"• {emp['name']}\n"
            f"  TG ID: {emp['telegram_id']}\n"
            f"  Bitrix ID: {emp['bitrix_id']}"
        )

    update.message.reply_text("\n".join(lines))

# ==========================================
# КОМАНДА: /list_categories
# ==========================================

def handle_list_categories_command(update: Update, context: CallbackContext):
    """Список всех категорий"""
    categories = get_all_categories(use_cache=False)

    if not categories:
        update.message.reply_text("❌ Немає категорій у базі")
        return

    lines = ["🧩 Список категорій:\n"]
    for cat in categories:
        lines.append(f"• {cat['code']} — {cat['name']}")

    update.message.reply_text("\n".join(lines))

# ==========================================
# КОМАНДА: /add_employee (только для админа)
# ==========================================

def start_add_employee(update: Update, context: CallbackContext):
    """Начало добавления сотрудника"""
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return ConversationHandler.END

    update.message.reply_text("Введіть Telegram ID співробітника:")
    return ADD_EMPLOYEE_TG_ID

def add_employee_tg_id(update: Update, context: CallbackContext):
    """Получение Telegram ID"""
    try:
        tg_id = int(update.message.text.strip())
        context.user_data['new_employee_tg_id'] = tg_id
        update.message.reply_text("Введіть Bitrix ID співробітника:")
        return ADD_EMPLOYEE_BITRIX_ID
    except ValueError:
        update.message.reply_text("❌ Невірний формат. Введіть число (Telegram ID):")
        return ADD_EMPLOYEE_TG_ID

def add_employee_bitrix_id(update: Update, context: CallbackContext):
    """Получение Bitrix ID"""
    try:
        bitrix_id = int(update.message.text.strip())
        context.user_data['new_employee_bitrix_id'] = bitrix_id
        update.message.reply_text("Введіть ПІБ співробітника:")
        return ADD_EMPLOYEE_NAME
    except ValueError:
        update.message.reply_text("❌ Невірний формат. Введіть число (Bitrix ID):")
        return ADD_EMPLOYEE_BITRIX_ID

def add_employee_name(update: Update, context: CallbackContext):
    """Получение имени и сохранение"""
    name = update.message.text.strip()
    tg_id = context.user_data['new_employee_tg_id']
    bitrix_id = context.user_data['new_employee_bitrix_id']

    success = add_employee(tg_id, name, bitrix_id)

    if success:
        update.message.reply_text(
            f"✅ Співробітник додано:\n"
            f"• Telegram ID: {tg_id}\n"
            f"• Bitrix ID: {bitrix_id}\n"
            f"• ПІБ: {name}"
        )
    else:
        update.message.reply_text("❌ Помилка при додаванні співробітника")

    # Очистка
    context.user_data.clear()
    return ConversationHandler.END

def cancel_conversation(update: Update, context: CallbackContext):
    """Отмена разговора"""
    update.message.reply_text("❌ Операція скасована")
    context.user_data.clear()
    return ConversationHandler.END

# ==========================================
# КОМАНДА: /delete_employee (только для админа)
# ==========================================

def handle_delete_employee_command(update: Update, context: CallbackContext):
    """
    Команда: /delete_employee TELEGRAM_ID
    Удаляет сотрудника
    """
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return

    text = update.message.text.strip()
    m = re.match(r"^/delete_employee\s+(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /delete_employee TELEGRAM_ID\nНапр.: /delete_employee 123456789")
        return

    tg_id = int(m.group(1))
    success = delete_employee(tg_id)

    if success:
        update.message.reply_text(f"✅ Співробітник з Telegram ID {tg_id} видалено")
    else:
        update.message.reply_text(f"❌ Співробітник з Telegram ID {tg_id} не знайдений")

# ==========================================
# КОМАНДА: /add_category (только для админа)
# ==========================================

def start_add_category(update: Update, context: CallbackContext):
    """Начало добавления категории"""
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return ConversationHandler.END

    update.message.reply_text("Введіть код категорії (наприклад, CL1):")
    return ADD_CATEGORY_CODE

def add_category_code(update: Update, context: CallbackContext):
    """Получение кода категории"""
    code = update.message.text.strip().upper()
    if not re.match(r"^[A-Z0-9]{2,10}$", code):
        update.message.reply_text("❌ Невірний формат коду. Використовуйте 2-10 літер/цифр:")
        return ADD_CATEGORY_CODE

    context.user_data['new_category_code'] = code
    update.message.reply_text("Введіть назву категорії:")
    return ADD_CATEGORY_NAME

def add_category_name(update: Update, context: CallbackContext):
    """Получение названия и сохранение"""
    name = update.message.text.strip()
    code = context.user_data['new_category_code']

    success = add_category(code, name)

    if success:
        update.message.reply_text(f"✅ Категорія додано: {code} — {name}")
    else:
        update.message.reply_text("❌ Помилка при додаванні категорії")

    context.user_data.clear()
    return ConversationHandler.END

# ==========================================
# КОМАНДА: /delete_category (только для админа)
# ==========================================

def handle_delete_category_command(update: Update, context: CallbackContext):
    """
    Команда: /delete_category CODE
    Удаляет категорию
    """
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("❌ У вас немає доступу до цієї команди")
        return

    text = update.message.text.strip()
    m = re.match(r"^/delete_category\s+([A-Z0-9]+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /delete_category CODE\nНапр.: /delete_category CL1")
        return

    code = m.group(1).upper()
    success = delete_category(code)

    if success:
        update.message.reply_text(f"✅ Категорію {code} видалено")
    else:
        update.message.reply_text(f"❌ Категорію {code} не знайдено")

# ==========================================
# ОБРАБОТКА РАБОЧИХ СООБЩЕНИЙ
# ==========================================

def handle_message(update: Update, context: CallbackContext):
    """Обработка рабочих сообщений"""
    # Логируем для отладки
    print(f"📨 Получено сообщение из чата: {update.message.chat_id}", flush=True)
    print(f"🔧 Настроенный SUPPORT_CHAT_ID: {SUPPORT_CHAT_ID}", flush=True)
    print(f"📝 Текст сообщения: {update.message.text}", flush=True)

    # Проверка чата
    if SUPPORT_CHAT_ID != 0 and update.message.chat_id != SUPPORT_CHAT_ID:
        print(f"⚠️ Сообщение из неразрешенного чата, игнорируем")
        return

    # Если это ответ на подтверждение дубликата
    if context.user_data.get('awaiting_duplicate_confirmation'):
        handle_duplicate_confirmation(update, context)
        return

    parsed = parse_message(update.message.text)
    if not parsed:
        return

    code, phone, comment = parsed

    # Проверка категории
    category = get_category_by_code(code)
    if not category:
        update.message.reply_text(f"❌ Невідома категорія: {code}")
        return

    category_name = category['name']

    # Проверка сотрудника
    employee = get_employee_by_telegram_id(update.message.from_user.id)
    if employee:
        employee_name = employee['name']
        responsible_id = employee['bitrix_id']
    else:
        employee_name = update.message.from_user.full_name
        responsible_id = RESPONSIBLE_ID

    # Проверка дубликата
    is_duplicate = check_duplicate_record(
        update.message.from_user.id,
        code,
        phone,
        minutes=5
    )

    if is_duplicate:
        # Сохраняем данные для подтверждения
        context.user_data['awaiting_duplicate_confirmation'] = True
        context.user_data['pending_record'] = {
            'code': code,
            'phone': phone,
            'comment': comment,
            'category_name': category_name,
            'employee_name': employee_name,
            'responsible_id': responsible_id
        }

        keyboard = [['Так', 'Ні']]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        update.message.reply_text(
            f"⚠️ Ви вже записували категорію {code} для цього клієнта менше 5 хв тому.\n"
            f"Продовжити?",
            reply_markup=reply_markup
        )
        return

    # Запись в БД
    save_record(update, context, code, phone, comment, category_name, employee_name, responsible_id)

def handle_duplicate_confirmation(update: Update, context: CallbackContext):
    """Обработка подтверждения дубликата"""
    response = update.message.text.strip().lower()
    context.user_data['awaiting_duplicate_confirmation'] = False

    if response in ['так', 'yes', 'y', 'да']:
        pending = context.user_data.get('pending_record')
        if pending:
            save_record(
                update, context,
                pending['code'],
                pending['phone'],
                pending['comment'],
                pending['category_name'],
                pending['employee_name'],
                pending['responsible_id']
            )
    else:
        update.message.reply_text("❌ Операція скасована", reply_markup=ReplyKeyboardRemove())

    context.user_data.clear()

def save_record(update, context, code, phone, comment, category_name, employee_name, responsible_id):
    """Сохранить запись в БД и Bitrix"""
    # Контакт в Bitrix
    contact = find_contact_by_phone(phone)
    if not contact:
        update.message.reply_text("❗ Клієнт не знайдений у CRM", reply_markup=ReplyKeyboardRemove())
        return

    # Задача в Bitrix
    create_task(contact["ID"], category_name, comment, responsible_id)

    # Запись в БД
    record_id = add_record(
        update.message.from_user.id,
        code,
        phone,
        comment
    )

    if record_id:
        client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip()
        update.message.reply_text(
            f"✅ Запис збережено: {category_name} – {client_name}",
            reply_markup=ReplyKeyboardRemove()
        )
    else:
        update.message.reply_text(
            "⚠ Помилка збереження у БД, але задача у Bitrix створена",
            reply_markup=ReplyKeyboardRemove()
        )

# ==========================================
# MAIN
# ==========================================

def main():
    # Инициализация пула соединений
    init_pool()

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    # Команда /info
    dp.add_handler(CommandHandler("info", handle_info_command))

    # Команда /team_stats
    dp.add_handler(CommandHandler("team_stats", handle_team_stats_command))

    # Команда /export
    dp.add_handler(CommandHandler("export", handle_export_command))

    # Команда /list_employees
    dp.add_handler(CommandHandler("list_employees", handle_list_employees_command))

    # Команда /list_categories
    dp.add_handler(CommandHandler("list_categories", handle_list_categories_command))

    # Команда /delete_employee
    dp.add_handler(CommandHandler("delete_employee", handle_delete_employee_command))

    # Команда /delete_category
    dp.add_handler(CommandHandler("delete_category", handle_delete_category_command))

    # ConversationHandler для /add_employee
    add_employee_handler = ConversationHandler(
        entry_points=[CommandHandler("add_employee", start_add_employee)],
        states={
            ADD_EMPLOYEE_TG_ID: [MessageHandler(Filters.text & ~Filters.command, add_employee_tg_id)],
            ADD_EMPLOYEE_BITRIX_ID: [MessageHandler(Filters.text & ~Filters.command, add_employee_bitrix_id)],
            ADD_EMPLOYEE_NAME: [MessageHandler(Filters.text & ~Filters.command, add_employee_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )
    dp.add_handler(add_employee_handler)

    # ConversationHandler для /add_category
    add_category_handler = ConversationHandler(
        entry_points=[CommandHandler("add_category", start_add_category)],
        states={
            ADD_CATEGORY_CODE: [MessageHandler(Filters.text & ~Filters.command, add_category_code)],
            ADD_CATEGORY_NAME: [MessageHandler(Filters.text & ~Filters.command, add_category_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )
    dp.add_handler(add_category_handler)

    # Логирование рабочих сообщений
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    updater.start_polling()
    print("✅ Бот запущено!")
    updater.idle()

if __name__ == "__main__":
    main()
