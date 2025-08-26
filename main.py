import re
import os
import json
import requests
import gspread
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext, CommandHandler
from google.oauth2.service_account import Credentials
from collections import Counter, defaultdict

# === НАСТРОЙКИ ===
BOT_TOKEN = os.environ["BOT_TOKEN"]

# Вебхуки Bitrix24
BITRIX_CONTACT_URL = os.environ["BITRIX_CONTACT_URL"]
BITRIX_TASK_URL = os.environ["BITRIX_TASK_URL"]

# Google Sheets
SPREADSHEET_NAME = os.environ["SPREADSHEET_NAME"]

RESPONSIBLE_ID = 596

# Категории
CATEGORIES = {
    "CL1": "Дзвінки дрібні",
    "CL2": "Дзвінки середні",
    "CL3": "Дзвінки довготривалі",
    "SMS": "СМС",
    "SEC": "СБ (супровід)",
    "CNF": "Конференція",
    "NEW": "Перший контакт",
    "HS1": "Опрацювання історії легке",
    "HS2": "Опрацювання історії середнє",
    "HS3": "Опрацювання історії складне",
    "REP": "Повторне звернення"
}

# Оцінка тривалості (хвилини) для кожної категорії — за потреби змініть
DURATION_MIN = {
    "CL1": 3,
    "CL2": 7,
    "CL3": 15,
    "SMS": 0,
    "SEC": 10,
    "CNF": 30,
    "NEW": 5,
    "HS1": 10,
    "HS2": 20,
    "HS3": 35,
    "REP": 5,
}

# Сотрудники (по Telegram ID)
EMPLOYEES = {
    727013047: {"name": "Іваненко Андрій", "b24_id": 596},
    5690994847: {"name": "Заиц Валерия", "b24_id": 2289},
    6555660815: {"name": "Ніконова Тетяна", "b24_id": 594},
    1062635787: {"name": "Грушева Тетяна", "b24_id": 592},
    878632178: {"name": "Станіславова Анастасія", "b24_id": 632},
    887279899: {"name": "Лабік Геннадій", "b24_id": 631},
    724515180: {"name": "Гайсіна Ганна", "b24_id": 1104},
    531712678: {"name": "Петрич Стелла", "b24_id": 1106}
}

# === Google Sheets Init ===
def init_gsheets():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_path = "/etc/secrets/gsheets.json"  # путь по умолчанию в Render
    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open(os.environ["SPREADSHEET_NAME"]).sheet1
    return sheet

# === Вспомогательные функции для телефонов ===
def clean_phone(p: str) -> str:
    return re.sub(r"\D", "", p)

def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+" + digits

# === Парсинг сообщений (рабочие записи) ===
def parse_message(text: str):
    match = re.match(
        r"^(CL1|CL2|CL3|SMS|SEC|CNF|NEW|REP|HS1|HS2|HS3)\s+(\+?[0-9]+)\s*\|\s*(.+)",
        text.strip(),
        re.IGNORECASE | re.S
    )
    if not match:
        return None
    code, phone, comment = match.groups()

    phone = normalize_phone(phone)
    return code.upper(), phone, comment.strip()

# === Bitrix: пошук контакта з повною посторінковою загрузкою ===
def find_contact_by_phone(phone):
    norm_phone_full = normalize_phone(phone)  # Наприклад: +380631234567
    print(f"🔍 Шукаємо по API фільтром: {norm_phone_full}")

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
        print(f"❌ Помилка при запиті до Bitrix24: {e}")
        return None

    result = data.get("result", [])
    if not result:
        print("❌ Клієнт не знайдений")
        return None

    # Перевірка точного співпадіння
    for c in result:
        for ph in c.get("PHONE", []):
            if clean_phone(ph.get("VALUE", "")) == clean_phone(norm_phone_full):
                print(f"✅ Знайдено: {c.get('NAME')} {c.get('LAST_NAME')}")
                return c

    print("❌ Клієнт не знайдений навіть у результатах")
    return None

# === Bitrix: создание задачи ===
def create_task(contact_id, category, comment, responsible_id):
    now = datetime.now()
    deadline = now + timedelta(days=1)
    deadline_str = deadline.strftime("%Y-%m-%dT%H:%M:%S+03:00")

    # 1. Створення задачі
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
        print(f"❌ Помилка при створенні задачі: {task_res.text}")
        return

    task_data = task_res.json()
    task_id = task_data.get("result")
    if not task_id:
        print("❌ Bitrix24 не повернув ID задачі")
        return

    print(f"✅ Задача створена: ID={task_id}")

    # 2. Додаємо коментар до таймлайну клієнта
    comment_url = BITRIX_CONTACT_URL.replace("crm.contact.list", "crm.timeline.comment.add")
    timeline_payload = {
        "fields": {
            "ENTITY_ID": contact_id,
            "ENTITY_TYPE": "contact",
            "COMMENT": f"📌 {category}: {comment}",
            "AUTHOR_ID": responsible_id
        }
    }

    comment_res = requests.post(comment_url, json=timeline_payload)
    if comment_res.status_code == 200:
        print("✅ Коментар додано до таймлайну")
    else:
        print(f"⚠️ Коментар не додано: {comment_res.text}")

    # 3. Завершуємо задачу
    complete_url = BITRIX_TASK_URL.replace("task.item.add", "task.complete")
    complete_payload = {"id": task_id}
    complete_res = requests.post(complete_url, json=complete_payload)

    if complete_res.status_code == 200:
        print("✅ Задача закрита одразу (API повернув 200)")
    else:
        print(f"⚠️ Не вдалося завершити задачу: {complete_res.text}")

# === Утиліти для форматування ===
def format_minutes(total_min: int) -> str:
    if total_min <= 0:
        return "0 хв"
    hours, minutes = divmod(total_min, 60)
    if hours and minutes:
        return f"{hours} год {minutes} хв"
    if hours:
        return f"{hours} год"
    return f"{minutes} хв"

def safe_str(x):
    return "" if x is None else str(x)

# === Агрегація з Google Sheets для /info ===
def aggregate_client_info_from_sheet(phone: str, days: int):
    sheet = init_gsheets()
    values = sheet.get_all_values()  # очікуємо, що кожен рядок: [timestamp, employee, category, phone, comment, status]

    phone_norm = normalize_phone(phone)
    since_dt = datetime.now() - timedelta(days=days)

    rows = []
    for row in values:
        if len(row) < 4:
            continue

        ts_raw = safe_str(row[0]).strip()
        employee = safe_str(row[1]).strip()
        category = safe_str(row[2]).strip().upper()
        phone_row = normalize_phone(safe_str(row[3]).strip())
        comment = safe_str(row[4]).strip() if len(row) > 4 else ""

        # Якщо перший рядок заголовки — пропускаємо
        if ts_raw.lower() in ("timestamp", "дата", "time"):
            continue

        try:
            # ваш код пише у форматі "%Y-%m-%d %H:%M:%S"
            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            # якщо інший формат — пробуємо ISO або пропускаємо
            try:
                ts = datetime.fromisoformat(ts_raw)
            except Exception:
                continue

        if ts < since_dt:
            continue
        if phone_row != phone_norm:
            continue

        rows.append({
            "ts": ts,
            "employee": employee,
            "category": category,
            "comment": comment
        })

    total = len(rows)
    by_emp = Counter(r["employee"] for r in rows if r["employee"])
    by_cat = Counter(r["category"] for r in rows if r["category"])

    total_minutes = 0
    cat_minutes = defaultdict(int)
    for cat, cnt in by_cat.items():
        m = DURATION_MIN.get(cat, 0) * cnt
        cat_minutes[cat] = m
        total_minutes += m

    # Останні записи (до 5)
    latest = sorted(rows, key=lambda x: x["ts"], reverse=True)[:5]

    return {
        "total": total,
        "by_emp": by_emp,
        "by_cat": by_cat,
        "total_minutes": total_minutes,
        "cat_minutes": cat_minutes,
        "latest": latest,
        "since": since_dt
    }

# === Команда /info +380..., N ===
def handle_info_command(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    m = re.match(r"^/info\s+([+\d()\-\s]+)\s*,\s*(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /info +380XXXXXXXXX, N\nНапр.: /info +380631234567, 7")
        return

    phone_raw, days_str = m.groups()
    phone = normalize_phone(phone_raw)
    days = int(days_str)

    data = aggregate_client_info_from_sheet(phone, days)

    # Побудова відповіді
    header = f"ℹ️ Інформація по клієнту {phone}\nПеріод: останні {days} дн. (з {data['since'].strftime('%Y-%m-%d')})"
    total_line = f"• Звернень: {data['total']}"
    time_line = f"• Орієнтовний час спілкування: {format_minutes(data['total_minutes'])}"

    # За співробітниками
    if data["by_emp"]:
        emp_lines = "\n".join([f"   — {emp}: {cnt}" for emp, cnt in data["by_emp"].most_common()])
        emp_block = f"👤 За співробітниками:\n{emp_lines}"
    else:
        emp_block = "👤 За співробітниками: —"

    # По категоріях
    if data["by_cat"]:
        cat_lines = []
        for cat, cnt in data["by_cat"].most_common():
            label = CATEGORIES.get(cat, cat)
            mins = data["cat_minutes"].get(cat, 0)
            extra = f" (~{format_minutes(mins)})" if mins else ""
            cat_lines.append(f"   — {label} ({cat}): {cnt}{extra}")
        cat_block = "🧩 По категоріях:\n" + "\n".join(cat_lines)
    else:
        cat_block = "🧩 По категоріях: —"

    # Останні записи (до 5)
    if data["latest"]:
        last_lines = []
        for r in data["latest"]:
            ts = r["ts"].strftime("%Y-%m-%d %H:%M")
            label = CATEGORIES.get(r["category"], r["category"])
            comment = r["comment"]
            employee = r["employee"] or "—"
            # трохи обрізаємо коментар
            if len(comment) > 120:
                comment = comment[:117] + "..."
            last_lines.append(f"   • {ts} — {label} — {employee} — {comment}")
        latest_block = "🗒️ Останні записи:\n" + "\n".join(last_lines)
    else:
        latest_block = "🗒️ Останні записи: —"

    reply = "\n".join([header, total_line, time_line, emp_block, cat_block, latest_block])
    update.message.reply_text(reply)

# === Обработка рабочих сообщений (категорії) ===
def handle_message(update: Update, context: CallbackContext):
    parsed = parse_message(update.message.text)
    if not parsed:
        return

    code, phone, comment = parsed
    category = CATEGORIES.get(code, "Невідома категорія")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # отримуємо інфо про співробітника
    employee_data = EMPLOYEES.get(update.message.from_user.id)
    if employee_data:
        employee_name = employee_data["name"]
        responsible_id = employee_data["b24_id"]
    else:
        employee_name = update.message.from_user.full_name
        responsible_id = RESPONSIBLE_ID  # дефолтний відповідальний

    # пошук клієнта
    contact = find_contact_by_phone(phone)
    if not contact:
        update.message.reply_text("❗ Клієнт не знайдений у CRM")
        return

    # створення задачі (від імені менеджера)
    create_task(contact["ID"], category, comment, responsible_id)

    # запис у Google Sheets
    try:
        sheet = init_gsheets()
        sheet.append_row([timestamp, employee_name, code, phone, comment, "Виконано"])
    except Exception as e:
        update.message.reply_text(f"⚠ Помилка Google Sheets: {e}")
        return

    client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip()
    update.message.reply_text(f"✅ Запис збережено: {category} – {client_name}")

# === MAIN ===
def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    # Команда /info
    dp.add_handler(CommandHandler("info", handle_info_command))

    # Текстові записи по категоріях
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
