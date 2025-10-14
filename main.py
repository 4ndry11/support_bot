import re
import os
import json
import requests
import gspread
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext, CommandHandler
from google.oauth2.service_account import Credentials
from collections import Counter
from typing import List, Dict, Any
import pytz  # <<< используем pytz для таймзон

# === НАСТРОЙКИ ===
BOT_TOKEN = os.environ["BOT_TOKEN"]

# Вебхуки Bitrix24
BITRIX_CONTACT_URL = os.environ["BITRIX_CONTACT_URL"]  # crm.contact.list
BITRIX_TASK_URL = os.environ.get("BITRIX_TASK_URL", "")        # task.item.add (для ДР не обязателен)
BITRIX_USERS_URL = os.environ.get("BITRIX_USERS_URL", "")       # user.get (для сотрудников)

# Куда слать отчёты по ДР (несколько ID через запятую)
SUPPORT_CHAT_IDS = [
    int(x) for x in os.environ.get("SUPPORT_CHAT_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
]

# Таймзона/время ежедневной проверки (pytz)
TZ_NAME = os.environ.get("TZ_NAME", "Europe/Kyiv")
BIRTHDAY_CHECK_HOUR = int(os.environ.get("BIRTHDAY_CHECK_HOUR", "9"))
BIRTHDAY_CHECK_MINUTE = int(os.environ.get("BIRTHDAY_CHECK_MINUTE", "0"))

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

# Сотрудники (по Telegram ID)
EMPLOYEES = {
    727013047: {"name": "Іваненко Андрій", "b24_id": 596},
    5690994847: {"name": "Заиц Валерия", "b24_id": 2289},
    6555660815: {"name": "Ніконова Тетяна", "b24_id": 594},
    1062635787: {"name": "Грушева Тетяна", "b24_id": 592},
    878632178: {"name": "Станіславова Анастасія", "b24_id": 632},
    887279899: {"name": "Лабік Геннадій", "b24_id": 631},
    724515180: {"name": "Гайсіна Ганна", "b24_id": 1104},
    531712678: {"name": "Петрич Стелла", "b24_id": 1106},
    8183276948: {"name": "Швець Максим", "b24_id": 2627}
}

# === Google Sheets Init ===
def init_gsheets():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_path = "/etc/secrets/gsheets.json"  # Render default path
    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).sheet1
    return sheet

# === Телефоны ===
def clean_phone(p: str) -> str:
    return re.sub(r"\D", "", p or "")

def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    if not digits:
        raise ValueError("empty phone")
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+" + digits

# === Парсинг рабочих сообщений (логирование) ===
def parse_message(text: str):
    match = re.match(
        r"^(CL1|CL2|CL3|SMS|SEC|CNF|NEW|REP|HS1|HS2|HS3)\s+(\+?[0-9()\-\s]+)\s*\|\s*(.+)",
        (text or "").strip(),
        re.IGNORECASE | re.S
    )
    if not match:
        return None
    code, phone, comment = match.groups()
    phone = normalize_phone(phone)
    return code.upper(), phone, comment.strip()

# === Bitrix helpers ===
def b24_paged_get(url: str, base_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Универсальная пагинация Bitrix24: добавляет start, собирает все result/ items.
    """
    items: List[Dict[str, Any]] = []
    start = 0
    while True:
        params = dict(base_params or {})
        params["start"] = start
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"❌ Bitrix request failed: {e}")
            break

        chunk = data.get("result", [])
        if isinstance(chunk, dict) and "items" in chunk:
            chunk = chunk.get("items", [])
        if not chunk:
            break

        items.extend(chunk)

        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start
    return items

# === Bitrix: поиск контакта по телефону ===
def find_contact_by_phone(phone):
    norm_phone_full = normalize_phone(phone)  # напр.: +380631234567
    try:
        r = requests.get(
            BITRIX_CONTACT_URL,
            params={
                "filter[PHONE]": norm_phone_full,
                "select[]": ["ID", "NAME", "LAST_NAME", "PHONE"]
            },
            timeout=30
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
        for ph in c.get("PHONE", []) or []:
            if clean_phone(ph.get("VALUE", "")) == clean_phone(norm_phone_full):
                return c
    return None

# === Bitrix: дни рождения ===
def _now_tz():
    try:
        return datetime.now(pytz.timezone(TZ_NAME))
    except Exception:
        return datetime.now()

def today_month_day():
    now = _now_tz()
    return now.month, now.day

def parse_b24_date(d: str):
    """Принимает 'YYYY-MM-DD' или ISO, возвращает (month, day) либо None."""
    if not d:
        return None
    s = d.strip()[:10]
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.month, dt.day
    except Exception:
        return None

def b24_get_employees_birthday_today() -> List[Dict[str, Any]]:
    """
    Возвращает список сотрудников с ДР сегодня.
    Требует BITRIX_USERS_URL (метод user.get). Поле: PERSONAL_BIRTHDAY.
    """
    if not BITRIX_USERS_URL:
        print("⚠ BITRIX_USERS_URL not set; skip employees birthdays")
        return []

    month_today, day_today = today_month_day()
    items = b24_paged_get(
        BITRIX_USERS_URL,
        {
            "FILTER[ACTIVE]": "Y",
            "SELECT[]": ["ID", "NAME", "LAST_NAME", "PERSONAL_BIRTHDAY", "ACTIVE"]
        }
    )

    result = []
    for u in items:
        md = parse_b24_date(u.get("PERSONAL_BIRTHDAY"))
        if md and md == (month_today, day_today):
            full_name = f"{(u.get('NAME') or '').strip()} {(u.get('LAST_NAME') or '').strip()}".strip() or "Без імені"
            result.append({"id": u.get("ID"), "name": full_name})

    result.sort(key=lambda x: x["name"].lower())
    return result

def b24_get_clients_birthday_today() -> List[Dict[str, Any]]:
    """
    Возвращает список клиентов с ДР сегодня, с телефонами.
    Поля контакта: BIRTHDATE, NAME, LAST_NAME, PHONE.
    """
    month_today, day_today = today_month_day()
    items = b24_paged_get(
        BITRIX_CONTACT_URL,
        {
            "filter[!BIRTHDATE]": "",  # только у кого заполнено BIRTHDATE
            "select[]": ["ID", "NAME", "LAST_NAME", "BIRTHDATE", "PHONE"]
        }
    )

    result = []
    for c in items:
        md = parse_b24_date(c.get("BIRTHDATE"))
        if not md or md != (month_today, day_today):
            continue

        full_name = f"{(c.get('NAME') or '').strip()} {(c.get('LAST_NAME') or '').strip()}".strip() or "Без імені"
        phones = []
        for ph in c.get("PHONE", []) or []:
            val = ph.get("VALUE")
            if not val:
                continue
            try:
                phones.append(normalize_phone(val))
            except Exception:
                pass

        # уникализируем телефоны
        seen = set()
        uniq_phones = []
        for p in phones:
            k = clean_phone(p)
            if k not in seen:
                seen.add(k)
                uniq_phones.append(p)

        result.append({"id": c.get("ID"), "name": full_name, "phones": uniq_phones})

    result.sort(key=lambda x: x["name"].lower())
    return result

def format_birthday_message() -> str:
    employees = b24_get_employees_birthday_today()
    clients = b24_get_clients_birthday_today()

    if not employees and not clients:
        return "📅 На сьогодні днів народження немає."

    lines = ["🎂 Щоденна перевірка днів народження:"]
    if employees:
        lines.append("\n👥 Співробітники:")
        for e in employees:
            lines.append(f"• {e['name']}")

    if clients:
        lines.append("\n🧑‍💼 Клієнти:")
        for c in clients:
            if c["phones"]:
                lines.append(f"• {c['name']} — {', '.join(c['phones'])}")
            else:
                lines.append(f"• {c['name']} — (тел. відсутній)")

    return "\n".join(lines)

def notify_birthday_today(context: CallbackContext):
    """Ежедневный джоб: собрать и отправить сообщение в SUPPORT_CHAT_IDS."""
    try:
        text = format_birthday_message()
    except Exception as e:
        print(f"❌ format_birthday_message failed: {e}")
        text = "⚠ Не вдалося отримати інформацію про дні народження. Перевірте логи/доступи Bitrix (user.get / crm.contact.list)."

    if not SUPPORT_CHAT_IDS:
        print("⚠ SUPPORT_CHAT_IDS is empty; nowhere to send birthday report")
        return

    for chat_id in SUPPORT_CHAT_IDS:
        try:
            context.bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            print(f"❌ send_message to {chat_id} failed: {e}")

# === Bitrix: создание/закрытие задачи (для рабочих записей) ===
def create_task(contact_id, category, comment, responsible_id):
    if not BITRIX_TASK_URL:
        print("⚠ BITRIX_TASK_URL not set; skip create_task")
        return

    now = _now_tz()
    deadline = now + timedelta(days=1)
    # Строка в локальном времени, часовой пояс фиксируем как +03:00 (как было у вас)
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

    try:
        task_res = requests.post(BITRIX_TASK_URL, json=payload, timeout=30)
        task_res.raise_for_status()
    except Exception as e:
        print(f"❌ create_task request failed: {e}")
        return

    task_id = (task_res.json() or {}).get("result")
    if not task_id:
        print("❌ create_task: no task id in response")
        return

    # таймлайн
    try:
        comment_url = BITRIX_CONTACT_URL.replace("crm.contact.list", "crm.timeline.comment.add")
        timeline_payload = {
            "fields": {
                "ENTITY_ID": contact_id,
                "ENTITY_TYPE": "contact",
                "COMMENT": f"📌 {category}: {comment}",
                "AUTHOR_ID": responsible_id
            }
        }
        requests.post(comment_url, json=timeline_payload, timeout=30)
    except Exception as e:
        print(f"⚠ timeline comment failed: {e}")

    # завершить
    try:
        complete_url = BITRIX_TASK_URL.replace("task.item.add", "task.complete")
        requests.post(complete_url, json={"id": task_id}, timeout=30)
    except Exception as e:
        print(f"⚠ task complete failed: {e}")

# === Утилиты ===
def safe_str(x):
    return "" if x is None else str(x)

# === Агрегация из Google Sheets для /info (без расчёта времени) ===
def aggregate_client_info_from_sheet(phone: str, days: int):
    sheet = init_gsheets()
    values = sheet.get_all_values()  # ожидаем: [timestamp, employee, category(code), phone, comment, status]

    phone_norm = normalize_phone(phone)
    since_dt = _now_tz() - timedelta(days=days)

    rows = []
    for row in values:
        if len(row) < 4:
            continue

        ts_raw = safe_str(row[0]).strip()
        employee = safe_str(row[1]).strip()
        category = safe_str(row[2]).strip().upper()
        phone_row = normalize_phone(safe_str(row[3]).strip())
        comment = safe_str(row[4]).strip() if len(row) > 4 else ""

        # пропускаем заголовки
        if ts_raw.lower() in ("timestamp", "дата", "time"):
            continue

        # парсим дату
        ts = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                ts = datetime.strptime(ts_raw, fmt)
                break
            except Exception:
                continue
        if ts is None:
            try:
                ts = datetime.fromisoformat(ts_raw)
            except Exception:
                continue

        if ts < since_dt.replace(tzinfo=None) or phone_row != phone_norm:
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

    latest = sorted(rows, key=lambda x: x["ts"], reverse=True)[:5]

    return {
        "total": total,
        "by_emp": by_emp,
        "by_cat": by_cat,
        "latest": latest,
        "since": since_dt
    }

# === Команда /info +380..., N ===
def handle_info_command(update: Update, context: CallbackContext):
    text = (update.message.text or "").strip()
    m = re.match(r"^/info\s+([+\d()\-\s]+)\s*,\s*(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /info +380XXXXXXXXX, N\nНапр.: /info +380631234567, 7")
        return

    phone_raw, days_str = m.groups()
    try:
        phone = normalize_phone(phone_raw)
    except Exception:
        update.message.reply_text("Некоректний номер. Приклад: +380631234567")
        return
    days = int(days_str)

    # ФИО клиента из CRM
    contact = find_contact_by_phone(phone)
    client_name = None
    if contact:
        client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip() or None

    data = aggregate_client_info_from_sheet(phone, days)

    header_name = client_name if client_name else "Не знайдений у CRM"
    header = (
        f"ℹ️ Інформація по клієнту: {header_name}\n"
        f"📞 Телефон: {phone}\n"
        f"Період: останні {days} дн. (з {data['since'].strftime('%Y-%m-%d')})"
    )
    total_line = f"• Звернень: {data['total']}"

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
            cat_lines.append(f"   — {label} ({cat}): {cnt}")
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
            if len(comment) > 120:
                comment = comment[:117] + "..."
            last_lines.append(f"   • {ts} — {label} — {employee} — {comment}")
        latest_block = "🗒️ Останні записи:\n" + "\n".join(last_lines)
    else:
        latest_block = "🗒️ Останні записи: —"

    reply = "\n".join([header, total_line, emp_block, cat_block, latest_block])
    update.message.reply_text(reply)

# === Команда /birthdays ===
def handle_birthdays_command(update: Update, context: CallbackContext):
    try:
        text = format_birthday_message()
    except Exception as e:
        print(f"❌ /birthdays failed: {e}")
        text = "⚠ Помилка під час отримання переліку днів народження."
    update.message.reply_text(text)

# === Обработка рабочих сообщений (категорії) ===
def handle_message(update: Update, context: CallbackContext):
    parsed = parse_message(update.message.text)
    if not parsed:
        return

    code, phone, comment = parsed
    category = CATEGORIES.get(code, "Невідома категорія")
    timestamp = _now_tz().strftime("%Y-%m-%d %H:%M:%S")

    # співробітник
    employee_data = EMPLOYEES.get(update.message.from_user.id)
    if employee_data:
        employee_name = employee_data["name"]
        responsible_id = employee_data["b24_id"]
    else:
        employee_name = update.message.from_user.full_name
        responsible_id = RESPONSIBLE_ID

    # контакт
    contact = find_contact_by_phone(phone)
    if not contact:
        update.message.reply_text("❗ Клієнт не знайдений у CRM")
        return

    # задача в Bitrix
    create_task(contact["ID"], category, comment, responsible_id)

    # запись в Google Sheets
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

    # Команды
    dp.add_handler(CommandHandler("info", handle_info_command))
    dp.add_handler(CommandHandler("birthdays", handle_birthdays_command))  # ручной запуск проверки ДР

    # Логирование рабочих сообщений
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    import datetime as _dt
    try:
        tz = pytz.timezone(TZ_NAME)
    except Exception:
        tz = pytz.utc

    # Старт polling
    updater.start_polling()

    # Регистрируем job после старта polling (ВАЖНО!)
    job_queue = updater.job_queue
    job_queue.run_daily(
        notify_birthday_today,
        time=_dt.time(hour=BIRTHDAY_CHECK_HOUR, minute=BIRTHDAY_CHECK_MINUTE, tzinfo=tz),
        name="daily_birthdays"
    )
    print(f"✅ Daily birthday report scheduled at {BIRTHDAY_CHECK_HOUR}:{BIRTHDAY_CHECK_MINUTE} {TZ_NAME}")

    updater.idle()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
