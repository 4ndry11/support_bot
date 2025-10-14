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
from typing import List, Dict, Any, Optional
import pytz

TIMEZONE = os.environ.get("TIMEZONE", "Europe/Kyiv")
TZ = pytz.timezone(TIMEZONE)

# === НАСТРОЙКИ ===
BOT_TOKEN = os.environ["BOT_TOKEN"]

# Вебхуки Bitrix24
BITRIX_CONTACT_URL = os.environ["BITRIX_CONTACT_URL"]  # crm.contact.list
BITRIX_TASK_URL = os.environ["BITRIX_TASK_URL"]        # task.item.add

# Google Sheets
SPREADSHEET_NAME = os.environ["SPREADSHEET_NAME"]

RESPONSIBLE_ID = 596

# опційно: TYPE_ID для "Клієнт" (якщо знаєш точне)
CLIENT_TYPE_ID="Клієнт"

# куди слати нагадування (через кому — можна один і той самий чат, де бот вже працює)
BIRTHDAY_CHATS=-1003053461710

# антидубль на день (1 вкл, 0 викл)
DEDUP_PER_DAY=1


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
    8183276948:{"name": "Швець Максим", "b24_id": 2627}
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
    sheet = client.open(os.environ["SPREADSHEET_NAME"]).sheet1
    return sheet

# === Телефоны ===
def clean_phone(p: str) -> str:
    return re.sub(r"\D", "", p)

def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+" + digits

# === Парсинг рабочих сообщений (логирование) ===
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

# === Bitrix: поиск контакта по телефону ===
def find_contact_by_phone(phone):
    norm_phone_full = normalize_phone(phone)  # напр.: +380631234567
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

def _make_b24_url(method_name: str) -> str:
    """
    Робимо URL для іншого методу Bitrix24 на базі вже наявного вебхука.
    Напр.: crm.contact.list -> user.search / crm.status.list / crm.timeline.comment.add
    """
    if "crm.contact.list" in BITRIX_CONTACT_URL:
        return BITRIX_CONTACT_URL.replace("crm.contact.list", method_name)
    # fallback: якщо BITRIX_CONTACT_URL інший — просто підмінюємо все після останнього "/"
    base = BITRIX_CONTACT_URL.rsplit("/", 1)[0]
    return f"{base}/{method_name}"

def _b24_paginated_get(url: str, params: Dict[str, Any], result_key: str = "result") -> List[Dict[str, Any]]:
    """
    Гет із посторінковою вибіркою (Bitrix24 повертає 'next'; додаємо &start=...).
    """
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        q = params.copy()
        q["start"] = start
        r = requests.get(url, params=q, timeout=30)
        r.raise_for_status()
        data = r.json()
        chunk = data.get(result_key, [])
        if isinstance(chunk, dict) and "items" in chunk:  # іноді result = { items: [...] }
            chunk = chunk["items"]
        out.extend(chunk)
        next_start = data.get("next")
        if next_start is None:
            break
        start = next_start
    return out

def _normalize_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            if "%z" in fmt:
                return datetime.strptime(s, fmt).astimezone(TZ)
            return datetime.strptime(s, fmt).replace(tzinfo=TZ)
        except ValueError:
            continue
    return None

def _today_mm_dd() -> tuple[int, int]:
    now = datetime.now(TZ)
    return now.month, now.day

def _phones_from_contact(item: Dict[str, Any]) -> List[str]:
    vals = []
    for ph in item.get("PHONE", []) or []:
        v = (ph.get("VALUE") or "").strip()
        if v:
            vals.append(v)
    return vals

def _emails_from_contact(item: Dict[str, Any]) -> List[str]:
    vals = []
    for em in item.get("EMAIL", []) or []:
        v = (em.get("VALUE") or "").strip()
        if v:
            vals.append(v)
    return vals

def _fio_from_bits(obj: Dict[str, Any]) -> str:
    parts = [obj.get("LAST_NAME") or "", obj.get("NAME") or "", obj.get("SECOND_NAME") or ""]
    return " ".join(p for p in parts if p).strip()

def _contacts_birthday_today() -> List[Dict[str, Any]]:
    _resolve_client_type_id()
    url = BITRIX_CONTACT_URL  # crm.contact.list
    select = ["ID", "NAME", "SECOND_NAME", "LAST_NAME", "TYPE_ID", "BIRTHDATE", "PHONE", "EMAIL"]
    contacts = _b24_paginated_get(url, {"select[]": select, "filter[!BIRTHDATE]": None, "order[ID]": "ASC"})
    mm, dd = _today_mm_dd()
    out = []
    for c in contacts:
        bd = _normalize_date(c.get("BIRTHDATE") or "")
        if not bd:
            continue
        if bd.month == mm and bd.day == dd:
            if CLIENT_TYPE_ID:  # фільтр лише Клієнтів
                if str(c.get("TYPE_ID", "")).strip() != str(CLIENT_TYPE_ID):
                    continue
            out.append(c)
    return out

def _users_birthday_today() -> List[Dict[str, Any]]:
    url = _make_b24_url("user.search")
    # user.search повертає список співробітників; додатково вибирати поля не обов'язково, але вони там є
    users = _b24_paginated_get(url, {"FILTER[ACTIVE]": True}, result_key="result")
    mm, dd = _today_mm_dd()
    out = []
    for u in users:
        bd = _normalize_date(u.get("PERSONAL_BIRTHDAY") or "")
        if bd and bd.month == mm and bd.day == dd:
            out.append(u)
    return out

def _load_birthday_cache() -> Dict[str, Any]:
    if not os.path.exists(BIRTHDAY_CACHE_FILE):
        return {}
    try:
        with open(BIRTHDAY_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_birthday_cache(c: Dict[str, Any]) -> None:
    try:
        with open(BIRTHDAY_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(c, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Cache write error: {e}")

def _already_sent_today(key: str) -> bool:
    if not DEDUP_PER_DAY:
        return False
    cache = _load_birthday_cache()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    return cache.get(key) == today

def _mark_sent_today(key: str) -> None:
    if not DEDUP_PER_DAY:
        return
    cache = _load_birthday_cache()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    cache[key] = today
    _save_birthday_cache(cache)

def _build_birthday_message(contacts: List[Dict[str, Any]], users: List[Dict[str, Any]]) -> Optional[str]:
    if not contacts and not users:
        return None
    date_str = datetime.now(TZ).strftime("%d.%m")
    lines = [f"🎉 <b>Дні народження на {date_str}</b>"]

    if contacts:
        lines.append("\n<b>Клієнти:</b>")
        for c in contacts:
            fio = _fio_from_bits(c) or f"Contact #{c.get('ID')}"
            phs = ", ".join(_phones_from_contact(c)) or "—"
            ems = ", ".join(_emails_from_contact(c)) or "—"
            lines.append(f"• {fio} | 📞 {phs} | ✉️ {ems}")

    if users:
        lines.append("\n<b>Співробітники:</b>")
        for u in users:
            fio = _fio_from_bits(u) or f"User #{u.get('ID')}"
            phone = (u.get("PERSONAL_MOBILE") or u.get("PERSONAL_PHONE") or "—")
            email = (u.get("EMAIL") or "—")
            lines.append(f"• {fio} | 📞 {phone} | ✉️ {email}")

    lines.append("\nБудь ласка, привітайте 🎂")
    return "\n".join(lines)

# Ручний виклик: /birthdays (показати сьогоднішній список)
def handle_birthdays_command(update: Update, context: CallbackContext):
    contacts = _contacts_birthday_today()
    users = _users_birthday_today()
    msg = _build_birthday_message(contacts, users)
    if not msg:
        update.message.reply_text("Сьогодні іменинників немає.")
        return
    update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)

# Джоб: щоденне авто-нагадування
def job_send_birthdays(context: CallbackContext):
    key = "birthdays_daily_v1"
    if _already_sent_today(key):
        return
    contacts = _contacts_birthday_today()
    users = _users_birthday_today()
    msg = _build_birthday_message(contacts, users)
    if not msg:
        return
    for chat_id in (BIRTHDAY_CHATS or []):
        try:
            context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"Birthday send error to {chat_id}: {e}")
    _mark_sent_today(key)


# === Bitrix: создание/закрытие задачи (для рабочих записей) ===
def create_task(contact_id, category, comment, responsible_id):
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

    # таймлайн
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

    # завершить
    complete_url = BITRIX_TASK_URL.replace("task.item.add", "task.complete")
    requests.post(complete_url, json={"id": task_id})

# === Утилиты ===
def safe_str(x):
    return "" if x is None else str(x)

# === Агрегация из Google Sheets для /info (без расчёта времени) ===
def aggregate_client_info_from_sheet(phone: str, days: int):
    sheet = init_gsheets()
    values = sheet.get_all_values()  # ожидаем: [timestamp, employee, category(code), phone, comment, status]

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

        if ts < since_dt or phone_row != phone_norm:
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
    text = update.message.text.strip()
    m = re.match(r"^/info\s+([+\d()\-\s]+)\s*,\s*(\d+)$", text, re.IGNORECASE)
    if not m:
        update.message.reply_text("Формат: /info +380XXXXXXXXX, N\nНапр.: /info +380631234567, 7")
        return

    phone_raw, days_str = m.groups()
    phone = normalize_phone(phone_raw)
    days = int(days_str)

    # ФИО клиента из CRM
    contact = find_contact_by_phone(phone)
    client_name = None
    if contact:
        client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip()
        if not client_name:
            client_name = None

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

    # По категоріях (без минут, только счётчики)
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

# === Обработка рабочих сообщений (категорії) ===
def handle_message(update: Update, context: CallbackContext):
    parsed = parse_message(update.message.text)
    if not parsed:
        return

    code, phone, comment = parsed
    category = CATEGORIES.get(code, "Невідома категорія")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

    # Команди
    dp.add_handler(CommandHandler("info", handle_info_command))
    dp.add_handler(CommandHandler("birthdays", handle_birthdays_command))  # <- нова команда

    # Логування робочих повідомлень
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    # === JobQueue: щодня о 09:00 за вашою TZ ===
    # Можеш змінити час тут:
    run_hour = 11
    run_minute = 10
    job_time = datetime.now(TZ).replace(hour=run_hour, minute=run_minute, second=0, microsecond=0).timetz()

    jq = updater.job_queue
    jq.run_daily(job_send_birthdays, time=job_time, name="birthdays_daily")

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
