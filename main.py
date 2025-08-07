import re
import os
import json
import requests
import gspread
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Updater, MessageHandler, Filters, CallbackContext
from google.oauth2.service_account import Credentials


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
    json_creds = os.environ["GSHEETS_CREDENTIALS_JSON"]
    creds_dict = json.loads(json_creds)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open(os.environ["SPREADSHEET_NAME"]).sheet1
    return sheet


# === Парсинг сообщений ===
def parse_message(text: str):
    match = re.match(r"^(CL1|CL2|CL3|SMS|SEC|CNF|NEW|REP)\s+(\+?[0-9]+)\s*\|\s*(.+)$", text.strip(), re.IGNORECASE)
    if not match:
        return None
    code, phone, comment = match.groups()

    if phone.startswith("0"):
        phone = "+38" + phone
    elif phone.startswith("380"):
        phone = "+" + phone
    elif not phone.startswith("+380"):
        phone = "+380" + phone.lstrip("380")

    return code.upper(), phone, comment.strip()


# === Вспомогательные функции для телефонов ===
def clean_phone(p: str) -> str:
    return re.sub(r"\D", "", p)  # убираем всё кроме цифр

def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+{}".format(digits)  # ← тепер з плюсом


# === Bitrix: поиск контакта ===
def normalize_phone(phone: str) -> str:
    digits = clean_phone(phone)
    if digits.startswith("0"):
        digits = "38" + digits
    if not digits.startswith("380"):
        digits = "380" + digits.lstrip("380")
    return "+{}".format(digits)  # ← тепер з плюсом

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
            if clean_phone(ph["VALUE"]) == clean_phone(norm_phone_full):
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
            "AUTHOR_ID": responsible_id  # ← це і є співробітник, від імені якого пишеться
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




# === Обработка сообщений ===
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
        sheet.append_row([timestamp, employee_name, category, phone, comment, "Виконано"])
    except Exception as e:
        update.message.reply_text(f"⚠ Помилка Google Sheets: {e}")
        return

    client_name = f"{contact.get('NAME', '')} {contact.get('LAST_NAME', '')}".strip()
    update.message.reply_text(f"✅ Запис збережено: {category} – {client_name}")



# === MAIN ===
def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
