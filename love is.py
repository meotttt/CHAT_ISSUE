
import logging
import json
import random
import time
import os
import re
import asyncio
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from telegram import Update, User, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, ChatPermissions
from telegram.ext import (
    Application,
    ApplicationBuilder, # ApplicationBuilder from second code
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)
from telegram.helpers import mention_html
from telegram.constants import ParseMode

# --- Общая Конфигурация ---
# ВАЖНО: Используйте ОДИН свой токен для бота. Я выбрал из второго скрипта.
TOKEN = "8086930010:AAH1elkRFf6497_Ls9-XnZrUeIh_rWyMF5c"

# --- Конфигурация из первого скрипта (Лависки) ---
PHOTO_BASE_PATH = r"C:\Users\anana\PycharmProjects\PythonProject2\photo — копия" # r-строка для корректной обработки обратных слешей
NUM_PHOTOS = 74
USER_DATA_FILE = "user_data.json" # Для данных Лависки
COOLDOWN_SECONDS = 5  # Задержка между командами "лав иска"
SPIN_COST = 200  # Стоимость крутки в кристаллах
REPEAT_CRYSTALS_BONUS = 80  # Кристаллы за повторную карточку
COLLECTION_MENU_IMAGE_PATH = os.path.join(PHOTO_BASE_PATH, "collection_menu_background.jpg")

# --- Конфигурация из второго скрипта (Брак, Админ, Евангелие) ---
GROUP_CHAT_ID: int = -1002372051836  # ID вашей группы (для брачного бота)
GROUP_USERNAME = "@CHAT_ISSUE"  # Имя группы (для информационных сообщений)
ADMIN_ID = '2123680656'  # ID администратора (из второго скрипта)
MARRIAGE_DATABASE_NAME = "BBRRAACC.db"
REUNION_PERIOD_DAYS = 3  # Количество дней для льготного периода после развода
ADMIN_DATABASE_NAME = "baza.sql"
GOSPEL_GAME_DATABASE_NAME = "gospel_game.db"

# --- Настройка логирования ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Глобальный счетчик для фото (из второго скрипта) ---
photo_counter = 0

# --- ДАННЫЕ ПО ФОТОГРАФИЯМ И ПОДПИСЯМ (из первого скрипта) ---
# ВАЖНО: Вам нужно будет заполнить этот словарь для всех 74 фотографий!
# Пример:
PHOTO_DETAILS = {
    1: {"path": os.path.join(PHOTO_BASE_PATH, "1.jpg"), "caption": "❤️‍🔥 LOVE IS…\nрай!\n\n🔖…1!"},
    2: {"path": os.path.join(PHOTO_BASE_PATH, "2.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда вместе!\n\n🔖…2"},
    3: {"path": os.path.join(PHOTO_BASE_PATH, "3.jpg"), "caption": "❤️‍🔥 LOVE IS…\nуметь переглядываться!\n\n🔖…3!"},
    4: {"path": os.path.join(PHOTO_BASE_PATH, "4.jpg"), "caption": "❤️‍🔥 LOVE IS…\nбыть на коне!\n\n🔖…4!"},
    5: {"path": os.path.join(PHOTO_BASE_PATH, "5.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпочувствовать легкое головокружение!\n\n🔖…5!"},
    6: {"path": os.path.join(PHOTO_BASE_PATH, "6.jpg"), "caption": "❤️‍🔥 LOVE IS…\nобнимашки!\n\n🔖…6!"},
    7: {"path": os.path.join(PHOTO_BASE_PATH, "7.jpg"), "caption": "❤️‍🔥 LOVE IS…\nне только сахар!\n\n🔖…7!"},
    8: {"path": os.path.join(PHOTO_BASE_PATH, "8.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпонимать друг друга без слов!\n\n🔖…8!"},
    9: {"path": os.path.join(PHOTO_BASE_PATH, "9.jpg"), "caption": "❤️‍🔥 LOVE IS…\nуметь успокоить!\n\n🔖…9!"},
    10: {"path": os.path.join(PHOTO_BASE_PATH, "10.jpg"), "caption": "❤️‍🔥 LOVE IS…\nсуметь удержаться!\n\n🔖…10!"},
    11: {"path": os.path.join(PHOTO_BASE_PATH, "11.jpg"), "caption": "❤️‍🔥 LOVE IS…\nне дать себя запутать!\n\n🔖…11!"},
    12: {"path": os.path.join(PHOTO_BASE_PATH, "12.jpg"), "caption": "❤️‍🔥 LOVE IS…\nсуметь сохранить секретик!\n\n🔖…12!"},
    13: {"path": os.path.join(PHOTO_BASE_PATH, "13.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпод прикрытием\n\n🔖…13!"},
    14: {"path": os.path.join(PHOTO_BASE_PATH, "14.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда нам по пути!\n\n🔖…14!"},
    15: {"path": os.path.join(PHOTO_BASE_PATH, "15.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпрорыв.\n\n🔖…15!"},
    16: {"path": os.path.join(PHOTO_BASE_PATH, "16.jpg"), "caption": "❤️‍🔥 LOVE IS…\nзагадывать желание\n\n🔖…16! "},
    17: {"path": os.path.join(PHOTO_BASE_PATH, "17.jpg"), "caption": "❤️‍🔥 LOVE IS…\nлето круглый год!\n\n🔖…17!"},
    18: {"path": os.path.join(PHOTO_BASE_PATH, "18.jpg"), "caption": "❤️‍🔥 LOVE IS…\nромантика!\n\n🔖…18!"},
    19: {"path": os.path.join(PHOTO_BASE_PATH, "19.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда жарко!\n\n🔖…19!"},
    20: {"path": os.path.join(PHOTO_BASE_PATH, "20.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nраскрываться!\n\n🔖…20!"},
    21: {"path": os.path.join(PHOTO_BASE_PATH, "21.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвыполнять обещания\n\n🔖…21!"},
    22: {"path": os.path.join(PHOTO_BASE_PATH, "22.jpg"), "caption": "❤️‍🔥 LOVE IS…\nцирк вдвоем!\n\n🔖…22!"},
    23: {"path": os.path.join(PHOTO_BASE_PATH, "23.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nслышать друг друга!\n\n🔖…23!"},
    24: {"path": os.path.join(PHOTO_BASE_PATH, "24.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсладость\n\n🔖…24!"},
    25: {"path": os.path.join(PHOTO_BASE_PATH, "25.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nне упустить волну!\n\n🔖…25!"},
    26: {"path": os.path.join(PHOTO_BASE_PATH, "26.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсказать о важном!\n\n🔖…26!"},
    27: {"path": os.path.join(PHOTO_BASE_PATH, "27.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nискриться!\n\n🔖…27!"},
    28: {"path": os.path.join(PHOTO_BASE_PATH, "28.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nтолько мы вдвоём\n\n🔖…28!"},
    29: {"path": os.path.join(PHOTO_BASE_PATH, "29.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпервое прикосновение\n\n🔖…29!"},
    30: {"path": os.path.join(PHOTO_BASE_PATH, "30.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвзять дело в свои руки\n\n🔖…30!"},
    31: {"path": os.path.join(PHOTO_BASE_PATH, "31.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда не важно какая погода\n\n🔖…31!"},
    32: {"path": os.path.join(PHOTO_BASE_PATH, "32.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nуметь прощать!\n\n🔖…32!"},
    33: {"path": os.path.join(PHOTO_BASE_PATH, "33.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nотметиться!\n\n🔖…33!"},
    34: {"path": os.path.join(PHOTO_BASE_PATH, "34.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпервый поцелуй\n\n🔖…34!"},
    35: {"path": os.path.join(PHOTO_BASE_PATH, "35.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда без интернета! \n\n🔖…35!"},
    36: {"path": os.path.join(PHOTO_BASE_PATH, "36.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nлегкое головокружение\n\n🔖…36!"},
    37: {"path": os.path.join(PHOTO_BASE_PATH, "37.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпозвонить просто так\n\n🔖…37!"},
    38: {"path": os.path.join(PHOTO_BASE_PATH, "38.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвсё что нужно\n\n🔖…38!"},
    39: {"path": os.path.join(PHOTO_BASE_PATH, "39.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nто, что создаёшь ты\n\n🔖…39!"},
    40: {"path": os.path.join(PHOTO_BASE_PATH, "40.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсвобода\n\n🔖…40!"},
    41: {"path": os.path.join(PHOTO_BASE_PATH, "41.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда пробежала искра!\n\n🔖…41!"},
    42: {"path": os.path.join(PHOTO_BASE_PATH, "42.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nизображать недотрогу \n\n🔖…42!"},
    43: {"path": os.path.join(PHOTO_BASE_PATH, "43.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсварить ему борщ)\n\n🔖…43!"},
    44: {"path": os.path.join(PHOTO_BASE_PATH, "44.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпотрясать мир \n\n🔖…44!"},
    45: {"path": os.path.join(PHOTO_BASE_PATH, "45.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда он не ангел!\n\n🔖…45!"},
    46: {"path": os.path.join(PHOTO_BASE_PATH, "46.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпритягивать разных!\n\n🔖…46!"},
    47: {"path": os.path.join(PHOTO_BASE_PATH, "47.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nтепло внутри, когда холодно снаружи \n\n🔖…47!"},
    48: {"path": os.path.join(PHOTO_BASE_PATH, "48.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nделать покупки друг друга\n\n🔖…48!"},
    49: {"path": os.path.join(PHOTO_BASE_PATH, "49.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nнемного колкости\n\n🔖…49!"},
    50: {"path": os.path.join(PHOTO_BASE_PATH, "50.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда тянет магнитом \n\n🔖…50!"},
    51: {"path": os.path.join(PHOTO_BASE_PATH, "51.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nбыть на седьмом небе!\n\n🔖…51!"},
    52: {"path": os.path.join(PHOTO_BASE_PATH, "52.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nты и я\n\n🔖…52!"},
    53: {"path": os.path.join(PHOTO_BASE_PATH, "53.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда купил самое необходимое!\n\n🔖…53!"},
    54: {"path": os.path.join(PHOTO_BASE_PATH, "54.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкак первый день весны!\n\n🔖…54!"},
    55: {"path": os.path.join(PHOTO_BASE_PATH, "55.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпоздравить первым!\n\n🔖…55!"},
    56: {"path": os.path.join(PHOTO_BASE_PATH, "56.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nоставить след!\n\n🔖…56!"},
    57: {"path": os.path.join(PHOTO_BASE_PATH, "57.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nмикс чувств!\n\n🔖…57!"},
    58: {"path": os.path.join(PHOTO_BASE_PATH, "58.jpg"), "caption": "❤️‍🔥 LOVE IS…\nслучайные порывы!\n\n🔖…58!"},
    59: {"path": os.path.join(PHOTO_BASE_PATH, "59.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда мысли сходятся!\n\n🔖…59!"},
    60: {"path": os.path.join(PHOTO_BASE_PATH, "60.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпосильная ноша!\n\n🔖…60!"},
    61: {"path": os.path.join(PHOTO_BASE_PATH, "61.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвыбрать свое сердце!\n\n🔖…61!"},
    62: {"path": os.path.join(PHOTO_BASE_PATH, "62.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nто, что требует заботы!\n\n🔖…62!"},
    63: {"path": os.path.join(PHOTO_BASE_PATH, "63.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nбессонный ночи!\n\n🔖…63!"},
    64: {"path": os.path.join(PHOTO_BASE_PATH, "64.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nбыть на вершине мира\n\n🔖…64!"},
    65: {"path": os.path.join(PHOTO_BASE_PATH, "65.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nисправлять ошибки!\n\n🔖…65!"},
    66: {"path": os.path.join(PHOTO_BASE_PATH, "66.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nлюбоваться друг другом!\n\n🔖…66!"},
    67: {"path": os.path.join(PHOTO_BASE_PATH, "67.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nдарить главное!\n\n🔖…67!"},
    68: {"path": os.path.join(PHOTO_BASE_PATH, "68.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда совсем не холодно!\n\n🔖…68!"},
    69: {"path": os.path.join(PHOTO_BASE_PATH, "69.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nдобавить изюминку!\n\n🔖…69!"},
    70: {"path": os.path.join(PHOTO_BASE_PATH, "70.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nснится друг другу!\n\n🔖…70!"},
    71: {"path": os.path.join(PHOTO_BASE_PATH, "71.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпикник на двоих!\n\n🔖…71!"},
    72: {"path": os.path.join(PHOTO_BASE_PATH, "72.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nдурачиться, как дети\n\n🔖…72!"},
    73: {"path": os.path.join(PHOTO_BASE_PATH, "73.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nдарить себя!\n\n🔖…73!"},
    74: {"path": os.path.join(PHOTO_BASE_PATH, "74.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nгорячее сердце!\n\n🔖…74!"},
}

# Генерация заглушек, если PHOTO_DETAILS не заполнен до конца
for i in range(1, NUM_PHOTOS + 1):
    if i not in PHOTO_DETAILS:
        PHOTO_DETAILS[i] = {
            "path": os.path.join(PHOTO_BASE_PATH, f"{i}.jpg"),
            "caption": f"Лависка номер {i}. Пока без уникальной подписи."
        }


# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ДАННЫМИ ПОЛЬЗОВАТЕЛЕЙ (Лависки - JSON) ---
def load_user_data():
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_user_data(data):
    with open(USER_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def get_user_data(user_id, username):
    data = load_user_data()
    if str(user_id) not in data:
        data[str(user_id)] = {
            "username": username,
            "cards": {},  # {card_id: count}
            "crystals": 0,
            "spins": 0,  # "бесплатные" крутки (купленные за кристаллы)
            "last_spin_time": 0,  # UNIX timestamp
            "current_collection_view_index": 0  # Для отслеживания текущей просматриваемой карточки в коллекции
        }
        save_user_data(data)
    return data[str(user_id)]


def update_user_data(user_id, new_data):
    data = load_user_data()
    data[str(user_id)].update(new_data)
    save_user_data(data)

# --- Функции для работы с базами данных (Брак, Админ, Евангелие - SQLite) ---

# --- Инициализация баз данных ---
def init_marriage_db():
    conn = None
    try:
        conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT UNIQUE,
                first_name TEXT,
                last_name TEXT,
                updated_at TEXT,
                last_message_in_group_at TEXT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marriages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                initiator_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                accepted_at TEXT NULL,
                divorced_at TEXT NULL,
                prev_accepted_at TEXT NULL,
                reunion_period_end_at TEXT NULL,
                UNIQUE(initiator_id, target_id) ON CONFLICT REPLACE
            )
        """)
        conn.commit()
        logger.info(f"База данных '{MARRIAGE_DATABASE_NAME}' инициализирована.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации базы данных '{MARRIAGE_DATABASE_NAME}': {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def init_admin_db():
    conn = None
    try:
        conn = sqlite3.connect(ADMIN_DATABASE_NAME, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        cursor.execute(
            '''CREATE TABLE IF NOT EXISTS muted_users (user_id INTEGER PRIMARY KEY, chat_id INTEGER, mute_until DATETIME)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS banned_users (user_id INTEGER PRIMARY KEY, chat_id INTEGER)''')
        conn.commit()
        logger.info(f"База данных '{ADMIN_DATABASE_NAME}' инициализирована.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации базы данных '{ADMIN_DATABASE_NAME}': {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def init_gospel_game_db():
    conn = None
    try:
        conn = sqlite3.connect(GOSPEL_GAME_DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                prayer_count INTEGER DEFAULT 0,
                total_piety_score REAL DEFAULT 0,
                last_prayer_time DATETIME,
                initialized BOOLEAN NOT NULL DEFAULT 0,
                cursed_until DATETIME NULL,
                gospel_found BOOLEAN NOT NULL DEFAULT 0,
                first_name_cached TEXT,
                username_cached TEXT
            )
        ''')
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN gospel_found BOOLEAN NOT NULL DEFAULT 0')
        except sqlite3.OperationalError as e:
            if "duplicate column name: gospel_found" not in str(e):
                logger.warning(f"Ошибка при добавлении столбца gospel_found (возможно, уже существует): {e}")

        try:
            cursor.execute('ALTER TABLE users ADD COLUMN cursed_until DATETIME NULL')
        except sqlite3.OperationalError as e:
            if "duplicate column name: cursed_until" not in str(e):
                logger.warning(f"Ошибка при добавлении столбца cursed_until (возможно, уже существует): {e}")

        try:
            cursor.execute('ALTER TABLE users ADD COLUMN first_name_cached TEXT')
        except sqlite3.OperationalError as e:
            if "duplicate column name: first_name_cached" not in str(e):
                logger.warning(f"Ошибка при добавлении столбца first_name_cached (возможно, уже существует): {e}")

        try:
            cursor.execute('ALTER TABLE users ADD COLUMN username_cached TEXT')
        except sqlite3.OperationalError as e:
            if "duplicate column name: username_cached" not in str(e):
                logger.warning(f"Ошибка при добавлении столбца username_cached (возможно, уже существует): {e}")

        conn.commit()
        logger.info(f"База данных '{GOSPEL_GAME_DATABASE_NAME}' инициализирована.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации базы данных '{GOSPEL_GAME_DATABASE_NAME}': {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# --- Функции для Брачного Бота (SQLite) ---

def save_marriage_user_data(user: User, from_group_chat: bool = False):
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    current_time = datetime.now().isoformat()

    existing_user_data = get_marriage_user_data_by_id(user.id)
    last_msg_in_group = existing_user_data.get('last_message_in_group_at') if existing_user_data else None

    if from_group_chat:
        last_msg_in_group = current_time

    try:
        cursor.execute("""
            INSERT INTO users (user_id, username, first_name, last_name, updated_at, last_message_in_group_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = ?,
                first_name = ?,
                last_name = ?,
                updated_at = ?,
                last_message_in_group_at = COALESCE(?, last_message_in_group_at)
        """, (
            user.id, user.username, user.first_name, user.last_name, current_time, last_msg_in_group,
            user.username, user.first_name, user.last_name, current_time, last_msg_in_group
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных пользователя {user.id} в MARRIAGE_DB: {e}")
    finally:
        conn.close()


def get_marriage_user_data_by_id(user_id: int) -> dict:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_id, username, first_name, last_name, last_message_in_group_at FROM users WHERE user_id = ?",
        (user_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "user_id": row[0],
            "username": row[1],
            "first_name": row[2],
            "last_name": row[3],
            "last_message_in_group_at": row[4]
        }
    return {}


def get_marriage_user_id_from_username_db(username: str) -> Optional[int]:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (username,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def get_active_marriage(user_id: int) -> Optional[dict]:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, initiator_id, target_id, chat_id, status, created_at, accepted_at, divorced_at, prev_accepted_at, reunion_period_end_at FROM marriages
        WHERE (initiator_id = ? OR target_id = ?) AND status = 'accepted'
    """, (user_id, user_id))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0], "initiator_id": row[1], "target_id": row[2], "chat_id": row[3],
            "status": row[4], "created_at": row[5], "accepted_at": row[6], "divorced_at": row[7],
            "prev_accepted_at": row[8], "reunion_period_end_at": row[9]
        }
    return None


def get_pending_marriage_proposal(initiator_id: int, target_id: int) -> Optional[dict]:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, initiator_id, target_id, status, chat_id, accepted_at, prev_accepted_at, reunion_period_end_at FROM marriages
        WHERE (
                (initiator_id = ? AND target_id = ?) OR
                (initiator_id = ? AND target_id = ?)
              )
              AND status = 'pending'
    """, (initiator_id, target_id, target_id, initiator_id))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0], "initiator_id": row[1], "target_id": row[2],
            "status": row[3], "chat_id": row[4], "accepted_at": row[5],
            "prev_accepted_at": row[6], "reunion_period_end_at": row[7]
        }
    return None


def get_recent_divorce_for_reunion(user1_id: int, user2_id: int) -> Optional[dict]:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    current_time = datetime.now().isoformat()

    cursor.execute("""
        SELECT id, accepted_at, divorced_at, prev_accepted_at, reunion_period_end_at
        FROM marriages
        WHERE (
                (initiator_id = ? AND target_id = ?) OR
                (initiator_id = ? AND target_id = ?)
              )
              AND status = 'divorced'
              AND reunion_period_end_at > ?
        ORDER BY divorced_at DESC
        LIMIT 1
    """, (user1_id, user2_id, user2_id, user1_id, current_time))
    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0],
            "accepted_at": row[1],
            "divorced_at": row[2],
            "prev_accepted_at": row[3],
            "reunion_period_end_at": row[4]
        }
    return None


def create_marriage_proposal_db(initiator_id: int, target_id: int, chat_id: int) -> bool:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    current_time = datetime.now().isoformat()
    try:
        cursor.execute("""
            INSERT INTO marriages (initiator_id, target_id, chat_id, status, created_at)
            VALUES (?, ?, ?, 'pending', ?)
        """, (initiator_id, target_id, chat_id, current_time))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создании предложения о венчании: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def accept_marriage_proposal_db(proposal_id: int, initiator_id: int, target_id: int) -> bool:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    current_time = datetime.now().isoformat()

    reunion_info = get_recent_divorce_for_reunion(initiator_id, target_id)

    accepted_at_to_use = current_time
    prev_accepted_at_to_save = None

    if reunion_info and reunion_info['reunion_period_end_at'] and datetime.fromisoformat(
            reunion_info['reunion_period_end_at']) > datetime.now():
        logger.info(f"Восстановление брака для {initiator_id} и {target_id}. Используем предыдущий стаж.")
        if reunion_info['prev_accepted_at']:
            accepted_at_to_use = reunion_info['prev_accepted_at']
        elif reunion_info['accepted_at']:
            accepted_at_to_use = reunion_info['accepted_at']
        prev_accepted_at_to_save = accepted_at_to_use

    try:
        cursor.execute("""
            UPDATE marriages SET status = 'accepted', accepted_at = ?, prev_accepted_at = ?, divorced_at = NULL, reunion_period_end_at = NULL
            WHERE id = ? AND status = 'pending'
        """, (accepted_at_to_use, prev_accepted_at_to_save, proposal_id))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Ошибка при принятии предложения о венчании: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def reject_marriage_proposal_db(proposal_id: int) -> bool:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE marriages SET status = 'rejected'
            WHERE id = ? AND status = 'pending'
        """, (proposal_id,))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Ошибка при отклонении предложения о венчании: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def divorce_user_db_confirm(user_id: int) -> Optional[Tuple[int, int]]:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    current_time = datetime.now().isoformat()
    reunion_period_end = (datetime.now() + timedelta(
        days=REUNION_PERIOD_DAYS)).isoformat()

    try:
        cursor.execute("""
            SELECT id, initiator_id, target_id, accepted_at, prev_accepted_at FROM marriages
            WHERE (initiator_id = ? OR target_id = ?) AND status = 'accepted'
        """, (user_id, user_id))
        marriage_row = cursor.fetchone()

        if marriage_row:
            marriage_id, initiator, target, accepted_at, prev_accepted_at = marriage_row

            actual_accepted_at = prev_accepted_at if prev_accepted_at else accepted_at

            cursor.execute("""
                UPDATE marriages SET
                    status = 'divorced',
                    divorced_at = ?,
                    reunion_period_end_at = ?,
                    prev_accepted_at = ?
                WHERE id = ?
            """, (current_time, reunion_period_end, actual_accepted_at, marriage_id))
            conn.commit()
            return initiator, target
        return None
    except sqlite3.Error as e:
        logger.error(f"Ошибка при разводе пользователя {user_id}: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def get_all_marriages_db() -> List[dict]:
    conn = sqlite3.connect(MARRIAGE_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.id, m.initiator_id, u1.first_name, u1.username, m.target_id, u2.first_name, u2.username, m.accepted_at, m.chat_id, m.prev_accepted_at
        FROM marriages m
        JOIN users u1 ON m.initiator_id = u1.user_id
        JOIN users u2 ON m.target_id = u2.user_id
        WHERE m.status = 'accepted'
    """)
    marriages = []
    for row in cursor.fetchall():
        marriages.append({
            "id": row[0],
            "initiator_id": row[1],
            "partner1_name": row[2],
            "partner1_username": row[3],
            "target_id": row[4],
            "partner2_name": row[5],
            "partner2_username": row[6],
            "accepted_at": row[7],
            "chat_id": row[8],
            "prev_accepted_at": row[9]
        })
    conn.close()
    return marriages


# --- Хелперы для Брачного Бота ---
def get_marriage_user_display_name(user_data: dict) -> str:
    if user_data.get('first_name'):
        return user_data['first_name']
    return f"@{user_data['username']}" if user_data.get('username') else f"Пользователь (ID: {user_data['user_id']})"


def format_duration(start_time_str: str) -> str:
    start_time = datetime.fromisoformat(start_time_str)
    duration = datetime.now() - start_time

    days = duration.days
    hours = duration.seconds // 3600
    minutes = (duration.seconds % 3600) // 60

    parts = []
    if days > 0:
        parts.append(f"{days} дн.")
    if hours > 0:
        parts.append(f"{hours} ч.")
    if minutes > 0:
        parts.append(f"{minutes} мин.")

    if not parts:
        return "менее минуты"
    return ", ".join(parts)


async def check_marriage_user_eligibility(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, str]:
    user_data = await asyncio.to_thread(get_marriage_user_data_by_id, user_id)

    if not user_data:
        return False, f"Ваши данные не найдены в базе бота. Пожалуйста, отправьте сообщение в группе {GROUP_USERNAME}, чтобы бот вас зарегистрировал."

    try:
        chat_member = await context.bot.get_chat_member(GROUP_CHAT_ID, user_id)
        if chat_member.status not in ['member', 'administrator', 'creator']:
            return False, f"Вы должны быть активным участником группы {GROUP_USERNAME}, чтобы использовать эту команду."
    except Exception as e:
        logger.error(f"Ошибка при проверке членства в группе для {user_id}: {e}")
        return False, "💍 Чтоб иметь возможность венчаться вам нужно состоять в чате @CHAT_ISSUE 👾\n\n👾 Для активации своего аккаунта в ISSUE | CHAT BOT напишите «я в деле»\nОтвета на сообщение не будет, но вы сможете пользоваться ботом!"

    last_message_str = user_data.get('last_message_in_group_at')
    if not last_message_str:
        return False, f"👾 Ваше последнее сообщение в группе {GROUP_USERNAME} не найдено. Пожалуйста, отправьте сообщение в группе, чтобы бот обновил вашу активность."

    last_message_dt = datetime.fromisoformat(last_message_str)
    one_week_ago = datetime.now() - timedelta(weeks=1)

    if last_message_dt < one_week_ago:
        return False, f"👾 Ваше последнее сообщение в группе {GROUP_USERNAME} было более недели назад. Пожалуйста, отправьте сообщение в группе, чтобы обновить вашу активность."

    return True, ""


# --- Функции для Мут/Бан Бота (SQLite) ---
async def unmute_user_after_timer(context):
    job = context.job
    chat_id = job.data['chat_id']
    user_id = job.data['user_id']

    conn = sqlite3.connect(ADMIN_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM muted_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
    conn.commit()
    conn.close()

    permissions = ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_pin_messages=True
    )
    await context.bot.restrict_chat_member(chat_id, user_id, permissions)
    user_info = await context.bot.get_chat_member(chat_id, user_id)
    logger.info(
        f"Пользователь {user_id} (@{user_info.user.username or user_info.user.first_name}) был размучен в чате {chat_id}.")
    await context.bot.send_message(chat_id,
                                   f"Пользователь {mention_html(user_id, user_info.user.first_name)} был размучен.",
                                   parse_mode=ParseMode.HTML)


def parse_mute_duration(duration_str: str) -> Optional[timedelta]:
    try:
        num = int("".join(filter(str.isdigit, duration_str)))
        unit = "".join(filter(str.isalpha, duration_str)).lower()

        if unit in ('м', 'min', 'm', 'мин'):
            return timedelta(minutes=num)
        elif unit in ('ч', 'h', 'час'):
            return timedelta(hours=num)
        elif unit in ('д', 'd', 'день', 'дн'):
            return timedelta(days=num)
        elif unit in ('н', 'w', 'неделя', 'нед'):
            return timedelta(weeks=num)
        else:
            return None
    except (ValueError, IndexError):
        return None


async def admin_mute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.message.chat.type not in ['group', 'supergroup']:
        if update.message:
            await update.message.reply_text("Эта команда доступна только в группах.")
        return

    chat_id = update.message.chat.id
    target_user = update.message.reply_to_message.from_user if update.message.reply_to_message else None

    if not target_user:
        await update.message.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого хотите замутить.")
        return

    try:
        chat_member = await context.bot.get_chat_member(chat_id, update.effective_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            await update.message.reply_text("У вас нет прав для выполнения этой команды.")
            return
    except Exception as e:
        logger.error(f"Ошибка при проверке прав администратора: {e}")
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    duration_str = context.args[0] if context.args else None
    duration = None
    mute_until = None

    if duration_str:
        duration = parse_mute_duration(duration_str)
        if not duration:
            await update.message.reply_text("Неверный формат длительности. Пример: `10м`, `1ч`, `3д`.",
                                            parse_mode=ParseMode.MARKDOWN)
            return
        mute_until = datetime.now() + duration
    else:
        duration = timedelta(hours=1)
        mute_until = datetime.now() + duration

    try:
        permissions = ChatPermissions(
            can_send_messages=False,
            can_send_media_messages=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
            can_pin_messages=False
        )
        await context.bot.restrict_chat_member(chat_id, target_user.id, permissions, until_date=mute_until)

        conn = sqlite3.connect(ADMIN_DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO muted_users (user_id, chat_id, mute_until) VALUES (?, ?, ?)',
                       (target_user.id, chat_id, mute_until))
        conn.commit()
        conn.close()

        context.job_queue.run_once(
            unmute_user_after_timer,
            duration.total_seconds(),
            data={'chat_id': chat_id, 'user_id': target_user.id},
            name=f"unmute_{target_user.id}_{chat_id}" # Добавлено имя для отмены
        )

        hours = int(duration.total_seconds() // 3600)
        minutes = int((duration.total_seconds() % 3600) // 60)

        response_message = f"Пользователь {mention_html(target_user.id, target_user.first_name)} замучен на "
        if hours > 0:
            response_message += f"{hours} час(а/ов) "
        if minutes > 0:
            response_message += f"{minutes} минут(у/ы)"
        if hours == 0 and minutes == 0:
            response_message += "очень короткий срок."

        await update.message.reply_text(response_message, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Ошибка при муте пользователя {target_user.id} в чате {chat_id}: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при попытке замутить пользователя. Возможно, я не имею достаточных прав или пользователь является администратором.")


async def admin_unmute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.message.chat.type not in ['group', 'supergroup']:
        if update.message:
            await update.message.reply_text("Эта команда доступна только в группах.")
        return

    chat_id = update.message.chat.id
    target_user = update.message.reply_to_message.from_user if update.message.reply_to_message else None

    if not target_user:
        await update.message.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого хотите размутить.")
        return

    try:
        chat_member = await context.bot.get_chat_member(chat_id, update.effective_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            await update.message.reply_text("У вас нет прав для выполнения этой команды.")
            return
    except Exception as e:
        logger.error(f"Ошибка при проверке прав администратора: {e}")
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    try:
        permissions = ChatPermissions(
            can_send_messages=True,
            can_send_media_messages=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True,
            can_pin_messages=True
        )
        await context.bot.restrict_chat_member(chat_id, target_user.id, permissions)

        conn = sqlite3.connect(ADMIN_DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM muted_users WHERE user_id = ? AND chat_id = ?', (target_user.id, chat_id))
        conn.commit()
        conn.close()

        current_jobs = context.job_queue.get_jobs_by_name(f"unmute_{target_user.id}_{chat_id}")
        for job in current_jobs:
            job.schedule_removal()

        await update.message.reply_text(
            f"Пользователь {mention_html(target_user.id, target_user.first_name)} был размучен.",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при размуте пользователя {target_user.id} в чате {chat_id}: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при попытке размутить пользователя. Возможно, я не имею достаточных прав. Ошибка: {e}")


async def admin_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.message.chat.type not in ['group', 'supergroup']:
        if update.message:
            await update.message.reply_text("Эта команда доступна только в группах.")
        return

    chat_id = update.message.chat.id
    target_user = update.message.reply_to_message.from_user if update.message.reply_to_message else None

    if not target_user:
        await update.message.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого хотите забанить.")
        return

    try:
        chat_member = await context.bot.get_chat_member(chat_id, update.effective_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            await update.message.reply_text("У вас нет прав для выполнения этой команды.")
            return
    except Exception as e:
        logger.error(f"Ошибка при проверке прав администратора: {e}")
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    try:
        await context.bot.ban_chat_member(chat_id, target_user.id)

        conn = sqlite3.connect(ADMIN_DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO banned_users (user_id, chat_id) VALUES (?, ?)',
                       (target_user.id, chat_id))
        conn.commit()
        conn.close()

        await update.message.reply_text(
            f"Пользователь {mention_html(target_user.id, target_user.first_name)} ЗАБАНЕН",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Ошибка при бане пользователя {target_user.id} в чате {chat_id}: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при попытке забанить пользователя. Возможно, я не имею достаточных прав. Ошибка: {e}")


async def admin_unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or update.message.chat.type not in ['group', 'supergroup']:
        if update.message:
            await update.message.reply_text("Эта команда доступна только в группах.")
        return

    chat_id = update.message.chat.id
    target_user = update.message.reply_to_message.from_user if update.message.reply_to_message else None

    if not target_user:
        await update.message.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого хотите разбанить.")
        return

    try:
        chat_member = await context.bot.get_chat_member(chat_id, update.effective_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            await update.message.reply_text("У вас нет прав для выполнения этой команды.")
            return
    except Exception as e:
        logger.error(f"Ошибка при проверке прав администратора: {e}")
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    try:
        await context.bot.unban_chat_member(chat_id, target_user.id)

        conn = sqlite3.connect(ADMIN_DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM banned_users WHERE user_id = ? AND chat_id = ?', (target_user.id, chat_id))
        conn.commit()
        conn.close()

        invite_link = await context.bot.export_chat_invite_link(chat_id)
        try:
            await context.bot.send_message(target_user.id,
                                           f"Вы были разблокированы в группе {update.message.chat.title}! "
                                           f"Вы можете присоединиться по ссылке: {invite_link}",
                                           parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"Не удалось отправить сообщение разблокированному пользователю {target_user.id}: {e}")

        await update.message.reply_text(
            f"Пользователь {mention_html(target_user.id, target_user.first_name)} был разблокирован!",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Ошибка при разбане пользователя {target_user.id} в чате {chat_id}: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при попытке разблокировать пользователя. Возможно, я не имею достаточных прав. Ошибка: {e}")


# --- Функции для Игрового Бота "Евангелие" (SQLite) ---

def add_gospel_game_user(user_id: int, first_name: str, username: Optional[str] = None):
    conn = sqlite3.connect(GOSPEL_GAME_DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO users (user_id, initialized, gospel_found, first_name_cached, username_cached)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, False, False, first_name, username))
    conn.commit()
    conn.close()


def update_gospel_game_user_cached_data(user_id: int, first_name: str, username: Optional[str] = None):
    conn = sqlite3.connect(GOSPEL_GAME_DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE users SET first_name_cached = ?, username_cached = ? WHERE user_id = ?
        ''', (first_name, username, user_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при обновлении кэшированных данных пользователя {user_id} в gospel_game.db: {e}")
    finally:
        conn.close()


def get_gospel_game_user_data(user_id: int) -> Optional[dict]:
    conn = sqlite3.connect(GOSPEL_GAME_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    conn.close()
    if user_data:
        return dict(user_data)
    return None


def update_gospel_game_user_data(user_id: int, prayer_count: int, total_piety_score: float, last_prayer_time: datetime,
                                 cursed_until: Optional[datetime], gospel_found: bool,
                                 first_name_cached: str, username_cached: Optional[str]):
    conn = sqlite3.connect(GOSPEL_GAME_DATABASE_NAME)
    cursor = conn.cursor()
    cursed_until_str = cursed_until.isoformat() if cursed_until else None
    last_prayer_time_str = last_prayer_time.isoformat() if last_prayer_time else None

    cursor.execute(
        '''UPDATE users SET prayer_count = ?, total_piety_score = ?, last_prayer_time = ?, cursed_until = ?, gospel_found = ?, first_name_cached = ?, username_cached = ? WHERE user_id = ?''',
        (prayer_count, total_piety_score, last_prayer_time_str, cursed_until_str, gospel_found, first_name_cached,
         username_cached, user_id)
    )
    conn.commit()
    conn.close()


async def find_gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)
    if user_data and user_data['gospel_found']:
        await update.message.reply_text("Вы уже нашли Евангелие. Отправляйтесь на службу!")
        return

    lp_time_obj = datetime.fromisoformat(user_data['last_prayer_time']) if user_data and user_data['last_prayer_time'] else None
    cursed_until_obj = datetime.fromisoformat(user_data['cursed_until']) if user_data and user_data['cursed_until'] else None

    await asyncio.to_thread(update_gospel_game_user_data, user_id,
                            user_data['prayer_count'] if user_data else 0,
                            user_data['total_piety_score'] if user_data else 0.0,
                            lp_time_obj,
                            cursed_until_obj,
                            True,
                            user.first_name, user.username
                            )

    await update.message.reply_text(
        "Успех! ✨\nВаши реликвии у вас в руках!\n\nВам открылась возможность:\n⛩️ «мольба» — ходить на службу\n📜«Евангелие» — смотреть свои Евангелие\n📃 «Топ Евангелий» — и следить за вашими успехами!\nЖелаем удачи! 🍀"
    )


async def prayer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)

    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫"
        )
        return

    current_time = datetime.utcnow()
    cursed_until_str = user_data['cursed_until']
    cursed_until = datetime.fromisoformat(cursed_until_str) if cursed_until_str else None

    if cursed_until and current_time < cursed_until:
        remaining_time = cursed_until - current_time
        hours = int(remaining_time.total_seconds() // 3600)
        minutes = int((remaining_time.total_seconds() % 3600) // 60)
        await update.message.reply_text(
            f'У вас бесноватость 👹\n📿 Вы не сможете молиться еще {hours} часа(ов), {minutes} минут(ы).'
        )
        return

    is_friday = current_time.weekday() == 4
    is_early_morning = (0 <= current_time.hour < 4)

    if (is_friday or is_early_morning) and random.random() < 0.10:
        cursed_until = current_time + timedelta(days=1)
        lp_time_obj = datetime.fromisoformat(user_data['last_prayer_time']) if user_data['last_prayer_time'] else None
        await asyncio.to_thread(update_gospel_game_user_data, user_id,
                                user_data['prayer_count'], user_data['total_piety_score'],
                                lp_time_obj,
                                cursed_until, user_data['gospel_found'],
                                user.first_name, user.username)
        await update.message.reply_text(
            "У вас бесноватость 👹\nПохоже вашу мольбу услышал кое-кто….другой\n\n📿 Вы не сможете молиться сутки."
        )
        return

    last_prayer_time_str = user_data['last_prayer_time']
    last_prayer_time = datetime.fromisoformat(last_prayer_time_str) if last_prayer_time_str else None

    prayer_count = user_data['prayer_count']
    total_piety_score = user_data['total_piety_score']

    if last_prayer_time and current_time < last_prayer_time + timedelta(hours=1):
        remaining_time = (last_prayer_time + timedelta(hours=1)) - current_time
        minutes = int(remaining_time.total_seconds() // 60)
        seconds = int(remaining_time.total_seconds() % 60)
        await update.message.reply_text(
            f'.....Похоже никто не слышит вашей мольбы\n📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы).'
        )
        return

    gained_piety = round(random.uniform(1, 20) / 2, 1)
    prayer_count += 1
    total_piety_score += gained_piety

    await asyncio.to_thread(update_gospel_game_user_data, user_id, prayer_count, total_piety_score,
                            current_time, None, user_data['gospel_found'],
                            user.first_name, user.username)

    await update.message.reply_text(
        f'⛩️ Ваши мольбы были услышаны! \n✨ Набожность +{gained_piety}\nНа следующую службу можно будет выйти через час 📿'
    )


async def gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)

    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫"
        )
        return

    prayer_count = user_data['prayer_count']
    total_piety_score = user_data['total_piety_score']

    await update.message.reply_text(
        f'📜 Ваше евангелие:\n\nМолитвы — {prayer_count}📿\nНабожность — {total_piety_score:.1f} ✨'
    )

PAGE_SIZE = 50

async def _get_leaderboard_message(context: ContextTypes.DEFAULT_TYPE, view: str, page: int = 1) -> Tuple[
    str, InlineKeyboardMarkup]:
    conn = sqlite3.connect(GOSPEL_GAME_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT user_id, prayer_count, first_name_cached, username_cached FROM users WHERE gospel_found = 1 ORDER BY prayer_count DESC')
        all_prayer_leaderboard = cursor.fetchall()

        cursor.execute(
            'SELECT user_id, total_piety_score, first_name_cached, username_cached FROM users WHERE gospel_found = 1 ORDER BY total_piety_score DESC')
        all_piety_leaderboard = cursor.fetchall()
    finally:
        conn.close()

    leaderboard_data = []
    if view == 'prayers':
        leaderboard_data = all_prayer_leaderboard
    elif view == 'piety':
        leaderboard_data = all_piety_leaderboard

    total_users = len(leaderboard_data)
    total_pages = (total_users + PAGE_SIZE - 1) // PAGE_SIZE

    if page < 1:
        page = 1
    if total_users > 0 and page > total_pages:
        page = total_pages
    elif total_users == 0:
        page = 0

    start_index = (page - 1) * PAGE_SIZE
    end_index = start_index + PAGE_SIZE
    current_page_leaderboard = leaderboard_data[start_index:end_index]

    message_text = "✨ <b>Топ Евангелий</b> ✨\n\n"
    keyboard_buttons = []

    if total_users == 0:
        message_text += "<i>Пока нет ни одного игрока, нашедшего Евангелие. Будьте первым!</i>"
        return message_text, InlineKeyboardMarkup([])

    if view == 'prayers':
        message_text += "<b>📿 Услышанные молитвы:</b>\n"
        for rank_offset, row in enumerate(current_page_leaderboard):
            uid = row['user_id']
            count = row['prayer_count']
            cached_first_name = row['first_name_cached']
            cached_username = row['username_cached']

            rank = start_index + rank_offset + 1

            display_text_for_mention = ""
            if cached_first_name:
                display_text_for_mention = cached_first_name
            elif cached_username:
                display_text_for_mention = f"@{cached_username}"
            else:
                display_text_for_mention = f"ID: {uid}"

            message_text += f"<code>{rank}.</code> {mention_html(uid, display_text_for_mention)} — <b>{count}</b> молитв\n"

        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("<< Назад", callback_data=f"gospel_top_prayers_page_{page - 1}"))
        nav_row.append(
            InlineKeyboardButton(f"{page}/{total_pages}", callback_data="ignore_page_num"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Вперед >>", callback_data=f"gospel_top_prayers_page_{page + 1}"))
        if nav_row:
            keyboard_buttons.append(nav_row)
        keyboard_buttons.append([InlineKeyboardButton("✨ Набожность", callback_data="gospel_top_piety_page_1")])

    elif view == 'piety':
        message_text += "<b>✨ Набожность:</b>\n"
        for rank_offset, row in enumerate(current_page_leaderboard):
            uid = row['user_id']
            score = row['total_piety_score']
            cached_first_name = row['first_name_cached']
            cached_username = row['username_cached']

            rank = start_index + rank_offset + 1

            display_text_for_mention = ""
            if cached_first_name:
                display_text_for_mention = cached_first_name
            elif cached_username:
                display_text_for_mention = f"@{cached_username}"
            else:
                display_text_for_mention = f"ID: {uid}"

            message_text += f"<code>{rank}.</code> {mention_html(uid, display_text_for_mention)} — <b>{score:.1f}</b> набожности\n"

        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("<< Назад", callback_data=f"gospel_top_piety_page_{page - 1}"))
        nav_row.append(
            InlineKeyboardButton(f"{page}/{total_pages}", callback_data="ignore_page_num"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Вперед >>", callback_data=f"gospel_top_piety_page_{page + 1}"))
        if nav_row:
            keyboard_buttons.append(nav_row)
        keyboard_buttons.append([InlineKeyboardButton("📿 Молитвы", callback_data="gospel_top_prayers_page_1")])

    return message_text, InlineKeyboardMarkup(keyboard_buttons)


async def top_gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)

    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫"
        )
        return

    message_text, reply_markup = await _get_leaderboard_message(context, 'prayers', 1)
    try:
        await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения топа Евангелий (prayers): {e}")
        if "Too long" in str(e) or "message is too long" in str(e).lower():
            await update.message.reply_text(
                "Список Евангелий (молитвы) слишком длинный для одного сообщения. Пожалуйста, обратитесь к администратору или попробуйте позже.",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                "Произошла ошибка при получении топа молитв. Пожалуйста, попробуйте еще раз.",
                parse_mode=ParseMode.HTML
            )


# --- ОБРАБОТЧИКИ КОМАНД (Лависки) ---

async def lav_iska(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    user_data = await asyncio.to_thread(get_user_data, user_id, username)

    current_time = time.time()
    if current_time - user_data["last_spin_time"] < COOLDOWN_SECONDS:
        remaining_time = int(COOLDOWN_SECONDS - (current_time - user_data["last_spin_time"]))
        await update.message.reply_text(
            f"Вы уже получали лависку! Повторите через {remaining_time} секунд."
        )
        return

    user_data["last_spin_time"] = current_time

    owned_card_ids = [int(cid) for cid in user_data["cards"].keys()]
    new_card_ids = [i for i in range(1, NUM_PHOTOS + 1) if i not in owned_card_ids]

    chosen_card_id = None
    is_new_card = False

    if user_data["spins"] > 0:
        if new_card_ids:
            chosen_card_id = random.choice(new_card_ids)
            is_new_card = True
            user_data["spins"] -= 1
        else:
            chosen_card_id = random.choice(owned_card_ids) if owned_card_ids else random.choice(
                range(1, NUM_PHOTOS + 1))
            await update.message.reply_text("У вас уже есть все карточки! Крутка возвращена.")
    else:
        if new_card_ids and owned_card_ids:
            if random.random() < 0.8:
                chosen_card_id = random.choice(new_card_ids)
                is_new_card = True
            else:
                chosen_card_id = random.choice(owned_card_ids)
        elif new_card_ids:
            chosen_card_id = random.choice(new_card_ids)
            is_new_card = True
        elif owned_card_ids:
            chosen_card_id = random.choice(owned_card_ids)
        else:
            await update.message.reply_text("Что-то пошло не так. Нет доступных карточек.")
            await asyncio.to_thread(update_user_data, user_id, user_data)
            return

    if chosen_card_id is None:
        await update.message.reply_text("Не удалось выбрать карточку.")
        await asyncio.to_thread(update_user_data, user_id, user_data)
        return

    card_id_str = str(chosen_card_id)
    if is_new_card:
        user_data["cards"][card_id_str] = 1
        caption_suffix = " Новая карточка добавлена в вашу коллекцию!"
    else:
        user_data["cards"][card_id_str] = user_data["cards"].get(card_id_str, 0) + 1
        user_data["crystals"] += REPEAT_CRYSTALS_BONUS
        caption_suffix = f" 👀 Это повторная карточка!\n\nВы получили {REPEAT_CRYSTALS_BONUS} 💌 фрагментов!" \
                         f"\nУ вас теперь {user_data['cards'][card_id_str]} таких карточек"

    photo_path = PHOTO_DETAILS[chosen_card_id]["path"]
    caption = PHOTO_DETAILS[chosen_card_id]["caption"] + caption_suffix

    try:
        await update.message.reply_photo(photo=open(photo_path, "rb"), caption=caption)
    except FileNotFoundError:
        await update.message.reply_text(f"Ошибка: Файл фотографии не найден по пути {photo_path}")
        logger.error(f"File not found: {photo_path}")
    except Exception as e:
        await update.message.reply_text(f"Произошла ошибка при отправке фото: {e}")
        logger.error(f"Error sending photo: {e}")

    await asyncio.to_thread(update_user_data, user_id, user_data)


async def my_collection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    user_data = await asyncio.to_thread(get_user_data, user_id, username)

    total_owned_cards = len(user_data["cards"])

    keyboard = [
        [InlineKeyboardButton(f"Лависки {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("Купить крутки", callback_data="buy_spins")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"Пользователь: @{username}\n"
        f"Круток: {user_data['spins']}\n"
        f"Кристаллов: {user_data['crystals']}\n"
        f"Коллекции: 1 — Лависки"
    )

    try:
        await update.message.reply_photo(
            photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
            caption=message_text,
            reply_markup=reply_markup
        )
    except FileNotFoundError:
        logger.error(f"Collection menu image not found: {COLLECTION_MENU_IMAGE_PATH}")
        await update.message.reply_text(
            message_text + "\n\n(Ошибка: фоновая картинка коллекции не найдена)",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error sending collection menu photo: {e}")
        await update.message.reply_text(
            message_text + f"\n\n(Ошибка при отправке фоновой картинки: {e})",
            reply_markup=reply_markup
        )


async def send_collection_card(query, user_data, card_id):
    user_id = query.from_user.id
    owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])

    if not owned_card_ids:
        await my_collection_edit_message(query)
        return

    current_index = owned_card_ids.index(card_id)

    card_count = user_data["cards"].get(str(card_id), 0)
    photo_path = PHOTO_DETAILS[card_id]["path"]
    caption_text = (
        f"{PHOTO_DETAILS[card_id]['caption']}\n\n"
        f"Эта карточка выпадала вам {card_count} раз."
    )

    keyboard = []
    nav_buttons = []
    if len(owned_card_ids) > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Предыдущая", callback_data=f"nav_card_prev"))
        nav_buttons.append(InlineKeyboardButton("Следующая ➡️", callback_data=f"nav_card_next"))

    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("Выйти в мою коллекцию", callback_data="back_to_main_collection")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_media(
            media=InputMediaPhoto(media=open(photo_path, "rb"), caption=caption_text),
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.warning(f"Failed to edit message media, sending new photo instead: {e}")
        try:
            await query.message.delete()
        except Exception as del_e:
            logger.warning(f"Could not delete old message during card view refresh: {del_e}")
        await query.message.reply_photo(
            photo=open(photo_path, "rb"),
            caption=caption_text,
            reply_markup=reply_markup
        )


async def my_collection_edit_message(query):
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name
    user_data = await asyncio.to_thread(get_user_data, user_id, username)

    total_owned_cards = len(user_data["cards"])

    keyboard = [
        [InlineKeyboardButton(f"Лависки {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("Купить крутки", callback_data="buy_spins")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"Пользователь: @{username}\n"
        f"Круток: {user_data['spins']}\n"
        f"Кристаллов: {user_data['crystals']}\n"
        f"Коллекции: 1 — Лависки"
    )

    try:
        await query.edit_message_media(
            media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text),
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.warning(f"Failed to edit message to main collection photo, sending new photo instead: {e}")
        try:
            await query.message.delete()
        except Exception as del_e:
            logger.warning(f"Could not delete old message during collection menu refresh: {del_e}")
        await query.message.reply_photo(
            photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
            caption=message_text,
            reply_markup=reply_markup
        )


# --- Основные обработчики Telegram (Объединенные) ---

async def unified_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await asyncio.to_thread(save_marriage_user_data, user, from_group_chat=False)
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    keyboard = [
        [InlineKeyboardButton('Вступить в чат 💬', url='https://t.me/CHAT_ISSUE')],
        [InlineKeyboardButton('Новогоднее голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
        [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa')],
        [InlineKeyboardButton('Команды ⚙️', callback_data='show_commands')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_name = user.username or user.first_name or 'друг'
    await update.message.reply_text(
        f'Привет, {user_name}! 🪐\nЭто бот чата 𝙄𝙎𝙎𝙐𝐄 \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, '
        'принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!',
        reply_markup=reply_markup
    )


async def get_chat_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = update.effective_chat.title if chat_type != 'private' else 'Личный чат'

    response = (
        f"ID этого чата: `{chat_id}`\n"
        f"Тип чата: `{chat_type}`\n"
        f"Название чата: `{chat_title}`"
    )
    await update.message.reply_text(response, parse_mode="Markdown")


# Предварительно компилируем регулярные выражения для команд Лависок
LAV_ISKA_REGEX = re.compile(r"^(лав иска)$", re.IGNORECASE)
MY_COLLECTION_REGEX = re.compile(r"^(моя коллекция)$", re.IGNORECASE)


async def unified_text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    full_message_text = update.message.text.strip()
    message_text_lower = full_message_text.lower()

    if user and not user.is_bot:
        from_group = (chat_id == GROUP_CHAT_ID)
        await asyncio.to_thread(save_marriage_user_data, user, from_group_chat=from_group)
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

        # --- Команды Лависки ---
        if LAV_ISKA_REGEX.match(message_text_lower):
            await lav_iska(update, context)
            return
        elif MY_COLLECTION_REGEX.match(message_text_lower):
            await my_collection(update, context)
            return

        # --- Административные команды ---
        if message_text_lower.startswith("мут"):
            parts = full_message_text.split(maxsplit=1)
            context.args = [parts[1]] if len(parts) > 1 else []
            await admin_mute_user(update, context)
            return
        elif message_text_lower == "говори":
            await admin_unmute_user(update, context)
            return
        elif message_text_lower == "вон":
            await admin_ban_user(update, context)
            return
        elif message_text_lower == "вернуть":
            await admin_unban_user(update, context)
            return

        # --- Команды Брачного Бота ---
        elif message_text_lower.startswith("венчаться"):
            is_eligible, reason = await check_marriage_user_eligibility(user.id, context)
            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            initiator_id = user.id
            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, initiator_id)
            initiator_display_name = get_marriage_user_display_name(initiator_info)

            target_user_id: Optional[int] = None
            target_user_data: Optional[dict] = None

            if update.message.reply_to_message and update.message.reply_to_message.from_user:
                replied_user = update.message.reply_to_message.from_user
                if replied_user.id == user.id:
                    await context.bot.send_message(chat_id=chat_id, text="👾 Вы не можете венчаться с самим собой!")
                    return

                target_user_id = replied_user.id
                await asyncio.to_thread(save_marriage_user_data, replied_user, from_group_chat=True)
                await asyncio.to_thread(add_gospel_game_user, replied_user.id, replied_user.first_name,
                                        replied_user.username)
                await asyncio.to_thread(update_gospel_game_user_cached_data, replied_user.id, replied_user.first_name,
                                        replied_user.username)
                target_user_data = await asyncio.to_thread(get_marriage_user_data_by_id, target_user_id)

            if not target_user_id:
                parts = full_message_text.split(maxsplit=1)
                if len(parts) == 2 and parts[1].startswith('@'):
                    second_username_raw = parts[1][1:].strip()
                    resolved_target_id = await asyncio.to_thread(get_marriage_user_id_from_username_db,
                                                                 second_username_raw)
                    if resolved_target_id:
                        target_user_id = resolved_target_id
                        target_user_data = await asyncio.to_thread(get_marriage_user_data_by_id, target_user_id)
                        if not target_user_data:
                            await context.bot.send_message(chat_id=chat_id,
                                                           text="👾 Пользователь по указанному юзернейму не найден в базе данных бота (внутренняя ошибка).",
                                                           parse_mode=ParseMode.HTML)
                            return
                    else:
                        await context.bot.send_message(chat_id=chat_id,
                                                       text="👾 Пользователь не найден в базе данных бота по указанному юзернейму. Убедитесь, что он писал сообщения в группе и у него есть публичный username.",
                                                       parse_mode=ParseMode.HTML)
                        return
                elif len(parts) == 1 and message_text_lower == "венчаться":
                    await context.bot.send_message(chat_id=chat_id,
                                                   text="👾 Чтобы венчаться, ответьте на сообщение пользователя или укажите его `@username` (например: `Венчаться @username`).",
                                                   parse_mode=ParseMode.HTML)
                    return

            if not target_user_id or not target_user_data:
                await context.bot.send_message(chat_id=chat_id,
                                               text="👾 Не удалось определить пользователя для венчания. Убедитесь, что вы отвечаете на сообщение или указываете действительный `@username`.",
                                               parse_mode=ParseMode.HTML)
                return

            target_display_name = get_marriage_user_display_name(target_user_data)

            initiator_mention = mention_html(initiator_id, initiator_display_name)
            target_mention = mention_html(target_user_id, target_display_name)

            if initiator_id == target_user_id:
                await context.bot.send_message(chat_id=chat_id, text="👾 Вы не можете венчаться с самим собой!")
                return

            is_target_eligible, target_reason = await check_marriage_user_eligibility(target_user_id, context)
            if not is_target_eligible:
                await context.bot.send_message(chat_id=chat_id,
                                               text=f"👾 {target_mention} не может быть венчан: {target_reason}",
                                               parse_mode=ParseMode.HTML)
                return

            if await asyncio.to_thread(get_active_marriage, initiator_id):
                await context.bot.send_message(chat_id=chat_id, text="👾 Вы уже состоите в браке.",
                                               parse_mode=ParseMode.HTML)
                return

            if await asyncio.to_thread(get_active_marriage, target_user_id):
                await context.bot.send_message(chat_id=chat_id, text=f"👾 {target_mention} уже состоит в браке.",
                                               parse_mode=ParseMode.HTML)
                return

            existing_proposal = await asyncio.to_thread(get_pending_marriage_proposal, initiator_id, target_user_id)
            if existing_proposal:
                if existing_proposal['initiator_id'] == initiator_id:
                    await context.bot.send_message(chat_id=chat_id,
                                                   text=f"👾 Вы уже предложили венчаться {target_mention}. Дождитесь ответа.",
                                                   parse_mode=ParseMode.HTML)
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"👾 {target_mention} уже предложил(а) венчаться вам. Используйте кнопку 'Да' в сообщении с предложением (в личке от бота).",
                        parse_mode=ParseMode.HTML
                    )
                return

            if await asyncio.to_thread(create_marriage_proposal_db, initiator_id, target_user_id, chat_id):
                await update.message.reply_text(f"💍 Вы отправили предложение венчаться пользователю {target_mention} !",
                                                parse_mode=ParseMode.HTML)

                callback_data_yes = f"marry_yes_{initiator_id}_{target_user_id}"
                callback_data_no = f"marry_no_{initiator_id}_{target_user_id}"

                keyboard = [
                    [InlineKeyboardButton("Да", callback_data=callback_data_yes)],
                    [InlineKeyboardButton("Нет", callback_data=callback_data_no)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                try:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=f"{target_mention}, вам предложил венчаться пользователь {initiator_mention}!\n"
                             f"Вы хотите принять это предложение?",
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
                    await update.message.reply_text(
                        f"💍 Ваше предложение уже в личных сообщениях {target_mention}.\n\nДержим за вас кулачки ✊🏻",
                        parse_mode=ParseMode.HTML)

                except Exception as e:
                    logger.error(f"Не удалось отправить личное сообщение {target_mention} (ID: {target_user_id}): {e}",
                                 exc_info=True)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"Не удалось отправить личное сообщение {target_mention} с предложением (возможно, бот заблокирован или пользователь не начинал диалог). "
                             f"Пожалуйста, сообщите {target_mention}, что нужно написать боту в личку, чтобы увидеть предложение.",
                        parse_mode=ParseMode.HTML
                    )
            else:
                await context.bot.send_message(chat_id=chat_id,
                                               text="Произошла ошибка при создании предложения. Пожалуйста, попробуйте еще раз.",
                                               parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "бракосочетания":
            is_eligible, reason = await check_marriage_user_eligibility(user.id, context)
            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            marriages = await asyncio.to_thread(get_all_marriages_db)
            if not marriages:
                await context.bot.send_message(chat_id=chat_id, text="Активных браков пока нет 💔",
                                               parse_mode=ParseMode.HTML)
                return

            response_text = "💍 <b>Активные браки:</b>\n"
            for marriage in marriages:
                p1_mention = mention_html(marriage['initiator_id'], marriage['partner1_name'])
                p2_mention = mention_html(marriage['target_id'], marriage['partner2_name'])

                start_date_str = marriage['prev_accepted_at'] if marriage['prev_accepted_at'] else marriage[
                    'accepted_at']
                duration = format_duration(start_date_str)
                start_date_formatted = datetime.fromisoformat(start_date_str).strftime('%d.%m.%Y')

                response_text += (
                    f"- {p1_mention} и {p2_mention} "
                    f"(с {start_date_formatted}, {duration})\n"
                )
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "мой брак":
            is_eligible, reason = await check_marriage_user_eligibility(user.id, context)
            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            marriage = await asyncio.to_thread(get_active_marriage, user.id)

            if not marriage:
                await context.bot.send_message(chat_id=chat_id, text="Вы пока не состоите в браке.",
                                               parse_mode=ParseMode.HTML)
                return

            partner_id = marriage['target_id'] if marriage['initiator_id'] == user.id else marriage['initiator_id']
            partner_info = await asyncio.to_thread(get_marriage_user_data_by_id, partner_id)
            partner_display_name = get_marriage_user_display_name(partner_info)
            partner_mention = mention_html(partner_id, partner_display_name)

            start_date_str = marriage['prev_accepted_at'] if marriage['prev_accepted_at'] else marriage['accepted_at']
            duration = format_duration(start_date_str)
            start_date_formatted = datetime.fromisoformat(start_date_str).strftime('%d.%m.%Y')

            response_text = (
                f"💍 Вы состоите в браке с {partner_mention} 💞\n\n"
                f"📆 Дата бракосочетания: {start_date_formatted} ({duration})."
            )
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "развестись":
            is_eligible, reason = await check_marriage_user_eligibility(user.id, context)
            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            marriage = await asyncio.to_thread(get_active_marriage, user.id)

            if not marriage:
                await context.bot.send_message(chat_id=chat_id, text="Вы не состоите в браке",
                                               parse_mode=ParseMode.HTML)
                return

            partner_id = marriage['target_id'] if marriage['initiator_id'] == user.id else marriage['initiator_id']
            partner_info = await asyncio.to_thread(get_marriage_user_data_by_id, partner_id)
            partner_display_name = get_marriage_user_display_name(partner_info)
            partner_mention = mention_html(partner_id, partner_display_name)

            keyboard = [
                [InlineKeyboardButton("Уверен(а)", callback_data=f"divorce_confirm_{user.id}_{partner_id}")],
                [InlineKeyboardButton("Отмена", callback_data=f"divorce_cancel_{user.id}_{partner_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"💔 Вы действительно хотите развестись с {partner_mention}? \nПосле развода у вас будет {REUNION_PERIOD_DAYS} дня на повторное венчание без потери стажа брака.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            return

        # --- Команды Игрового Бота "Евангелие" ---
        elif message_text_lower == "найти евангелие":
            await find_gospel_command(update, context)
            return
        elif message_text_lower == "мольба":
            await prayer_command(update, context)
            return
        elif message_text_lower == "евангелие":
            await gospel_command(update, context)
            return
        elif message_text_lower == "топ евангелий":
            await top_gospel_command(update, context)
            return
        elif message_text_lower == 'моя инфа':
            await update.message.reply_text(f'Ваш ID: {user.id}', parse_mode=ParseMode.HTML)
            return

        # --- Команды Общей Информации ---
        elif message_text_lower == 'иссуе':
            keyboard = [
                [InlineKeyboardButton('Вступить в чат 💬', url='https://t.me/CHAT_ISSUE')],
                [InlineKeyboardButton('Новогоднее голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
                [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa')],
                [InlineKeyboardButton('Команды ⚙️', callback_data='show_commands')],
            ]
            markup = InlineKeyboardMarkup(keyboard)
            await context.bot.send_message(chat_id,
                                           f'Привет, {user.username or user.first_name}! 🪐\nЭто бот чата 𝙄𝐒𝐒𝙐𝐄 \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!',
                                           reply_markup=markup,
                                           parse_mode=ParseMode.HTML)
            return


async def send_command_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_list = """
<b>⚙️ Список команд:</b>

<b>💍 Брачный Бот:</b>
<code>Венчаться @username</code> - Предложить венчаться пользователю по юзернейму.
<code>Венчаться</code> (в ответ на сообщение) - Предложить венчаться автору сообщения.
<code>Бракосочетания</code> - Посмотреть список всех активных браков в группе.
<code>Мой брак</code> - Узнать статус своего брака.
<code>Развестись</code> - Запросить развод (с подтверждением).

<b>👮 Административные Команды:</b>
(Эти команды должны быть ответом на сообщение пользователя)
<code>Мут &lt;длительность&gt;</code> - Замутить пользователя. Пример: <code>Мут 10м</code>, <code>Мут 1ч</code>, <code>Мут 3д</code>.
<code>Говори</code> - Размутить пользователя.
<code>Вон</code> - Забанить пользователя.
<code>Вернуть</code> - Разбанить пользователя.

<b>📜 Игра "Евангелие":</b>
<code>Найти Евангелие</code> - Начать игру и найти Евангелие.
<code>Мольба</code> - Молиться и увеличивать набожность (доступно раз в час, возможна бесноватость).
<code>Евангелие</code> - Посмотреть свои текущие показатели молитв и набожности.
<code>Топ Евангелий</code> - Просмотреть рейтинг самых набожных и молящихся игроков.

<b>📸 Лависка (Коллекция карточек):</b>
<code>Лав иска</code> - Получить новую карточку Лависки или повтор.
<code>Моя коллекция</code> - Просмотреть свою коллекцию, кристаллы и крутки.

<b>💬 Общие Команды:</b>
<code>/start</code> - Начало работы с ботом, приветствие.
<code>Иссуе</code> - Показать основную информацию о боте и кнопки.
<code>Моя инфа</code> - Показать ваш ID.
<code>/get_chat_id</code> - Узнать ID текущего чата.
"""
    await update.effective_message.reply_text(command_list, parse_mode=ParseMode.HTML)


async def unified_button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    current_user_id = query.from_user.id
    current_user_first_name = query.from_user.first_name
    current_user_username = query.from_user.username

    await asyncio.to_thread(update_gospel_game_user_cached_data, current_user_id, current_user_first_name,
                            current_user_username)

    # --- Обработка кнопок Брачного Бота ---
    if data.startswith("marry_") or data.startswith("divorce_"):
        parts = data.split('_')
        action_type = parts[0]
        action = parts[1]
        user1_id = int(parts[2])
        user2_id = int(parts[3])

        if action_type == "marry":
            if current_user_id != user2_id:
                await query.edit_message_text(text="Это предложение адресовано не вам!")
                return

            is_eligible, reason = await check_marriage_user_eligibility(current_user_id, context)
            if not is_eligible:
                await query.edit_message_text(
                    text=f"Вы не соответствуете условиям для принятия/отклонения предложения: {reason}",
                    parse_mode=ParseMode.HTML)
                return

            proposal = await asyncio.to_thread(get_pending_marriage_proposal, user1_id, user2_id)

            if not proposal:
                await query.edit_message_text(text="Это предложение уже неактивно или истекло.")
                return

            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, user1_id)
            target_info = await asyncio.to_thread(get_marriage_user_data_by_id, user2_id)

            if not initiator_info or not target_info:
                await query.edit_message_text(text="Не удалось получить данные о пользователях.")
                return

            initiator_display_name = get_marriage_user_display_name(initiator_info)
            target_display_name = get_marriage_user_display_name(target_info)

            initiator_mention = mention_html(user1_id, initiator_display_name)
            target_mention = mention_html(user2_id, target_display_name)

            if action == "yes":
                if await asyncio.to_thread(get_active_marriage, user1_id) or \
                        await asyncio.to_thread(get_active_marriage, user2_id):
                    await query.edit_message_text(text="К сожалению, один из вас уже вступил в брак.",
                                                  parse_mode=ParseMode.HTML)
                    await asyncio.to_thread(reject_marriage_proposal_db, proposal['id'])
                    return

                if await asyncio.to_thread(accept_marriage_proposal_db, proposal['id'], user1_id, user2_id):
                    await query.edit_message_text(text=f"Вы успешно венчались с {initiator_mention}!",
                                                  parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(
                            chat_id=proposal['chat_id'],
                            text=f"{target_mention} и {initiator_mention} успешно венчались!",
                            parse_mode=ParseMode.HTML
                        )
                        await context.bot.send_message(
                            chat_id=user1_id,
                            text=f"💍 Ваше предложение венчаться с {target_mention} было принято!",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.warning(
                            f"💔 Не удалось отправить уведомление о браке в чат {proposal['chat_id']} или инициатору {user1_id}: {e}")
                else:
                    await query.edit_message_text(
                        text="💔 Произошла ошибка при принятии предложения. Пожалуйста, попробуйте еще раз.",
                        parse_mode=ParseMode.HTML)
            elif action == "no":
                if await asyncio.to_thread(reject_marriage_proposal_db, proposal['id']):
                    await query.edit_message_text(text=f"💔 Вы отклонили предложение венчаться от {initiator_mention}.",
                                                  parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(
                            chat_id=user1_id,
                            text=f"💔 {target_mention} отклонил(а) ваше предложение венчаться.",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.warning(f"💔 Не удалось отправить уведомление об отклонении инициатору {user1_id}: {e}")
                else:
                    await query.edit_message_text(
                        text="💔 Произошла ошибка при отклонении предложения. Пожалуйста, попробуйте еще раз.",
                        parse_mode=ParseMode.HTML)

        elif action_type == "divorce":
            if current_user_id != user1_id:
                await query.edit_message_text(text="Не суй свой носик в чужие дела!")
                return

            partner_id = user2_id

            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, current_user_id)
            partner_info = await asyncio.to_thread(get_marriage_user_data_by_id, partner_id)

            if not initiator_info or not partner_info:
                await query.edit_message_text(text="Не удалось получить данные о пользователях.")
                return

            initiator_display_name = get_marriage_user_display_name(initiator_info)
            partner_display_name = get_marriage_user_display_name(partner_info)

            initiator_mention = mention_html(current_user_id, initiator_display_name)
            partner_mention = mention_html(partner_id, partner_display_name)

            if action == "confirm":
                divorced_partners = await asyncio.to_thread(divorce_user_db_confirm, current_user_id)

                if divorced_partners:
                    await query.edit_message_text(
                        text=f"💔 Вы развелись с {partner_mention}. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери стажа брака",
                        parse_mode=ParseMode.HTML
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=partner_id,
                            text=f"💔 Ваш брак с {initiator_mention} был расторгнут. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери стажа брака",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.warning(f"💔 Не удалось уведомить партнера {partner_id} о разводе: {e}")
                else:
                    await query.edit_message_text(
                        text="❤️‍🩹 Произошла ошибка при попытке развода. Пожалуйста, попробуйте еще раз",
                        parse_mode=ParseMode.HTML
                    )
            elif action == "cancel":
                await query.edit_message_text(text="❤️‍🩹 Развод отменен", parse_mode=ParseMode.HTML)

    # --- Обработка кнопок Лависки ---
    elif query.data == "show_collection":
        owned_card_ids = sorted([int(cid) for cid in (await asyncio.to_thread(get_user_data, current_user_id, current_user_username))["cards"].keys()])
        if not owned_card_ids:
            keyboard = [[InlineKeyboardButton("Купить крутки", callback_data="buy_spins")],
                        [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message_text = (
                f"Пользователь: @{current_user_username}\n"
                f"Круток: {(await asyncio.to_thread(get_user_data, current_user_id, current_user_username))['spins']}\n"
                f"Кристаллов: {(await asyncio.to_thread(get_user_data, current_user_id, current_user_username))['crystals']}\n"
                f"Коллекции: 1 — Лависки\n\n"
                f"У вас пока нет ни одной Лависки! Используйте 'лав иска', чтобы получить первую."
            )
            try:
                await query.edit_message_media(
                    media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text),
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.warning(f"Failed to edit message media for empty collection view, sending new photo: {e}")
                try: await query.message.delete()
                except: pass
                await query.message.reply_photo(
                    photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                    caption=message_text,
                    reply_markup=reply_markup
                )
            return

        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        user_data["current_collection_view_index"] = 0
        await asyncio.to_thread(update_user_data, current_user_id, user_data)

        await send_collection_card(query, user_data, owned_card_ids[0])

    elif query.data.startswith("view_card_"):
        parts = query.data.split("_")
        card_to_view_id = int(parts[2])

        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])
        if not owned_card_ids:
            await my_collection_edit_message(query)
            return

        current_index = owned_card_ids.index(card_to_view_id)
        user_data["current_collection_view_index"] = current_index
        await asyncio.to_thread(update_user_data, current_user_id, user_data)

        await send_collection_card(query, user_data, card_to_view_id)

    elif query.data.startswith("nav_card_"):
        direction = query.data.split("_")[2]

        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])
        if not owned_card_ids:
            await my_collection_edit_message(query)
            return

        current_index = user_data.get("current_collection_view_index", 0)

        if direction == "next":
            next_index = (current_index + 1) % len(owned_card_ids)
        elif direction == "prev":
            next_index = (current_index - 1 + len(owned_card_ids)) % len(owned_card_ids)
        else:
            return

        user_data["current_collection_view_index"] = next_index
        await asyncio.to_thread(update_user_data, current_user_id, user_data)

        await send_collection_card(query, user_data, owned_card_ids[next_index])

    elif query.data == "back_to_main_collection":
        await my_collection_edit_message(query)

    elif query.data == "buy_spins":
        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        keyboard = [
            [InlineKeyboardButton(f"Обменять {SPIN_COST} кристаллов на крутку",
                                  callback_data="exchange_crystals_for_spin")],
            [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text_for_buy_spins = (
            f"У вас {user_data['crystals']} кристаллов.\n"
            f"Стоимость одной крутки: {SPIN_COST} кристаллов."
        )
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text_for_buy_spins),
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"Failed to edit message media for buy_spins, sending new photo: {e}")
            try: await query.message.delete()
            except: pass
            await query.message.reply_photo(
                photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                caption=message_text_for_buy_spins,
                reply_markup=reply_markup
            )

    elif query.data == "exchange_crystals_for_spin":
        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        if user_data["crystals"] >= SPIN_COST:
            user_data["crystals"] -= SPIN_COST
            user_data["spins"] += 1
            await asyncio.to_thread(update_user_data, current_user_id, user_data)

            keyboard = [
                [InlineKeyboardButton(f"Обменять {SPIN_COST} кристаллов на крутку",
                                      callback_data="exchange_crystals_for_spin")],
                [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message_text_success = (
                f"Вы успешно купили крутку! Теперь у вас {user_data['spins']} круток и {user_data['crystals']} кристаллов."
            )
            try:
                await query.edit_message_media(
                    media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text_success),
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.warning(
                    f"Failed to edit message media for exchange_crystals_for_spin success, sending new photo: {e}")
                try: await query.message.delete()
                except: pass
                await query.message.reply_photo(
                    photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                    caption=message_text_success,
                    reply_markup=reply_markup
                )
        else:
            await query.answer("Недостаточно кристаллов для покупки крутки!", show_alert=True)

            user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
            keyboard = [
                [InlineKeyboardButton(f"Обменять {SPIN_COST} кристаллов на крутку",
                                      callback_data="exchange_crystals_for_spin")],
                [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message_text_fail = (
                f"У вас {user_data['crystals']} кристаллов.\n"
                f"Стоимость одной крутки: {SPIN_COST} кристаллов.\n"
                f"Недостаточно кристаллов для покупки крутки!"
            )
            try:
                await query.edit_message_media(
                    media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text_fail),
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.warning(
                    f"Failed to edit message media for exchange_crystals_for_spin fail, sending new photo: {e}")
                try: await query.message.delete()
                except: pass
                await query.message.reply_photo(
                    photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                    caption=message_text_fail,
                    reply_markup=reply_markup
                )

    # --- Обработка кнопок Игрового Бота "Евангелие" ---
    elif data == 'send_papa':
        await query.message.reply_text(
            'Добро пожаловать в мир "Евангелия" — интерактивной игры бота ISSUE! 🪐\n\n'
            '▎Что вас ждет в "Евангелии"? \n\n'
            '1. ⛩️ Хождение на службу — Молитвы: Каждый раз, когда вы молитесь, вы не просто выполняете рутинное действие — вы получаете повышения своей набожности\n\n'
            '2. ✨ Система Набожности: Ваши молитвы влияют на вашу духовную силу. Чем больше вы молитесь, тем выше ваша набожность. Станьте одним из самых набожных игроков!\n\n'
            '3. 📃 Соревнования и Достижения: Вы можете видеть, кто из игроков находится на вершине таблицы лидеров! Сравните свои достижения с друзьями и стремитесь занять первое место в рейтингах молитв и набожности.\n\n'
            '4. 👹 Неожиданные Повороты: Будьте готовы к неожиданным событиям! У вас есть шанс столкнуться с "бесноватостью".\n\n'
            'Поговаривают что стоит молиться аккуратнее с 00:00 до 04:00 и быть предельно осторожным в пятницу!\n\n'
            '─────── ⋆⋅☆⋅⋆ ───────\n\n'
            '⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие\n\n'
            'Возможно если вы взовете к помощи, вы обязательно ее получите \n\n'
            '📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫',
            parse_mode=ParseMode.HTML
        )
    elif data == 'show_commands':
        await send_command_list(update, context)
    elif data.startswith('gospel_top_'):
        parts = data.split('_')
        view = parts[2]
        page = int(parts[4]) if len(parts) > 4 else 1

        message_text, reply_markup = await _get_leaderboard_message(context, view, page)
        try:
            await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка при обновлении сообщения топа Евангелий (callback, view={view}, page={page}): {e}")
            if "message is not modified" not in str(e) and "MESSAGE_TOO_LONG" not in str(e):
                await query.message.reply_text(
                    "Произошла ошибка при обновлении топа. Пожалуйста, попробуйте еще раз.",
                    parse_mode=ParseMode.HTML
                )


async def get_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global photo_counter
    photo_counter += 1
    if photo_counter % 20 == 0:
        await update.message.reply_text('Нихуевое фото братан')


async def process_any_message_for_user_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    if user and not user.is_bot:
        from_group = (chat_id == GROUP_CHAT_ID)
        await asyncio.to_thread(save_marriage_user_data, user, from_group_chat=from_group)
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.warning(f'Update "{update}" вызвал ошибку "{context.error}"')
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "Произошла ошибка! Пожалуйста, попробуйте еще раз или свяжитесь с администратором.",
            parse_mode=ParseMode.HTML)


def main():
    init_marriage_db()
    init_admin_db()
    init_gospel_game_db()

    application = ApplicationBuilder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", unified_start_command))
    application.add_handler(CommandHandler("get_chat_id", get_chat_id_command))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unified_text_message_handler))

    application.add_handler(MessageHandler(filters.PHOTO, get_photo_handler))

    application.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND & ~filters.TEXT & ~filters.PHOTO,
                       process_any_message_for_user_data))

    # Расширенный паттерн для CallbackQueryHandler, чтобы включить все callback_data
    application.add_handler(
        CallbackQueryHandler(unified_button_callback_handler,
                             pattern=r"^(marry_|divorce_|send_papa|show_commands|gospel_top_|show_collection|view_card_|nav_card_|back_to_main_collection|buy_spins|exchange_crystals_for_spin)"))

    application.add_error_handler(error_handler)

    logger.info("Бот запущен. Ожидание сообщений...")
    application.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()

