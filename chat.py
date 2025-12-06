import asyncio
import json
import logging
import os
import random
from psycopg2 import Error
import re
import time
import httpx
import psycopg2
from telegram.ext import Application, ApplicationBuilder, CallbackContext, CommandHandler, ContextTypes, filters, \
    MessageHandler, CallbackQueryHandler
from telegram import Update, User, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, ChatPermissions, Message
from telegram.constants import ChatAction, ParseMode
from datetime import datetime, timezone, timedelta
from collections import defaultdict, OrderedDict
from typing import Optional, Tuple, List, Dict
from telegram.helpers import mention_html
from psycopg2.extras import DictCursor
from telegram.error import BadRequest
from functools import wraps, partial
from dotenv import load_dotenv

load_dotenv()  # Эта строка загружает переменные из .env

# --- Диагностика (закомментируйте в продакшене) ---
# print(f"Текущая рабочая директория: {os.getcwd()}")
# print(f"Существует ли файл .env в текущей директории: {os.path.exists('.env')}")
# print(f"Значение TELEGRAM_BOT_TOKEN после load_dotenv: {os.environ.get('TELEGRAM_BOT_TOKEN')}")
# --- Конец Диагностики ---

# --- Общая Конфигурация ---
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не установлен в переменных окружения!")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения!")

# Получаем ID чатов и админа из переменных окружения с дефолтными значениями
# ВАЖНО: замените дефолтные ID на свои!
GROUP_CHAT_ID: int = int(os.environ.get("GROUP_CHAT_ID", "-1002372051836"))  # Основной ID вашей группы
AQUATORIA_CHAT_ID: Optional[int] = int(
    os.environ.get("AQUATORIA_CHAT_ID", "-1002197024170"))  # ID другой группы, если есть
ADMIN_ID = os.environ.get('ADMIN_ID', '2123680656')  # ID администратора

# Настройки для ссылок на группу:
# Если у вашей группы есть публичное имя пользователя (например, @my_public_group), укажите его.
# Если группа приватная, оставьте пустым и используйте GROUP_CHAT_INVITE_LINK.
GROUP_USERNAME_PLAIN = os.environ.get("GROUP_USERNAME_PLAIN", "CHAT_ISSUE_PLACEHOLDER")
# Если группа приватная, укажите здесь полную ссылку-приглашение.
# Если используется GROUP_USERNAME_PLAIN, это поле не обязательно.
GROUP_CHAT_INVITE_LINK = os.environ.get("GROUP_CHAT_INVITE_LINK")

# --- Конфигурация из первого скрипта (Лависки) ---
PHOTO_BASE_PATH = "."  # Относительный путь к папке с фотографиями
NUM_PHOTOS = 74
COOLDOWN_SECONDS = 10800  # Задержка между командами "лав иска"
SPIN_COST = 200  # Стоимость крутки в кристаллах
ACHIEVEMENTS = [
    {"id": "ach_10", "name": "1. «Новичок»\nСобрал 10 уникальных карточек", "threshold": 10,
     "reward": {"type": "spins", "amount": 5}},
    {"id": "ach_25", "name": "2. «Любитель»\nСобрал 25 уникальных карточек", "threshold": 25,
     "reward": {"type": "spins", "amount": 5}},
    {"id": "ach_50", "name": "3. «Мастер»\nСобрал 50 уникальных карточек", "threshold": 50,
     "reward": {"type": "spins", "amount": 10}},
    {"id": "ach_all", "name": "4. «Гуру»\nСобрал 74 уникальных карточек", "threshold": NUM_PHOTOS,
     "reward": {"type": "crystals", "amount": 1000}},
]

# Короткий откат при использовании крутки (в секундах)
SPIN_USED_COOLDOWN = 600  # 10 минут
REPEAT_CRYSTALS_BONUS = 80  # Кристаллы за повторную карточку
COLLECTION_MENU_IMAGE_PATH = os.path.join(PHOTO_BASE_PATH, "collection_menu_background.jpg")

# --- Конфигурация из второго скрипта (Брак, Админ, Евангелие) ---
REUNION_PERIOD_DAYS = 3  # Количество дней для льготного периода после развода

# --- Настройка логирования ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Глобальный счетчик для фото (из второго скрипта) ---
photo_counter = 0

# --- ДАННЫЕ ПО ФОТОГРАФИЯМ И ПОДПИСЯМ ---
# ВАЖНО: Вам нужно будет заполнить этот словарь для всех 74 фотографий!
# Пример:
PHOTO_DETAILS = {
    1: {"path": os.path.join(PHOTO_BASE_PATH, "1.jpg"), "caption": "❤️‍🔥 LOVE IS…\nрай!\n\n🔖…1!"},
    2: {"path": os.path.join(PHOTO_BASE_PATH, "2.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда вместе!\n\n🔖…2! "},
    3: {"path": os.path.join(PHOTO_BASE_PATH, "3.jpg"), "caption": "❤️‍🔥 LOVE IS…\nуметь переглядываться!\n\n🔖…3! "},
    4: {"path": os.path.join(PHOTO_BASE_PATH, "4.jpg"), "caption": "❤️‍🔥 LOVE IS…\nбыть на коне!\n\n🔖…4! "},
    5: {"path": os.path.join(PHOTO_BASE_PATH, "5.jpg"),
        "caption": "❤️‍🔥 LOVE IS…\nпочувствовать легкое головокружение!\n\n🔖…5! "},
    6: {"path": os.path.join(PHOTO_BASE_PATH, "6.jpg"), "caption": "❤️‍🔥 LOVE IS…\nобнимашки!\n\n🔖…6! "},
    7: {"path": os.path.join(PHOTO_BASE_PATH, "7.jpg"), "caption": "❤️‍🔥 LOVE IS…\nне только сахар!\n\n🔖…7! "},
    8: {"path": os.path.join(PHOTO_BASE_PATH, "8.jpg"),
        "caption": "❤️‍🔥 LOVE IS…\nпонимать друг друга без слов!\n\n🔖…8! "},
    9: {"path": os.path.join(PHOTO_BASE_PATH, "9.jpg"), "caption": "❤️‍🔥 LOVE IS…\nуметь успокоить!\n\n🔖…9! "},
    10: {"path": os.path.join(PHOTO_BASE_PATH, "10.jpg"), "caption": "❤️‍🔥 LOVE IS…\nсуметь удержаться!\n\n🔖…10! "},
    11: {"path": os.path.join(PHOTO_BASE_PATH, "11.jpg"), "caption": "❤️‍🔥 LOVE IS…\nне дать себя запутать!\n\n🔖…11! "},
    12: {"path": os.path.join(PHOTO_BASE_PATH, "12.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nсуметь сохранить секретик!\n\n🔖…12! "},
    13: {"path": os.path.join(PHOTO_BASE_PATH, "13.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпод прикрытием\n\n🔖…13! "},
    14: {"path": os.path.join(PHOTO_BASE_PATH, "14.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда нам по пути!\n\n🔖…14! "},
    15: {"path": os.path.join(PHOTO_BASE_PATH, "15.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпрорыв.\n\n🔖…15! "},
    16: {"path": os.path.join(PHOTO_BASE_PATH, "16.jpg"), "caption": "❤️‍🔥 LOVE IS…\nзагадывать желание\n\n🔖…16!  "},
    17: {"path": os.path.join(PHOTO_BASE_PATH, "17.jpg"), "caption": "❤️‍🔥 LOVE IS…\nлето круглый год!\n\n🔖…17! "},
    18: {"path": os.path.join(PHOTO_BASE_PATH, "18.jpg"), "caption": "❤️‍🔥 LOVE IS…\nромантика!\n\n🔖…18! "},
    19: {"path": os.path.join(PHOTO_BASE_PATH, "19.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда жарко!\n\n🔖…19! "},
    20: {"path": os.path.join(PHOTO_BASE_PATH, "20.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nраскрываться!\n\n🔖…20! "},
    21: {"path": os.path.join(PHOTO_BASE_PATH, "21.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвыполнять обещания\n\n🔖…21! "},
    22: {"path": os.path.join(PHOTO_BASE_PATH, "22.jpg"), "caption": "❤️‍🔥 LOVE IS…\nцирк вдвоем!\n\n🔖…22! "},
    23: {"path": os.path.join(PHOTO_BASE_PATH, "23.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nслышать друг друга!\n\n🔖…23! "},
    24: {"path": os.path.join(PHOTO_BASE_PATH, "24.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсладость\n\n🔖…24! "},
    25: {"path": os.path.join(PHOTO_BASE_PATH, "25.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nне упустить волну!\n\n🔖…25! "},
    26: {"path": os.path.join(PHOTO_BASE_PATH, "26.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсказать о важном!\n\n🔖…26! "},
    27: {"path": os.path.join(PHOTO_BASE_PATH, "27.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nискриться!\n\n🔖…27! "},
    28: {"path": os.path.join(PHOTO_BASE_PATH, "28.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nтолько мы вдвоём\n\n🔖…28! "},
    29: {"path": os.path.join(PHOTO_BASE_PATH, "29.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпервое прикосновение\n\n🔖…29! "},
    30: {"path": os.path.join(PHOTO_BASE_PATH, "30.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nвзять дело в свои руки\n\n🔖…30! "},
    31: {"path": os.path.join(PHOTO_BASE_PATH, "31.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда не важно какая погода\n\n🔖…31! "},
    32: {"path": os.path.join(PHOTO_BASE_PATH, "32.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nуметь прощать!\n\n🔖…32! "},
    33: {"path": os.path.join(PHOTO_BASE_PATH, "33.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nотметиться!\n\n🔖…33! "},
    34: {"path": os.path.join(PHOTO_BASE_PATH, "34.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпервый поцелуй\n\n🔖…34!"},
    35: {"path": os.path.join(PHOTO_BASE_PATH, "35.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда без интернета! \n\n🔖…35!"},
    36: {"path": os.path.join(PHOTO_BASE_PATH, "36.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nлегкое головокружение\n\n🔖…36!"},
    37: {"path": os.path.join(PHOTO_BASE_PATH, "37.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпозвонить просто так\n\n🔖…37!"},
    38: {"path": os.path.join(PHOTO_BASE_PATH, "38.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвсё что нужно\n\n🔖…38!"},
    39: {"path": os.path.join(PHOTO_BASE_PATH, "39.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nто, что создаёшь ты\n\n🔖…39!"},
    40: {"path": os.path.join(PHOTO_BASE_PATH, "40.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсвобода\n\n🔖…40!"},
    41: {"path": os.path.join(PHOTO_BASE_PATH, "41.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда пробежала искра!\n\n🔖…41!"},
    42: {"path": os.path.join(PHOTO_BASE_PATH, "42.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nизображать недотрогу \n\n🔖…42!"},
    43: {"path": os.path.join(PHOTO_BASE_PATH, "43.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсварить ему борщ)\n\n🔖…43!"},
    44: {"path": os.path.join(PHOTO_BASE_PATH, "44.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпотрясать мир \n\n🔖…44!"},
    45: {"path": os.path.join(PHOTO_BASE_PATH, "45.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда он не ангел!\n\n🔖…45!"},
    46: {"path": os.path.join(PHOTO_BASE_PATH, "46.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпритягивать разных!\n\n🔖…46!"},
    47: {"path": os.path.join(PHOTO_BASE_PATH, "47.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nтепло внутри, когда холодно снаружи \n\n🔖…47!"},
    48: {"path": os.path.join(PHOTO_BASE_PATH, "48.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nделать покупки друг друга\n\n🔖…48!"},
    49: {"path": os.path.join(PHOTO_BASE_PATH, "49.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nнемного колкости\n\n🔖…49!"},
    50: {"path": os.path.join(PHOTO_BASE_PATH, "50.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда тянет магнитом \n\n🔖…50!"},
    51: {"path": os.path.join(PHOTO_BASE_PATH, "51.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nбыть на седьмом небе!\n\n🔖…51!"},
    52: {"path": os.path.join(PHOTO_BASE_PATH, "52.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nты и я\n\n🔖…52!"},
    53: {"path": os.path.join(PHOTO_BASE_PATH, "53.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда купил самое необходимое!\n\n🔖…53!"},
    54: {"path": os.path.join(PHOTO_BASE_PATH, "54.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкак первый день весны!\n\n🔖…54!"},
    55: {"path": os.path.join(PHOTO_BASE_PATH, "55.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпоздравить первым!\n\n🔖…55!"},
    56: {"path": os.path.join(PHOTO_BASE_PATH, "56.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nоставить след!\n\n🔖…56!"},
    57: {"path": os.path.join(PHOTO_BASE_PATH, "57.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nмикс чувств!\n\n🔖…57!"},
    58: {"path": os.path.join(PHOTO_BASE_PATH, "58.jpg"), "caption": "❤️‍🔥 LOVE IS…\nслучайные порывы!\n\n🔖…58!"},
    59: {"path": os.path.join(PHOTO_BASE_PATH, "59.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nкогда мысли сходятся!\n\n🔖…59!"},
    60: {"path": os.path.join(PHOTO_BASE_PATH, "60.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nпосильная ноша!\n\n🔖…60!"},
    61: {"path": os.path.join(PHOTO_BASE_PATH, "61.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвыбрать свое сердце!\n\n🔖…61!"},
    62: {"path": os.path.join(PHOTO_BASE_PATH, "62.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nто, что требует заботы!\n\n🔖…62!"},
    63: {"path": os.path.join(PHOTO_BASE_PATH, "63.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nбессонные ночи!\n\n🔖…63!"},
    64: {"path": os.path.join(PHOTO_BASE_PATH, "64.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nбыть на вершине мира\n\n🔖…64!"},
    65: {"path": os.path.join(PHOTO_BASE_PATH, "65.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nисправлять ошибки!\n\n🔖…65!"},
    66: {"path": os.path.join(PHOTO_BASE_PATH, "66.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nлюбоваться друг другом!\n\n🔖…66!"},
    67: {"path": os.path.join(PHOTO_BASE_PATH, "67.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nдарить главное!\n\n🔖…67!"},
    68: {"path": os.path.join(PHOTO_BASE_PATH, "68.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда совсем не холодно!\n\n🔖…68!"},
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


# --- Глобальная функция проверки доступа к командам ---
async def check_command_eligibility(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, str]:
    """
    Проверяет, соответствует ли пользователь и чат условиям для выполнения команды.
    Возвращает True и пустую строку, если команда разрешена,
    иначе False и сообщение с причиной отказа.
    """
    user = update.effective_user
    chat = update.effective_chat

    if user.is_bot:
        return False, "Боты не могут использовать эту команду."

    # Разрешить команды в личных чатах
    if chat.type == 'private':
        return True, ""

    # Проверка, находится ли команда в разрешенных группах
    if chat.id == GROUP_CHAT_ID:  # Для команд, предназначенных для основной группы
        return True, ""
    elif AQUATORIA_CHAT_ID and chat.id == AQUATORIA_CHAT_ID:  # Для команд, разрешенных в Aquatoria
        return True, ""

    # По умолчанию не разрешено в других групповых чатах
    return False, f"Эта команда может быть использована только в личных сообщениях с ботом или в чате {GROUP_USERNAME_PLAIN}."


# --- Хелперы для работы с пользовательскими данными и отображением ---
def get_marriage_user_display_name(user_data: dict) -> str:
    """Возвращает наилучшее доступное отображаемое имя для пользователя (first_name, затем username, затем ID)."""
    if user_data:
        if user_data.get('first_name'):
            return user_data['first_name']
        if user_data.get('username'):
            return user_data['username']
        if user_data.get('user_id'):
            return f"Пользователь {user_data['user_id']}"
    return "Неизвестный пользователь"


async def format_duration(start_date_obj: datetime) -> str:
    """
    Вычисляет и форматирует продолжительность с даты начала.
    Принимает объект datetime.
    """
    try:
        now = datetime.now(timezone.utc)
        duration = now - start_date_obj

        days = duration.days
        hours = duration.seconds // 3600
        minutes = (duration.seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days} дн")
        if hours > 0:
            parts.append(f"{hours} ч")
        if minutes > 0:
            parts.append(f"{minutes} мин")

        if not parts:
            return "меньше минуты"
        return ", ".join(parts)
    except Exception as e:
        logger.error(f"Ошибка форматирования длительности для {start_date_obj}: {e}")
        return "неизвестно"


# --- ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ (PostgreSQL) ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Error as e:
        logger.error(f"Ошибка подключения к базе данных PostgreSQL: {e}", exc_info=True)
        raise


# --- Инициализация всех таблиц в PostgreSQL ---
def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS laviska_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                data JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_laviska_users_username ON laviska_users (username);
        """)
        # Таблицы для Брачного Бота
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marriage_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                updated_at TIMESTAMP WITH TIME ZONE,
                last_message_in_group_at TIMESTAMP WITH TIME ZONE NULL
            );
            CREATE INDEX IF NOT EXISTS idx_marriage_users_username ON marriage_users (LOWER(username));
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marriages (
                id SERIAL PRIMARY KEY,
                initiator_id BIGINT NOT NULL,
                target_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                accepted_at TIMESTAMP WITH TIME ZONE NULL,
                divorced_at TIMESTAMP WITH TIME ZONE NULL,
                prev_accepted_at TIMESTAMP WITH TIME ZONE NULL,
                reunion_period_end_at TIMESTAMP WITH TIME ZONE NULL,
                private_message_id BIGINT NULL,
                UNIQUE(initiator_id, target_id)
            );
        """)

        # Таблицы для Мут/Бан Бота
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS muted_users (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                mute_until TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (user_id, chat_id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            );
        """)

        # Таблицы для Игрового Бота "Евангелие"
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gospel_users (
                user_id BIGINT PRIMARY KEY,
                prayer_count INTEGER DEFAULT 0,
                total_piety_score REAL DEFAULT 0,
                last_prayer_time TIMESTAMP WITH TIME ZONE,
                initialized BOOLEAN NOT NULL DEFAULT FALSE,
                cursed_until TIMESTAMP WITH TIME ZONE NULL,
                gospel_found BOOLEAN NOT NULL DEFAULT FALSE,
                first_name_cached TEXT,
                username_cached TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_gospel_users_piety ON gospel_users (total_piety_score DESC);
            CREATE INDEX IF NOT EXISTS idx_gospel_users_prayers ON gospel_users (prayer_count DESC);
        """)

        conn.commit()
        logger.info("Все базы данных (таблицы PostgreSQL) инициализированы.")
    except Error as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# --- Функции для работы с данными пользователей (Лависки - PostgreSQL JSONB) ---
def get_user_data(user_id, username) -> dict:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("SELECT data FROM laviska_users WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()

        if row:
            # Извлекаем JSONB данные, они уже будут в виде dict
            user_data = row['data']
            # Обновляем username, если он изменился или отсутствует
            if user_data.get('username') != username:
                user_data['username'] = username
                update_user_data(user_id, {"username": username})  # Отдельный вызов для обновления в БД
            return user_data
        else:
            # Создаем новую запись, если пользователь не найден
            initial_data = {
                "username": username,
                "cards": {},
                "crystals": 0,
                "spins": 0,
                "last_spin_time": 0,
                "last_spin_cooldown": COOLDOWN_SECONDS,
                "current_collection_view_index": 0,
                "achievements": []
            }
            cursor.execute(
                """INSERT INTO laviska_users (user_id, username, data) VALUES (%s, %s, %s) 
                   ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, data = EXCLUDED.data, updated_at = NOW()""",
                (user_id, username, json.dumps(initial_data))  # json.dumps для хранения dict как JSONB
            )
            conn.commit()
            return initial_data
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении данных пользователя Лависки {user_id}: {e}", exc_info=True)
        return {}  # Возвращаем пустой дикт в случае ошибки, чтобы не ломать логику
    finally:
        if conn:
            conn.close()


def update_user_data(user_id, new_data: dict):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        # Получаем текущие данные
        cursor.execute("SELECT data FROM laviska_users WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if not row:
            # Если пользователя нет, возможно, он не был создан через get_user_data.
            # Создаем с начальными данными, затем обновляем.
            initial_data = {
                "username": new_data.get("username", "unknown"),
                "cards": {}, "crystals": 0, "spins": 0, "last_spin_time": 0,
                "last_spin_cooldown": COOLDOWN_SECONDS, "current_collection_view_index": 0,
                "achievements": []
            }
            initial_data.update(new_data)  # Добавляем новые данные
            cursor.execute(
                """INSERT INTO laviska_users (user_id, username, data, updated_at) VALUES (%s, %s, %s, NOW())
                   ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, data = EXCLUDED.data, updated_at = NOW()""",
                (user_id, initial_data.get("username"), json.dumps(initial_data))
            )
        else:
            # Объединяем старые и новые данные
            existing_data = row['data']
            existing_data.update(new_data)
            # Обновляем в базе
            cursor.execute(
                """UPDATE laviska_users SET data = %s, username = %s, updated_at = NOW() WHERE user_id = %s""",
                (json.dumps(existing_data), existing_data.get("username", "unknown"), user_id)
            )
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении данных пользователя Лависки {user_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


# --- Функции для Брачного Бота (PostgreSQL) ---
def save_marriage_user_data(user: User, from_group_chat: bool = False):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        current_time = datetime.now(timezone.utc)

        # Если сообщение пришло из группы, обновляем last_message_in_group_at
        last_msg_in_group_update_clause = ""
        last_msg_in_group_value = None
        if from_group_chat:
            last_msg_in_group_update_clause = ", last_message_in_group_at = EXCLUDED.last_message_in_group_at"
            last_msg_in_group_value = current_time

        cursor.execute(f"""
            INSERT INTO marriage_users (user_id, username, first_name, last_name, updated_at, last_message_in_group_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                updated_at = EXCLUDED.updated_at
                {last_msg_in_group_update_clause}
        """, (user.id, user.username, user.first_name, user.last_name, current_time, last_msg_in_group_value))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при сохранении данных пользователя {user.id} в MARRIAGE_DB: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def get_marriage_user_data_by_id(user_id: int) -> dict:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute(
            "SELECT user_id, username, first_name, last_name, last_message_in_group_at FROM marriage_users WHERE user_id = %s",
            (user_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return {}
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении данных пользователя {user_id} из MARRIAGE_DB: {e}", exc_info=True)
        return {}
    finally:
        if conn:
            conn.close()


def get_marriage_user_data_by_username(username: str) -> Optional[dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute(
            "SELECT user_id, username, first_name, last_name, last_message_in_group_at FROM marriage_users WHERE LOWER(username) = LOWER(%s)",
            (username,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении данных пользователя по username '{username}' из MARRIAGE_DB: {e}",
                     exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_marriage_user_id_from_username_db(username: str) -> Optional[int]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM marriage_users WHERE LOWER(username) = LOWER(%s)", (username,))
        result = cursor.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении user_id по username '{username}' из MARRIAGE_DB: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_active_marriage(user_id: int) -> Optional[dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT id, initiator_id, target_id, chat_id, status, created_at, accepted_at, divorced_at, prev_accepted_at, reunion_period_end_at, private_message_id FROM marriages
            WHERE (initiator_id = %s OR target_id = %s) AND status = 'accepted'
        """, (user_id, user_id))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении активного брака для пользователя {user_id}: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_pending_marriage_proposal(user1_id: int, user2_id: int) -> Optional[dict]:
    """
    Ищет *любое* незавершенное предложение между двумя пользователями, независимо от того, кто инициатор.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT id, initiator_id, target_id, status, chat_id, created_at, accepted_at, prev_accepted_at, reunion_period_end_at, private_message_id FROM marriages
            WHERE (
                    (initiator_id = %s AND target_id = %s) OR
                    (initiator_id = %s AND target_id = %s)
                  )
                  AND status = 'pending'
        """, (user1_id, user2_id, user2_id, user1_id))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении ожидающего предложения брака: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_initiator_pending_proposal(initiator_id: int, target_id: int) -> Optional[dict]:
    """
    Ищет незавершенное предложение, где user_id является *инициатором*, а target_id - *целью*.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT id, initiator_id, target_id, status, chat_id, created_at, private_message_id FROM marriages
            WHERE initiator_id = %s AND target_id = %s AND status = 'pending'
        """, (initiator_id, target_id))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении предложения, где {initiator_id} является инициатором: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def get_target_pending_proposals(target_id: int) -> List[dict]:
    """
    Возвращает список всех незавершенных предложений, где target_id является *целью*.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT id, initiator_id, target_id, status, chat_id, created_at, private_message_id FROM marriages
            WHERE target_id = %s AND status = 'pending'
            ORDER BY created_at DESC
        """, (target_id,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении входящих предложений для {target_id}: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


def create_marriage_proposal_db(initiator_id: int, target_id: int, chat_id: int, private_message_id: Optional[int]) -> \
        Optional[int]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        current_time = datetime.now(timezone.utc)
        # ON CONFLICT DO UPDATE используется для имитации ON CONFLICT REPLACE
        cursor.execute("""
            INSERT INTO marriages (initiator_id, target_id, chat_id, status, created_at, private_message_id)
            VALUES (%s, %s, %s, 'pending', %s, %s)
            ON CONFLICT(initiator_id, target_id) DO UPDATE SET
                status = 'pending',
                created_at = %s,
                private_message_id = EXCLUDED.private_message_id, -- Обновляем на новое ID
                accepted_at = NULL,
                divorced_at = NULL,
                prev_accepted_at = NULL,
                reunion_period_end_at = NULL
            RETURNING id;
        """, (initiator_id, target_id, chat_id, current_time, private_message_id,
              current_time))
        proposal_id = cursor.fetchone()[0]
        conn.commit()
        return proposal_id
    except psycopg2.Error as e:
        logger.error(f"Ошибка при создании/обновлении предложения о венчании: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def update_proposal_private_message_id(proposal_id: int, new_message_id: Optional[int]) -> bool:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE marriages SET private_message_id = %s
            WHERE id = %s AND status = 'pending'
        """, (new_message_id, proposal_id))
        conn.commit()
        return cursor.rowcount > 0
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении private_message_id для предложения {proposal_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def accept_marriage_proposal_db(proposal_id: int, initiator_id: int, target_id: int) -> bool:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        current_time = datetime.now(timezone.utc)

        reunion_info = get_recent_divorce_for_reunion(initiator_id, target_id)

        accepted_at_to_use = current_time
        prev_accepted_at_to_save = None

        if reunion_info and reunion_info.get('reunion_period_end_at'):
            reunion_end_dt = reunion_info['reunion_period_end_at']
            if reunion_end_dt > datetime.now(timezone.utc):
                logger.info(
                    f"Восстановление брака для {initiator_id} и {target_id}. Используем предыдущий длительности.")
                if reunion_info.get('prev_accepted_at'):
                    accepted_at_to_use = reunion_info['prev_accepted_at']
                elif reunion_info.get('accepted_at'):
                    accepted_at_to_use = reunion_info['accepted_at']
                prev_accepted_at_to_save = accepted_at_to_use
            else:
                logger.info(f"Период воссоединения для {initiator_id} и {target_id} истек.")

        cursor.execute("""
            UPDATE marriages SET status = 'accepted', accepted_at = %s, prev_accepted_at = %s, divorced_at = NULL, reunion_period_end_at = NULL
            WHERE id = %s AND status = 'pending'
        """, (accepted_at_to_use, prev_accepted_at_to_save, proposal_id))
        conn.commit()
        return cursor.rowcount > 0
    except psycopg2.Error as e:
        logger.error(f"Ошибка при принятии предложения о венчании: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def get_recent_divorce_for_reunion(user1_id: int, user2_id: int) -> Optional[dict]:
    """
    Ищет недавний развод между двумя пользователями для возможности восстановления стажа.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT id, accepted_at, divorced_at, prev_accepted_at, reunion_period_end_at
            FROM marriages
            WHERE ((initiator_id = %s AND target_id = %s) OR (initiator_id = %s AND target_id = %s))
              AND status = 'divorced'
            ORDER BY divorced_at DESC
            LIMIT 1
        """, (user1_id, user2_id, user2_id, user1_id))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении недавнего развода для восстановления: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def reject_marriage_proposal_db(proposal_id: int) -> Optional[dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("SELECT * FROM marriages WHERE id = %s AND status = 'pending'", (proposal_id,))
        proposal = cursor.fetchone()
        if proposal:
            cursor.execute("""
                UPDATE marriages SET status = 'rejected'
                WHERE id = %s AND status = 'pending'
            """, (proposal_id,))
            conn.commit()
            return dict(proposal)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при отклонении предложения о венчании: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def cancel_marriage_proposal_db(initiator_id: int, target_id: int) -> Optional[dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT id, private_message_id, initiator_id, target_id FROM marriages
            WHERE initiator_id = %s AND target_id = %s AND status = 'pending'
        """, (initiator_id, target_id))
        proposal = cursor.fetchone()

        if proposal:
            proposal_id = proposal['id']
            cursor.execute("""
                UPDATE marriages SET status = 'rejected'
                WHERE id = %s AND status = 'pending'
            """, (proposal_id,))
            conn.commit()
            return dict(proposal)
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при отмене предложения о венчании: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def divorce_user_db_confirm(user_id: int) -> Optional[Tuple[int, int]]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        current_time = datetime.now(timezone.utc)
        reunion_period_end = current_time + timedelta(days=REUNION_PERIOD_DAYS)

        cursor.execute("""
            SELECT id, initiator_id, target_id, accepted_at, prev_accepted_at FROM marriages
            WHERE (initiator_id = %s OR target_id = %s) AND status = 'accepted'
        """, (user_id, user_id))
        marriage_row = cursor.fetchone()

        if marriage_row:
            marriage_id, initiator, target, accepted_at, prev_accepted_at = marriage_row

            actual_accepted_at = prev_accepted_at if prev_accepted_at else accepted_at

            cursor.execute("""
                UPDATE marriages SET
                    status = 'divorced',
                    divorced_at = %s,
                    reunion_period_end_at = %s,
                    prev_accepted_at = %s
                WHERE id = %s
            """, (current_time, reunion_period_end, actual_accepted_at, marriage_id))
            conn.commit()
            return initiator, target
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при разводе пользователя {user_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def get_all_marriages_db() -> List[dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("""
            SELECT
                m.id,
                m.initiator_id,
                u1.first_name AS initiator_first_name,
                u1.username AS initiator_username,
                m.target_id,
                u2.first_name AS target_first_name,
                u2.username AS target_username,
                m.accepted_at,
                m.chat_id,
                m.prev_accepted_at
            FROM marriages m
            JOIN marriage_users u1 ON m.initiator_id = u1.user_id
            JOIN marriage_users u2 ON m.target_id = u2.user_id
            WHERE m.status = 'accepted'
        """)
        marriages = [dict(row) for row in cursor.fetchall()]
        return marriages
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении всех браков: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


# --- Функции для Мут/Бан Бота (PostgreSQL) ---
async def unmute_user_after_timer(context):
    job = context.job
    chat_id = job.data['chat_id']
    user_id = job.data['user_id']

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM muted_users WHERE user_id = %s AND chat_id = %s', (user_id, chat_id))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при удалении записи о муте из БД: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    permissions = ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_pin_messages=True
    )
    try:
        await context.bot.restrict_chat_member(chat_id, user_id, permissions)
        user_info = await context.bot.get_chat_member(chat_id, user_id)
        logger.info(
            f"Пользователь {user_id} (@{user_info.user.username or user_info.user.first_name}) был размучен в чате {chat_id}.")
        await context.bot.send_message(chat_id,
                                       f"Пользователь {mention_html(user_id, user_info.user.first_name)} был размучен.",
                                       parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при размучивании пользователя {user_id} в чате {chat_id} (job): {e}", exc_info=True)


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
        logger.error(f"Ошибка при проверке прав администратора для мута: {e}", exc_info=True)
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
        mute_until = datetime.now(timezone.utc) + duration
    else:
        duration = timedelta(hours=1)
        mute_until = datetime.now(timezone.utc) + duration

    conn = None
    try:
        permissions = ChatPermissions(
            can_send_messages=False,
            can_send_media_messages=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
            can_pin_messages=False
        )
        await context.bot.restrict_chat_member(chat_id, target_user.id, permissions, until_date=mute_until)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO muted_users (user_id, chat_id, mute_until) VALUES (%s, %s, %s) ON CONFLICT (user_id, chat_id) DO UPDATE SET mute_until = EXCLUDED.mute_until',
            (target_user.id, chat_id, mute_until))
        conn.commit()

        context.job_queue.run_once(
            unmute_user_after_timer,
            duration.total_seconds(),
            data={'chat_id': chat_id, 'user_id': target_user.id},
            name=f"unmute_{target_user.id}_{chat_id}"
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
        logger.error(f"Ошибка при муте пользователя {target_user.id} в чате {chat_id}: {e}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка при попытке замутить пользователя. Возможно, я не имею достаточных прав или пользователь является администратором.")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


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
        logger.error(f"Ошибка при проверке прав администратора для размута: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    conn = None
    try:
        permissions = ChatPermissions(
            can_send_messages=True,
            can_send_media_messages=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True,
            can_pin_messages=True
        )
        await context.bot.restrict_chat_member(chat_id, target_user.id, permissions)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM muted_users WHERE user_id = %s AND chat_id = %s', (target_user.id, chat_id))
        conn.commit()

        current_jobs = context.job_queue.get_jobs_by_name(f"unmute_{target_user.id}_{chat_id}")
        for job in current_jobs:
            job.schedule_removal()

        await update.message.reply_text(
            f"Пользователь {mention_html(target_user.id, target_user.first_name)} был размучен.",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при размуте пользователя {target_user.id} в чате {chat_id}: {e}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка при попытке размутить пользователя. Возможно, я не имею достаточных прав. Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


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
        logger.error(f"Ошибка при проверке прав администратора для бана: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    conn = None
    try:
        await context.bot.ban_chat_member(chat_id, target_user.id)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO banned_users (user_id, chat_id) VALUES (%s, %s) ON CONFLICT (user_id, chat_id) DO NOTHING',
            (target_user.id, chat_id))
        conn.commit()

        await update.message.reply_text(
            f"Пользователь {mention_html(target_user.id, target_user.first_name)} ЗАБАНЕН",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Ошибка при бане пользователя {target_user.id} в чате {chat_id}: {e}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка при попытке забанить пользователя. Возможно, я не имею достаточных прав. Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


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
        logger.error(f"Ошибка при проверке прав администратора для разбана: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка при проверке ваших прав.")
        return

    conn = None
    try:
        await context.bot.unban_chat_member(chat_id, target_user.id)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM banned_users WHERE user_id = %s AND chat_id = %s', (target_user.id, chat_id))
        conn.commit()

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
        logger.error(f"Ошибка при разбане пользователя {target_user.id} в чате {chat_id}: {e}", exc_info=True)
        await update.message.reply_text(
            f"Произошла ошибка при попытке разблокировать пользователя. Возможно, я не имею достаточных прав. Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# --- Функции для Игрового Бота "Евангелие" (PostgreSQL) ---

def update_piety_and_prayer_db(user_id: int, gained_piety: float, last_prayer_time: datetime):
    """Атомарно увеличивает счетчик молитв и набожности."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE gospel_users SET
                prayer_count = prayer_count + 1,
                total_piety_score = total_piety_score + %s,
                last_prayer_time = %s
            WHERE user_id = %s
        ''', (gained_piety, last_prayer_time, user_id))
        conn.commit()
        if cursor.rowcount == 0:
            logger.warning(f"Попытка атомарного обновления молитвы для {user_id}, но пользователь не найден.")
    except psycopg2.Error as e:
        logger.error(f"Ошибка при атомарном обновлении молитвы для {user_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def update_curse_db(user_id: int, cursed_until: datetime):
    """Атомарно устанавливает время проклятия."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE gospel_users SET
                cursed_until = %s
            WHERE user_id = %s
        ''', (cursed_until, user_id))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении проклятия для {user_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def add_gospel_game_user(user_id: int, first_name: str, username: Optional[str] = None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gospel_users (user_id, initialized, gospel_found, first_name_cached, username_cached)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
        ''', (user_id, False, False, first_name, username))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при добавлении пользователя {user_id} в gospel_game.db: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def update_gospel_game_user_cached_data(user_id: int, first_name: str, username: Optional[str] = None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE gospel_users SET first_name_cached = %s, username_cached = %s WHERE user_id = %s
        ''', (first_name, username, user_id))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении кэшированных данных пользователя {user_id} в gospel_game.db: {e}",
                     exc_info=True)
    finally:
        if conn:
            conn.close()


def get_gospel_game_user_data(user_id: int) -> Optional[dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute('SELECT * FROM gospel_users WHERE user_id = %s', (user_id,))
        user_data = cursor.fetchone()
        if user_data:
            data = dict(user_data)
            # Убедимся, что числовые поля всегда возвращаются как числа
            data['prayer_count'] = data.get('prayer_count') or 0
            data['total_piety_score'] = data.get('total_piety_score') or 0.0
            return data
        return None
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении данных пользователя {user_id} из gospel_game.db: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def update_gospel_game_user_data(user_id: int, prayer_count: int, total_piety_score: float, last_prayer_time: datetime,
                                 cursed_until: Optional[datetime], gospel_found: bool,
                                 first_name_cached: str, username_cached: Optional[str]):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''UPDATE gospel_users SET prayer_count = %s, total_piety_score = %s, last_prayer_time = %s, cursed_until = %s, gospel_found = %s, first_name_cached = %s, username_cached = %s WHERE user_id = %s''',
            (prayer_count, total_piety_score, last_prayer_time, cursed_until, gospel_found, first_name_cached,
             username_cached, user_id)
        )
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении данных пользователя {user_id} в gospel_game.db: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


async def find_gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    is_eligible, reason = await check_command_eligibility(update, context)
    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)
    if user_data and user_data['gospel_found']:
        await update.message.reply_text("Вы уже нашли Евангелие. Отправляйтесь на службу!")
        return

    # Если пользователя нет в базе или gospel_found = 0, инициализируем
    if not user_data:
        await asyncio.to_thread(add_gospel_game_user, user_id, user.first_name, user.username)
        user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)
        if not user_data:
            await update.message.reply_text("Ошибка инициализации данных. Попробуйте позже.")
            return

    # Преобразуем строковые даты в datetime объекты (или None) для передачи в update_gospel_game_user_data
    # PostgreSQL работает напрямую с datetime объектами
    last_prayer_time_obj = user_data['last_prayer_time'] if user_data.get('last_prayer_time') else None
    cursed_until_obj = user_data['cursed_until'] if user_data.get('cursed_until') else None

    await asyncio.to_thread(update_gospel_game_user_data, user_id,
                            user_data['prayer_count'],
                            user_data['total_piety_score'],
                            last_prayer_time_obj,
                            cursed_until_obj,
                            True,  # Gospel found
                            user.first_name, user.username
                            )

    await update.message.reply_text(
        "Успех! ✨\nВаши реликвии у вас в руках!\n\nВам открылась возможность:\n⛩️ «мольба» — ходить на службу\n📜«Евангелие» — смотреть свои Евангелие\n📃 «Топ Евангелий» — и следить за вашими успехами!\nЖелаем удачи! 🍀"
    )


async def prayer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    is_eligible, reason = await check_command_eligibility(update, context)

    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)

    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫"
        )
        return

    current_time = datetime.now(timezone.utc)
    cursed_until = user_data['cursed_until']

    if cursed_until and current_time < cursed_until:
        remaining_time = cursed_until - current_time
        hours = int(remaining_time.total_seconds() // 3600)
        minutes = int((remaining_time.total_seconds() % 3600) // 60)
        await update.message.reply_text(
            f'У вас бесноватость 👹\n📿 Вы не сможете молиться еще {hours} часа(ов), {minutes} минут(ы).'
        )
        return

    is_friday = current_time.weekday() == 4
    is_early_morning = (21 <= current_time.hour < 1)

    if (is_friday or is_early_morning) and random.random() < 0.08:
        cursed_until_new = current_time + timedelta(hours=8)

        # Используем новую атомарную функцию для установки проклятия
        await asyncio.to_thread(update_curse_db, user_id, cursed_until_new)

        await update.message.reply_text(
            "У вас бесноватость 👹\nПохоже вашу мольбу услышал кое-кто….другой\n\n📿 Вы не сможете молиться сутки."
        )
        return

    last_prayer_time = user_data['last_prayer_time']

    if last_prayer_time and current_time < last_prayer_time + timedelta(hours=1):
        remaining_time = (last_prayer_time + timedelta(hours=1)) - current_time
        minutes = int(remaining_time.total_seconds() // 60)
        seconds = int(remaining_time.total_seconds() % 60)
        await update.message.reply_text(
            f'.....Похоже никто не слышит вашей мольбы\n📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы).'
        )
        return

    gained_piety = round(random.uniform(1, 20) / 2, 1)

    # ИСПОЛЬЗУЕМ АТОМАРНОЕ ОБНОВЛЕНИЕ
    await asyncio.to_thread(update_piety_and_prayer_db, user_id, gained_piety, current_time)

    await update.message.reply_text(
        f'⛩️ Ваши мольбы были услышаны! \n✨ Набожность +{gained_piety}\nНа следующую службу можно будет выйти через час 📿')


async def gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    is_eligible, reason = await check_command_eligibility(update, context)  # Единая проверка
    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

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
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        cursor.execute(
            'SELECT user_id, prayer_count, first_name_cached, username_cached FROM gospel_users WHERE gospel_found = TRUE ORDER BY prayer_count DESC')
        all_prayer_leaderboard = cursor.fetchall()

        cursor.execute(
            'SELECT user_id, total_piety_score, first_name_cached, username_cached FROM gospel_users WHERE gospel_found = TRUE ORDER BY total_piety_score DESC')
        all_piety_leaderboard = cursor.fetchall()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении данных для лидерборда: {e}", exc_info=True)
        return "Произошла ошибка при получении данных для топа. Попробуйте позже.", InlineKeyboardMarkup([])
    finally:
        if conn:
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
    is_eligible, reason = await check_command_eligibility(update, context)

    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

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
        logger.error(f"Ошибка при отправке сообщения топа Евангелий (prayers): {e}", exc_info=True)
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


async def check_and_award_achievements(update_or_user_id, context: ContextTypes.DEFAULT_TYPE, user_data: dict):
    """
    Если update_or_user_id — объект Update, используется update.message.reply_text для уведомлений,
    иначе если это просто user_id (int) — используется context.bot.send_message(user_id, ...).
    Функция изменяет user_data (должна быть сохранена вызывающей стороной).
    """
    # уточним интерфейс отправки сообщений
    send_direct = None
    user_id = None
    if isinstance(update_or_user_id, Update):  # передан Update
        user_id = update_or_user_id.effective_user.id

        async def send_direct_func(text):
            try:
                await update_or_user_id.message.reply_text(text, parse_mode=ParseMode.HTML)
            except Exception:
                # fallback
                try:
                    await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
                except Exception:
                    logger.warning("Не удалось отправить уведомление об достижении.")

        send_direct = send_direct_func
    else:
        # предполагаем, что передан user_id (int)
        user_id = int(update_or_user_id)

        async def send_direct_func(text):
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
            except Exception:
                logger.warning("Не удалось отправить уведомление об достижении по user_id.")

        send_direct = send_direct_func

    unique_count = len(user_data.get("cards", {}))
    newly_awarded = []

    for ach in ACHIEVEMENTS:
        ach_id = ach["id"]
        if ach_id in user_data.get("achievements", []):
            continue
        if unique_count >= ach["threshold"]:
            # выдаём награду
            reward = ach["reward"]
            if reward["type"] == "spins":
                user_data["spins"] = user_data.get("spins", 0) + int(reward["amount"])
                msg = f"🏆 Достижение: {ach['name']}\n🧧 Вы получили {reward['amount']} жетонов!"
            elif reward["type"] == "crystals":
                user_data["crystals"] = user_data.get("crystals", 0) + int(reward["amount"])
                msg = f"🏆 Достижение: {ach['name']}\nВам начислено {reward['amount']} 🧩!"
            else:
                msg = f"🏆 Достижение: {ach['name']}\nНаграда: {reward}"

            # пометить как полученное
            user_data.setdefault("achievements", []).append(ach_id)
            newly_awarded.append(msg)

    # сохраняем если что-то выдали
    if newly_awarded:
        await asyncio.to_thread(update_user_data, user_id, user_data)
        # отправляем уведомления (можно собрать в одно сообщение)
        for text in newly_awarded:
            await send_direct(text)


# --- ОБРАБОТЧИКИ КОМАНД (Лависки) ---
async def lav_iska(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    is_eligible, reason = await check_command_eligibility(update, context)
    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    user_data = await asyncio.to_thread(get_user_data, user_id, username)

    current_time = time.time()
    last_time = user_data.get("last_spin_time", 0)
    last_cd = user_data.get("last_spin_cooldown", COOLDOWN_SECONDS)

    if current_time - last_time < last_cd:
        remaining = int(last_cd - (current_time - last_time))
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        seconds = remaining % 60
        parts = []
        if hours > 0:
            parts.append(f"{hours} ч")
        if minutes > 0:
            parts.append(f"{minutes} мин")
        if hours == 0 and minutes == 0:
            parts.append(f"{seconds} сек")
        await update.message.reply_text(f"⏳ Вы уже использовали получали loveisку. Повторите через {' '.join(parts)}")
        return

    # Решаем кто выпадет: если у пользователя есть крутки -> потребляем 1 и даём гарантированно новую (если есть новые)
    owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])
    new_card_ids = [i for i in range(1, NUM_PHOTOS + 1) if i not in owned_card_ids]

    chosen_card_id = None
    is_new_card = False
    used_spin = False

    if user_data.get("spins", 0) > 0:
        # потребляем крутку и ставим короткий откат
        user_data["spins"] -= 1
        used_spin = True
        user_data["last_spin_time"] = current_time
        user_data["last_spin_cooldown"] = SPIN_USED_COOLDOWN  # 10 минут

        if new_card_ids:
            chosen_card_id = random.choice(new_card_ids)
            is_new_card = True
            await update.message.reply_text(
                "Вы потратили жетон и получили уникальную каточку! Следующую команду можно написать через 10 минут.")
        else:
            # все карточки собраны — даём кристаллы вместо новой карточки
            # логика прежняя: начисляем REPEAT_CRYSTALS_BONUS
            chosen_card_id = random.choice(owned_card_ids) if owned_card_ids else random.choice(
                range(1, NUM_PHOTOS + 1))
            user_data["crystals"] += REPEAT_CRYSTALS_BONUS
            caption_suffix = f" (все карточки собраны, получено {REPEAT_CRYSTALS_BONUS} 🧩 фрагментов)"
            await update.message.reply_text(
                f"У вас уже есть все карточки! Вы потратили жетон, вам начислены {REPEAT_CRYSTALS_BONUS} 🧩 фрагментов. Следующую команду можно написать через 10 минут.")
    else:
        # нет круток — стандартная логика и длинный откат
        user_data["last_spin_time"] = current_time
        user_data["last_spin_cooldown"] = COOLDOWN_SECONDS  # 3 часа

        if new_card_ids and owned_card_ids:
            if random.random() < 0.8:  # 80% шанс на новую, если есть новые и старые
                chosen_card_id = random.choice(new_card_ids)
                is_new_card = True
            else:
                chosen_card_id = random.choice(owned_card_ids)
        elif new_card_ids:  # только новые
            chosen_card_id = random.choice(new_card_ids)
            is_new_card = True
        elif owned_card_ids:  # всё собрано
            chosen_card_id = random.choice(owned_card_ids)
        else:  # совсем пусто
            chosen_card_id = random.choice(range(1, NUM_PHOTOS + 1))
            is_new_card = True

    if chosen_card_id is None:
        await update.message.reply_text("Не удалось выбрать карточку. Пожалуйста, свяжитесь с администратором.")
        await asyncio.to_thread(update_user_data, user_id, user_data)
        return

    card_id_str = str(chosen_card_id)
    caption_suffix_actual = ""

    if is_new_card:
        user_data["cards"][card_id_str] = 1
        caption_suffix_actual = " Новая карточка добавлена в вашу коллекцию!"
    else:
        user_data["cards"][card_id_str] = user_data["cards"].get(card_id_str, 0) + 1
        user_data["crystals"] += REPEAT_CRYSTALS_BONUS
        caption_suffix_actual = f" 👀 Это повторная карточка!\n\nВы получили {REPEAT_CRYSTALS_BONUS} 💌 фрагментов!\nУ вас теперь {user_data['cards'][card_id_str]} таких карточек"

    if 'caption_suffix' in locals():  # случай "все карточки собраны" выше
        caption_suffix_actual = caption_suffix + caption_suffix_actual

    photo_path = PHOTO_DETAILS[chosen_card_id]["path"]
    caption = PHOTO_DETAILS[chosen_card_id]["caption"] + caption_suffix_actual

    try:
        await update.message.reply_photo(photo=open(photo_path, "rb"), caption=caption)
    except FileNotFoundError:
        await update.message.reply_text(f"Ошибка: Файл фотографии не найден по пути {photo_path}")
        logger.error(f"File not found: {photo_path}")
    except Exception as e:
        await update.message.reply_text(f"Произошла ошибка при отправке фото: {e}")
        logger.error(f"Error sending photo: {e}", exc_info=True)

    # проверяем и выдаём достижения, если нужно
    await check_and_award_achievements(update, context, user_data)

    # сохраняем состояние пользователя
    await asyncio.to_thread(update_user_data, user_id, user_data)


async def my_collection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    is_eligible, reason = await check_command_eligibility(update, context)
    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    user_data = await asyncio.to_thread(get_user_data, user_id, username)

    total_owned_cards = len(user_data["cards"])

    keyboard = [
        [InlineKeyboardButton(f"❤️‍🔥 LOVE IS... {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements"),
         InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"🪪 Пользователь: @{username}\n\n"
        f"🧧 Жетоны: {user_data['spins']}\n"
        f"🧩 Фрагменты: {user_data['crystals']}\n"
    )

    try:
        await update.message.reply_photo(
            photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
            caption=message_text,
            reply_markup=reply_markup
        )
    except FileNotFoundError:
        logger.error(f"Collection menu image not found: {COLLECTION_MENU_IMAGE_PATH}", exc_info=True)
        await update.message.reply_text(
            message_text + "\n\n(Ошибка: фоновая картинка коллекции не найдена)",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error sending collection menu photo: {e}", exc_info=True)
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

    card_count = user_data["cards"].get(str(card_id), 0)
    photo_path = PHOTO_DETAILS[card_id]["path"]
    caption_text = (
        f"{PHOTO_DETAILS[card_id]['caption']}"
        f" Таких карт у вас - {card_count}"
    )

    keyboard = []
    nav_buttons = []
    if len(owned_card_ids) > 1:
        nav_buttons.append(InlineKeyboardButton("← Предыдущая", callback_data=f"nav_card_prev"))
        nav_buttons.append(InlineKeyboardButton("Следующая →", callback_data=f"nav_card_next"))

    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("Выйти в мою коллекцию", callback_data="back_to_main_collection")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_media(
            media=InputMediaPhoto(media=open(photo_path, "rb"), caption=caption_text),
            reply_markup=reply_markup
        )
    except BadRequest as e:  # Catch BadRequest specifically
        logger.warning(
            f"Failed to edit message media for card view (likely old message or user blocked bot): {e}. Sending new message.",
            exc_info=True)
        try:
            # Send a new message if editing failed
            await query.bot.send_photo(
                chat_id=query.from_user.id,
                photo=open(photo_path, "rb"),
                caption=caption_text,
                reply_markup=reply_markup
            )
        except Exception as new_send_e:
            logger.error(f"Failed to send new photo for card view after edit failure: {new_send_e}", exc_info=True)
            await query.message.reply_text(
                "Произошла ошибка при отображении карточки. Пожалуйста, попробуйте еще раз."
            )
    except Exception as e:
        logger.error(f"Failed to edit message media for card view with unexpected error: {e}", exc_info=True)
        await query.message.reply_text(
            "Произошла ошибка при отображении карточки. Пожалуйста, попробуйте еще раз."
        )


async def my_collection_edit_message(query):
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name
    user_data = await asyncio.to_thread(get_user_data, user_id, username)

    total_owned_cards = len(user_data["cards"])

    keyboard = [
        [InlineKeyboardButton(f"❤️‍🔥 LOVE IS... {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements"),
         InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"🪪 Пользователь: @{username}\n\n"
        f"🧧 Жетоны: {user_data['spins']}\n"
        f"🧩 Фрагменты: {user_data['crystals']}\n"
    )

    try:
        await query.edit_message_media(
            media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text),
            reply_markup=reply_markup
        )
    except BadRequest as e:  # Catch BadRequest specifically
        logger.warning(
            f"Failed to edit message to main collection photo (likely old message or user blocked bot): {e}. Sending new message.",
            exc_info=True)
        try:
            # Send a new message if editing failed
            await query.bot.send_photo(
                chat_id=query.from_user.id,
                photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                caption=message_text,
                reply_markup=reply_markup
            )
        except Exception as new_send_e:
            logger.error(f"Failed to send new photo for collection menu after edit failure: {new_send_e}",
                         exc_info=True)
            await query.message.reply_text(
                "Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз."
            )
    except Exception as e:
        logger.error(f"Failed to edit message to main collection photo with unexpected error: {e}", exc_info=True)
        await query.message.reply_text(
            "Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз."
        )


# --- ОБРАБОТЧИКИ RP КОМАНД ---
async def rp_command_template(update: Update, context: ContextTypes.DEFAULT_TYPE, responses: List[str],
                              action_name: str):
    user = update.effective_user
    chat_id = update.effective_chat.id
    is_eligible, reason = await check_command_eligibility(update, context)

    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    target_user_id: Optional[int] = None
    target_user_data: Optional[dict] = None

    # Попытка получить цель из ответа на сообщение
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        replied_user = update.message.reply_to_message.from_user
        if replied_user.is_bot:
            await update.message.reply_text(f"👾 Вы не можете {action_name} бота!")
            return
        if replied_user.id == user.id:
            await update.message.reply_text(f"👾 Вы не можете {action_name} самого себя!")
            return
        target_user_id = replied_user.id
        await asyncio.to_thread(save_marriage_user_data, replied_user, from_group_chat=True)
        target_user_data = await asyncio.to_thread(get_marriage_user_data_by_id, target_user_id)
        if not target_user_data:  # Если данные пользователя не в Marriage DB, используем данные из Telegram
            target_user_data = {"user_id": replied_user.id, "first_name": replied_user.first_name,
                                "username": replied_user.username}

    # Попытка получить цель из @username в аргументах
    if not target_user_id and context.args:
        username_arg = context.args[0]
        if username_arg.startswith('@'):
            username_arg = username_arg[1:]

        target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_username, username_arg)
        if target_user_data_from_db:
            target_user_id = target_user_data_from_db['user_id']
            target_user_data = target_user_data_from_db
        else:
            await update.message.reply_text(
                f"👾 Пользователь '{username_arg}' не найден в базе данных бота. Возможно, он еще не писал в чат или не имеет публичного username.")
            return

    if not target_user_id:
        await update.message.reply_text(
            f"👾 Чтобы {action_name}, ответьте на сообщение пользователя или укажите его `@username` (например: `/{action_name} @username`).")
        return

    # Убедимся, что данные целевого пользователя достаточно полные для mention_html
    if not target_user_data or not (target_user_data.get('first_name') or target_user_data.get('username')):
        # Попытка в последний раз получить данные из Telegram, если они отсутствуют
        try:
            target_tg_user_info = await context.bot.get_chat_member(chat_id, target_user_id)
            target_user_data = {"user_id": target_tg_user_info.user.id,
                                "first_name": target_tg_user_info.user.first_name,
                                "username": target_tg_user_info.user.username}
        except Exception:
            target_user_data = {"user_id": target_user_id, "first_name": f"Пользователь {target_user_id}",
                                "username": None}
            logger.warning(
                f"Не удалось получить полные данные о целевом пользователе {target_user_id} для RP команды. Используем запасное имя.")

    actor_mention = mention_html(user.id, user.first_name)
    target_mention = mention_html(target_user_data['user_id'], get_marriage_user_display_name(target_user_data))

    response_template = random.choice(responses)
    response_text = f"{actor_mention} {response_template.format(target_mention=target_mention)}"

    await update.message.reply_text(response_text, parse_mode=ParseMode.HTML)


# --- Хелпер для повторной отправки предложений ---
async def _resend_pending_proposals_to_target(target_user_id: int, context: ContextTypes.DEFAULT_TYPE):
    pending_proposals = await asyncio.to_thread(get_target_pending_proposals, target_user_id)

    if not pending_proposals:
        logger.debug(f"Нет входящих предложений для {target_user_id} для переотправки.")
        return

    for proposal in pending_proposals:
        initiator_id = proposal['initiator_id']
        proposal_id = proposal['id']
        private_message_id = proposal['private_message_id']

        initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, initiator_id)
        target_info = await asyncio.to_thread(get_marriage_user_data_by_id, target_user_id)

        if not initiator_info or not target_info:
            logger.error(
                f"Не удалось получить данные для инициатора {initiator_id} или цели {target_user_id} для предложения {proposal_id}. Пропускаем.")
            continue

        initiator_display_name = get_marriage_user_display_name(initiator_info)
        initiator_mention = mention_html(initiator_id, initiator_display_name)

        target_display_name = get_marriage_user_display_name(target_info)
        target_mention = mention_html(target_user_id, target_display_name)

        message_text = (
            f"{target_mention}, вам предложил венчаться пользователь {initiator_mention}!\n"
            f"Вы хотите принять это предложение?"
        )
        keyboard = [
            [InlineKeyboardButton("Да", callback_data=f"marry_yes_{initiator_id}_{target_user_id}")],
            [InlineKeyboardButton("Нет", callback_data=f"marry_no_{initiator_id}_{target_user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        message_sent_or_edited = False
        if private_message_id:
            try:
                # Попытка отредактировать существующее сообщение
                await context.bot.edit_message_text(
                    chat_id=target_user_id,
                    message_id=private_message_id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
                message_sent_or_edited = True
                logger.info(
                    f"Отредактировано сообщение {private_message_id} для {target_user_id} по предложению {proposal_id}")
            except BadRequest as e:  # Bot blocked, message not found, etc.
                logger.warning(
                    f"Не удалось отредактировать сообщение {private_message_id} для {target_user_id} (предложение {proposal_id}): {e}. Отправляем новое.",
                    exc_info=True)
                # Если редактирование не удалось, сбрасываем private_message_id в БД для этого предложения
                await asyncio.to_thread(update_proposal_private_message_id, proposal_id, None)
            except Exception as e:
                logger.error(
                    f"Общая ошибка при редактировании сообщения {private_message_id} для {target_user_id} (предложение {proposal_id}): {e}",
                    exc_info=True)
                await asyncio.to_thread(update_proposal_private_message_id, proposal_id, None)

        if not message_sent_or_edited:
            try:
                # Отправка нового сообщения
                sent_msg = await context.bot.send_message(
                    chat_id=target_user_id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
                # Обновляем private_message_id в БД
                await asyncio.to_thread(update_proposal_private_message_id, proposal_id, sent_msg.message_id)
                logger.info(
                    f"Отправлено новое сообщение {sent_msg.message_id} для {target_user_id} по предложению {proposal_id}")
            except Exception as e:
                logger.error(
                    f"Не удалось отправить личное сообщение {target_mention} (ID: {target_user_id}) о предложении {proposal_id}: {e}",
                    exc_info=True)
                # Если не удалось отправить, убеждаемся, что private_message_id сброшен в БД
                await asyncio.to_thread(update_proposal_private_message_id, proposal_id, None)


# --- Основные обработчики Telegram (Объединенные) ---

async def unified_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await asyncio.to_thread(save_marriage_user_data, user, from_group_chat=False)
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    chat_url = GROUP_CHAT_INVITE_LINK if GROUP_CHAT_INVITE_LINK else f'https://t.me/{GROUP_USERNAME_PLAIN}'

    keyboard = [
        [InlineKeyboardButton(f'Вступить в чат 💬', url=chat_url)],
        [InlineKeyboardButton('Новогоднее голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
        [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa')],
        [InlineKeyboardButton('Команды ⚙️', callback_data='show_commands')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_name = user.username or user.first_name or 'друг'
    await update.message.reply_text(
        f'Привет, {user_name}! 🪐\nЭто бот чата 𝙄𝙎𝙎𝙐𝐄 \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, '
        'принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!',
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )
    # После приветствия проверяем и повторно отправляем/обновляем предложения
    await _resend_pending_proposals_to_target(user.id, context)


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
VENCHATSYA_REGEX = re.compile(r"^(венчаться)(?:\s+@?(\w+))?$", re.IGNORECASE)  # Adjusted regex
OTMENIT_VENCHANIE_REGEX = re.compile(r"^(отменить венчание)(?:\s+@?(\w+))?$", re.IGNORECASE)  # Adjusted regex


async def unified_text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message: Optional[Message] = None

    if update.message:
        message = update.message
    elif update.edited_message:
        message = update.edited_message

    if not message or not message.text:  # Обрабатываем только текстовые сообщения
        return

    user = message.from_user
    chat_id = message.chat_id
    full_message_text = message.text
    message_text_lower = full_message_text.lower().strip()

    if user and not user.is_bot:
        from_group = (chat_id == GROUP_CHAT_ID or (AQUATORIA_CHAT_ID and chat_id == AQUATORIA_CHAT_ID))
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

        # --- Административные команды (текстовые, без слеша) ---
        if message_text_lower.startswith("исмут"):
            if chat_id not in [GROUP_CHAT_ID, AQUATORIA_CHAT_ID] or str(user.id) != ADMIN_ID:
                await update.message.reply_text("У вас нет прав для выполнения этой команды.")
                return
            if not update.message.reply_to_message:
                await update.message.reply_text("Используйте эту команду ответом на сообщение пользователя.")
                return
            parts = full_message_text.split(maxsplit=1)
            context.args = [parts[1]] if len(parts) > 1 else []
            await admin_mute_user(update, context)
            return
        elif message_text_lower == "исговори":
            if chat_id not in [GROUP_CHAT_ID, AQUATORIA_CHAT_ID] or str(user.id) != ADMIN_ID:
                await update.message.reply_text("У вас нет прав для выполнения этой команды.")
                return
            if not update.message.reply_to_message:
                await update.message.reply_text(
                    "Используйте эту команду ответом на сообщение пользователя.")
                return
            await admin_unmute_user(update, context)
            return
        elif message_text_lower == "вон":
            if chat_id not in [GROUP_CHAT_ID, AQUATORIA_CHAT_ID] or str(user.id) != ADMIN_ID:
                await update.message.reply_text("У вас нет прав для выполнения этой команды.")
                return
            if not update.message.reply_to_message:
                await update.message.reply_text(
                    "Используйте эту команду ответом на сообщение пользователя.")
                return
            await admin_ban_user(update, context)
            return
        elif message_text_lower == "вернуть":
            if chat_id not in [GROUP_CHAT_ID, AQUATORIA_CHAT_ID] or str(user.id) != ADMIN_ID:
                await update.message.reply_text("У вас нет прав для выполнения этой команды.")
                return
            if not update.message.reply_to_message:
                await update.message.reply_text(
                    "Используйте эту команду ответом на сообщение пользователя.")
                return
            await admin_unban_user(update, context)
            return

        # --- Команды Брачного Бота ---

        elif VENCHATSYA_REGEX.match(message_text_lower):
            is_eligible, reason = await check_command_eligibility(update, context)
            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            initiator_id = user.id
            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, initiator_id)
            if not initiator_info:
                # Fallback to Telegram user info if not in DB
                initiator_info = {"user_id": initiator_id, "first_name": user.first_name, "username": user.username}
            initiator_display_name = get_marriage_user_display_name(initiator_info)
            initiator_mention = mention_html(initiator_id, initiator_display_name)

            target_user_id: Optional[int] = None
            target_user_data: Optional[dict] = None

            match = VENCHATSYA_REGEX.match(message_text_lower)
            username_from_args = match.group(2) if match else None

            if username_from_args:
                target_username = username_from_args.lstrip('@')
                target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_username, target_username)
                if target_user_data_from_db:
                    target_user_id = target_user_data_from_db['user_id']
                    target_user_data = target_user_data_from_db
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"👾 Пользователь '@{target_username}' не найден в базе данных бота. "
                             "Убедитесь, что он писал сообщения в группе и у него есть публичный username, "
                             "либо попросите его написать `/start` боту в личные сообщения.",
                        parse_mode=ParseMode.HTML
                    )
                    return

            elif update.message.reply_to_message and update.message.reply_to_message.from_user:
                target_telegram_user = update.message.reply_to_message.from_user
                target_user_id = target_telegram_user.id
                target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_id, target_user_id)
                if target_user_data_from_db:
                    target_user_data = target_user_data_from_db
                else:
                    target_user_data = {"user_id": target_telegram_user.id,
                                        "first_name": target_telegram_user.first_name,
                                        "username": target_telegram_user.username}
                    await asyncio.to_thread(save_marriage_user_data, target_telegram_user, from_group_chat=True)

            if not target_user_id or not target_user_data:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Чтобы предложить пожениться, ответьте на сообщение пользователя "
                         "или укажите его юзернейм после команды (например, `Венчаться @username`).",
                    parse_mode=ParseMode.HTML
                )
                return

            if initiator_id == target_user_id:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Вы не можете пожениться сами с собой! "
                         "Пожалуйста, выберите другого пользователя.",
                    parse_mode=ParseMode.HTML
                )
                return

            if target_user_data.get('user_id') == context.bot.id or \
                    (update.message.reply_to_message and update.message.reply_to_message.from_user.is_bot):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Вы не можете предлагать пожениться ботам. "
                         "Они заняты служением человечеству, а не брачными узами.",
                    parse_mode=ParseMode.HTML
                )
                return

            target_display_name = get_marriage_user_display_name(target_user_data)
            target_mention = mention_html(target_user_id, target_display_name)

            if await asyncio.to_thread(get_active_marriage, initiator_id):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"{initiator_mention}, вы уже состоите в браке. "
                         "Для создания нового брака необходимо развестись с текущим супругом.",
                    parse_mode=ParseMode.HTML
                )
                return

            if await asyncio.to_thread(get_active_marriage, target_user_id):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"{target_mention} уже состоит в браке. "
                         "Выберите другого пользователя для предложения.",
                    parse_mode=ParseMode.HTML
                )
                return

            existing_proposal = await asyncio.to_thread(get_pending_marriage_proposal, initiator_id, target_user_id)
            if existing_proposal:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Между вами и {target_mention} уже есть активное предложение "
                         "о браке. Дождитесь ответа или отмените свое.",
                    parse_mode=ParseMode.HTML
                )
                return

            private_msg_id: Optional[int] = None
            message_to_initiator_in_group: str = ""

            try:
                keyboard = [
                    [InlineKeyboardButton("Да", callback_data=f"marry_yes_{initiator_id}_{target_user_id}")],
                    [InlineKeyboardButton("Нет", callback_data=f"marry_no_{initiator_id}_{target_user_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                sent_msg = await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"{target_mention}, вам предложил венчаться пользователь {initiator_mention}!\n"
                         f"Вы хотите принять это предложение?",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
                private_msg_id = sent_msg.message_id
                message_to_initiator_in_group = (
                    f"💍 Ваше предложение отправлено {target_mention} в личные сообщения!\n\n"
                    f"Держим за вас кулачки ✊🏻"
                )

            except BadRequest as e:
                logger.warning(f"Не удалось отправить личное сообщение {target_mention} (ID: {target_user_id}): {e}",
                               exc_info=True)
                private_msg_id = None
                message_to_initiator_in_group = (
                    f"Если ваш избранник {target_mention} не получил предложение (возможно, бот заблокирован или пользователь не начинал диалог ему нужно будет написать `/start` и ввести команду `предложения`)"
                )
            except Exception as e:
                logger.error(
                    f"Общая ошибка при отправке личного сообщения {target_mention} (ID: {target_user_id}): {e}",
                    exc_info=True)
                private_msg_id = None
                message_to_initiator_in_group = (
                    f"Произошла ошибка при попытке отправить личное сообщение {target_mention}. "
                    f"Возможно, бот заблокирован или пользователь не начинал диалог. "
                    f"Попросите его написать `/start` боту в личные сообщения, затем ввести `предложения`."
                )

            if await asyncio.to_thread(create_marriage_proposal_db, initiator_id, target_user_id, chat_id,
                                       private_msg_id):
                await update.message.reply_text(message_to_initiator_in_group, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(chat_id=chat_id,
                                               text="❗️ Ваше предложение не было зарегистрировано из-за внутренней ошибки. Пожалуйста, попробуйте еще раз.",
                                               parse_mode=ParseMode.HTML)
            return

        elif OTMENIT_VENCHANIE_REGEX.match(message_text_lower):
            is_eligible, reason = await check_command_eligibility(update, context)
            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            initiator_id = user.id
            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, initiator_id)
            if not initiator_info:
                initiator_info = {"user_id": initiator_id, "first_name": user.first_name, "username": user.username}
            initiator_display_name = get_marriage_user_display_name(initiator_info)
            initiator_mention = mention_html(initiator_id, initiator_display_name)

            target_user_id: Optional[int] = None
            target_user_data: Optional[dict] = None

            match = OTMENIT_VENCHANIE_REGEX.match(message_text_lower)
            username_from_args = match.group(2) if match else None

            if update.message.reply_to_message and update.message.reply_to_message.from_user:
                replied_user = update.message.reply_to_message.from_user
                if replied_user.is_bot:
                    await context.bot.send_message(chat_id=chat_id, text="👾 Нельзя отменить предложение боту!")
                    return
                if replied_user.id == user.id:
                    await context.bot.send_message(chat_id=chat_id,
                                                   text="👾 Вы не можете отменить предложение самому себе!")
                    return
                target_user_id = replied_user.id
                target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_id, target_user_id)
                if target_user_data_from_db:
                    target_user_data = target_user_data_from_db
                else:
                    target_user_data = {"user_id": replied_user.id, "first_name": replied_user.first_name,
                                        "username": replied_user.username}
                    await asyncio.to_thread(save_marriage_user_data, replied_user, from_group_chat=True)

            elif username_from_args:
                target_username = username_from_args.lstrip('@')
                target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_username, target_username)
                if target_user_data_from_db:
                    target_user_id = target_user_data_from_db['user_id']
                    target_user_data = target_user_data_from_db
                else:
                    await context.bot.send_message(chat_id=chat_id,
                                                   text=f"👾 Пользователь '@{target_username}' не найден в базе данных бота. Убедитесь, что он писал сообщения в группе.",
                                                   parse_mode=ParseMode.HTML)
                    return
            else:
                await context.bot.send_message(chat_id=chat_id,
                                               text="👾 Чтобы отменить предложение, ответьте на сообщение пользователя или укажите его `@username` (например: `Отменить венчание @username`).",
                                               parse_mode=ParseMode.HTML)
                return

            if not target_user_id or not target_user_data:
                await context.bot.send_message(chat_id=chat_id,
                                               text="👾 Не удалось определить пользователя, которому вы хотите отменить предложение. "
                                                    "Возможно, его нет в базе данных бота или вы указали неверно.",
                                               parse_mode=ParseMode.HTML)
                return

            target_display_name = get_marriage_user_display_name(target_user_data)
            target_mention = mention_html(target_user_id, target_display_name)

            proposal_to_cancel = await asyncio.to_thread(get_initiator_pending_proposal, initiator_id, target_user_id)

            if not proposal_to_cancel:
                await context.bot.send_message(chat_id=chat_id,
                                               text=f"👾 Вы не отправляли предложение венчаться {target_mention}, которое можно отменить. Или оно уже было принято/отклонено.",
                                               parse_mode=ParseMode.HTML)
                return

            cancelled_proposal = await asyncio.to_thread(cancel_marriage_proposal_db, initiator_id, target_user_id)

            if cancelled_proposal:
                await update.message.reply_text(
                    f"💔 Вы отменили свое предложение венчаться пользователю {target_mention}.",
                    parse_mode=ParseMode.HTML)

                private_msg_id = cancelled_proposal.get('private_message_id')
                if private_msg_id:
                    try:
                        await context.bot.edit_message_text(
                            chat_id=target_user_id,
                            message_id=private_msg_id,
                            text=f"💔 Предложение венчаться от {initiator_mention} было отменено.",
                            reply_markup=None,
                            parse_mode=ParseMode.HTML
                        )
                    except BadRequest as e:
                        logger.warning(
                            f"Не удалось отредактировать личное сообщение {target_user_id} об отмене предложения: {e}. Пытаемся отправить новое.",
                            exc_info=True)
                        try:
                            await context.bot.send_message(
                                chat_id=target_user_id,
                                text=f"💔 Предложение венчаться от {initiator_mention} было отменено.",
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e_new:
                            logger.error(f"Не удалось уведомить {target_user_id} об отмене предложения: {e_new}",
                                         exc_info=True)
                    except Exception as e:
                        logger.error(f"Общая ошибка при редактировании сообщения {target_user_id} об отмене: {e}",
                                     exc_info=True)
            else:
                await context.bot.send_message(chat_id=chat_id,
                                               text="Произошла ошибка при отмене предложения. Пожалуйста, попробуйте еще раз.",
                                               parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "бракосочетания":
            is_eligible, reason = await check_command_eligibility(update, context)

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
                initiator_display_name = get_marriage_user_display_name({
                    "user_id": marriage['initiator_id'],
                    "first_name": marriage['initiator_first_name'],
                    "username": marriage['initiator_username']
                })
                target_display_name = get_marriage_user_display_name({
                    "user_id": marriage['target_id'],
                    "first_name": marriage['target_first_name'],
                    "username": marriage['target_username']
                })

                p1_mention = mention_html(marriage['initiator_id'], initiator_display_name)
                p2_mention = mention_html(marriage['target_id'], target_display_name)

                start_date = marriage['prev_accepted_at'] if marriage['prev_accepted_at'] else marriage['accepted_at']
                duration = await format_duration(start_date)
                start_date_formatted = start_date.strftime('%d.%m.%Y')

                response_text += (
                    f"- {p1_mention} и {p2_mention} "
                    f"(с {start_date_formatted}, {duration})\n"
                )
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "мой брак":
            is_eligible, reason = await check_command_eligibility(update, context)

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

            start_date = marriage['prev_accepted_at'] if marriage['prev_accepted_at'] else marriage['accepted_at']
            duration = await format_duration(start_date)
            start_date_formatted = start_date.strftime('%d.%m.%Y')

            response_text = (
                f"💍 Вы состоите в браке с {partner_mention} 💞\n\n"
                f"📆 Дата бракосочетания: {start_date_formatted} ({duration})."
            )
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "развестись":
            is_eligible, reason = await check_command_eligibility(update, context)

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
                f"💔 Вы действительно хотите развестись с {partner_mention}? \nПосле развода у вас будет {REUNION_PERIOD_DAYS} дня на повторное венчание без потери длительности брака.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            return

        elif message_text_lower == "предложения":
            is_eligible, reason = await check_command_eligibility(update, context)

            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            pending_proposals = await asyncio.to_thread(get_target_pending_proposals, user.id)

            if not pending_proposals:
                await update.message.reply_text("У вас нет активных предложений о венчании.", parse_mode=ParseMode.HTML)
                return

            response_text_parts = ["💌 <b>Входящие предложения о венчании:</b>\n\n"]
            for proposal in pending_proposals:
                initiator_id = proposal['initiator_id']
                initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, initiator_id)
                initiator_mention = mention_html(initiator_id, get_marriage_user_display_name(initiator_info))

                response_text_for_one_proposal = (
                    f"От: {initiator_mention} (отправлено {proposal['created_at'].strftime('%d.%m.%Y %H:%M')})\n"
                )
                keyboard = [
                    [InlineKeyboardButton("✅ Принять", callback_data=f"marry_yes_{initiator_id}_{user.id}")],
                    [InlineKeyboardButton("❌ Отклонить", callback_data=f"marry_no_{initiator_id}_{user.id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(response_text_for_one_proposal, reply_markup=reply_markup,
                                                parse_mode=ParseMode.HTML)

            await update.message.reply_text("Все активные предложения также обновлены в личных сообщениях.")
            await _resend_pending_proposals_to_target(user.id,
                                                      context)  # Обновляем/переотправляем сообщения с предложениями в личку, чтобы они были актуальными
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
            chat_url = GROUP_CHAT_INVITE_LINK if GROUP_CHAT_INVITE_LINK else f'https://t.me/{GROUP_USERNAME_PLAIN}'
            keyboard = [
                [InlineKeyboardButton(f'Вступить в чат 💬', url=chat_url)],
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

<b>💍 Венчания:</b>
<code>Венчаться @username</code> ( или в ответ на сообщение) - Предложить пользователю обвенчаться и обьеденить ваши сердца в одно.
<code>Отменить венчание @username</code> ( или в ответ на сообщение) - Отменить ваше исходящее предложение о венчании.
<code>предложения</code> - Посмотреть все входящие предложения о венчаниях.
<code>Бракосочетания</code> - Посмотреть список всех активных браков.
<code>Мой брак</code> - Узнать статус своего брака.
<code>Развестись</code> - Запросить развод (с подтверждением).

<b>📜 "Евангелие":</b>
<code>Найти Евангелие</code> - Начать игру и найти Евангелие.
<code>Мольба</code> - Молиться и увеличить набожность (доступно раз в час, возможна бесноватость).
<code>Евангелие</code> - Посмотреть свои текущие показатели молитв и набожности.
<code>Топ Евангелий</code> - Просмотреть рейтинг самых набожных и молящихся игроков.

<b>❤️‍🔥 LOVE IS...:</b>
<code>Лав иска</code> - Получить карточку loveisку.
<code>Моя коллекция</code> - Просмотреть свою коллекцию, жетоны и фрагменты.

<b>💬 Общие Команды:</b>
<code>/start</code> - Начало работы с ботом, приветствие.
<code>Иссуе</code> - Показать основную информацию о боте и кнопки.
<code>Моя инфа</code> - Показать ваш ID.
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
        action_type = parts[0]  # marry or divorce
        action = parts[1]  # yes/no or confirm/cancel
        user1_id = int(parts[2])  # initiator_id for marry, current_user_id for divorce
        user2_id = int(parts[3])  # target_id for marry, partner_id for divorce

        if action_type == "marry":
            if current_user_id != user2_id:
                try:
                    await query.edit_message_text(text="Это предложение адресовано не вам!")
                except BadRequest:
                    await query.message.reply_text("Это предложение адресовано не вам!")
                return

            is_eligible, reason = await check_command_eligibility(update, context)

            if not is_eligible:
                try:
                    await query.edit_message_text(
                        text=f"Вы не соответствуете условиям для принятия/отклонения предложения: {reason}",
                        parse_mode=ParseMode.HTML)
                except BadRequest:
                    await query.message.reply_text(
                        f"Вы не соответствуете условиям для принятия/отклонения предложения: {reason}",
                        parse_mode=ParseMode.HTML)
                return

            proposal = await asyncio.to_thread(get_pending_marriage_proposal, user1_id, user2_id)

            if not proposal:
                try:
                    await query.edit_message_text(text="Это предложение уже неактивно или истекло.")
                except BadRequest:
                    await query.message.reply_text("Это предложение уже неактивно или истекло.")
                return

            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, user1_id)
            target_info = await asyncio.to_thread(get_marriage_user_data_by_id, user2_id)

            if not initiator_info or not target_info:
                try:
                    await query.edit_message_text(text="Не удалось получить данные о пользователях.")
                except BadRequest:
                    await query.message.reply_text("Не удалось получить данные о пользователях.")
                return

            initiator_display_name = get_marriage_user_display_name(initiator_info)
            target_display_name = get_marriage_user_display_name(target_info)

            initiator_mention = mention_html(user1_id, initiator_display_name)
            target_mention = mention_html(user2_id, target_display_name)

            if action == "yes":
                if await asyncio.to_thread(get_active_marriage, user1_id) or \
                        await asyncio.to_thread(get_active_marriage, user2_id):
                    try:
                        await query.edit_message_text(text="К сожалению, один из вас уже состоит в браке.",
                                                      parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.message.reply_text("К сожалению, один из вас уже состоит в браке.",
                                                       parse_mode=ParseMode.HTML)
                    await asyncio.to_thread(reject_marriage_proposal_db, proposal['id'])  # Reject to clear state
                    return

                if await asyncio.to_thread(accept_marriage_proposal_db, proposal['id'], user1_id, user2_id):
                    try:
                        await query.edit_message_text(text=f"Вы успешно венчались с {initiator_mention}!",
                                                      parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.message.reply_text(text=f"Вы успешно венчались с {initiator_mention}!",
                                                       parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(
                            chat_id=proposal['chat_id'],
                            text=f"{target_mention} и {initiator_mention} успешно венчались!",
                            parse_mode=ParseMode.HTML
                        )
                        # Уведомляем инициатора
                        await context.bot.send_message(
                            chat_id=user1_id,
                            text=f"💍 Ваше предложение венчаться с {target_mention} было принято!",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.warning(
                            f"💔 Не удалось отправить уведомление о браке в чат {proposal['chat_id']} или инициатору {user1_id}: {e}",
                            exc_info=True)
                else:
                    try:
                        await query.edit_message_text(
                            text="💔 Произошла ошибка при принятии предложения. Пожалуйста, попробуйте еще раз.",
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.message.reply_text(
                            text="💔 Произошла ошибка при принятии предложения. Пожалуйста, попробуйте еще раз.",
                            parse_mode=ParseMode.HTML)
            elif action == "no":
                if await asyncio.to_thread(reject_marriage_proposal_db, proposal['id']):
                    try:
                        await query.edit_message_text(
                            text=f"💔 Вы отклонили предложение венчаться от {initiator_mention}.",
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.message.reply_text(
                            text=f"💔 Вы отклонили предложение венчаться от {initiator_mention}.",
                            parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(
                            chat_id=user1_id,
                            text=f"💔 {target_mention} отклонил(а) ваше предложение венчаться.",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.warning(f"💔 Не удалось отправить уведомление об отклонении инициатору {user1_id}: {e}",
                                       exc_info=True)
                else:
                    try:
                        await query.edit_message_text(
                            text="💔 Произошла ошибка при отклонении предложения. Пожалуйста, попробуйте еще раз.",
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.message.reply_text(
                            text="💔 Произошла ошибка при отклонении предложения. Пожалуйста, попробуйте еще раз.",
                            parse_mode=ParseMode.HTML)

        elif action_type == "divorce":
            if current_user_id != user1_id:
                try:
                    await query.edit_message_text(text="Не суй свой носик в чужие дела!")
                except BadRequest:
                    await query.message.reply_text("Не суй свой носик в чужие дела!")
                return

            partner_id = user2_id

            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, current_user_id)
            partner_info = await asyncio.to_thread(get_marriage_user_data_by_id, partner_id)

            if not initiator_info or not partner_info:
                try:
                    await query.edit_message_text(text="Не удалось получить данные о пользователях.")
                except BadRequest:
                    await query.message.reply_text("Не удалось получить данные о пользователях.")
                return

            initiator_display_name = get_marriage_user_display_name(initiator_info)
            partner_display_name = get_marriage_user_display_name(partner_info)

            initiator_mention = mention_html(current_user_id, initiator_display_name)
            partner_mention = mention_html(partner_id, partner_display_name)

            if action == "confirm":
                divorced_partners = await asyncio.to_thread(divorce_user_db_confirm, current_user_id)

                if divorced_partners:
                    try:
                        await query.edit_message_text(
                            text=f"💔 Вы развелись с {partner_mention}. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери длительности брака.",
                            parse_mode=ParseMode.HTML
                        )
                    except BadRequest:
                        await query.message.reply_text(
                            text=f"💔 Вы развелись с {partner_mention}. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери длительности брака.",
                            parse_mode=ParseMode.HTML
                        )
                    try:
                        await context.bot.send_message(
                            chat_id=partner_id,
                            text=f"💔 Ваш брак с {initiator_mention} был расторгнут. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери длительности брака.",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.warning(f"💔 Не удалось уведомить партнера {partner_id} о разводе: {e}", exc_info=True)
                else:
                    try:
                        await query.edit_message_text(
                            text="❤️‍🩹 Произошла ошибка при попытке развода. Пожалуйста, попробуйте еще раз",
                            parse_mode=ParseMode.HTML
                        )
                    except BadRequest:
                        await query.message.reply_text(
                            text="❤️‍🩹 Произошла ошибка при попытке развода. Пожалуйста, попробуйте еще раз",
                            parse_mode=ParseMode.HTML
                        )
            elif action == "cancel":
                try:
                    await query.edit_message_text(text="❤️‍🩹 Развод отменен", parse_mode=ParseMode.HTML)
                except BadRequest:
                    await query.message.reply_text(text="❤️‍🩹 Развод отменен", parse_mode=ParseMode.HTML)


    elif query.data == "show_achievements":
        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        unique_count = len(user_data.get("cards", {}))
        achieved_ids = set(user_data.get("achievements", []))

        lines = ["🏆 Доступные достижения: \n"]
        for ach in ACHIEVEMENTS:
            if ach["id"] in achieved_ids:
                lines.append(
                    f"✅ {ach['name']} — получено ({ach['reward']['amount']} {('жетонов' if ach['reward']['type'] == 'spins' else 'фрагментов')})")
            else:
                # прогресс: unique_count / threshold
                lines.append(f"🃏 ▎ {ach['name']} — {unique_count}/{ach['threshold']}\n")

        lines.append("✨ Так держать! Не останавливайся! Кто знает, может в будущем это пригодится…")
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")]])
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption="\n".join(lines)),
                reply_markup=reply_markup
            )
        except BadRequest as e:
            logger.warning(
                f"Failed to show achievements media (likely old message or user blocked bot): {e}. Sending new message.",
                exc_info=True)
            try:
                await query.bot.send_photo(
                    chat_id=query.from_user.id,
                    photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                    caption="\n".join(lines),
                    reply_markup=reply_markup
                )
            except Exception as new_send_e:
                logger.error(f"Failed to send new photo for achievements after edit failure: {new_send_e}",
                             exc_info=True)
                await query.message.reply_text(
                    "Произошла ошибка при показе достижений. Пожалуйста, попробуйте снова."
                )
        except Exception as e:
            logger.error(f"Failed to show achievements media with unexpected error: {e}", exc_info=True)
            await query.message.reply_text(
                "Произошла ошибка при показе достижений. Пожалуйста, попробуйте снова."
            )

    # --- Обработка кнопок Лависки ---
    elif query.data == "show_collection":
        user_data_laviska = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        owned_card_ids = sorted([int(cid) for cid in user_data_laviska["cards"].keys()])
        if not owned_card_ids:
            keyboard = [[InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")],
                        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements")],
                        [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message_text = (
                f"🪪 Пользователь: @{current_user_username}\n\n"
                f"🧧 Жетоны: {user_data_laviska['spins']}\n"
                f"🧩 Фрагменты: {user_data_laviska['crystals']}\n\n"
                f"У вас пока нет ни одной карточки LOVE IS..! Используйте 'лав иска', чтобы получить первую"
            )
            try:
                await query.edit_message_media(
                    media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text),
                    reply_markup=reply_markup
                )
            except BadRequest as e:
                logger.warning(
                    f"Failed to edit message media for empty collection view (likely old message or user blocked bot), sending new photo: {e}",
                    exc_info=True)
                try:
                    await query.bot.send_photo(
                        chat_id=query.from_user.id,
                        photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                        caption=message_text,
                        reply_markup=reply_markup
                    )
                except Exception as new_send_e:
                    logger.error(f"Failed to send new photo for empty collection view after edit failure: {new_send_e}",
                                 exc_info=True)
                    await query.message.reply_text(
                        "Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз."
                    )
            except Exception as e:
                logger.error(f"Failed to edit message media for empty collection view with unexpected error: {e}",
                             exc_info=True)
                await query.message.reply_text(
                    "Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз."
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
            [InlineKeyboardButton(f"Обменять {SPIN_COST} 🧩 на жетон",
                                  callback_data="exchange_crystals_for_spin")],
            [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text_for_buy_spins = (
            f"🧧 Стоимость: {SPIN_COST} 🧩\n\n"
            f"У вас  {user_data['crystals']} 🧩 фрагментов."
        )
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text_for_buy_spins),
                reply_markup=reply_markup
            )
        except BadRequest as e:
            logger.warning(
                f"Failed to edit message media for buy_spins (likely old message or user blocked bot), sending new photo: {e}",
                exc_info=True)
            try:
                await query.bot.send_photo(
                    chat_id=query.from_user.id,
                    photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                    caption=message_text_for_buy_spins,
                    reply_markup=reply_markup
                )
            except Exception as new_send_e:
                logger.error(f"Failed to send new photo for buy_spins after edit failure: {new_send_e}", exc_info=True)
                await query.message.reply_text(
                    "Произошла ошибка при попытке обмена. Пожалуйста, попробуйте еще раз."
                )
        except Exception as e:
            logger.error(f"Failed to edit message media for buy_spins with unexpected error: {e}", exc_info=True)
            await query.message.reply_text(
                "Произошла ошибка при попытке обмена. Пожалуйста, попробуйте еще раз."
            )

    elif query.data == "exchange_crystals_for_spin":
        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        if user_data["crystals"] >= SPIN_COST:
            user_data["crystals"] -= SPIN_COST
            user_data["spins"] += 1
            await asyncio.to_thread(update_user_data, current_user_id, user_data)

            keyboard = [
                [InlineKeyboardButton(f"Обменять {SPIN_COST} 🧩 на жетон",
                                      callback_data="exchange_crystals_for_spin")],
                [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message_text_success = (
                f"🧧 Вы успешно купили жетон! Теперь у вас {user_data['spins']} жетонов и {user_data['crystals']} фрагментов!"
            )
            try:
                await query.edit_message_media(
                    media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text_success),
                    reply_markup=reply_markup
                )
            except BadRequest as e:
                logger.warning(
                    f"Failed to edit message media for exchange_crystals_for_spin success (likely old message or user blocked bot), sending new photo: {e}",
                    exc_info=True)
                try:
                    await query.bot.send_photo(
                        chat_id=query.from_user.id,
                        photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                        caption=message_text_success,
                        reply_markup=reply_markup
                    )
                except Exception as new_send_e:
                    logger.error(
                        f"Failed to send new photo for exchange_crystals_for_spin success after edit failure: {new_send_e}",
                        exc_info=True)
                    await query.message.reply_text(
                        "Произошла ошибка при обновлении баланса. Пожалуйста, попробуйте еще раз."
                    )
            except Exception as e:
                logger.error(
                    f"Failed to edit message media for exchange_crystals_for_spin success with unexpected error: {e}",
                    exc_info=True)
                await query.message.reply_text(
                    "Произошла ошибка при обновлении баланса. Пожалуйста, попробуйте еще раз."
                )
        else:
            await query.answer("Недостаточно фрагментов для покупки жетона!", show_alert=True)

            user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
            keyboard = [
                [InlineKeyboardButton(f"Обменять {SPIN_COST} 🧩 на жетон",
                                      callback_data="exchange_crystals_for_spin")],
                [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message_text_fail = (
                f"🧩 У вас {user_data['crystals']} фрагментов\n"
                f"Стоимость одного жетона: {SPIN_COST} 🧩.\n\n"
                f"Недостаточно фрагментов для покупки жетона!"
            )
            try:
                await query.edit_message_media(
                    media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text_fail),
                    reply_markup=reply_markup
                )
            except BadRequest as e:
                logger.warning(
                    f"Failed to edit message media for exchange_crystals_for_spin fail (likely old message or user blocked bot), sending new photo: {e}",
                    exc_info=True)
                try:
                    await query.bot.send_photo(
                        chat_id=query.from_user.id,
                        photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                        caption=message_text_fail,
                        reply_markup=reply_markup
                    )
                except Exception as new_send_e:
                    logger.error(
                        f"Failed to send new photo for exchange_crystals_for_spin fail after edit failure: {new_send_e}",
                        exc_info=True)
                    await query.message.reply_text(
                        "Произошла ошибка при обновлении баланса. Пожалуйста, попробуйте еще раз."
                    )
            except Exception as e:
                logger.error(
                    f"Failed to edit message media for exchange_crystals_for_spin fail with unexpected error: {e}",
                    exc_info=True)
                await query.message.reply_text(
                    "Произошла ошибка при обновлении баланса. Пожалуйста, попробуйте еще раз."
                )

    # --- Обработка кнопок Игрового Бота "Евангелие" ---
    elif data == 'send_papa':
        try:
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
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения 'send_papa': {e}", exc_info=True)
            await query.message.reply_text("Произошла ошибка. Пожалуйста, попробуйте снова.")
    elif data == 'show_commands':
        await send_command_list(update, context)
    elif data.startswith('gospel_top_'):
        parts = data.split('_')
        view = parts[2]
        page = int(parts[4]) if len(parts) > 4 else 1

        message_text, reply_markup = await _get_leaderboard_message(context, view, page)
        try:
            await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except BadRequest as e:  # Catch BadRequest specifically
            logger.warning(
                f"Ошибка при обновлении сообщения топа Евангелий (callback, view={view}, page={page}, likely old message or user blocked bot): {e}. Sending new message.",
                exc_info=True)
            try:
                await query.bot.send_message(
                    chat_id=query.from_user.id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
            except Exception as new_send_e:
                logger.error(f"Failed to send new message for gospel top after edit failure: {new_send_e}",
                             exc_info=True)
                await query.message.reply_text(
                    "Произошла ошибка при обновлении топа. Пожалуйста, попробуйте еще раз.",
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            logger.error(
                f"Ошибка при обновлении сообщения топа Евангелий (callback, view={view}, page={page}) с неожиданной ошибкой: {e}",
                exc_info=True)
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
        from_group = (chat_id == GROUP_CHAT_ID or (AQUATORIA_CHAT_ID and chat_id == AQUATORIA_CHAT_ID))
        await asyncio.to_thread(save_marriage_user_data, user, from_group_chat=from_group)
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f'Update "{update}" вызвал ошибку "{context.error}"', exc_info=True)
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Произошла ошибка! Пожалуйста, попробуйте еще раз или свяжитесь с администратором.",
                parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке пользователю: {e}", exc_info=True)


def main():
    init_db()  # Единая функция инициализации для всех таблиц в PostgreSQL

    application = ApplicationBuilder().token(TOKEN).build()

    # Command Handlers
    application.add_handler(CommandHandler("start", unified_start_command))
    application.add_handler(CommandHandler("get_chat_id", get_chat_id_command))

    # Message Handler for text commands and general messages
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unified_text_message_handler))
    application.add_handler(MessageHandler(filters.PHOTO, get_photo_handler))

    # Handler for any other message type to update user data
    application.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND & ~filters.TEXT & ~filters.PHOTO,
                       process_any_message_for_user_data))

    # Callback Query Handler for all inline buttons
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler))

    application.add_error_handler(error_handler)

    logger.info("Бот запущен. Ожидание сообщений...")
    application.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()





