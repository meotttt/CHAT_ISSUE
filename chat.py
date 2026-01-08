import asyncio
import json
import logging
import os
import random
from psycopg2 import Error
import re
import time
import html
import httpx
import psycopg2
from dateutil import parser as date_parser
from telegram.ext import Application, ApplicationBuilder, CallbackContext, CommandHandler, ContextTypes, filters, \
    MessageHandler, CallbackQueryHandler, PreCheckoutQueryHandler
from telegram import Update, User, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, ChatPermissions, \
    Message, LabeledPrice
from telegram.constants import ChatAction, ParseMode
from datetime import datetime, timezone, timedelta
from collections import defaultdict, OrderedDict
from typing import Optional, Tuple, List, Dict
from telegram.helpers import mention_html
from psycopg2.extras import DictCursor
from telegram.error import BadRequest
from functools import wraps, partial
from dotenv import load_dotenv
import uuid
import urllib.parse


_CALLBACK_LAST_TS: Dict[Tuple[int, str], float] = {}
DEBOUNCE_SECONDS = 2
load_dotenv()  # Эта строка загружает переменные из .env

NOTEBOOK_MENU_CAPTION = (
    "─────── ⋆⋅☆⋅⋆ ───────\n📙Блокнот с картами 📙\n➖➖➖➖➖➖➖➖➖➖\n👤 Профиль: {username}\n🔖 ID: {user_id}\n➖➖➖➖➖➖➖➖➖➖\n🧧 Жетоны: {token_count}\n🧩 Фрагменты: {fragment_count}\n─────── ⋆⋅☆⋅⋆ ───────\n")

NOTEBOOK_MENU_OWNERSHIP: Dict[Tuple[int, int], int] = {}

# --- Общая Конфигурация ---
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не установлен в переменных окружения!")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения!")
COLLECTIONS_PER_PAGE = 5
# Получаем ID чатов и админа из переменных окружения с дефолтными значениями
GROUP_CHAT_ID: int = int(os.environ.get("GROUP_CHAT_ID", "-1002372051836"))  # Основной ID вашей группы
AQUATORIA_CHAT_ID: Optional[int] = int(
    os.environ.get("AQUATORIA_CHAT_ID", "-1003405511585"))  # ID другой группы, если есть
ADMIN_ID = os.environ.get('ADMIN_ID', '2123680656')  # ID администратора

# --- НОВЫЕ ПЕРЕМЕННЫЕ ДЛЯ КАНАЛА ---
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "EXCLUSIVE_SUNRISE")
CHAT_USERNAME = os.getenv("CHAT_USERNAME", "CHAT_SUNRISE")
CHANNEL_ID = f"@{CHANNEL_USERNAME}"
CHAT_ID = f"@{CHAT_USERNAME}"
GROUP_USERNAME_PLAIN = os.environ.get("GROUP_USERNAME_PLAIN", "CHAT_SUNRISE")
GROUP_CHAT_INVITE_LINK = os.environ.get("GROUP_CHAT_INVITE_LINK")
PHOTO_BASE_PATH = "."  # Относительный путь к папке с фотографиями
NUM_PHOTOS = 74
COOLDOWN_SECONDS = 10800  # Задержка между командами "лав иска"
SPIN_COST = 200  # Стоимость крутки в кристаллах
SPIN_USED_COOLDOWN = 600  # 10 минут
REPEAT_CRYSTALS_BONUS = 80  # Кристаллы за повторную карточку
COLLECTION_MENU_IMAGE_PATH = os.path.join(PHOTO_BASE_PATH, "photo_2025-12-17_17-01-44.jpg")
NOTEBOOK_MENU_IMAGE_PATH = os.path.join(PHOTO_BASE_PATH, "photo_2025-12-17_17-03-14.jpg")
REUNION_PERIOD_DAYS = 3  # Количество дней для льготного периода после развода
CACHED_CHANNEL_ID = None
CACHED_GROUP_ID = None
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK")  # Добавил переменную для инвайт-линка канала
NOTEBOOK_MENU_OWNERSHIP: Dict[Tuple[int, int], int] = {}
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
LIFETIME_PREMIUM_USER_IDS = {2123680656}
ADMIN_ID = 123456789  # Ваш ID
DEFAULT_PROFILE_IMAGE = r"C:\Users\anana\PycharmProjects\PythonProject2\images\d41aeb3c-2496-47f7-8a8c-11bcddcbc0c4.png"

LAV_ISKA_REGEX = re.compile(r"^(лав иска)$", re.IGNORECASE)
MY_COLLECTION_REGEX = re.compile(r"^(блокнот)$", re.IGNORECASE)
VENCHATSYA_REGEX = re.compile(r"^(венчаться)(?:\s+@?(\w+))?$", re.IGNORECASE)  # Adjusted regex
OTMENIT_VENCHANIE_REGEX = re.compile(r"^(отменить венчание)(?:\s+@?(\w+))?$", re.IGNORECASE)

ACHIEVEMENTS = [{"id": "ach_10", "name": "1. «Новичок»\nСобрал 10 уникальных карточек", "threshold": 10,
                 "reward": {"type": "spins", "amount": 5}},
                {"id": "ach_25", "name": "2. «Любитель»\nСобрал 25 уникальных карточек", "threshold": 25,
                 "reward": {"type": "spins", "amount": 5}},
                {"id": "ach_50", "name": "3. «Мастер»\nСобрал 50 уникальных карточек", "threshold": 50,
                 "reward": {"type": "spins", "amount": 10}},
                {"id": "ach_all", "name": "4. «Гуру»\nСобрал 74 уникальных карточек", "threshold": NUM_PHOTOS,
                 "reward": {"type": "crystals", "amount": 1000}}, ]


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

def format_first_card_date_iso(iso_str: Optional[str]) -> str:
    if not iso_str:
        return "—"
    try:
        try:
            dt = date_parser.parse(iso_str)
        except Exception:
            dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "—"

photo_counter = 0
PHOTO_DETAILS = {
    1: {"path": os.path.join(PHOTO_BASE_PATH, "1 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nрай!\n\n🔖…1!"},
    2: {"path": os.path.join(PHOTO_BASE_PATH, "2 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда вместе!\n\n🔖…2! "},
    3: {"path": os.path.join(PHOTO_BASE_PATH, "3 — копия.jpg"),
        "caption": "❤️‍🔥 LOVE IS…\nуметь переглядываться!\n\n🔖…3! "},
    4: {"path": os.path.join(PHOTO_BASE_PATH, "4 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nбыть на коне!\n\n🔖…4! "},
    5: {"path": os.path.join(PHOTO_BASE_PATH, "5 — копия.jpg"),
        "caption": "❤️‍🔥 LOVE IS…\nпочувствовать легкое головокружение!\n\n🔖…5! "},
    6: {"path": os.path.join(PHOTO_BASE_PATH, "6 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nобнимашки!\n\n🔖…6! "},
    7: {"path": os.path.join(PHOTO_BASE_PATH, "7 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nне только сахар!\n\n🔖…7! "},
    8: {"path": os.path.join(PHOTO_BASE_PATH, "8 — копия.jpg"),
        "caption": "❤️‍🔥 LOVE IS…\nпонимать друг друга без слов!\n\n🔖…8! "},
    9: {"path": os.path.join(PHOTO_BASE_PATH, "9 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nуметь успокоить!\n\n🔖…9! "},
    10: {"path": os.path.join(PHOTO_BASE_PATH, "10 — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nсуметь удержаться!\n\n🔖…10! "},
    11: {"path": os.path.join(PHOTO_BASE_PATH, "11 — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nне дать себя запутать!\n\n🔖…11! "},
    12: {"path": os.path.join(PHOTO_BASE_PATH, "12 — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nсуметь сохранить секретик!\n\n🔖…12! "},
    13: {"path": os.path.join(PHOTO_BASE_PATH, "13 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпод прикрытием\n\n🔖…13! "},
    14: {"path": os.path.join(PHOTO_BASE_PATH, "14 — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nкогда нам по пути!\n\n🔖…14! "},
    15: {"path": os.path.join(PHOTO_BASE_PATH, "15 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nпрорыв.\n\n🔖…15! "},
    16: {"path": os.path.join(PHOTO_BASE_PATH, "16 — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nзагадывать желание\n\n🔖…16!  "},
    17: {"path": os.path.join(PHOTO_BASE_PATH, "17 — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nлето круглый год!\n\n🔖…17! "},
    18: {"path": os.path.join(PHOTO_BASE_PATH, "18 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nромантика!\n\n🔖…18! "},
    19: {"path": os.path.join(PHOTO_BASE_PATH, "19 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nкогда жарко!\n\n🔖…19! "},
    20: {"path": os.path.join(PHOTO_BASE_PATH, "20 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nраскрываться!\n\n🔖…20! "},
    21: {"path": os.path.join(PHOTO_BASE_PATH, "21 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nвыполнять обещания\n\n🔖…21! "},
    22: {"path": os.path.join(PHOTO_BASE_PATH, "22 — копия.jpg"), "caption": "❤️‍🔥 LOVE IS…\nцирк вдвоем!\n\n🔖…22! "},
    23: {"path": os.path.join(PHOTO_BASE_PATH, "23 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nслышать друг друга!\n\n🔖…23! "},
    24: {"path": os.path.join(PHOTO_BASE_PATH, "24 — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсладость\n\n🔖…24! "},
    25: {"path": os.path.join(PHOTO_BASE_PATH, "25 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nне упустить волну!\n\n🔖…25! "},
    26: {"path": os.path.join(PHOTO_BASE_PATH, "26 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nсказать о важном!\n\n🔖…26! "},
    27: {"path": os.path.join(PHOTO_BASE_PATH, "27 — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nискриться!\n\n🔖…27! "},
    28: {"path": os.path.join(PHOTO_BASE_PATH, "28 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nтолько мы вдвоём\n\n🔖…28! "},
    29: {"path": os.path.join(PHOTO_BASE_PATH, "29 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпервое прикосновение\n\n🔖…29! "},
    30: {"path": os.path.join(PHOTO_BASE_PATH, "30 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nвзять дело в свои руки\n\n🔖…30! "},
    31: {"path": os.path.join(PHOTO_BASE_PATH, "31 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда не важно какая погода\n\n🔖…31! "},
    32: {"path": os.path.join(PHOTO_BASE_PATH, "32 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nуметь прощать!\n\n🔖…32! "},
    33: {"path": os.path.join(PHOTO_BASE_PATH, "33 — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nотметиться!\n\n🔖…33! "},
    34: {"path": os.path.join(PHOTO_BASE_PATH, "34 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпервый поцелуй\n\n🔖…34!"},
    35: {"path": os.path.join(PHOTO_BASE_PATH, "35 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда без интернета! \n\n🔖…35!"},
    36: {"path": os.path.join(PHOTO_BASE_PATH, "36 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nлегкое головокружение\n\n🔖…36!"},
    37: {"path": os.path.join(PHOTO_BASE_PATH, "37 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпозвонить просто так\n\n🔖…37!"},
    38: {"path": os.path.join(PHOTO_BASE_PATH, "38 — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nвсё что нужно\n\n🔖…38!"},
    39: {"path": os.path.join(PHOTO_BASE_PATH, "39 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nто, что создаёшь ты\n\n🔖…39!"},
    40: {"path": os.path.join(PHOTO_BASE_PATH, "40 — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nсвобода\n\n🔖…40!"},
    41: {"path": os.path.join(PHOTO_BASE_PATH, "41 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда пробежала искра!\n\n🔖…41!"},
    42: {"path": os.path.join(PHOTO_BASE_PATH, "42 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nизображать недотрогу \n\n🔖…42!"},
    43: {"path": os.path.join(PHOTO_BASE_PATH, "43 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nсварить ему борщ)\n\n🔖…43!"},
    44: {"path": os.path.join(PHOTO_BASE_PATH, "44 — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпотрясать мир \n\n🔖…44!"},
    45: {"path": os.path.join(PHOTO_BASE_PATH, "45. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда он не ангел!\n\n🔖…45!"},
    46: {"path": os.path.join(PHOTO_BASE_PATH, "46. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпритягивать разных!\n\n🔖…46!"},
    47: {"path": os.path.join(PHOTO_BASE_PATH, "47. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nтепло внутри, когда холодно снаружи \n\n🔖…47!"},
    48: {"path": os.path.join(PHOTO_BASE_PATH, "48. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nделать покупки друг друга\n\n🔖…48!"},
    49: {"path": os.path.join(PHOTO_BASE_PATH, "49. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nнемного колкости\n\n🔖…49!"},
    50: {"path": os.path.join(PHOTO_BASE_PATH, "50. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда тянет магнитом \n\n🔖…50!"},
    51: {"path": os.path.join(PHOTO_BASE_PATH, "51. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nбыть на седьмом небе!\n\n🔖…51!"},
    52: {"path": os.path.join(PHOTO_BASE_PATH, "52. — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nты и я\n\n🔖…52!"},
    53: {"path": os.path.join(PHOTO_BASE_PATH, "53. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда купил самое необходимое!\n\n🔖…53!"},
    54: {"path": os.path.join(PHOTO_BASE_PATH, "54. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкак первый день весны!\n\n🔖…54!"},
    55: {"path": os.path.join(PHOTO_BASE_PATH, "55. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпоздравить первым!\n\n🔖…55!"},
    56: {"path": os.path.join(PHOTO_BASE_PATH, "56. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nоставить след!\n\n🔖…56!"},
    57: {"path": os.path.join(PHOTO_BASE_PATH, "57. — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nмикс чувств!\n\n🔖…57!"},
    58: {"path": os.path.join(PHOTO_BASE_PATH, "58. — копия.jpg"),
         "caption": "❤️‍🔥 LOVE IS…\nслучайные порывы!\n\n🔖…58!"},
    59: {"path": os.path.join(PHOTO_BASE_PATH, "59. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда мысли сходятся!\n\n🔖…59!"},
    60: {"path": os.path.join(PHOTO_BASE_PATH, "60. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпосильная ноша!\n\n🔖…60!"},
    61: {"path": os.path.join(PHOTO_BASE_PATH, "61. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nвыбрать свое сердце!\n\n🔖…61!"},
    62: {"path": os.path.join(PHOTO_BASE_PATH, "62. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nто, что требует заботы!\n\n🔖…62!"},
    63: {"path": os.path.join(PHOTO_BASE_PATH, "63. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nбессонные ночи!\n\n🔖…63!"},
    64: {"path": os.path.join(PHOTO_BASE_PATH, "64. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nбыть на вершине мира\n\n🔖…64!"},
    65: {"path": os.path.join(PHOTO_BASE_PATH, "65. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nисправлять ошибки!\n\n🔖…65!"},
    66: {"path": os.path.join(PHOTO_BASE_PATH, "66. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nлюбоваться друг другом!\n\n🔖…66!"},
    67: {"path": os.path.join(PHOTO_BASE_PATH, "67. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nдарить главное!\n\n🔖…67!"},
    68: {"path": os.path.join(PHOTO_BASE_PATH, "68. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nкогда совсем не холодно!\n\n🔖…68!"},
    69: {"path": os.path.join(PHOTO_BASE_PATH, "69. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nдобавить изюминку!\n\n🔖…69!"},
    70: {"path": os.path.join(PHOTO_BASE_PATH, "70. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nснится друг другу!\n\n🔖…70!"},
    71: {"path": os.path.join(PHOTO_BASE_PATH, "71. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nпикник на двоих!\n\n🔖…71!"},
    72: {"path": os.path.join(PHOTO_BASE_PATH, "72. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nдурачиться, как дети\n\n🔖…72!"},
    73: {"path": os.path.join(PHOTO_BASE_PATH, "73. — копия.jpg"), "caption": "️‍❤️‍🔥 LOVE IS…\nдарить себя!\n\n🔖…73!"},
    74: {"path": os.path.join(PHOTO_BASE_PATH, "74. — копия.jpg"),
         "caption": "️‍❤️‍🔥 LOVE IS…\nгорячее сердце!\n\n🔖…74!"},
}

# Генерация заглушек, если PHOTO_DETAILS не заполнен до конца
for i in range(1, NUM_PHOTOS + 1):
    if i not in PHOTO_DETAILS:
        PHOTO_DETAILS[i] = {"path": os.path.join(PHOTO_BASE_PATH, f"{i}.jpg"),
                            "caption": f"Лависка номер {i}. Пока без уникальной подписи."}

RARITY_STATS = {
    "regular card": {"min_bo": 100, "max_bo": 300, "points": 400, "min_diamonds": 1, "max_diamonds": 2},
    "rare card": {"min_bo": 301, "max_bo": 600, "points": 500, "min_diamonds": 2, "max_diamonds": 3},
    "exclusive card": {"min_bo": 601, "max_bo": 900, "points": 800, "min_diamonds": 3, "max_diamonds": 4},
    "epic card": {"min_bo": 901, "max_bo": 1200, "points": 1000, "min_diamonds": 4, "max_diamonds": 5},
    "collectible card": {"min_bo": 901, "max_bo": 1200, "points": 1500, "min_diamonds": 4, "max_diamonds": 5},
    "LIMITED": {"min_bo": 901, "max_bo": 1200, "points": 2500, "min_diamonds": 4, "max_diamonds": 5}}
RARITY_CHANCES = {
    "regular card": 25, "rare card": 20,
    "exclusive card": 19, "epic card": 12,
    "collectible card": 18, "LIMITED": 5}
PREMIUM_RARITY_CHANCES = {"regular card": 12,
                          "rare card": 12, "exclusive card": 25,
                          "epic card": 20, "collectible card": 25, "LIMITED": 10}

CARDS = {
    1: {"name": "Angela", "collection": "KISHIN DENSETSU", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "1.jpg")},
    2: {"name": "Karrie", "collection": "KISHIN DENSETSU", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "2.jpg")},
    3: {"name": "Lancelot", "collection": "KISHIN DENSETSU", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "3.jpg")},
    4: {"name": "Miya", "collection": "ATOMIC POP", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "4.jpg")},
    5: {"name": "Eudora", "collection": "ATOMIC POP", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "5.jpg")},
    6: {"name": "Yin", "collection": "ATTACK ON TITAN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "6.jpg")},
    7: {"name": "Martis", "collection": "ATTACK ON TITAN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "7.jpg")},
    8: {"name": "Fanny", "collection": "ATTACK ON TITAN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "8.jpg")},
    9: {"name": "Balmond", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "9.jpg")},
    10: {"name": "Lylia", "collection": "NEOBEASTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "10.jpg")},
    11: {"name": "Fasha", "collection": "NEOBEASTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "11.jpg")},
    12: {"name": "Ling", "collection": "NEOBEASTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "12.jpg")},
    13: {"name": "Brody", "collection": "NEOBEASTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "13.jpg")},
    14: {"name": "Fredrinn", "collection": "NEOBEASTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "14.jpg")},
    15: {"name": "Hanabi", "collection": "SOUL VESSELS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "15.jpg")},
    16: {"name": "Aamon", "collection": "SOUL VESSELS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "16.jpg")},
    17: {"name": "Hayabusa", "collection": "EXORCIST", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "17.jpg")},
    18: {"name": "Kagura", "collection": "EXORCIST", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "18.jpg")},
    19: {"name": "Granger", "collection": "EXORCIST", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "18.jpg")},
    20: {"name": "Chong", "collection": "EXORCIST", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "20.jpg")},
    21: {"name": "Lesley", "collection": "MYSTIC MEOW", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "21.jpg")},
    22: {"name": "Julian", "collection": "MYSTIC MEOW", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "22.jpg")},
    23: {"name": "Silvanna", "collection": "MYSTIC MEOW", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "23.jpg")},
    24: {"name": "Ling", "collection": "M-WORLD", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "24.jpg")},
    25: {"name": "Wanwan", "collection": "M-WORLD", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "25.jpg")},
    26: {"name": "Yin", "collection": "M-WORLD", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "26.jpg")},
    27: {"name": "Chang'e", "collection": "SANRIO CHARASTERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "27.jpg")},
    28: {"name": "Floryn", "collection": "SANRIO CHARASTERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "28.jpg")},
    29: {"name": "Claude", "collection": "SANRIO CHARASTERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "29.jpg")},
    30: {"name": "Angela", "collection": "SANRIO CHARASTERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "30.jpg")},
    31: {"name": "Xavier", "collection": "CLOUD", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "31.jpg")},
    32: {"name": "Kagura", "collection": "CLOUD", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "32.jpg")},
    33: {"name": "Edith", "collection": "CLOUD", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "33.jpg")},
    34: {"name": "Nana", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "34.jpg")},
    35: {"name": "Dyrroth", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "35.jpg")},
    36: {"name": "Karina", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "36.jpg")},
    37: {"name": "Guinevere", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "37.jpg")},
    38: {"name": "Masha", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "38.jpg")},
    39: {"name": "Valir", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "39.jpg")},
    40: {"name": "Chou", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "40.jpg")},
    41: {"name": "Gusion", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "41.jpg")},
    42: {"name": "Paquito", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "42.jpg")},
    43: {"name": "Aurora", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "43.jpg")},
    44: {"name": "Selena", "collection": "STUN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "44.jpg")},
    45: {"name": "Brody", "collection": "STUN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "45.jpg")},
    46: {"name": "Chou", "collection": "STUN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "46.jpg")},
    47: {"name": "Wanwan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "47.jpg")},
    48: {"name": "Atlas", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "48.jpg")},
    49: {"name": "Bane", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "49.jpg")},
    50: {"name": "Chang'e", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "50.jpg")},
    51: {"name": "Ruby", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "51.jpg")},
    52: {"name": "Fanny", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "52.jpg")},
    53: {"name": "Angela", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "53.jpg")},
    54: {"name": "Lesley", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "54.jpg")},
    55: {"name": "Layla", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "55.jpg")},
    56: {"name": "Guinevere", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "56.jpg")},
    57: {"name": "Vexana", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "57.jpg")},
    58: {"name": "Lukas", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "58.jpg")},
    59: {"name": "Hayabusa", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "59.jpg")},
    60: {"name": "Suyou", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "60.jpg")},
    61: {"name": "Kalea", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "61.jpg")},
    62: {"name": "Vale", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "62.jpg")},
    63: {"name": "Chip", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "63.jpg")},
    64: {"name": "Rafaela", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "64.jpg")},
    65: {"name": "Thamu", "collection": "KUNG FU PANDA", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "65.jpg")},
    66: {"name": "Ling", "collection": "KUNG FU PANDA", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "66.jpg")},
    67: {"name": "Akai", "collection": "KUNG FU PANDA", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "67.jpg")},
    68: {"name": "Eudura", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "68.jpg")},
    69: {"name": "Natalia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "69.jpg")},
    70: {"name": "Valir", "collection": "SAINTS SERIES", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "70.jpg")},
    71: {"name": "Chou", "collection": "SAINTS SERIES", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "71.jpg")},
    72: {"name": "Badang", "collection": "SAINTS SERIES", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "72.jpg")},
    73: {"name": "Hano", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "73.jpg")},
    74: {"name": "Helcurt", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "74.jpg")},
    75: {"name": "Angela", "collection": "VENOM", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "75.jpg")},
    76: {"name": "Hanabi", "collection": "VENOM", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "76.jpg")},
    77: {"name": "Gusion", "collection": "VENOM", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "77.jpg")},
    78: {"name": "Dyrroth", "collection": "VENOM", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "78.jpg")},
    79: {"name": "Harley", "collection": "VENOM", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "79.jpg")},
    80: {"name": "Grock", "collection": "VENOM", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "80.jpg")},
    81: {"name": "Irithel", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "81.jpg")},
    82: {"name": "Leomord", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "82.jpg")},
    83: {"name": "Benedetta", "collection": "LIMITED", "path": os.path.join(PHOTO_BASE_PATH, "83.jpg")},
    84: {"name": "Nana", "collection": "MISTBENDERS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "84.jpg")},
    85: {"name": "Aldous", "collection": "MISTBENDERS", "points": 15001500, "path": os.path.join(PHOTO_BASE_PATH, "85.jpg")},
    86: {"name": "Julian", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "86.jpg")},
    87: {"name": "Dyrroth", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "87.jpg")},
    88: {"name": "Harith", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "88.jpg")},
    89: {"name": "Cecilion", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "89.jpg")},
    90: {"name": "Benedetta", "collection": "COVENANT", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "90.jpg")},
    91: {"name": "Lesley", "collection": "COVENANT", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "91.jpg")},
    92: {"name": "Thamu", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "92.jpg")},
    93: {"name": "Valentine", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "93.jpg")},
    94: {"name": "Kadita", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "94.jpg")},
    95: {"name": "Cyclops", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "95.jpg")},
    96: {"name": "Alucard", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "96.jpg")},
    97: {"name": "Argus", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "97.jpg")},
    98: {"name": "Kimmy", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "98.jpg")},
    99: {"name": "Obsisia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "99.jpg")},
    100: {"name": "Fanny", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "100.jpg")},
    101: {"name": "Harith", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "101.jpg")},
    102: {"name": "Alucard", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "102.jpg")},
    103: {"name": "Granger", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "103.jpg")},
    104: {"name": "Tigreal", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "104.jpg")},
    105: {"name": "Xavier", "collection": "JUJUTSU KAISEN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "105.jpg")},
    106: {"name": "Julian", "collection": "JUJUTSU KAISEN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "106.jpg")},
    107: {"name": "Yin", "collection": "JUJUTSU KAISEN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "107.jpg")},
    108: {"name": "Melissa", "collection": "JUJUTSU KAISEN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "108.jpg")},
    109: {"name": "Suyou", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "109.jpg")},
    110: {"name": "Granger", "collection": "TRANSFORMERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "110.jpg")},
    111: {"name": "Johnson", "collection": "TRANSFORMERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "111.jpg")},
    112: {"name": "X.Borg", "collection": "TRANSFORMERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "112.jpg")},
    113: {"name": "Roger", "collection": "TRANSFORMERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "113.jpg")},
    114: {"name": "Popol and Kupa", "collection": "TRANSFORMERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "114.jpg")},
    115: {"name": "Aldous", "collection": "TRANSFORMERS", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "115.jpg")},
    116: {"name": "Novaria", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "116.jpg")},
    117: {"name": "Barats", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "117.jpg")},
    118: {"name": "Phoveus", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "118.jpg")},
    119: {"name": "Aulus", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "119.jpg")},
    120: {"name": "Gusion", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "120.jpg")},
    121: {"name": "Franco", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "121.jpg")},
    122: {"name": "Saber", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "122.jpg")},
    123: {"name": "Miya", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "123.jpg")},
    124: {"name": "Granger", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "124.jpg")},
    125: {"name": "Gord", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "125.jpg")},
    126: {"name": "Alucard", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "126.jpg")},
    127: {"name": "Lesley", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "127.jpg")},
    128: {"name": "Valir", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "128.jpg")},
    129: {"name": "Guinevere", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "129.jpg")},
    130: {"name": "Lunox", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "130.jpg")},
    131: {"name": "Freya", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "131.jpg")},
    132: {"name": "Alpha", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "132.jpg")},
    133: {"name": "Johnson", "collection": "LEGEND", "points": 2000, "path": os.path.join(PHOTO_BASE_PATH, "133.jpg")},
    134: {"name": "Joy", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "134.jpg")},
    135: {"name": "Joy", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "135.jpg")},
    136: {"name": "Arlott", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "136.jpg")},
    137: {"name": "Ixia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "137.jpg")},
    138: {"name": "Cici", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "138.jpg")},
    139: {"name": "Suyou", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "139.jpg")},
    140: {"name": "huxin", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "140.jpg")},
    141: {"name": "huxin", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "141.jpg")},
    142: {"name": "Kalea", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "142.jpg")},
    143: {"name": "Sora", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "143.jpg")},
    144: {"name": "Lukas", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "144.jpg")},
    145: {"name": "Novaria", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "145.jpg")},
    146: {"name": "Cici", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "146.jpg")},
    147: {"name": "Ixia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "147.jpg")},
    148: {"name": "Melissa", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "148.jpg")},
    149: {"name": "Aanom", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "149.jpg")},
    150: {"name": "Edith", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "150.jpg")},
    151: {"name": "Aulus", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "151.jpg")},
    152: {"name": "Beatrix", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "152.jpg")},
    153: {"name": "Natan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "153.jpg")},
    154: {"name": "Gloo", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "154.jpg")},
    155: {"name": "Gloo", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "155.jpg")},
    156: {"name": "Barats", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "156.jpg")},
    157: {"name": "Yu hong", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "157.jpg")},
    158: {"name": "Atlas", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "158.jpg")},
    159: {"name": "Fasha", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "159.jpg")},
    160: {"name": "Cecilion", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "160.jpg")},
    161: {"name": "Wanwan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "161.jpg")},
    162: {"name": "Tigreal", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "162.jpg")},
    163: {"name": "Bruno", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "163.jpg")},
    164: {"name": "Clint", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "164.jpg")},
    165: {"name": "Harley", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "165.jpg")},
    166: {"name": "Diggie", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "166.jpg")},
    167: {"name": "Leomord", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "167.jpg")},
    168: {"name": "Hylos", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "168.jpg")},
    169: {"name": "Kimmy", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "169.jpg")},
    170: {"name": "Minsitthar", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "170.jpg")},
    171: {"name": "Faramis", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "171.jpg")},
    172: {"name": "Khufra", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "172.jpg")},
    173: {"name": "Terila", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "173.jpg")},
    174: {"name": "X.Borg", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "174.jpg")},
    175: {"name": "Ling", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "175.jpg")},
    176: {"name": "Terila", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "176.jpg")},
    177: {"name": "Baxia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "177.jpg")},
    178: {"name": "Masha", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "178.jpg")},
    179: {"name": "Alice", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "179.jpg")},
    180: {"name": "Karina", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "180.jpg")},
    181: {"name": "Karina", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "181.jpg")},
    182: {"name": "Bane", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "182.jpg")},
    183: {"name": "Wanwan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "183.jpg")},
    184: {"name": "ilong", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "184.jpg")},
    185: {"name": "Natalia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "185.jpg")},
    186: {"name": "Minotaur", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "186.jpg")},
    187: {"name": "Freya", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "187.jpg")},
    188: {"name": "Kagura", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "188.jpg")},
    189: {"name": "Alpha", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "189.jpg")},
    190: {"name": "Hilda", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "190.jpg")},
    191: {"name": "Vexana", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "191.jpg")},
    192: {"name": "Karrie", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "192.jpg")},
    193: {"name": "Gatotkaca", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "193.jpg")},
    194: {"name": "Grock", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "194.jpg")},
    195: {"name": "Odette", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "195.jpg")},
    196: {"name": "Lancelot", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "196.jpg")},
    197: {"name": "hask", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "197.jpg")},
    198: {"name": "Helcurt", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "198.jpg")},
    199: {"name": "Jawhead", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "199.jpg")},
    200: {"name": "Martis", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "200.jpg")},
    201: {"name": "Uranus", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "201.jpg")},
    202: {"name": "Kaja", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "202.jpg")},
    203: {"name": "Claude", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "203.jpg")},
    204: {"name": "Valt", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "204.jpg")},
    205: {"name": "Kagura", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "205.jpg")},
    206: {"name": "Kimmy", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "206.jpg")},
    207: {"name": "Belerick", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "207.jpg")},
    208: {"name": "Minsitthar", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "208.jpg")},
    209: {"name": "Badang", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "209.jpg")},
    210: {"name": "Guinevere", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "210.jpg")},
    211: {"name": "Guinevere", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "211.jpg")},
    212: {"name": "Yve", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "212.jpg")},
    213: {"name": "Lylia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "213.jpg")},
    214: {"name": "Sun", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "214.jpg")},
    215: {"name": "Kadita", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "215.jpg")},
    216: {"name": "Silvanna", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "216.jpg")},
    217: {"name": "Silvanna", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "217.jpg")},
    218: {"name": "Carmilla", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "218.jpg")},
    219: {"name": "Luo Yi", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "219.jpg")},
    220: {"name": "Luo Yi", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "220.jpg")},
    221: {"name": "Khaleed", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "221.jpg")},
    222: {"name": "Mathilda", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "222.jpg")},
    223: {"name": "Mathilda", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "223.jpg")},
    224: {"name": "Gusion", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "224.jpg")},
    225: {"name": "Xavier", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "225.jpg")},
    226: {"name": "Estes", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "226.jpg")},
    227: {"name": "Selena", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "227.jpg")},
    228: {"name": "Nolan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "228.jpg")},
    229: {"name": "Nolan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "229.jpg")},
    230: {"name": "Fanny", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "230.jpg")},
    231: {"name": "Lesley", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "231.jpg")},
    232: {"name": "Cecilion", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "232.jpg")},
    233: {"name": "Clint", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "233.jpg")},
    234: {"name": "Selena", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "234.jpg")},
    235: {"name": "Arlott", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "235.jpg")},
    236: {"name": "Moskov", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "236.jpg")},
    237: {"name": "Lapu-Lapu", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "237.jpg")},
    238: {"name": "Roger", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "238.jpg")},
    239: {"name": "Brody", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "239.jpg")},
    240: {"name": "Popol and Kupa", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "240.jpg")},
    241: {"name": "Beatrix", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "241.jpg")},
    242: {"name": "Valentina", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "242.jpg")},
    243: {"name": "Melissa", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "243.jpg")},
    244: {"name": "Natan", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "244.jpg")},
    245: {"name": "Edith", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "245.jpg")},
    246: {"name": "Ling", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "246.jpg")},
    247: {"name": "X.Borg", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "247.jpg")},
    248: {"name": "Aurora", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "248.jpg")},
    249: {"name": "Yi Sun-shin", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "249.jpg")},
    250: {"name": "Fanny", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "250.jpg")},
    251: {"name": "Cyclops", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "251.jpg")},
    252: {"name": "Floryn", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "252.jpg")},
    253: {"name": "Esmeralda", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "253.jpg")},
    254: {"name": "Khufra", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "254.jpg")},
    255: {"name": "Harith", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "255.jpg")},
    256: {"name": "Jawhead", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "256.jpg")},
    257: {"name": "Moskov", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "257.jpg")},
    258: {"name": "Lolita", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "258.jpg")},
    259: {"name": "Sun", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "259.jpg")},
    260: {"name": "Layla", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "260.jpg")},
    261: {"name": "Melissa", "collection": "SPARKLE", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "261.jpg")},
    262: {"name": "Fredrinn", "collection": "SPARKLE", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "262.jpg")},
    263: {"name": "Estes", "collection": "SPARKLE", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "263.jpg")},
    264: {"name": "Fasha", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "264.jpg")},
    265: {"name": "Karina", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "265.jpg")},
    266: {"name": "Fanny", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "266.jpg")},
    267: {"name": "Natalia", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "267.jpg")},
    268: {"name": "Layla", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "268.jpg")},
    269: {"name": "Guinevere", "collection": "", "path": os.path.join(PHOTO_BASE_PATH, "269.jpg")},
}

# 3. Фиксированная редкость для каждой карты по ее ID.
FIXED_CARD_RARITIES = {
    1: "collectible card", 2: "collectible card", 3: "collectible card", 4: "collectible card",
    5: "collectible card", 6: "collectible card", 7: "collectible card", 8: "collectible card",
    9: "regular card", 10: "collectible card", 11: "collectible card", 12: "collectible card",
    13: "collectible card", 14: "collectible card", 15: "collectible card", 16: "collectible card",
    17: "collectible card", 18: "collectible card", 19: "collectible card", 20: "collectible card",
    21: "collectible card", 22: "collectible card", 23: "collectible card", 24: "collectible card",
    25: "collectible card", 26: "collectible card", 27: "collectible card", 28: "collectible card",
    29: "collectible card", 30: "collectible card", 31: "collectible card", 32: "collectible card",
    33: "collectible card", 34: "regular card", 35: "LIMITED", 36: "LIMITED", 37: "LIMITED", 38: "LIMITED",
    39: "LIMITED", 40: "LIMITED",
    41: "LIMITED", 42: "LIMITED", 43: "LIMITED", 44: "collectible card", 45: "collectible card", 46: "collectible card",
    47: "regular card", 48: "regular card", 49: "regular card", 50: "collectible card", 51: "collectible card",
    52: "collectible card",
    53: "collectible card", 54: "collectible card", 55: "collectible card", 56: "collectible card",
    57: "collectible card", 58: "collectible card", 59: "collectible card", 60: "collectible card",
    61: "collectible card", 62: "collectible card", 63: "regular card", 64: "regular card",
    65: "collectible card", 66: "collectible card", 67: "collectible card", 68: "regular card",
    69: "regular card", 70: "collectible card", 71: "collectible card", 72: "collectible card",
    73: "regular card", 74: "regular card", 75: "collectible card", 76: "collectible card",
    77: "collectible card", 78: "collectible card", 79: "collectible card", 80: "collectible card",
    81: "LIMITED", 82: "LIMITED", 83: "LIMITED", 84: "collectible card",
    85: "collectible card", 86: "collectible card", 87: "collectible card", 88: "collectible card",
    89: "collectible card", 90: "collectible card", 91: "collectible card", 92: "regular card",
    93: "regular card", 94: "regular card", 95: "collectible card", 96: "collectible card",
    97: "collectible card", 98: "collectible card", 99: "regular card", 100: "collectible card",
    101: "collectible card", 102: "collectible card", 103: "collectible card",
    104: "collectible card", 105: "collectible card", 106: "collectible card", 107: "collectible card",
    108: "collectible card", 109: "regular card", 110: "collectible card", 111: "collectible card",
    112: "collectible card", 113: "collectible card", 114: "collectible card", 115: "collectible card",
    116: "regular card", 117: "regular card", 118: "regular card", 119: "regular card",
    120: "collectible card", 121: "collectible card", 122: "collectible card", 123: "collectible card",
    124: "collectible card",
    125: "collectible card", 126: "collectible card", 127: "collectible card", 128: "collectible card",
    129: "collectible card",
    130: "collectible card", 131: "collectible card", 132: "collectible card", 133: "collectible card",
    134: "regular card",
    135: "regular card", 136: "regular card", 137: "regular card", 138: "regular card", 139: "regular card",
    140: "regular card",
    141: "regular card", 142: "regular card", 143: "regular card", 144: "regular card", 145: "regular card",
    146: "rare card", 147: "rare card",
    148: "rare card", 149: "rare card", 150: "rare card", 151: "rare card", 152: "rare card", 153: "rare card",
    154: "rare card", 155: "rare card",
    156: "rare card", 157: "rare card", 158: "rare card", 159: "rare card", 160: "rare card", 161: "rare card",
    162: "rare card", 163: "rare card",
    164: "rare card", 165: "rare card", 166: "rare card", 167: "rare card", 168: "rare card", 169: "rare card",
    170: "rare card", 171: "rare card",
    172: "rare card", 173: "rare card", 174: "rare card", 175: "rare card", 176: "rare card", 177: "rare card",
    178: "rare card", 179: "exclusive card",
    180: "exclusive card", 181: "exclusive card", 182: "exclusive card", 183: "exclusive card", 184: "exclusive card",
    185: "exclusive card",
    186: "exclusive card", 187: "exclusive card", 188: "exclusive card", 189: "exclusive card", 190: "exclusive card",
    191: "exclusive card",
    192: "exclusive card", 193: "exclusive card", 194: "exclusive card", 195: "exclusive card", 196: "exclusive card",
    197: "exclusive card",
    198: "exclusive card", 199: "exclusive card", 200: "exclusive card", 201: "exclusive card", 202: "exclusive card",
    203: "exclusive card",
    204: "exclusive card", 205: "exclusive card", 206: "exclusive card", 207: "exclusive card", 208: "exclusive card",
    209: "exclusive card",
    210: "exclusive card", 211: "exclusive card", 212: "exclusive card", 213: "exclusive card", 214: "exclusive card",
    215: "exclusive card",
    216: "exclusive card", 217: "exclusive card", 218: "exclusive card", 219: "exclusive card", 220: "exclusive card",
    221: "exclusive card",
    222: "exclusive card", 223: "exclusive card", 224: "exclusive card", 225: "exclusive card", 226: "exclusive card",
    227: "exclusive card",
    228: "exclusive card", 229: "exclusive card", 230: "epic card", 231: "epic card", 232: "epic card",
    233: "epic card", 234: "epic card",
    235: "epic card", 236: "epic card", 237: "epic card", 238: "epic card", 239: "epic card", 240: "epic card",
    241: "epic card",
    242: "epic card", 243: "epic card", 244: "epic card", 245: "epic card", 246: "epic card", 247: "epic card",
    248: "epic card",
    249: "epic card", 250: "epic card", 251: "epic card", 252: "epic card", 253: "epic card", 254: "epic card",
    255: "epic card",
    256: "epic card", 257: "epic card", 258: "epic card", 259: "epic card", 260: "epic card", 261: "collectible card",
    262: "collectible card",
    263: "collectible card", 264: "rare card", 265: "rare card", 266: "rare card", 267: "rare card", 268: "rare card",
    269: "rare card",
}

# Данные о сезоне
season_data = {
    "start_date": datetime.now(),
    "season_number": 1}

RANK_NAMES = ["Воин", "Эпик", "Легенда", "Мифический", "Мифическая Слава"]

WIN_PHRASES = [
    " <b>MVP!</b> Ты затащил эту катку!",
    " Бог кустов! Враги боятся заходить в игру после твоих засад!",
    " <b>Double Kill!</b> Звезда летит в твою копилку!",
    " <b>Легендарный камбек!</b> Ты вырвал победу!",
    " Savage! Вся вражеская команда в таверне!",
    "Wiped Out! Ты снес их трон, пока они спорили в чате, кто виноват!",
    "Твой скилл — легенда! Тебя уже зовут в ONIC", 
    "Твой скин за 5к гемов затащил! Красиво жить не запретишь!",
    "Враги играли ногами? Другого объяснения нет!"
]

LOSE_PHRASES = [
    " Твой мозг ушел в АФК вместе с лесником!",
    " <b>Минус звезда.</b> Союзники решили пофидить",
    " <b>Трон упал!</b> Враги оказались сильнее в этот раз",
    " <b>Тебя загангали!</b> Звезда потеряна",
    " <b>Огромный пинг!</b> Купи наконец-то Wi-Fi ",
    " <b>Поражение.</b> Эпики в твоей команде — это приговор",
    " <b>Твой билд не сработал.</b> Попробуй в следующий раз",
    "Тормоз года!У твоего отца и то быстрее реакция!",
    "Эпик в крови! Можешь менять ник на 'Корм для врагов'!",
    "Купи телефон! Твйо POCO скоро взорвется!",
    "Скрытопульный тп от Лои на фантан врага!",
    "1% зарядки! Телефон вырубился в самый важный момент замеса!",
    "Мама забрала телефон, вы слили катку! Тебе же говорили — «сначала уроки!»"
]

def is_recent_callback(user_id: int, key: str, window: float = DEBOUNCE_SECONDS) -> bool:
    now = time.time()
    current = _CALLBACK_LAST_TS.get((user_id, key), 0.0)
    if now - current < window:
        # обновим время, чтобы многократные нажатия продолжали отклоняться в этом окне
        _CALLBACK_LAST_TS[(user_id, key)] = now
        return True
    _CALLBACK_LAST_TS[(user_id, key)] = now
    return False

def get_rank_info(stars):
    if stars <= 0:
        return "Без ранга", "0 звезд"
    # Порядок дивизионов в игре обратный: III, II, I или V, IV, III, II, I
    rank_configs = [
        ("Воин", 3, 3),  # 1-9 звезды
        ("Элита", 3, 4),  # 10-21 звезды
        ("Мастер", 4, 4),  # 22-37 звезды
        ("Грандмастер", 5, 5),  # 38-62 звезды
        ("Эпик", 5, 5),  # 63-87 звезды
        ("Легенда", 5, 5)  # 88-112 звезды
    ]
    current_threshold = 0
    for name, divs, stars_per_div in rank_configs:
        rank_total_stars = divs * stars_per_div
        if stars <= current_threshold + rank_total_stars:
            # Мы внутри этого ранга
            stars_in_rank = stars - current_threshold
            # Определяем дивизион (например, из 5 дивизионов: 5, 4, 3, 2, 1)
            div_index = (stars_in_rank - 1) // stars_per_div
            div_number = divs - div_index
            # Звезды внутри дивизиона
            stars_left = ((stars_in_rank - 1) % stars_per_div) + 1
            return f"{name} {div_number}", f"{stars_left}⭐️"
        current_threshold += rank_total_stars

    # Если звезд больше 112 — это Мифический уровень
    mythic_stars = stars - 112
    if mythic_stars < 25:
        return "Мифический", f"{mythic_stars}⭐️"
    elif mythic_stars < 50:
        return "Мифическая Честь", f"{mythic_stars}⭐️"
    elif mythic_stars < 100:
        return "Мифическая Слава", f"{mythic_stars}⭐️"
    else:
        return "Мифический Бессмертный", f"{mythic_stars}⭐️"
        
def get_mastery_info(reg_total):
    # Список порогов: (порог, название)
    levels = [
        (0, ""),
        (100, ""),
        (200, ""),
        (400, ""),
        (700, ""),
        (1000, ""),
        (2000, ""),
        (3500, ""), 
        (5000, ""), 
        (10000, ""), 
    ]
    
    current_title = ""
    next_threshold = 100
    
    for i in range(len(levels)):
        threshold, title = levels[i]
        if reg_total >= threshold:
            current_title = title
            # Если есть следующий уровень, берем его порог
            if i + 1 < len(levels):
                next_threshold = levels[i+1][0]
            else:
                next_threshold = None # Максимальный уровень
        else:
            break
            
    return current_title, next_threshold


# --- ОБРАБОТЧИК РЕГНУТЬ ---
async def regnut_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if update.message.text.lower().strip() != "регнуть":
        return
    user = get_moba_user(update.effective_user.id)
    now = time.time()
    # Кулдаун 15 секунд

    if now - user.get("last_reg_time", 0) < 15:
        wait = int(15 - (now - user["last_reg_time"]))
        await update.message.reply_text(
            f"⏳ <b>Поиск матча</b><blockquote>Катку можно регнуть через {wait} секунд</blockquote>",
            parse_mode=ParseMode.HTML)
        return
    user["last_reg_time"] = now

    # --- ОБНОВЛЕННАЯ ЛОГИКА ШАНСА ПОБЕДЫ ---
    # ШАНС ПОБЕДЫ (100% до 2 звезд, 60% до Грандмастера, дальше 50%)
    if user["stars"] < 2:  # Если у пользователя 0 или 1 звезда
        win_chance = 100
    elif user["stars"] < 38:  # Если у пользователя от 2 до 37 звезд (включительно)
        win_chance = 60
    else:  # Если у пользователя 38 звезд и более (Грандмастер и выше)
        win_chance = 50
    # --- КОНЕЦ ОБНОВЛЕННОЙ ЛОГИКИ ШАНСА ПОБЕДЫ ---

    win = random.randint(1, 100) <= win_chance
    coins = random.randint(15, 60)
    user["coins"] += coins
    user["reg_total"] += 1
    if win:
        user["stars"] += 1
        user["reg_success"] += 1
        if user["stars"] > user["max_stars"]: user["max_stars"] = user["stars"]
        msg = random.choice(WIN_PHRASES)
        change = "<b>⚡️ VICTORY ! </b>"
        rank_change_text = "<b>Текущий ранг повышен!</b>"
    else:
        if user.get("protection_active", 0) > 0:
            user["protection_active"] -= 1
            msg = "🛡 Защита сработала! Вы проиграли, но карта защиты сохранила вашу звезду."
            change = "📈 0 звезд"
            # save_moba_user вызывается позже в коде
        else:
            if user["stars"] > 0: user["stars"] -= 1
            msg = random.choice(LOSE_PHRASES)
            change = "<b>💢 DEFEAT ! </b>"
            rank_change_text = "<b>Текущий ранг понижен!</b>"
    title, next_val = get_mastery_info(user["reg_total"])
    if next_val:
        mastery_display = f"{title} {user['reg_total']}/{next_val}"
    else:
        mastery_display = f"{title} {user['reg_total']} (MAX)"
    rank_name, star_info = get_rank_info(user["stars"])
    # Проверка на деление на ноль, если reg_total равен 0
    wr = (user["reg_success"] / user["reg_total"]) * 100 if user["reg_total"] > 0 else 0
    save_moba_user(user)  # ОБЯЗАТЕЛЬНО добавить эту строку

    res = (f"<b>{change} {msg}</b>\n\n"
           f"<blockquote>{rank_change_text}</blockquote>\n"
           f"<b><i>{rank_name} ({star_info})  БО + {coins}! </i></b> \n\n"
           f"<b>💫 Мастерство {mastery_display}</b> "
           )
    await update.message.reply_text(res, parse_mode=ParseMode.HTML)

DIAMONDS_REWARD = {
    "regular card": 50,
    "rare card": 150,
    "exclusive card": 300,
    "epic card": 600,
    "collectible card": 1000,
    "LIMITED": 2500
}

def generate_card_stats(rarity: str, card_data: dict, is_repeat: bool = False) -> dict:
    stats_range = RARITY_STATS.get(rarity, RARITY_STATS["regular card"])
    base_points = card_data.get("points", stats_range["points"])
    
    if is_repeat:
        # За повторку: очки х3, алмазы по таблице, БО не даем (или по желанию)
        return {
            "rarity": rarity,
            "bo": 0,
            "points": base_points * 3,
            "diamonds": DIAMONDS_REWARD.get(rarity, 10)
        }
    else:
        # За новую: очки х1, алмазов 0
        return {
            "rarity": rarity,
            "bo": random.randint(stats_range["min_bo"], stats_range["max_bo"]),
            "points": base_points,
            "diamonds": 0
        }

async def id_detection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    pattern = r"^\d{9}\s\(\d{4}\)$"

    if re.match(pattern, text):
        context.user_data['temp_mlbb_id'] = text
        keyboard = [
            [InlineKeyboardButton("Добавить", callback_data="confirm_add_id"),
             InlineKeyboardButton("Пока не добавлять", callback_data="cancel_add_id")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "<b>👾 GAME ID</b>\n<blockquote>Хотите добавить свой айди в профиль?</blockquote>",
            reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def confirm_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:  # Добавим проверку на существование query
        return
    await query.answer()  # Отвечаем на callback_query, чтобы убрать "часики" на кнопке
    user_id = query.from_user.id
    # 1. Заменяем get_user на get_moba_user и оборачиваем в asyncio.to_thread
    user = await asyncio.to_thread(get_moba_user, user_id)
    if user is None:
        await query.edit_message_text("Произошла ошибка: не удалось найти ваш профиль.")
        return
    new_game_id = context.user_data.get('temp_mlbb_id')
    if new_game_id:
        user['game_id'] = new_game_id  # Сохраняем в профиль
        await asyncio.to_thread(save_moba_user, user)
        await query.edit_message_text("👾 GAME ID \n Твой GAME ID обновлен! Проверь профиль", parse_mode=ParseMode.HTML)
        context.user_data.pop('temp_mlbb_id', None)
    else:
        await query.edit_message_text(
            "❌ Произошла ошибка. Не удалось найти GAME ID для сохранения. Попробуйте отправить ID еще раз.")

async def cancel_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop('temp_mlbb_id', None)  # Удаляем временные данные
    await query.edit_message_text("<b>👾 GAME ID</b>\n<blockquote>Твой  ID не был добавлен.</blockquote>",
                                  parse_mode=ParseMode.HTML)

def get_moba_user(user_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        cursor.execute("SELECT * FROM moba_users WHERE user_id = %s", (user_id,))
        user_data = cursor.fetchone()

        if not user_data:
            logger.info(f"Создаем нового пользователя MOBA с user_id: {user_id}")
            cursor.execute("""
                    INSERT INTO moba_users (user_id) VALUES (%s)
                    RETURNING *
                """, (user_id,))
            user_data = cursor.fetchone()
            conn.commit()

        user_dict = dict(user_data)

        # Инициализация полей, если они отсутствуют или NULL (исправленный отступ)
        user_dict.setdefault('nickname', 'моблер')
        user_dict.setdefault('game_id', None)
        user_dict.setdefault('points', 0)
        user_dict.setdefault('diamonds', 0)
        user_dict.setdefault('coins', 0)
        user_dict.setdefault('stars', 0)
        user_dict.setdefault('max_stars', 0)
        user_dict.setdefault('stars_all_time', 0)
        user_dict.setdefault('reg_total', 0)
        user_dict.setdefault('reg_success', 0)
        user_dict.setdefault('premium_until', None)
        user_dict.setdefault('last_mobba_time', 0)
        user_dict.setdefault('last_reg_time', 0)

        # Загружаем карты из moba_inventory (исправленный отступ)
        user_cards = get_user_inventory(user_id)
        user_dict['cards'] = user_cards  # Присваиваем список карт

        return user_dict
    except Error as e:
        logger.error(f"Ошибка БД в get_moba_user для user_id {user_id}: {e}", exc_info=True)
        return None
    finally:
        if conn: conn.close()

async def _moba_send_filtered_card(query, context, cards: List[dict], index: int, back_cb: str = "moba_my_cards"):
    await query.answer()

    # База для дебаунса: всё, что до последнего _{index}
    try:
        base = (query.data or "moba_filtered").rsplit("_", 1)[0]
    except Exception:
        base = query.data or "moba_filtered"

    # Debounce: если недавно нажимали — игнорируем
    if is_recent_callback(query.from_user.id, base):
        return

    # Проверка наличия карт
    if not cards:
        try:
            await query.edit_message_text("У вас нет карт в этой категории.")
        except Exception:
            await context.bot.send_message(chat_id=query.from_user.id, text="У вас нет карт в этой категории.")
        return

    # Ограничиваем индекс
    if index < 0:
        index = 0
    if index >= len(cards):
        index = len(cards) - 1

    card = cards[index]

    # !!! ПЕРЕМЕЩЕНО ВЫШЕ: Определение photo_path и caption
    # Это гарантирует, что они будут определены до блока try/except для отправки медиа
    photo_path = card.get('image_path') or CARDS.get(card.get('card_id'), {}).get('path') or \
                 PHOTO_DETAILS.get(card.get('card_id'), {}).get('path')
    caption = _moba_card_caption(card, index, len(cards))

    # Определяем базу для callback'ов (всё кроме последнего _{index})
    try:
        base = (query.data or "moba_filtered").rsplit("_", 1)[0]
    except Exception:
        base = query.data or "moba_filtered"

    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton("<", callback_data=f"{base}_{index - 1}"))
    nav.append(InlineKeyboardButton(f"{index + 1}/{len(cards)}", callback_data="moba_ignore"))
    if index < len(cards) - 1:
        nav.append(InlineKeyboardButton(">", callback_data=f"{base}_{index + 1}"))

    keyboard = [nav, [InlineKeyboardButton("< В коллекцию", callback_data=back_cb)]]

    # Отправка / редактирование media
    try:
        if query.message and getattr(query.message, "photo", None):
            with open(photo_path, "rb") as ph:
                await query.edit_message_media(
                    InputMediaPhoto(media=ph, caption=caption, parse_mode=ParseMode.HTML),
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        else:
            # Удаляем старое сообщение (если есть) и шлём новое фото
            try:
                await query.message.delete()
            except Exception:
                pass
            with open(photo_path, "rb") as ph:
                await context.bot.send_photo(
                    chat_id=query.from_user.id,
                    photo=ph,
                    caption=caption,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
    except FileNotFoundError:
        logger.error(f"Photo not found for moba card: {photo_path}")
        try:
            await query.edit_message_text(caption + "\n\n(Фото не найдено)",
                                          reply_markup=InlineKeyboardMarkup(keyboard),
                                          parse_mode=ParseMode.HTML)
        except Exception:
            await context.bot.send_message(chat_id=query.from_user.id, text=caption + "\n\n(Фото не найдено)",
                                           reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception("Ошибка при отправке отфильтрованной карты MOBA: %s", e)
        # В случае ошибки, мы хотим отправить сообщение, даже если photo_path не был найден
        # caption теперь гарантированно определен
        try:
            await context.bot.send_message(chat_id=query.from_user.id, text=caption, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Не удалось отправить fallback сообщение при ошибке _moba_send_filtered_card.")

def save_moba_user(user_data):
    """Сохраняет измененные данные пользователя в БД."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE moba_users SET 
            nickname = %s, game_id = %s, points = %s, diamonds = %s, 
            coins = %s, stars = %s, max_stars = %s, stars_all_time = %s, 
            reg_total = %s, reg_success = %s, premium_until = %s,
            last_mobba_time = %s, last_reg_time = %s
        WHERE user_id = %s
    """, (
        user_data['nickname'],
        user_data['game_id'],
        user_data['points'],
        user_data['diamonds'],
        user_data['coins'],
        user_data['stars'],
        user_data['max_stars'],
        user_data['stars_all_time'],
        user_data['reg_total'],
        user_data['reg_success'],
        user_data['premium_until'],
        user_data['last_mobba_time'],
        user_data['last_reg_time'],
        user_data['user_id']
    ))
    conn.commit()
    conn.close()

def add_card_to_inventory(user_id, card):
    """Добавляет карту в инвентарь в БД."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO moba_inventory 
        (user_id, card_id, card_name, collection, rarity, bo, points, diamonds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        user_id, card['card_id'], card['name'], card['collection'],
        card['rarity'], card['bo'], card['points'], card['diamonds']
    ))
    conn.commit()
    conn.close()

def get_user_inventory(user_id):
    """Получает все карты игрока."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT * FROM moba_inventory WHERE user_id = %s", (user_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

async def check_season_reset():
    """Сбрасывает звезды каждые 3 месяца (90 дней)"""
    global season_data
    if datetime.now() > season_data["start_date"] + timedelta(days=90):
        for uid in users:
            users[uid]["stars"] = 0  # Сброс текущих звезд
        season_data["start_date"] = datetime.now()
        season_data["season_number"] += 1
        logging.info(f"Сезон {season_data['season_number']} начался!")

async def set_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # get_moba_user выполняет блокирующие операции с БД, поэтому оборачиваем в asyncio.to_thread
    user = await asyncio.to_thread(get_moba_user, user_id)

    # Проверка на существование пользователя (если get_moba_user может вернуть None)
    if user is None:
        await update.message.reply_text("Произошла ошибка: не удалось найти ваш профиль.")
        logger.error(f"set_name: Профиль пользователя {user_id} не найден.")  # Используем logger
        return

    new_name = " ".join(context.args).strip()

    if 5 <= len(new_name) <= 16:
        user["nickname"] = new_name  # Изменяем данные в локальном объекте/словаре

        # <<< ВОТ ЗДЕСЬ МЫ ИСПОЛЬЗУЕМ ВАШУ ФУНКЦИЮ save_moba_user! >>>
        # save_moba_user также выполняет блокирующие операции, поэтому оборачиваем ее.
        await asyncio.to_thread(save_moba_user, user)

        await update.message.reply_text(f"Ник изменен на: <b>{new_name}</b>", parse_mode=ParseMode.HTML)
        logger.info(f"set_name: Ник пользователя {user_id} успешно изменен на '{new_name}'.")  # Используем logger
    else:
        await update.message.reply_text(
            "<b>👾 Придумай свой ник</b>\n<blockquote>Длина от 5 до 16 символов\nПример: /name помидорка</blockquote>",
            parse_mode=ParseMode.HTML)
        logger.warning(
            f"set_name: Попытка установить невалидный ник: '{new_name}' (длина: {len(new_name)}) для user_id: {user_id}")  # Используем logger

async def mobba_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or update.message.text.lower() != "моба":
        return

    user = await asyncio.to_thread(get_moba_user, update.effective_user.id)
    now = time.time()
    
    # Расчет кулдауна
    is_premium = user["premium_until"] and user["premium_until"] > datetime.now(timezone.utc)
    base_cooldown = 3600 * 3 # 3 часа по умолчанию (из COOLDOWN_SECONDS)
    if is_premium: base_cooldown *= 0.75

    if now - user["last_mobba_time"] < base_cooldown:
        wait = int(base_cooldown - (now - user["last_mobba_time"]))
        await update.message.reply_text(f"⏳ Попробуйте через {wait // 60} мин. {wait % 60} сек.", parse_mode=ParseMode.HTML)
        return

    user["last_mobba_time"] = now

    # Логика Удачи (Luck)
    luck_bonus = 0
    if user.get("luck_active", 0) > 0:
        luck_bonus = 10 # +10% к шансу эпиков и выше
        user["luck_active"] -= 1

    # Выбор карты
    card_id = random.choice(list(CARDS.keys()))
    # Здесь можно усложнить выбор card_id с учетом luck_bonus, если редкость не фиксирована.
    # Но так как у вас FIXED_CARD_RARITIES, просто берем редкость:
    rarity = FIXED_CARD_RARITIES.get(card_id, "regular card")

    # Проверка на повторку
    inventory = await asyncio.to_thread(get_user_inventory, user["user_id"])
    is_repeat = any(c['card_id'] == card_id for c in inventory)

    stats = generate_card_stats(rarity, CARDS[card_id], is_repeat)

    if not is_repeat:
        await asyncio.to_thread(add_card_to_inventory, user["user_id"], {
            "card_id": card_id, "name": CARDS[card_id]["name"], "collection": CARDS[card_id].get("collection", "z"),
            "rarity": rarity, "bo": stats["bo"], "points": stats["points"], "diamonds": stats["diamonds"]
        })
        msg_type = "🆕 НОВАЯ КАРТА!"
    else:
        msg_type = "🔄 ПОВТОРНАЯ КАРТА (X3 ОЧКИ!)"

    user["points"] += stats["points"]
    user["diamonds"] += stats["diamonds"]
    user["coins"] += stats["bo"]
    
    await asyncio.to_thread(save_moba_user, user)
    caption = (
        f"<b><i>🃏 {full_card_data['collection']} •  {full_card_data['name']}</i></b>\n"
        f"<blockquote><b><i>+ {full_card_data['points']} ОЧКОВ !</i></b></blockquote>\n\n"
        f"<b>✨ Редкость •</b> <i>{full_card_data['rarity']}</i>\n"
        f"<b>💰 БО •</b><i> {full_card_data['bo']}</i>\n"
        f"<b>💎 Алмазы •</b> <i>{full_card_data['diamonds']}</i>\n\n"
        f"<blockquote><b><i>Добавлено в ваши карты!</i></b></blockquote>"    )
    with open(CARDS[card_id]["path"], 'rb') as photo:
        await update.message.reply_photo(photo, caption=caption, parse_mode=ParseMode.HTML)
# Добавь в твой файл:
async def get_unique_card_count_for_user(user_id):
    conn = None  # <-- Добавлен отступ
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT card_id) FROM moba_inventory WHERE user_id = %s", (user_id,))
        count = cursor.fetchone()[0]
        return count or 0
    except Exception as e:
        # Убедитесь, что logger инициализирован
        if 'logger' in globals() or 'logger' in locals():
            logger.error(f"Ошибка подсчета уникальных карт для {user_id}: {e}", exc_info=True)
        else:
            print(
                f"Ошибка подсчета уникальных карт для {user_id}: {e}")  # Запасной вариант, если logger не инициализирован
        return 0
    finally:
        if conn:
            conn.close()

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await asyncio.to_thread(get_moba_user, update.effective_user.id)
    if user is None:
        await update.message.reply_text("Произошла ошибка при получении данных профиля. Пожалуйста, попробуйте позже.")
        return

    is_premium = user["premium_until"] and user["premium_until"] > datetime.now(timezone.utc)  # <- Новая строка
    prem_status = "🚀 Счастливый обладатель Premium" if is_premium else "Не обладает Premium"
    # Расчет рангов
    curr_rank, curr_stars = get_rank_info(user["stars"])
    max_rank, max_stars_info = get_rank_info(user["max_stars"])
    # Расчет процента побед (регнуть)
    winrate = 0
    if user["reg_total"] > 0:
        winrate = (user["reg_success"] / user["reg_total"]) * 100
    # Получаем количество уникальных карт
    unique_card_count = await get_unique_card_count_for_user(update.effective_user.id)
    # Получаем общее количество карт (включая повторы)
    total_card_count = len(user.get('cards', []))  # user['cards'] теперь содержит все карты, включая повторы

    # Получаем фото профиля
    photos = await update.effective_user.get_profile_photos(limit=1)
    display_id = user.get('game_id') if user.get('game_id') else "Не добавлен"
    text = (
        f"Ценитель <b>MOBILE LEGENDS\n \n«{user['nickname']}»</b>\n"
        f"<blockquote><b>👾GAME ID •</b> <i>{display_id}</i></blockquote>\n\n"
        f"<b>🏆 Ранг •</b> <i>{curr_rank} ({curr_stars})</i>\n"
        f"<b>⚜️ Макс ранг •</b> <i>{max_rank}</i>\n"
        f"<b>🎗️ Win rate •</b> <i>{winrate:.1f}%</i>\n\n"
        f"<b>🃏 Карт •</b> <i>{len(user['cards'])}</i>\n"
        f"<b>✨ Очков •</b> <i>{user['points']}</i>\n"
        f"<b>💰 Монет • </b><i>{user['coins']}</i>\n"
        f"<b>💎 Алмазов • </b><i>{user['diamonds']}</i>\n\n"
        f"<blockquote>{prem_status}</blockquote>")

    keyboard = [
        [InlineKeyboardButton("🃏 Мои карты", callback_data="moba_my_cards"),
         InlineKeyboardButton("👝 Сумка", callback_data="bag")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    if photos.photos:
        await update.message.reply_photo(
            photo=photos.photos[0][0].file_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
    else:
        try:
            with open(DEFAULT_PROFILE_IMAGE, 'rb') as photo:
                await update.message.reply_photo(
                    photo=photo,
                    caption=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
        except FileNotFoundError:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def premium_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Генерируем ссылку заранее
    invoice_link = await context.bot.create_invoice_link(
        title="Премиум",
        description="30 дней подписки",
        payload="premium_30",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice("Цена", 1)]
    )

    text = (
        "🚀 <b>Premium</b>\n\n"
        "<blockquote>• 🔥 Шанс на особые карты увеличен на 10%\n"  # Это относится к случайной редкости, но у нас сейчас фиксированная. Можно переформулировать.
        "• ⏳ Время получения следующей карты снижено на 25%\n"
        "• 💰 Выпадение монет увеличено на 20 %\n"
        "• 🚀 Значок в топе\n\n"
        "Срок действия • 30 дней</blockquote>"
    )
    # Кнопка сразу ведет на оплату
    keyboard = [[InlineKeyboardButton("🚀 Купить за 3 • ⭐️", url=invoice_link)]]

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

def get_server_time():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

async def check_shop_reset(user):
    """Сброс лимитов: бустеры каждый день, удача/защита по понедельникам"""
    now = datetime.now(timezone.utc)
    last_reset = user.get("shop_last_reset")
    
    if not last_reset:
        user["shop_last_reset"] = now
        return user
    if isinstance(last_reset, str):
        last_reset = datetime.fromisoformat(last_reset)

    # Ежедневный сброс бустеров
    if now.date() > last_reset.date():
        user["bought_booster_today"] = 0
    
    # Сброс в понедельник
    if now.weekday() == 0 and last_reset.weekday() != 0:
        user["bought_luck_week"] = 0
        user["bought_protection_week"] = 0
    
    user["shop_last_reset"] = now
    return user

async def shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await asyncio.to_thread(get_moba_user, user_id)
    user = await check_shop_reset(user)
    await asyncio.to_thread(save_moba_user, user)

    time_str = get_server_time()
    server_time = datetime.now(timezone.utc).strftime("%H:%M")
    text = (
        f"🛒 <b>МАГАЗИН ОБНОВЛЕНИЙ</b>n"
        f"🕒 Время сервера: <code>{time_str}</code>n"
        f"📅 Обновление лимитов: Каждый понедельникn"
        f"➖➖➖➖➖➖➖➖➖➖n"
        f"💰 Баланс: {user['coins']} БО | {user['diamonds']} 💎n"
        f"➖➖➖➖➖➖➖➖➖➖n"
        f"1. ⚡️ <b>Бустер</b> (-2ч к мобе): 10 БОn"
        f"   <i>Куплено сегодня: {user.get('bought_booster_today', 0)}/2</i>n"
        f"2. 🍀 <b>Удача</b> (+10% к 4★+): 15 БОn"
        f"   <i>На неделю: {user.get('bought_luck_week', 0)}/5</i>n"
        f"3. 🛡 <b>Защита звезды</b>: 20 БОn"
        f"   <i>На неделю: {user.get('bought_protection_week', 0)}/2</i>n"
    )

    keyboard = [
        [InlineKeyboardButton("⚡️ Купить Бустер", callback_data="buy_item_booster"),
         InlineKeyboardButton("🍀 Купить Удачу", callback_data="buy_item_luck")],
        [InlineKeyboardButton("🛡 Защита звезды", callback_data="buy_item_protect")],
        [InlineKeyboardButton("📦 Наборы карт (за Алмазы)", callback_data="shop_packs_diamonds")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="delete_message")]
    ]
    
    if update.callback_query:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)


async def shop_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий кнопок в магазине"""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    
    user = await asyncio.to_thread(get_moba_user, user_id)
    user = await check_shop_reset(user) # На всякий случай проверяем сброс при клике
    
    response_text = ""

    if data == "buy_shop_booster":
        if user["coins"] < 10:
            await query.answer("❌ Недостаточно БО!", show_alert=True)
            return
        if user.get("bought_booster_today", 0) >= 2:
            await query.answer("❌ Лимит (2 в день) исчерпан!", show_alert=True)
            return
        
        user["coins"] -= 10
        user["bought_booster_today"] += 1
        user["last_mobba_time"] -= 7200 # -2 часа
        response_text = "✅ Бустер активирован! Кулдаун уменьшен на 2 часа."

    elif data == "buy_shop_luck":
        if user["coins"] < 15:
            await query.answer("❌ Недостаточно БО!", show_alert=True)
            return
        if user.get("bought_luck_week", 0) >= 5:
            await query.answer("❌ Лимит (5 в неделю) исчерпан!", show_alert=True)
            return
            
        user["coins"] -= 15
        user["bought_luck_week"] += 1
        user["luck_active"] = user.get("luck_active", 0) + 1
        response_text = "✅ Удача куплена! Шанс повышен на следующее получение карты."

    elif data == "buy_shop_protect":
        if user["coins"] < 20:
            await query.answer("❌ Недостаточно БО!", show_alert=True)
            return
        if user.get("bought_protection_week", 0) >= 2:
            await query.answer("❌ Лимит (2 в неделю) исчерпан!", show_alert=True)
            return
            
        user["coins"] -= 20
        user["bought_protection_week"] += 1
        user["protection_active"] = user.get("protection_active", 0) + 1
        response_text = "✅ Защита куплена! Сработает автоматически при проигрыше в 'регнуть'."

    if response_text:
        await asyncio.to_thread(save_moba_user, user)
        await query.answer(response_text, show_alert=True)
        # Обновляем сообщение магазина, чтобы цифры лимитов изменились
        # Здесь можно вызвать ту же логику отрисовки текста, что в shop()
        await query.edit_message_caption(caption="Обновление баланса...") # Упрощенно


async def handle_shop_purchase(query, user, item_type):
    now = datetime.now(timezone.utc)
    
    if item_type == "booster":
        if user["coins"] < 10: return "❌ Недостаточно БО"
        if user.get("bought_booster_today", 0) >= 2: return "❌ Лимит на сегодня исчерпан"
        
        user["coins"] -= 10
        user["bought_booster_today"] += 1
        # Сокращаем время последнего получения на 2 часа
        user["last_mobba_time"] -= 7200 
        await asyncio.to_thread(save_moba_user, user)
        return "✅ Бустер активирован! Время ожидания сокращено на 2 часа."

    elif item_type == "luck":
        if user["coins"] < 15: return "❌ Недостаточно БО"
        if user.get("bought_luck_week", 0) >= 5: return "❌ Лимит на неделю исчерпан"
        
        user["coins"] -= 15
        user["bought_luck_week"] += 1
        user["luck_active"] = user.get("luck_active", 0) + 1
        await asyncio.to_thread(save_moba_user, user)
        return "✅ Удача куплена! Шанс на редкие карты повышен на следующую попытку."

    elif item_type == "protect":
        if user["coins"] < 20: return "❌ Недостаточно БО"
        if user.get("bought_protection_week", 0) >= 2: return "❌ Лимит на неделю исчерпан"
        
        user["coins"] -= 20
        user["bought_protection_week"] += 1
        user["protection_active"] = user.get("protection_active", 0) + 1
        await asyncio.to_thread(save_moba_user, user)
        return "✅ Защита куплена! В следующий раз при проигрыше в 'регнуть' вы не потеряете звезду."

    return "Ошибка"


async def shop_packs_diamonds(query, user):
    text = (
        "📦 <b>Магазин наборов</b>\n"
        "Карты выпадают сразу в инвентарь!\n"
        f"💎 Баланс: {user['diamonds']}\n\n"
        "1★ (3 шт) — 1800 💎\n"
        "2★ (3 шт) — 2300 💎\n"
        "3★ (3 шт) — 3400 💎\n"
        "4★ (3 шт) — 5700 💎\n"
        "5★ (3 шт) — 7500 💎\n"
        "LIMITED (3 шт) — 15000 💎"
    )
    kb = [
        [InlineKeyboardButton("1★", callback_data="buy_pack_1"), InlineKeyboardButton("2★", callback_data="buy_pack_2")],
        [InlineKeyboardButton("3★", callback_data="buy_pack_3"), InlineKeyboardButton("4★", callback_data="buy_pack_4")],
        [InlineKeyboardButton("5★", callback_data="buy_pack_5"), InlineKeyboardButton("LTD", callback_data="buy_pack_ltd")],
        [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

# --- ОБРАБОТКА ПЛАТЕЖЕЙ (STARS) ---
async def start_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Параметры платежа (те же, что были)
    if query.data == "buy_prem":
        title = "Премиум подписка"
        description = "Доступ к премиум функциям на 30 дней"
        payload = "premium_30"
        price = 3
    elif query.data == "shop_coins":
        title = "100 Монет"
        description = "Игровая валюта"
        payload = "coins_100"
        price = 1
    else:
        return

    # 1. Генерируем прямую ссылку на оплату (Stars)
    invoice_link = await context.bot.create_invoice_link(
        title=title,
        description=description,
        payload=payload,
        provider_token="",  # Для Stars пусто
        currency="XTR",
        prices=[LabeledPrice("Цена", price)]
    )

    # 2. Создаем кнопку с этой ссылкой
    keyboard = [
        [InlineKeyboardButton(f"💳 Подтвердить оплату ({price} ⭐️)", url=invoice_link)],
        [InlineKeyboardButton("< Отмена", callback_query_handler="shop")]  # Или другой возврат
    ]

    # 3. Редактируем старое сообщение, вставляя кнопку оплаты
    await query.edit_message_text(
        text=f"{title}\n\n{description}\n\nНажмите на кнопку ниже для перехода к оплате:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

async def handle_bag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Текст сообщения
    msg_text = "<b>👝 Сумка</b>\n<blockquote>Ваша сумка пока пуста</blockquote>"

    # Кнопка возврата в профиль

    # Если в сообщении есть фото (профиль обычно с фото), его лучше удалить и отправить текст,
    # либо просто заменить подпись. Здесь мы заменяем текст/подпись:
    if query.message.photo:
        # Если хотим просто текст вместо фото:
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=msg_text,

            parse_mode=ParseMode.HTML
        )
    else:
        await query.edit_message_text(
            text=msg_text,

            parse_mode=ParseMode.HTML
        )

async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    # Всегда отвечаем True для Stars
    await query.answer(ok=True)

async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment = update.message.successful_payment
    user_id = update.effective_user.id
    payload = payment.invoice_payload

    logger.info(f"[{user_id}] successful_payment_callback triggered. Payload: {payload}")

    user = await asyncio.to_thread(get_moba_user, user_id)  # get_moba_user тоже блокирующая
    if user is None:
        logger.error(f"[{user_id}] Failed to get user data in successful_payment_callback.")
        await update.message.reply_text("Произошла ошибка при обработке платежа. Пожалуйста, попробуйте позже.")
        return

    current_premium_until_before = user.get("premium_until")
    logger.info(f"[{user_id}] Premium until BEFORE update: {current_premium_until_before}")

    current_time_utc = datetime.now(timezone.utc)

    if payload == "premium_30":
        # Если уже есть премиум, продлеваем его от текущей даты премиума, иначе от текущего момента
        if current_premium_until_before and current_premium_until_before > current_time_utc:
            user["premium_until"] = current_premium_until_before + timedelta(days=30)
            logger.info(f"[{user_id}] Extending premium. New premium_until: {user['premium_until']}")
        else:
            user["premium_until"] = current_time_utc + timedelta(days=30)
            logger.info(f"[{user_id}] Activating new premium. New premium_until: {user['premium_until']}")

        await update.message.reply_text("🚀 Премиум активирован на 30 дней!", parse_mode=ParseMode.HTML)
    elif payload == "coins_100":
        user["coins"] += 100
        await update.message.reply_text("💰 Вы купили 100 монет!")
    elif payload.startswith("card_pack_"):
        category = payload.split('_')[2]
        await update.message.reply_text(f"📦 Вы получили набор карт из категории '{category}'!")
    else:
        await update.message.reply_text("Спасибо за покупку, но не удалось определить, что вы купили.")

    logger.info(f"[{user_id}] Premium until AFTER update (before save): {user.get('premium_until')}")

    # --- ДОБАВЛЕННАЯ СТРОКА ---
    await asyncio.to_thread(save_moba_user, user)
    logger.info(f"[{user_id}] User data saved after payment.")

    # Дополнительная проверка: получить пользователя снова, чтобы убедиться, что данные сохранились
    updated_user = await asyncio.to_thread(get_moba_user, user_id)
    logger.info(f"[{user_id}] Premium until AFTER save and re-fetch: {updated_user.get('premium_until')}")

    if updated_user and updated_user.get("premium_until") and updated_user["premium_until"] > current_time_utc:
        logger.info(f"[{user_id}] Premium successfully updated and re-fetched from DB.")
    else:
        logger.warning(f"[{user_id}] Premium update might have failed or not reflected in re-fetch.")

# --- ТОП ---
async def top_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Первое окно при команде /top"""
    keyboard = [
        [InlineKeyboardButton("🃏 Карточный бот", callback_data="top_category_cards")],
        [InlineKeyboardButton("🎮 Игровой бот", callback_data="top_category_game")]
    ]
    msg = "🏆 <b>Главное меню рейтинга</b>\n\nВыберите категорию, по которой хотите увидеть лучших игроков:"

    if update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

async def top_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "top_category_cards":
        keyboard = [
            [InlineKeyboardButton("✨ По очкам", callback_data="top_points"),
             InlineKeyboardButton("🃏 По картам", callback_data="top_cards")],
            [InlineKeyboardButton("< Назад", callback_data="top_main")]
        ]
        await query.edit_message_text("🏆 <b>Рейтинг коллекционеров</b>", reply_markup=InlineKeyboardMarkup(keyboard),
                                      parse_mode=ParseMode.HTML)

    elif query.data == "top_category_game":
        keyboard = [
            [InlineKeyboardButton("🌟 Топ сезона", callback_data="top_stars_season"),
             InlineKeyboardButton("🌍 За все время", callback_data="top_stars_all")],
            [InlineKeyboardButton("< Назад", callback_data="top_main")]
        ]
        await query.edit_message_text("🏆 <b>Рейтинг игроков (Ранг)</b>", reply_markup=InlineKeyboardMarkup(keyboard),
                                      parse_mode=ParseMode.HTML)

async def show_specific_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    title = ""
    sorted_users = []

    if data == "top_points":
        title = "Топ по очкам"
        sorted_users = sorted(users.values(), key=lambda x: x['points'], reverse=True)[:10]
        suffix = "очков"
    elif data == "top_cards":
        title = "Топ по картам"
        sorted_users = sorted(users.values(), key=lambda x: len(x['cards']), reverse=True)[:10]
        suffix = "карт"
    elif data == "top_stars_season":
        title = "Топ сезона (Звезды)"
        sorted_users = sorted(users.values(), key=lambda x: x['stars'], reverse=True)[:10]
        suffix = "⭐"
    elif data == "top_stars_all":
        title = "Топ всех времен (Звезды)"
        sorted_users = sorted(users.values(), key=lambda x: x['stars_all_time'], reverse=True)[:10]
        suffix = "⭐"

    text = f"🏆 <b>{title}</b>\n\n"
    if not sorted_users:
        text += "<i>Рейтинг пока пуст</i>"
    else:
        for i, u in enumerate(sorted_users, 1):
            is_prem = u["premium_until"] and u["premium_until"] > datetime.now()
            prem_icon = "🚀 " if is_prem else ""

            if data == "top_points":
                val = u['points']
            elif data == "top_cards":
                val = len(u['cards'])
            elif data == "top_stars_season":
                val = u['stars']
            else:
                val = u['stars_all_time']

            text += f"{i}. {prem_icon}{u['nickname']} — <b>{val}</b> {suffix}\n"

    back_button = "top_category_cards" if data in ["top_points", "top_cards"] else "top_category_game"
    keyboard = [[InlineKeyboardButton("< Назад", callback_data=back_button)]]

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

async def show_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "top_points":
        sorted_users = sorted(users.values(), key=lambda x: x['points'], reverse=True)[:10]
        title = "Топ по очкам"
    else:
        sorted_users = sorted(users.values(), key=lambda x: len(x['cards']), reverse=True)[:10]
        title = "Топ по картам"

    text = f"🏆 **{title}**\n\n"
    if not sorted_users:
        text += "Топ пока пуст."
    else:
        for i, u in enumerate(sorted_users, 1):
            is_prem = u["premium_until"] and u["premium_until"] > datetime.now()
            prem_icon = "🚀 " if is_prem else ""
            val = u['points'] if query.data == "top_points" else len(u['cards'])
            text += f"{i}. {u['nickname']} {prem_icon} — {val}\n"

    # ПРОВЕРКА: Если есть фото, правим подпись, если нет - текст
    if query.message.photo:
        await query.edit_message_caption(caption=text, parse_mode="Markdown")
    else:
        await query.edit_message_text(text, parse_mode="Markdown")

# --- ОБРАБОТЧИК КАРТ (Мои карты) ---

async def handle_moba_my_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cb_base = (query.data or "moba_my_cards").rsplit("_", 1)[0]
    if is_recent_callback(query.from_user.id, cb_base):
        # уже обрабатывался недавно — просто игнорируем
        return
    user_id = query.from_user.id

    # Для подсчета общего количества карт используем get_user_inventory (который работает с moba_inventory)
    user_cards = await asyncio.to_thread(get_user_inventory, user_id)
    total_cards_count = len(user_cards)

    # Определяем, есть ли у пользователя карты для отображения
    has_cards = total_cards_count > 0

    if not has_cards:
        # Если карт нет, показываем сообщение и только кнопку "Назад в профиль"
        msg_text = ("<b>🃏 У тебя нет карт</b>\n"
                    "<blockquote>Получи карту командой «моба»</blockquote>")
        keyboard = None

    else:
        # Если карты есть, формируем меню как в старом примере, но с MOBA callback'ами
        msg_text = (f"<b>🃏 Ваши карты</b>\n"
                    f"<blockquote>Всего {len(user_cards)}/269 карт</blockquote>")  # Исправлено здесь
        keyboard_layout = [
            [InlineKeyboardButton("❤️‍🔥 Коллекции", callback_data="moba_show_collections")],
            [InlineKeyboardButton("🪬 LIMITED", callback_data="moba_show_cards_rarity_LIMITED_0")],
            [InlineKeyboardButton("🃏 Все карты", callback_data="moba_show_cards_all_0")]
        ]

        # Добавляем кнопку "Назад в профиль"

        keyboard = InlineKeyboardMarkup(keyboard_layout)

    # Логика отправки/редактирования сообщения
    if query.message.photo:
        # Если текущее сообщение - фото, мы не можем редактировать текст на текст.
        # Удаляем фото и шлем новое текстовое сообщение.
        await query.message.delete()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=msg_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    else:
        # Если текущее сообщение - текст, просто редактируем его.
        await query.edit_message_text(
            text=msg_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )

async def moba_get_sorted_user_cards_list(user_id: int) -> List[dict]:
    """Возвращает список карт пользователя (moba_inventory) в удобном для просмотра формате."""
    rows = get_user_inventory(user_id)  # возвращает list[dict] из БД
    # Сортируем по получению (obtained_at) если поле есть, иначе по card_id
    try:
        sorted_rows = sorted(rows, key=lambda r: r.get('obtained_at') or r.get('id') or r.get('card_id'))
    except Exception:
        sorted_rows = rows[:]
    return sorted_rows

def _moba_card_caption(card_row: dict, index: int, total: int) -> str:
    name = card_row.get('card_name') or CARDS.get(card_row.get('card_id'), {}).get('name', 'Карта')
    collection = card_row.get('collection') or CARDS.get(card_row.get('card_id'), {}).get('collection', '')
    rarity = card_row.get('rarity', '—')
    bo = card_row.get('bo', '—')
    points = card_row.get('points', '—')
    diamonds = card_row.get('diamonds', 0)
    caption = (f"<b>🃏 {collection} • {name}</b>\n"
               f"<blockquote>Очков: <b>{points}</b></blockquote>\n\n"
               f"✨ Редкость: <i>{rarity}</i>\n"
               f"💰 БО: <i>{bo}</i>\n"
               f"💎 Алмазы: <i>{diamonds}</i>\n\n"
               f"                                ")
    return caption

async def moba_show_cards_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cb_base = (query.data or "moba_my_cards").rsplit("_", 1)[0]
    if is_recent_callback(query.from_user.id, cb_base):
        return
    data = query.data
    try:
        index = int(data.split("_")[-1])
    except Exception:
        index = 0

    user_id = query.from_user.id
    logger.info(f"Вызов moba_get_sorted_user_cards_list для пользователя {user_id}")
    cards = await moba_get_sorted_user_cards_list(user_id)  # <--- ЗДЕСЬ БЫЛА ОШИБКА, НУЖНО await
    logger.info(f"Тип 'cards' после await: {type(cards)}")
    logger.info(f"Значение 'cards' после await: {cards}")

    if not cards:
        # Если у пользователя нет карт
        try:
            await query.edit_message_text(
                "У вас пока нет карт, полученных командой 'моба'. Попробуйте написать 'моба'.")
        except BadRequest:
            await query.bot.send_message(chat_id=user_id,
                                         text="У вас пока нет карт, полученных командой 'моба'. Попробуйте написать 'моба'.")
        return

    if index < 0: index = 0
    if index >= len(cards): index = len(cards) - 1

    card = cards[index]
    # Путь к фото: либо в колонке card_name/path, либо по CARDS[card_id]['path']
    photo_path = card.get('image_path') or CARDS.get(card.get('card_id'), {}).get('path') or PHOTO_DETAILS.get(
        card.get('card_id'), {}).get('path')

    caption = _moba_card_caption(card, index, len(cards))

    # Навигация
    nav = []
    if index > 0:
        nav.append(
            InlineKeyboardButton("<", callback_data=f"moba_show_cards_all_{index - 1}"))  # Исправлен callback_data
    nav.append(InlineKeyboardButton(f"{index + 1}/{len(cards)}", callback_data="moba_ignore"))
    if index < len(cards) - 1:
        nav.append(
            InlineKeyboardButton(">", callback_data=f"moba_show_cards_all_{index + 1}"))  # Исправлен callback_data

    keyboard = [nav, [InlineKeyboardButton("< В коллекцию",
                                           callback_data="moba_show_collections")]]  # Исправлена кнопка "Назад"

    try:
        if query.message.photo:  # Если текущее сообщение — фото, пробуем редактировать media
            with open(photo_path, "rb") as ph:
                await query.edit_message_media(InputMediaPhoto(media=ph, caption=caption, parse_mode=ParseMode.HTML),
                                               reply_markup=InlineKeyboardMarkup(keyboard))
        else:  # <--- ЭТОТ ELSE ДОЛЖЕН БЫТЬ НА ОДНОМ УРОВНЕ С IF ВНУТРИ TRY
            # Удаляем старое сообщение (если это нужно) и отправляем новое фото
            await query.message.delete()
            with open(photo_path, "rb") as ph:
                await context.bot.send_photo(chat_id=query.message.chat_id, photo=ph, caption=caption,
                                             reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except FileNotFoundError:
        logger.error(f"Photo not found for moba card: {photo_path}")
        try:
            await query.edit_message_text(caption + "\n\n(Фото не найдено)",
                                          reply_markup=InlineKeyboardMarkup(keyboard),
                                          parse_mode=ParseMode.HTML)
        except Exception:
            await query.bot.send_message(chat_id=query.from_user.id, text=caption + "\n\n(Фото не найдено)",
                                         reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except BadRequest as e:
        logger.warning(f"BadRequest in moba_show_cards_all: {e}", exc_info=True)
        try:
            with open(photo_path, "rb") as ph:
                await context.bot.send_photo(chat_id=query.from_user.id, photo=ph, caption=caption,
                                             reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        except Exception as e2:
            logger.error(f"Failed to fallback send photo in moba_show_cards_all: {e2}", exc_info=True)
            await context.bot.send_message(chat_id=query.from_user.id, text=caption, parse_mode=ParseMode.HTML)

async def handle_moba_collections(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    current_page = 0

    if query.data == "ignore_me":
        logger.info("handle_moba_collections: 'ignore_me' callback received, answering query and returning.")
        return
    cb_base = (query.data or "moba_my_cards").rsplit("_", 1)[0]
    if is_recent_callback(query.from_user.id, cb_base):
        return
    if query.data and query.data.startswith("moba_collections_page_"):
        try:
            current_page = int(query.data.split('_')[-1])
        except ValueError:
            current_page = 0

    rows = await asyncio.to_thread(get_user_inventory, user_id)
    if not rows:
        try:
            await query.edit_message_text("У вас пока нет карт, полученных командой 'моба'.")
        except Exception as e:
            # Логируем ошибку здесь, чтобы понять, что пошло не так с edit_message_text
            logger.error(f"Ошибка при edit_message_text в handle_moba_collections (нет карт): {e}")
            await context.bot.send_message(chat_id=user_id, text="У вас пока нет карт, полученных командой 'моба'.")
        return

    collections_data = {}
    for r in rows:
        col = r.get('collection') or "z"
        collections_data.setdefault(col, set()).add(r.get('card_id'))

    sorted_collection_names = sorted([col_name for col_name in collections_data.keys() if col_name != "z"])

    total_collections = len(sorted_collection_names)
    total_pages = (total_collections + COLLECTIONS_PER_PAGE - 1) // COLLECTIONS_PER_PAGE

    start_index = current_page * COLLECTIONS_PER_PAGE
    end_index = min(start_index + COLLECTIONS_PER_PAGE, total_collections)
    collections_on_page = sorted_collection_names[start_index:end_index]
    keyboard = []
    for col_name in collections_on_page:
        ids = collections_data[col_name]
        total_in_col = sum(1 for cid, cdata in CARDS.items() if cdata.get('collection') == col_name)
        owned_unique = len(ids)
        btn_text = f"{col_name} ({owned_unique}/{total_in_col})"
        safe_name = urllib.parse.quote_plus(col_name)
        callback_data_for_button = f"moba_view_col_{safe_name}_0"
        logger.info(
            f"Генерируем callback_data для коллекции: '{callback_data_for_button}' (длина: {len(callback_data_for_button.encode('utf-8'))} байт)")
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data_for_button)])
    # Добавляем кнопки пагинации
    pagination_buttons = []
    if current_page > 0:
        callback_data_prev = f"moba_collections_page_{current_page - 1}"
        logger.info(
            f"Генерируем callback_data для пагинации (назад): '{callback_data_prev}' (длина: {len(callback_data_prev.encode('utf-8'))} байт)")
        pagination_buttons.append(
            InlineKeyboardButton("< Назад", callback_data=callback_data_prev))
    # Добавляем индикатор страницы, если есть несколько страниц
    if total_pages > 1:
        callback_data_ignore = "ignore_me"
        logger.info(
            f"Генерируем callback_data для индикатора страницы: '{callback_data_ignore}' (длина: {len(callback_data_ignore.encode('utf-8'))} байт)")
        pagination_buttons.append(
            InlineKeyboardButton(f"{current_page + 1}/{total_pages}",
                                 callback_data=callback_data_ignore))  # Кнопка-заглушка
    if current_page < total_pages - 1:
        callback_data_next = f"moba_collections_page_{current_page + 1}"
        logger.info(
            f"Генерируем callback_data для пагинации (вперед): '{callback_data_next}' (длина: {len(callback_data_next.encode('utf-8'))} байт)")
        pagination_buttons.append(
            InlineKeyboardButton("Вперед >", callback_data=callback_data_next))
    if pagination_buttons:
        keyboard.append(pagination_buttons)
    callback_data_back_to_my_cards = "moba_my_cards"
    logger.info(
        f"Генерируем callback_data для кнопки 'Назад': '{callback_data_back_to_my_cards}' (длина: {len(callback_data_back_to_my_cards.encode('utf-8'))} байт)")
    keyboard.append([InlineKeyboardButton("< Назад", callback_data=callback_data_back_to_my_cards)])
    text = "❤️‍🔥 <b>Ваши коллекции</b>\n<blockquote>Выберите коллекцию для просмотра</blockquote>"
    if total_pages > 1:
        text += f"\n<i>Страница {current_page + 1} из {total_pages}</i>"
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при edit_message_text в handle_moba_collections: {e}")
        logger.error(f"Попытка отправить новое сообщение после ошибки edit_message_text. Клавиатура: {keyboard}")
        await context.bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def moba_view_collection_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показываем карты конкретной MOBA-коллекции: callback moba_view_col_{safe_col}_{index}"""
    query = update.callback_query
    await query.answer()

    # Ожидаемый формат callback: "moba_view_col_{safe_col}_{index}"
    prefix = "moba_view_col_"
    if not query.data.startswith(prefix):
        await query.answer("Неверный формат callback", show_alert=True)
        return

    rest = query.data[len(prefix):]  # всё после префикса
    # Разделяем на закодированное имя коллекции и индекс (по последнему '_')
    try:
        safe_enc, idx_str = rest.rsplit("_", 1)
        idx = int(idx_str)
    except Exception:
        safe_enc = rest
        idx = 0

    collection_name = urllib.parse.unquote_plus(safe_enc)

    # Получаем все карты пользователя и фильтруем по collection (точное совпадение)
    rows = await asyncio.to_thread(get_user_inventory, query.from_user.id)
    filtered = [r for r in rows if (r.get('collection') or "") == collection_name]

    if not filtered:
        try:
            await query.edit_message_text("У вас пока нет карт в этой коллекции.")
        except Exception:
            await context.bot.send_message(chat_id=query.from_user.id, text="У вас пока нет карт в этой коллекции.")
        return

    # Вызываем общую функцию показа фильтрованного набора
    await _moba_send_filtered_card(query, context, filtered, idx, back_cb="moba_show_collections")

async def moba_show_cards_by_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback: moba_show_cards_rarity_{RARITY}_{index}"""
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    # parts example: ['moba','show','cards','rarity','LIMITED','0']  OR if format is 'moba_show_cards_rarity_LIMITED_0'
    # try both:
    if len(parts) >= 6 and parts[0] == "moba" and parts[1] == "show":
        rarity = parts[4]
        try:
            index = int(parts[5])
        except:
            index = 0
    else:
        # fallback parse 'moba_show_cards_rarity_LIMITED_0'
        try:
            _, _, _, rarity, idx = query.data.split("_")
            index = int(idx)
        except Exception:
            fragments = query.data.split("_")
            rarity = fragments[-2] if len(fragments) >= 2 else fragments[-1]
            try:
                index = int(fragments[-1])
            except:
                index = 0

    # Получаем все карты пользователя
    rows = await asyncio.to_thread(get_user_inventory, query.from_user.id)
    filtered = [r for r in rows if (r.get('rarity') or "").upper() == rarity.upper()]

    if not filtered:
        await query.answer(f"У вас нет карт редкости {rarity}.", show_alert=True)
        return

    await _moba_send_filtered_card(query, context, filtered, index, back_cb="moba_my_cards")

# Навигация внутри отфильтрованных показов (для кнопок moba_filter_move_{index})
async def back_to_profile_from_moba(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # вызываем вашу функцию профиля — у вас есть `edit_to_notebook_menu` и `profile`.
    # Если хотите вернуть именно всплывающий профиль (как в profile()), просто вызовите profile
    # Но profile ожидает Update с message, а у нас callback — поэтому используем edit_to_notebook_menu если хотите медиа.
    # Я вызову edit_to_notebook_menu (который у вас есть) если он подходит:
    try:
        await edit_to_notebook_menu(query, context)
    except Exception:
        # fallback: если не получилось, просто отправим текстовый профиль
        user = get_moba_user(query.from_user.id)
        if user:
            curr_rank, curr_stars = get_rank_info(user.get("stars", 0))
            text = (f"👤 Профиль: {user.get('nickname', 'моблер')}\n"
                    f"🏆 Ранг: {curr_rank} ({curr_stars})\n"
                    f"🃏 Карт: {len(user.get('cards', []))}\n"
                    f"✨ Очков: {user.get('points', 0)}")
            try:
                await query.edit_message_text(text)
            except Exception:
                await context.bot.send_message(chat_id=query.from_user.id, text=text)

async def handle_collections_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = get_user(query.from_user.id)

    # 1. Получаем названия коллекций ТОЛЬКО тех карт, которые есть у пользователя
    # Мы проходим по user["cards"] и собираем уникальные имена коллекций
    user_owned_collections = sorted(list(set(c['collection'] for c in user["cards"] if c.get('collection'))))

    if not user_owned_collections:
        text = "❤️‍🔥 <b>Ваши коллекции</b>\n\n<blockquote>У вас пока нет карт, принадлежащих какой-либо коллекции.</blockquote>"
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("< Назад", callback_data="my_cards")]])
    else:
        keyboard = []
        for col_name in user_owned_collections:
            # Считаем сколько УНИКАЛЬНЫХ карт этой коллекции есть у игрока
            # (используем set, чтобы если у игрока 5 одинаковых карт, они считались как 1 в прогрессе коллекции)
            owned_ids_in_this_col = set(c['card_id'] for c in user["cards"] if c.get('collection') == col_name)
            count_in_col = len(owned_ids_in_this_col)

            # Считаем сколько всего карт в этой коллекции существует в глобальной базе CARDS
            total_in_col = sum(1 for c in CARDS if c.get('collection') == col_name)

            # Добавляем кнопку коллекции
            button_text = f"{col_name} ({count_in_col}/{total_in_col})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"view_col_{col_name}_0")])

        keyboard.append([InlineKeyboardButton("< Назад", callback_data="my_cards")])
        text = "❤️‍🔥 <b>Ваши коллекции</b>\n<blockquote>Выберите коллекцию для просмотра</blockquote>"
        markup = InlineKeyboardMarkup(keyboard)

    # Отображение
    try:
        await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Exception:
        await query.delete_message()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML
        )

# 2. ПРОСМОТР КАРТОЧЕК КОЛЛЕКЦИИ (с перелистыванием)
async def view_collection_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    cb_base = (query.data or "view_col").rsplit("_", 1)[0]
    if is_recent_callback(query.from_user.id, cb_base):
        return
    user = get_user(query.from_user.id)

    data = query.data.split("_")
    col_name, index = data[2], int(data[3])

    filtered = [c for c in user["cards"] if c["collection"] == col_name]
    card = filtered[index]

    caption = (f"<b><i>🃏 {col_name} •  {card['name']}</i></b>\n"
               f"<blockquote><b><i>Принесла вас {card['points']} очков !</i></b></blockquote>\n\n"
               f"<b>✨ Редкость •</b> <i>{card['rarity']}</i>\n"
               f"<b>💰 БО •</b><i> {card['bo']}</i>\n"
               f"<b>💎 Алмазы •</b> <i>{card['diamonds']}</i>\n\n"
               f"<blockquote><b><i>Карта добавлена в коллекцию!</i></b></blockquote>")

    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton("<", callback_data=f"view_col_{col_name}_{index - 1}"))
    if index < len(filtered) - 1:
        nav.append(InlineKeyboardButton(">", callback_data=f"view_col_{col_name}_{index + 1}"))

    kb = [nav, [InlineKeyboardButton("К коллекциям", callback_data="show_collections")]]

    with open(card["image_path"], 'rb') as photo:
        if query.message.photo:
            await query.edit_message_media(InputMediaPhoto(photo, caption=caption, parse_mode=ParseMode.HTML),
                                           reply_markup=InlineKeyboardMarkup(kb))
        else:
            await query.message.delete()
            await context.bot.send_photo(query.message.chat_id, photo, caption=caption,
                                         reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

def get_card_view_markup(card, index, total, filter_type, filter_value):
    caption = (
        f"<b>⚜️ «{card['collection']}»</b>\n"
        f"<blockquote><i>Карта: {card['name']}</i></blockquote>\n\n"
        f"<b>✨ Редкость •</b> <i>{card['rarity']}</i>\n"
        f"<b>💰 БО •</b><i> {card['bo']}</i>\n"
        f"<b>💎 Алмазы •</b> <i>{card['diamonds']}</i>\n"
        f"<b>🔢 {index + 1} из {total}</b>"
    )

    nav_buttons = []
    if index > 0:
        nav_buttons.append(InlineKeyboardButton("<", callback_data=f"move_{filter_type}_{filter_value}_{index - 1}"))
    if index < total - 1:
        nav_buttons.append(InlineKeyboardButton(">", callback_data=f"move_{filter_type}_{filter_value}_{index + 1}"))

    keyboard = [nav_buttons, [InlineKeyboardButton("< Назад", callback_data="my_cards")]]
    return caption, InlineKeyboardMarkup(keyboard)

async def show_filtered_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = get_user(query.from_user.id)

    # pattern: show_cards_{type}_{value}
    parts = query.data.split('_')
    if len(parts) < 4: return

    f_type, f_value = parts[2], parts[3]

    if f_type == "all":
        filtered = user["cards"]
    elif f_type == "rarity":
        filtered = [c for c in user["cards"] if c["rarity"] == f_value]
    else:
        filtered = []

    if not filtered:
        await query.answer("Карт не найдено", show_alert=True)
        return

    # Берем первую карту для показа
    card = filtered[0]
    caption, reply_markup = get_card_view_markup(card, 0, len(filtered), f_type, f_value)

    try:
        # Удаляем старое текстовое сообщение и отправляем фото
        await query.message.delete()
        with open(card["image_path"], 'rb') as photo:
            await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=photo,
                caption=caption,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logging.error(f"Error in show_filtered: {e}")
        await context.bot.send_message(query.message.chat_id, "Ошибка при загрузке фото.")

async def move_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = get_user(query.from_user.id)

    # pattern: move_{type}_{value}_{index}
    parts = query.data.split('_')
    f_type, f_value, index = parts[1], parts[2], int(parts[3])

    if f_type == "all":
        filtered = user["cards"]
    elif f_type == "rarity":
        filtered = [c for c in user["cards"] if c["rarity"] == f_value]
    else:
        filtered = []

    card = filtered[index]
    caption, reply_markup = get_card_view_markup(card, index, len(filtered), f_type, f_value)

    try:
        with open(card["image_path"], 'rb') as photo:
            await query.edit_message_media(
                media=InputMediaPhoto(media=photo, caption=caption, parse_mode=ParseMode.HTML),
                reply_markup=reply_markup
            )
    except Exception as e:
        logging.error(f"Error in move_card: {e}")

async def back_to_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Просто вызываем функцию профиля, но адаптированную под callback
    user = get_user(query.from_user.id)
    is_premium = user["premium_until"] and user["premium_until"] > datetime.now()
    prem_status = "✅ Есть" if is_premium else "❌ Нет"

    text = (
        f"👤 **Профиль: {user['nickname']}**\n"
        f"🆔 ID: `{user['id']}`\n"
        f"🎴 Карт: {len(user['cards'])}\n"
        f"📊 Очков: {user['points']}\n"
        f"💎 Алмазов: {user['diamonds']}\n"
        f"💰 Монет: {user['coins']}\n"
        f"👑 Премиум: {prem_status}"
    )
    keyboard = [[InlineKeyboardButton("🃏 Мои карты", callback_data="my_cards"),
                 InlineKeyboardButton("Сумка", callback_data="bag")]]

    # Так как профиль обычно с фото, а мы могли прийти из текстового меню:
    await query.message.delete()
    photos = await update.effective_user.get_profile_photos(limit=1)
    if photos.photos:
        await context.bot.send_photo(chat_id=query.message.chat_id, photo=photos.photos[0][0].file_id,
                                     caption=text, reply_markup=InlineKeyboardMarkup(keyboard),
                                     parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=query.message.chat_id, text=text,
                                       reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# Обертка для декоратора
def access_required(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        # Эта строка (1285) ДОЛЖНА иметь отступ 8 пробелов от левого края
        is_eligible, reason, *optional_markup = await check_command_eligibility(update, context)

        if is_eligible:
            return await func(update, context, *args, **kwargs)
        else:
            markup = optional_markup[0] if optional_markup else None

            # Проверяем, есть ли message, чтобы избежать ошибок в callback_query
            if update.message:
                await update.message.reply_text(reason, parse_mode=ParseMode.HTML, reply_markup=markup)
            elif update.callback_query:
                # Для callback_query отправляем сообщение в личку, если это возможно
                try:
                    await context.bot.send_message(update.callback_query.from_user.id, reason,
                                                   parse_mode=ParseMode.HTML, reply_markup=markup)
                    await update.callback_query.answer("Доступ ограничен. Проверьте личные сообщения.")
                except Exception:
                    await update.callback_query.answer("Доступ ограничен. Не удалось отправить сообщение в личку.")
            return

    return wrapper  # Этот return должен быть на том же уровне, что и @wraps

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
        # ... (внутри функции init_db)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moba_users (
                user_id BIGINT PRIMARY KEY,
                nickname TEXT DEFAULT 'моблер',
                game_id TEXT,
                points INTEGER DEFAULT 0,
                diamonds INTEGER DEFAULT 0,
                coins INTEGER DEFAULT 0,
                stars INTEGER DEFAULT 0,
                max_stars INTEGER DEFAULT 0,
                stars_all_time INTEGER DEFAULT 0,
                reg_total INTEGER DEFAULT 0,
                reg_success INTEGER DEFAULT 0,
                premium_until TIMESTAMP WITH TIME ZONE,
                last_mobba_time DOUBLE PRECISION DEFAULT 0,
                last_reg_time DOUBLE PRECISION DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)

        # Таблица инвентаря карт (у каждого игрока много карт)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moba_inventory (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES moba_users(user_id),
                card_id INTEGER,
                card_name TEXT,
                collection TEXT,
                rarity TEXT,
                bo INTEGER,
                points INTEGER,
                diamonds INTEGER,
                obtained_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        # Таблицы для Игрового Бота "Евангелие" (ГЛОБАЛЬНАЯ СТАТИСТИКА)
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

        # НОВАЯ ТАБЛИЦА: Статистика по чатам (ЛОКАЛЬНАЯ СТАТИСТИКА)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gospel_chat_activity (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                prayer_count INTEGER DEFAULT 0,
                total_piety_score REAL DEFAULT 0,
                PRIMARY KEY (user_id, chat_id)
            );
            CREATE INDEX IF NOT EXISTS idx_gospel_chat_activity_chat_id ON gospel_chat_activity (chat_id);
        """)

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
        cursor.execute("""
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS luck_active INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS protection_active INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS shop_last_reset TIMESTAMP WITH TIME ZONE DEFAULT NOW();
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS bought_booster_today INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS bought_luck_week INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS bought_protection_week INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS pending_boosters INTEGER DEFAULT 0;
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

def get_user_data(user_id, username) -> dict:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute("SELECT data FROM laviska_users WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if row:
            user_data = row['data']
            if user_data.get('username') != username:
                user_data['username'] = username
                update_user_data(user_id, {"username": username})  # Отдельный вызов для обновления в БД
            return user_data
        else:
            initial_data = {
                "username": username,
                "cards": {},
                "crystals": 0,
                "spins": 0,
                "last_spin_time": 0,
                "last_spin_cooldown": COOLDOWN_SECONDS,
                "current_collection_view_index": 0,
                "achievements": []}
            cursor.execute(
                """INSERT INTO laviska_users (user_id, username, data) VALUES (%s, %s, %s)
                   ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, data = EXCLUDED.data, updated_at = NOW()""",
                (user_id, username, json.dumps(initial_data)))
            conn.commit()
            return initial_data
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении данных пользователя Лависки {user_id}: {e}", exc_info=True)
        return {}  # Возвращаем пустой дикт в случае ошибки, чтобы не ломать логику
    finally:
        if conn:
            conn.close()

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
def update_piety_and_prayer_db_chat(user_id: int, chat_id: int, gained_piety: float):
    """Обновляет статистику молитв и набожности для конкретного чата."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Обновляем или вставляем запись для чата
        cursor.execute('''
            INSERT INTO gospel_chat_activity (user_id, chat_id, prayer_count, total_piety_score)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (user_id, chat_id) DO UPDATE SET
                prayer_count = gospel_chat_activity.prayer_count + 1,
                total_piety_score = gospel_chat_activity.total_piety_score + %s
        ''', (user_id, chat_id, gained_piety, gained_piety))

        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении чат-активности для {user_id} в чате {chat_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def get_gospel_leaderboard_by_chat(chat_id: int, sort_by: str, limit: int = 50) -> List[Dict]:
    """
    Получает топ активности для конкретного чата, отображая *глобальную* статистику
    только для пользователей, которые совершили хотя бы одну молитву в этом чате.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        order_clause = "gu.prayer_count DESC" if sort_by == 'prayers' else "gu.total_piety_score DESC"

        # ИЗМЕНЕННЫЙ ЗАПРОС:
        cursor.execute(f"""
            SELECT
                gu.user_id,
                gu.prayer_count,
                gu.total_piety_score,
                gu.first_name_cached,
                gu.username_cached
            FROM gospel_users gu
            WHERE EXISTS (
                SELECT 1
                FROM gospel_chat_activity gca
                WHERE gca.user_id = gu.user_id
                  AND gca.chat_id = %s
            )
            AND gu.gospel_found = TRUE -- Только те, кто нашел Евангелие
            ORDER BY {order_clause}
            LIMIT %s
        """, (chat_id, limit))

        return [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении чат-лидерборда для чата {chat_id}: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def get_gospel_leaderboard_global(sort_by: str, limit: int = 50) -> List[Dict]:
    """Получает глобальный топ активности."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        order_clause = "prayer_count DESC" if sort_by == 'prayers' else "total_piety_score DESC"

        cursor.execute(f"""
            SELECT
                user_id,
                prayer_count,
                total_piety_score,
                first_name_cached,
                username_cached
            FROM gospel_users
            WHERE gospel_found = TRUE
            ORDER BY {order_clause}
            LIMIT %s
        """, (limit,))

        return [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении глобального лидерборда: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

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

@access_required
async def find_gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    is_eligible, reason, markup = await check_command_eligibility(update, context)
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
    chat_id = update.effective_chat.id  # Получаем ID чата

    is_eligible, reason, markup = await check_command_eligibility(update, context)

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
            f'.....Похоже никто не слышит вашей мольбы\n\n📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы).'
        )
        return

    gained_piety = round(random.uniform(1, 20) / 2, 1)

    # ИСПОЛЬЗУЕМ АТОМАРНОЕ ОБНОВЛЕНИЕ (ГЛОБАЛЬНО)
    await asyncio.to_thread(update_piety_and_prayer_db, user_id, gained_piety, current_time)

    # НОВОЕ: ОБНОВЛЯЕМ АКТИВНОСТЬ ДЛЯ ТЕКУЩЕГО ЧАТА (ЭТОТ СЧЕТЧИК БУДЕТ СЛУЖИТЬ ТОЛЬКО ФИЛЬТРОМ ДЛЯ ЧАТ-ТОПА)
    if update.effective_chat.type in ['group', 'supergroup']:
        await asyncio.to_thread(update_piety_and_prayer_db_chat, user_id, chat_id, gained_piety)

    await update.message.reply_text(
        f'⛩️ Ваши мольбы были услышаны! \n✨ Набожность +{gained_piety}\n\nНа следующую службу можно будет выйти через час 📿')

async def gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id

    is_eligible, reason, markup = await check_command_eligibility(update, context)  # Единая проверка
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

async def _get_leaderboard_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, view: str, scope: str, page: int = 1) -> Tuple[
    str, InlineKeyboardMarkup]:
    limit = PAGE_SIZE  # Для глобального топа
    if scope == 'chat':
        # Для чата показываем только топ-10 или топ-20, чтобы не загромождать
        limit = 20
        leaderboard_data = await asyncio.to_thread(get_gospel_leaderboard_by_chat, chat_id, view, limit)
        # ИЗМЕНЕННЫЙ ТЕКСТ ДЛЯ ЧАТ-ТОПА:
        title = (f"⛩️ Топ {'услышанных молитв:' if view == 'prayers' else 'самых набожных:'}\n"
                 f"<i>\n*Чтобы ваше имя высветилось в «топ чата», вам нужно совершить хотя бы одну молитву в этом чате</i>")
    elif scope == 'global':
        leaderboard_data = await asyncio.to_thread(get_gospel_leaderboard_global, view)
        title = f"🪐 Общий топ {'услышанных молитв:' if view == 'prayers' else 'самых набожных:'}"
    else:
        return "Неверная область топа.", InlineKeyboardMarkup([])
    total_users = len(leaderboard_data)
    # Логика пагинации только для глобального топа (если нужно)
    if scope == 'global':
        total_pages = (total_users + PAGE_SIZE - 1) // PAGE_SIZE
        if page < 1: page = 1
        if total_users > 0 and page > total_pages: page = total_pages
        start_index = (page - 1) * PAGE_SIZE
        end_index = start_index + PAGE_SIZE
        current_page_leaderboard = leaderboard_data[start_index:end_index]
    else:
        total_pages = 1
        start_index = 0
        current_page_leaderboard = leaderboard_data[:limit]  # Ограничиваем для чата
    message_text = f"<b>{title}</b>\n\n"
    keyboard_buttons = []
    if total_users == 0:
        message_text += "<i>Пока нет активных пользователей.</i>"
        return message_text, InlineKeyboardMarkup([])
    for rank_offset, row in enumerate(current_page_leaderboard):
        uid = row['user_id']
        score = row['prayer_count'] if view == 'prayers' else row['total_piety_score']

        # Используем кэшированные данные для отображения
        cached_first_name = row['first_name_cached']
        cached_username = row['username_cached']

        rank = start_index + rank_offset + 1

        display_text = cached_first_name or (f"@{cached_username}" if cached_username else f"ID: {uid}")

        # Форматирование ников без ссылок (просто текст)
        # В PTB mention_html создает ссылку. Если вы хотите ТОЧНО без ссылки,
        # то нужно использовать просто текст, но тогда пользователь не сможет кликнуть на него.
        # Оставим mention_html, так как он стандартен для PTB и выглядит как "ник без ссылки" в контексте других ботов.

        mention = mention_html(uid, display_text)

        score_formatted = f"{score}" if view == 'prayers' else f"{score:.1f}"
        unit = "молитв" if view == 'prayers' else "набожности"

        message_text += f"<code>{rank}.</code> {mention} — <b>{score_formatted}</b> {unit}\n"
    # --- Кнопки переключения ---

    # 1. Кнопки переключения вида (Молитвы/Набожность)
    switch_view_button = InlineKeyboardButton(
        "✨ Набожность" if view == 'prayers' else "📿 Молитвы",
        callback_data=f"gospel_top_{'piety' if view == 'prayers' else 'prayers'}_scope_{scope}_page_1"
    )
    # 2. Кнопка переключения области (Чат/Глобальный)
    if scope == 'chat':
        # Если мы в чате, предлагаем перейти в глобальный топ
        scope_button = InlineKeyboardButton("🪐 Общий Топ", callback_data=f"gospel_top_{view}_scope_global_page_1")
        keyboard_buttons.append([scope_button, switch_view_button])
    else:  # scope == 'global'
        # Если мы в глобальном топе, предлагаем вернуться к чату (если чат-ID известен)
        scope_button = InlineKeyboardButton("🏠 Топ чата", callback_data=f"gospel_top_{view}_scope_chat_page_1")
        keyboard_buttons.append([scope_button, switch_view_button])

        # 3. Кнопки пагинации (только для глобального топа)
        if total_pages > 1:
            nav_row = []
            if page > 1:
                nav_row.append(
                    InlineKeyboardButton("<< Назад", callback_data=f"gospel_top_{view}_scope_global_page_{page - 1}"))
            nav_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="ignore_page_num"))
            if page < total_pages:
                nav_row.append(
                    InlineKeyboardButton("Вперед >>", callback_data=f"gospel_top_{view}_scope_global_page_{page + 1}"))
            if nav_row:
                keyboard_buttons.append(nav_row)
    return message_text, InlineKeyboardMarkup(keyboard_buttons)

async def top_gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    chat_id = update.effective_chat.id  # Получаем ID чата

    is_eligible, reason, markup = await check_command_eligibility(update, context)

    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)

    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩️ Для того чтоб просмотреть топ, вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫"
        )
        return

    # ПО УМОЛЧАНИЮ ПОКАЗЫВАЕМ ТОП ТЕКУЩЕГО ЧАТА
    scope = 'chat'

    # Если команда вызвана в личке (private chat), показываем глобальный топ
    if update.effective_chat.type == 'private':
        scope = 'global'

    message_text, reply_markup = await _get_leaderboard_message(context, chat_id, 'prayers', scope, 1)

    try:
        await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения топа Евангелий: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка при получении топа. Пожалуйста, попробуйте еще раз.")

async def check_and_award_achievements(update_or_user_id, context: ContextTypes.DEFAULT_TYPE, user_data: dict):
    # Определяем user_id в зависимости от того, что передали (Update или ID)
    if isinstance(update_or_user_id, Update):
        user_id = update_or_user_id.effective_user.id
    else:
        user_id = int(update_or_user_id)

    # Внутренняя функция для отправки уведомлений
    async def send_notification(text):
        # Сначала пытаемся ответить на сообщение, если передан Update
        if isinstance(update_or_user_id, Update) and update_or_user_id.message:
            try:
                await update_or_user_id.message.reply_text(text, parse_mode=ParseMode.HTML)
                return
            except Exception:
                pass
        # Если не Update или ошибка — шлем напрямую ботом
        try:
            await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            logger.warning(f"Не удалось отправить уведомление о достижении пользователю {user_id}")

    unique_count = len(user_data.get("cards", {}))
    newly_awarded = []

    for ach in ACHIEVEMENTS:
        ach_id = ach["id"]
        if ach_id in user_data.get("achievements", []):
            continue

        if unique_count >= ach["threshold"]:
            reward = ach["reward"]
            if reward["type"] == "spins":
                user_data["spins"] = user_data.get("spins", 0) + int(reward["amount"])
                msg = f"🏆 Достижение: {ach['name']}\n🧧 Вы получили {reward['amount']} жетонов!"
            elif reward["type"] == "crystals":
                user_data["crystals"] = user_data.get("crystals", 0) + int(reward["amount"])
                msg = f"🏆 Достижение: {ach['name']}\nВам начислено {reward['amount']} 🧩!"
            else:
                msg = f"🏆 Достижение: {ach['name']}\nНаграда получена!"

            user_data.setdefault("achievements", []).append(ach_id)
            newly_awarded.append(msg)

    if newly_awarded:
        # Сохраняем данные (используем вашу функцию обновления в БД)
        await asyncio.to_thread(update_user_data, user_id, user_data)
        for text in newly_awarded:
            await send_notification(text)

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

    # Получаем список уже собранных карточек
    owned_card_ids_set = set(user_data["cards"].keys())
    all_card_ids_set = set(str(i) for i in range(1, NUM_PHOTOS + 1))
    new_card_ids_available = list(all_card_ids_set - owned_card_ids_set)

    # Решаем кто выпадет: если у пользователя есть крутки -> потребляем 1 и даём гарантированно новую (если есть новые)
    chosen_card_id = None
    is_new_card = False
    used_spin = False

    if user_data.get("spins", 0) > 0:
        # потребляем крутку и ставим короткий откат
        user_data["spins"] -= 1
        used_spin = True
        user_data["last_spin_time"] = current_time
        user_data["last_spin_cooldown"] = SPIN_USED_COOLDOWN  # 10 минут

        if new_card_ids_available:
            chosen_card_id = int(random.choice(new_card_ids_available))
            is_new_card = True
            await update.message.reply_text(
                "Вы потратили жетон и получили уникальную карточку! Следующую команду можно написать через 10 минут.")
        else:
            # все карточки собраны — даём кристаллы вместо новой карточки
            chosen_card_id = int(random.choice(list(owned_card_ids_set))) if owned_card_ids_set else random.choice(
                range(1, NUM_PHOTOS + 1))
            user_data["crystals"] += REPEAT_CRYSTALS_BONUS
            await update.message.reply_text(
                f"У вас уже есть все карточки! Вы потратили жетон, вам начислены {REPEAT_CRYSTALS_BONUS} 🧩 фрагментов. Следующую команду можно написать через 10 минут.")
    else:
        # нет круток — стандартная логика и длинный откат
        user_data["last_spin_time"] = current_time
        user_data["last_spin_cooldown"] = COOLDOWN_SECONDS  # 3 часа

        if new_card_ids_available and owned_card_ids_set:
            if random.random() < 0.8:  # 80% шанс на новую, если есть новые и старые
                chosen_card_id = int(random.choice(new_card_ids_available))
                is_new_card = True
            else:
                chosen_card_id = int(random.choice(list(owned_card_ids_set)))
        elif new_card_ids_available:  # только новые
            chosen_card_id = int(random.choice(new_card_ids_available))
            is_new_card = True
        elif owned_card_ids_set:  # всё собрано
            chosen_card_id = int(random.choice(list(owned_card_ids_set)))
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
        # Если это первая карточка у пользователя — сохраняем дату начала игры
        if not owned_card_ids_set:  # Проверяем, что это действительно первая карточка
            # сохраняем в ISO формате с UTC для совместимости
            user_data["first_card_date"] = datetime.now(timezone.utc).isoformat()
        caption_suffix_actual = " Новая карточка добавлена в вашу коллекцию!"
    else:
        user_data["cards"][card_id_str] = user_data["cards"].get(card_id_str, 0) + 1
        user_data["crystals"] += REPEAT_CRYSTALS_BONUS
        caption_suffix_actual = f" 👀 Это повторная карточка!\n\nВы получили {REPEAT_CRYSTALS_BONUS} 🧩 фрагментов!\nУ вас теперь {user_data['cards'][card_id_str]} таких карточек"

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

async def check_command_eligibility(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CACHED_CHANNEL_ID, CACHED_GROUP_ID

    user = update.effective_user
    chat = update.effective_chat

    if not user or user.is_bot:
        return False, "Боты не могут использовать эту команду.", None
    if CACHED_CHANNEL_ID is None and CHANNEL_USERNAME:
        try:
            c = await context.bot.get_chat(CHANNEL_ID)  # CHANNEL_ID = @CHANNEL_USERNAME
            CACHED_CHANNEL_ID = c.id
            logger.info(f"Resolved channel {CHANNEL_ID} -> {CACHED_CHANNEL_ID}")
        except Exception as e:
            logger.warning(f"Не удалось получить chat для канала {CHANNEL_ID}: {e}")

    # 2. Кэширование ID группы
    if CACHED_GROUP_ID is None and GROUP_USERNAME_PLAIN:
        try:
            g = await context.bot.get_chat(f"@{GROUP_USERNAME_PLAIN}")
            CACHED_GROUP_ID = g.id
            logger.info(f"Resolved group @{GROUP_USERNAME_PLAIN} -> {CACHED_GROUP_ID}")
        except Exception as e:
            logger.warning(f"Не удалось получить chat для группы @{GROUP_USERNAME_PLAIN}: {e}")

    is_member = False
    if CACHED_CHANNEL_ID:
        try:
            cm = await context.bot.get_chat_member(CACHED_CHANNEL_ID, user.id)
            if cm.status in ('member', 'creator', 'administrator'):
                is_member = True
        except Exception as e:
            logger.debug(f"get_chat_member for channel {CACHED_CHANNEL_ID} returned {e}")

    # Проверяем членство в группе (если знаем ID)
    if not is_member and CACHED_GROUP_ID:
        try:
            gm = await context.bot.get_chat_member(CACHED_GROUP_ID, user.id)
            if gm.status in ('member', 'creator', 'administrator'):
                is_member = True
        except Exception as e:
            logger.debug(f"get_chat_member for group {CACHED_GROUP_ID} returned {e}")
    if is_member:
        return True, "", None
    buttons = []
    if CHANNEL_USERNAME:
        channel_url = CHANNEL_INVITE_LINK if CHANNEL_INVITE_LINK else f"https://t.me/{CHANNEL_USERNAME}"
        buttons.append([InlineKeyboardButton(f"Подписаться на канал @{CHANNEL_USERNAME}", url=channel_url)])

    if GROUP_CHAT_INVITE_LINK:
        buttons.append([InlineKeyboardButton(f"Вступить в чат @{GROUP_USERNAME_PLAIN}", url=GROUP_CHAT_INVITE_LINK)])
    elif GROUP_USERNAME_PLAIN:
        buttons.append([InlineKeyboardButton(f"Вступить в чат @{GROUP_USERNAME_PLAIN}",
                                             url=f"https://t.me/{GROUP_USERNAME_PLAIN}")])
    markup = InlineKeyboardMarkup(buttons) if buttons else None
    msg = (f"Для использования этой команды вы должны быть подписчиком канала "
           f"@{CHANNEL_USERNAME} ИЛИ участником чата @{GROUP_USERNAME_PLAIN}.")
    return False, msg, markup

def update_user_data(user_id, new_data: dict):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        # Получаем текущие данные
        cursor.execute("SELECT data FROM laviska_users WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if not row:
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
            existing_data = row['data']
            existing_data.update(new_data)
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

async def my_collection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    is_eligible, reason, markup = await check_command_eligibility(update, context)
    if not is_eligible:
        await update.message.reply_text(reason, parse_mode=ParseMode.HTML)
        return

    user_data = await asyncio.to_thread(get_user_data, user_id, username)
    total_owned_cards = len(user_data.get("cards", {}))
    notebook_menu_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❤️‍🔥 LOVE IS', callback_data='show_love_is_menu')],
        [InlineKeyboardButton('🗑️ Выйти', callback_data='delete_message')]])

    first_card_iso = user_data.get("first_card_date")
    try:
        message_text = NOTEBOOK_MENU_CAPTION.format(
            username=user_data.get('username', username),
            user_id=user_data.get('user_id', user_id),
            active_collection='лав иска',  # Или другое название активной коллекции
            card_count=total_owned_cards,
            token_count=user_data.get('spins', 0),
            fragment_count=user_data.get('crystals', 0),
            start_date=format_first_card_date_iso(first_card_iso))
    except Exception:
        # Fallback в случае ошибки форматирования
        message_text = (
            f"профиль: {username}\n"
            f"активная коллекция: лав иска\n"
            f"колво карточек: {total_owned_cards}\n"
            f"колво жетонов: {user_data.get('spins', 0)}\n"
            f"колво фрагментов: {user_data.get('crystals', 0)}\n")

    try:
        await update.message.reply_photo(
            photo=open(NOTEBOOK_MENU_IMAGE_PATH, "rb"),
            caption=message_text,
            reply_markup=notebook_menu_keyboard)
    except FileNotFoundError:
        logger.error(f"Collection menu image not found: {NOTEBOOK_MENU_IMAGE_PATH}", exc_info=True)
        await update.message.reply_text(
            message_text + "\n\n(Ошибка: фоновая картинка коллекции не найдена)",
            reply_markup=notebook_menu_keyboard)
    except Exception as e:
        logger.error(f"Error sending collection menu photo: {e}", exc_info=True)
        await update.message.reply_text(
            message_text + f"\n\n(Ошибка при отправке фоновой картинки: {e})",
            reply_markup=notebook_menu_keyboard)

# Добавьте эту новую функцию в ваш код
async def show_love_is_menu(query: Update.callback_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name
    user_data = await asyncio.to_thread(get_user_data, user_id, username)
    total_owned_cards = len(user_data.get("cards", {}))
    first_card_iso = user_data.get("first_card_date")
    # 1. Формирование кнопок
    keyboard = [
        [InlineKeyboardButton(f"❤️‍🔥 Мои карты {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements"),
         InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"─────── ⋆⋅☆⋅⋆ ───────\n"
        f"КОЛЛЕКЦИЯ «❤️‍🔥 LOVE IS…»\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🃏 Карты: {total_owned_cards}\n"
        f"🧧 Жетоны: {user_data.get('spins', 0)}\n"
        f"🧩 Фрагменты: {user_data.get('crystals', 0)}\n"
        f"─────── ⋆⋅☆⋅⋆ ───────\n")
    try:
        await query.edit_message_media(
            media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text),
            reply_markup=reply_markup)
    except BadRequest as e:
        logger.warning(
            f"Failed to edit message to love is menu photo (likely old message or user blocked bot): {e}. Sending new message.",
            exc_info=True)
        try:
            await query.bot.send_photo(  # Используем query.bot.send_photo для отправки в личку
                chat_id=query.from_user.id,
                photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),  # Здесь должно быть COLLECTION_MENU_IMAGE_PATH
                caption=message_text,
                reply_markup=reply_markup)
        except Exception as new_send_e:
            logger.error(f"Failed to send new photo for love is menu after edit failure: {new_send_e}",
                         exc_info=True)
            await query.bot.send_message(  # Используем query.bot.send_message для отправки текста в личку
                chat_id=query.from_user.id,
                text="Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз.")
    except Exception as e:
        logger.error(f"Failed to edit message to love is menu photo with unexpected error: {e}", exc_info=True)
        await query.bot.send_message(  # Используем query.bot.send_message для отправки текста в личку
            chat_id=query.from_user.id,
            text="Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз.")

async def edit_to_love_is_menu(query: Update.callback_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name
    user_data = await asyncio.to_thread(get_user_data, user_id, username)
    total_owned_cards = len(user_data.get("cards", {}))

    keyboard = [
        [InlineKeyboardButton(f"❤️‍🔥 Мои карты {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements"),
         InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")],
        [InlineKeyboardButton("Вернуться в блокнот", callback_data="back_to_notebook_menu")]  # Добавлена кнопка
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"─────── ⋆⋅☆⋅⋆ ───────\n"
        f"КОЛЛЕКЦИЯ «❤️‍🔥 LOVE IS…»\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🃏 Карты: {total_owned_cards}\n"
        f"🧧 Жетоны: {user_data.get('spins', 0)}\n"
        f"🧩 Фрагменты: {user_data.get('crystals', 0)}\n"
        f"─────── ⋆⋅☆⋅⋆ ───────\n")
    try:
        await query.edit_message_media(
            media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text),
            reply_markup=reply_markup)
    except BadRequest as e:
        logger.warning(
            f"Failed to edit message to main collection photo (likely old message or user blocked bot): {e}. Sending new message.",
            exc_info=True)
        try:
            await query.bot.send_photo(chat_id=query.from_user.id,
                                       photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                                       caption=message_text,
                                       reply_markup=reply_markup)
        except Exception as new_send_e:
            logger.error(f"Failed to send new photo for collection menu after edit failure: {new_send_e}",
                         exc_info=True)
            await query.bot.send_message(chat_id=query.from_user.id,
                                         text="Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз.")
    except Exception as e:
        logger.error(f"Failed to edit message to main collection photo with unexpected error: {e}", exc_info=True)
        await query.bot.send_message(chat_id=query.from_user.id,
                                     text="Произошла ошибка при отображении коллекции. Пожалуйста, попробуйте еще раз.")

async def edit_to_notebook_menu(query: Update.callback_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = query.from_user.id
    username_for_display = query.from_user.username
    if username_for_display:
        username_for_display = f"@{username_for_display}"
    else:
        username_for_display = query.from_user.first_name

    user_data = await asyncio.to_thread(get_user_data, user_id, username_for_display)
    if user_data is None:
        user_data = {}

    total_cards = len(user_data.get("cards", {}))
    spins = user_data.get("spins", 0)
    crystals = user_data.get("crystals", 0)
    start_date_formatted = format_first_card_date_iso(user_data.get('first_card_date'))

    NOTEBOOK_MENU_CAPTION = (
        "─────── *⋆⋅☆⋅⋆* ───────\n"
        "📙Блокнот с картами 📙\n"
        "➖➖➖➖➖➖➖➖➖➖\n"
        "👤 Профиль: {username}\n"
        "🔖 ID: `{user_id}`\n"
        "➖➖➖➖➖➖➖➖➖➖\n"
        "🧧 Жетоны: {token_count}\n"
        "🧩 Фрагменты: {fragment_count}\n"
        "─────── *⋆⋅☆⋅⋆* ───────")

    try:
        caption_text = NOTEBOOK_MENU_CAPTION.format(
            username=username_for_display,  # Используем подготовленное имя
            user_id=user_id,  # **Добавлено: передача user_id в .format()**
            active_collection=user_data.get('active_collection_name', 'Лав иска'),
            card_count=total_cards,
            token_count=spins,
            fragment_count=crystals,
            start_date=start_date_formatted)

    except Exception as e:
        logger.error(f"Error formatting caption: {e}")
        caption_text = (
            "─────── *⋆⋅☆⋅⋆* ───────\n"
            "📙Блокнот с картами 📙\n"
            "➖➖➖➖➖➖➖➖➖➖\n"
            f"👤 Профиль: {username_for_display}\n"
            f"🔖 ID: `{user_id}`\n"
            "➖➖➖➖➖➖➖➖➖➖\n"
            f"🧧 Жетоны: {spins}\n"
            f"🧩 Фрагменты: {crystals}\n"
            "─────── *⋆⋅☆⋅⋆* ───────")

        notebook_menu_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('❤️‍🔥 LOVE IS', callback_data='show_love_is_menu')],
            [InlineKeyboardButton('🗑️ Выйти', callback_data='delete_message')]])

        # Пытаемся отредактировать существующее сообщение (media + caption)
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(NOTEBOOK_MENU_IMAGE_PATH, "rb"),
                                      caption=caption_text,
                                      parse_mode=ParseMode.MARKDOWN_V2),  # **Добавлено: parse_mode**
                reply_markup=notebook_menu_keyboard)
        except BadRequest as e:
            # Если редактирование невозможно (старое сообщение/пользователь заблокировал бота),
            # отправляем новое личное сообщение с тем же содержимым
            logger.warning(f"edit_to_notebook_menu: edit failed, sending new message: {e}", exc_info=True)
            try:
                await query.bot.send_photo(
                    chat_id=query.from_user.id,
                    photo=open(NOTEBOOK_MENU_IMAGE_PATH, "rb"),
                    caption=caption_text,
                    parse_mode=ParseMode.MARKDOWN_V2,  # **Добавлено: parse_mode**
                    reply_markup=notebook_menu_keyboard)
            except Exception as send_e:
                logger.error(f"edit_to_notebook_menu: sending new photo failed: {send_e}", exc_info=True)
                # В крайнем случае — отправляем текст
                try:
                    await query.bot.send_message(  # Используем query.bot.send_message для отправки текста в личку
                        chat_id=query.from_user.id,
                        text=caption_text,
                        parse_mode=ParseMode.MARKDOWN_V2,  # **Добавлено: parse_mode**
                        reply_markup=notebook_menu_keyboard)
                except Exception:
                    logger.exception("edit_to_notebook_menu: cannot notify user about notebook menu.")

async def send_collection_card(query: Update.callback_query, user_data, card_id):
    user_id = query.from_user.id
    owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])
    if not owned_card_ids:
        await edit_to_love_is_menu(query,
                                   query.application)  # Передаем context, который хранится в query.application
        return
    card_count = user_data["cards"].get(str(card_id), 0)
    photo_path = PHOTO_DETAILS[card_id]["path"]
    caption_text = (
        f"{PHOTO_DETAILS[card_id]['caption']}"
        f" Таких карт у вас - {card_count}")
    keyboard = []
    nav_buttons = []
    if len(owned_card_ids) > 1:
        nav_buttons.append(InlineKeyboardButton("← Предыдущая", callback_data=f"nav_card_prev"))
        nav_buttons.append(InlineKeyboardButton("Следующая →", callback_data=f"nav_card_next"))
    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await query.edit_message_media(media=InputMediaPhoto(media=open(photo_path, "rb"), caption=caption_text),
                                       reply_markup=reply_markup)
    except BadRequest as e:
        logger.warning(
            f"Failed to edit message media for card view (likely old message or user blocked bot): {e}. Sending new message.",
            exc_info=True)
        try:
            await query.bot.send_photo(chat_id=query.from_user.id, photo=open(photo_path, "rb"),
                                       caption=caption_text, reply_markup=reply_markup)
        except Exception as new_send_e:
            logger.error(f"Failed to send new photo for card view after edit failure: {new_send_e}", exc_info=True)
            await query.bot.send_message(chat_id=query.from_user.id,
                                         text="Произошла ошибка при отображении карточки. Пожалуйста, попробуйте еще раз.")
    except Exception as e:
        logger.error(f"Failed to edit message media for card view with unexpected error: {e}", exc_info=True)
        await query.bot.send_message(  # Используем query.bot.send_message для отправки текста в личку
            chat_id=query.from_user.id,
            text="Произошла ошибка при отображении карточки. Пожалуйста, попробуйте еще раз.")

    # --- ОБРАБОТЧИКИ RP КОМАНД ---

async def rp_command_template(update: Update, context: ContextTypes.DEFAULT_TYPE, responses: List[str],
                              action_name: str):
    user = update.effective_user
    chat_id = update.effective_chat.id
    is_eligible, reason, markup = await check_command_eligibility(update, context)

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
        if not target_user_data:
            target_user_data = {"user_id": replied_user.id, "first_name": replied_user.first_name,
                                "username": replied_user.username}

    elif context.args:
        username_arg = context.args[0]
        if username_arg.startswith('@'):
            username_arg = username_arg[1:]

        target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_username, username_arg)
        if target_user_data_from_db:
            target_user_id = target_user_data_from_db['user_id']
            target_user_data = target_user_data_from_db
        else:
            await update.message.reply_text(f"👾 Пользователь '{username_arg}' не найден в базе.")
            return

    if not target_user_id:
        await update.message.reply_text(f"👾 Чтобы {action_name}, ответьте на сообщение или укажите @username.")
        return

    actor_mention = mention_html(user.id, user.first_name)
    target_mention = mention_html(target_user_data['user_id'], get_marriage_user_display_name(target_user_data))
    response_template = random.choice(responses)
    response_text = f"{actor_mention} {response_template.format(target_mention=target_mention)}"
    await update.message.reply_text(response_text, parse_mode=ParseMode.HTML)

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
            f"Вы хотите принять это предложение?")

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
                    f"Отредактировано сообщение {private_message_id} для {target_user_id} по предложению {proposal_id}"
                )
            except BadRequest as e:  # Bot blocked, message not found, etc.
                logger.warning(
                    f"Не удалось отредактировать сообщение {private_message_id} для {target_user_id} (предложение {proposal_id}): {e}. Отправляем новое.",
                    exc_info=True
                )
                # Если редактирование не удалось, сбрасываем private_message_id в БД для этого предложения
                await asyncio.to_thread(update_proposal_private_message_id, proposal_id, None)
            except Exception as e:
                logger.error(
                    f"Общая ошибка при редактировании сообщения {private_message_id} для {target_user_id} (предложение {proposal_id}): {e}",
                    exc_info=True
                )
                await asyncio.to_thread(update_proposal_private_message_id, proposal_id, None)

async def unified_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await asyncio.to_thread(save_marriage_user_data, user, from_group_chat=False)
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)
    chat_url = GROUP_CHAT_INVITE_LINK if GROUP_CHAT_INVITE_LINK else f'https://t.me/{GROUP_USERNAME_PLAIN}'
    keyboard = [
        [InlineKeyboardButton(f'Чат 💬', url=chat_url),
         InlineKeyboardButton('Голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
        [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa'),
         InlineKeyboardButton('Команды ⚙️', callback_data='show_commands')], ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_name = user.username or user.first_name or 'друг'
    await update.message.reply_text(
        f'Привет, {user_name}! 🪐\nЭто бот чата 𝗦𝗨𝗡𝗥𝗜𝗦𝗘  \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, '
        'принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!',
        reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    await _resend_pending_proposals_to_target(user.id, context)

async def get_chat_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = update.effective_chat.title if chat_type != 'private' else 'Личный чат'

    response = (f"ID этого чата: `{chat_id}`\n"
                f"Тип чата: `{chat_type}`\n"
                f"Название чата: `{chat_title}`")
    await update.message.reply_text(response, parse_mode="Markdown")

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

        if LAV_ISKA_REGEX.match(message_text_lower):
            await lav_iska(update, context)
            return
        elif MY_COLLECTION_REGEX.match(message_text_lower):
            await my_collection(update, context)
            return

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

        elif VENCHATSYA_REGEX.match(message_text_lower):
            is_eligible, reason, markup = await check_command_eligibility(update, context)
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
                target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_username,
                                                                   target_username)
                if target_user_data_from_db:
                    target_user_id = target_user_data_from_db['user_id']
                    target_user_data = target_user_data_from_db
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"👾 Пользователь '@{target_username}' не найден в базе данных бота. "
                             "Убедитесь, что он писал сообщения в группе и у него есть публичный username, "
                             "либо попросите его написать `/start` боту в личные сообщения.",
                        parse_mode=ParseMode.HTML)
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
                    parse_mode=ParseMode.HTML)
                return

            if initiator_id == target_user_id:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Вы не можете пожениться сами с собой! "
                         "Пожалуйста, выберите другого пользователя.",
                    parse_mode=ParseMode.HTML)
                return

            if target_user_data.get('user_id') == context.bot.id or \
                    (update.message.reply_to_message and update.message.reply_to_message.from_user.is_bot):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Вы не можете предлагать пожениться ботам. "
                         "Они заняты служением человечеству, а не брачными узами.",
                    parse_mode=ParseMode.HTML)
                return

            target_display_name = get_marriage_user_display_name(target_user_data)
            target_mention = mention_html(target_user_id, target_display_name)

            if await asyncio.to_thread(get_active_marriage, initiator_id):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"{initiator_mention}, вы уже состоите в браке. "
                         "Для создания нового брака необходимо развестись с текущим супругом.",
                    parse_mode=ParseMode.HTML)
                return

            if await asyncio.to_thread(get_active_marriage, target_user_id):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"{target_mention} уже состоит в браке. "
                         "Выберите другого пользователя для предложения.",
                    parse_mode=ParseMode.HTML)
                return

            existing_proposal = await asyncio.to_thread(get_pending_marriage_proposal, initiator_id, target_user_id)
            if existing_proposal:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Между вами и {target_mention} уже есть активное предложение "
                         "о браке. Дождитесь ответа или отмените свое.",
                    parse_mode=ParseMode.HTML)
                return

            private_msg_id: Optional[int] = None
            message_to_initiator_in_group: str = ""

            try:
                keyboard = [
                    [InlineKeyboardButton("Да", callback_data=f"marry_yes_{initiator_id}_{target_user_id}")],
                    [InlineKeyboardButton("Нет", callback_data=f"marry_no_{initiator_id}_{target_user_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)

                sent_msg = await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"{target_mention}, вам предложил венчаться пользователь {initiator_mention}!\n"
                         f"Вы хотите принять это предложение?",
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML)
                private_msg_id = sent_msg.message_id
                message_to_initiator_in_group = (
                    f"💍 Ваше предложение отправлено {target_mention} в личные сообщения!\n\n"
                    f"Держим за вас кулачки ✊🏻")

            except BadRequest as e:
                logger.warning(
                    f"Не удалось отправить личное сообщение {target_mention} (ID: {target_user_id}): {e}",
                    exc_info=True)
                private_msg_id = None
                message_to_initiator_in_group = (
                    f"Если ваш избранник {target_mention} не получил предложение (возможно, бот заблокирован или пользователь не начинал диалог ему нужно будет написать `/start` и ввести команду `предложения`)")
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
            is_eligible, reason, markup = await check_command_eligibility(update, context)
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
                target_user_data_from_db = await asyncio.to_thread(get_marriage_user_data_by_username,
                                                                   target_username)
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
                                               text="👾 Не удалось определить пользователя, которому вы хотите отменить предложение. ""Возможно, его нет в базе данных бота или вы указали неверно.",
                                               parse_mode=ParseMode.HTML)
                return

            target_display_name = get_marriage_user_display_name(target_user_data)
            target_mention = mention_html(target_user_id, target_display_name)

            proposal_to_cancel = await asyncio.to_thread(get_initiator_pending_proposal, initiator_id,
                                                         target_user_id)

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
            is_eligible, reason, markup = await check_command_eligibility(update, context)

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
                    "username": marriage['initiator_username']})
                target_display_name = get_marriage_user_display_name({
                    "user_id": marriage['target_id'],
                    "first_name": marriage['target_first_name'],
                    "username": marriage['target_username']})

                p1_mention = mention_html(marriage['initiator_id'], initiator_display_name)
                p2_mention = mention_html(marriage['target_id'], target_display_name)

                start_date = marriage['prev_accepted_at'] if marriage['prev_accepted_at'] else marriage[
                    'accepted_at']
                duration = await format_duration(start_date)
                start_date_formatted = start_date.strftime('%d.%m.%Y')

                response_text += (f"- {p1_mention} и {p2_mention} "
                                  f"(с {start_date_formatted}, {duration})\n")
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "мой брак":
            is_eligible, reason, markup = await check_command_eligibility(update, context)

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
                f"📆 Дата бракосочетания: {start_date_formatted} ({duration}).")
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "развестись":
            is_eligible, reason, markup = await check_command_eligibility(update, context)

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
                [InlineKeyboardButton("Отмена", callback_data=f"divorce_cancel_{user.id}_{partner_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"💔 Вы действительно хотите развестись с {partner_mention}? \nПосле развода у вас будет {REUNION_PERIOD_DAYS} дня на повторное венчание без потери длительности брака.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML)
            return

        elif message_text_lower == "предложения":
            is_eligible, reason, markup = await check_command_eligibility(update, context)

            if not is_eligible:
                await context.bot.send_message(chat_id=chat_id, text=reason, parse_mode=ParseMode.HTML)
                return

            pending_proposals = await asyncio.to_thread(get_target_pending_proposals, user.id)

            if not pending_proposals:
                await update.message.reply_text("У вас нет активных предложений о венчании.",
                                                parse_mode=ParseMode.HTML)
                return

            response_text_parts = ["🧩 <b>Входящие предложения о венчании:</b>\n\n"]
            for proposal in pending_proposals:
                initiator_id = proposal['initiator_id']
                initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, initiator_id)
                initiator_mention = mention_html(initiator_id, get_marriage_user_display_name(initiator_info))

                response_text_for_one_proposal = (
                    f"От: {initiator_mention} (отправлено {proposal['created_at'].strftime('%d.%m.%Y %H:%M')})\n")
                keyboard = [
                    [InlineKeyboardButton("✅ Принять", callback_data=f"marry_yes_{initiator_id}_{user.id}")],
                    [InlineKeyboardButton("❌ Отклонить", callback_data=f"marry_no_{initiator_id}_{user.id}")]]
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

        elif message_text_lower == 'санрайз':
            chat_url = GROUP_CHAT_INVITE_LINK if GROUP_CHAT_INVITE_LINK else f'https://t.me/{GROUP_USERNAME_PLAIN}'
            keyboard = [
                [InlineKeyboardButton(f'Чат 💬', url='https://t.me/CHAT_SUNRISE'),
                 InlineKeyboardButton('Голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
                [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa'),
                 InlineKeyboardButton('Команды ⚙️', callback_data='show_commands')], ]
            markup = InlineKeyboardMarkup(keyboard)
            await context.bot.send_message(chat_id, f'<b>Привет, {user.username or user.first_name}!</b> ✨\n'
                                                    '▎Добро пожаловать в чат-бот 𝗦𝗨𝗡𝗥𝗜𝗦𝗘  \n\n'
                                                    '<b>Здесь ты сможешь:</b>\n'  # <-- Начало цитаты
                                                    '<blockquote>— Погрузиться в увлекательную игру 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄  \n'
                                                    '— Принять участие в новогоднем голосовании  \n'
                                                    '— Получить всю необходимую помощь и поддержку!</blockquote>\n'  # <-- Конец цитаты
                                                    'Мы рады видеть тебя здесь! ❤️‍🔥', reply_markup=markup,
                                           parse_mode=ParseMode.HTML)

async def send_command_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_list = """
<b>⚙️ Список команд:</b>
"""

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(command_list, parse_mode=ParseMode.HTML)
        except BadRequest as e:
            logger.warning(f"Failed to edit command list message: {e}. Sending new one.", exc_info=True)
            await update.callback_query.message.reply_text(command_list, parse_mode=ParseMode.HTML)
    else:
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
    if data == "show_collections":
        await handle_collections_menu(update, context)
        return
    elif data.startswith("show_cards_"):
        await show_filtered_cards(update, context)
        return
    elif data.startswith("move_"):
        await move_card(update, context)
        return
    elif data.startswith("view_col_"):
        await view_collection_cards(update, context)
        return
    # --- Обработка кнопок Брачного Бота ---
    if data.startswith("marry_") or data.startswith("divorce_"):
        parts = data.split('_')
        action_type = parts[0]
        action = parts[1]
        user1_id = int(parts[2])
        user2_id = int(parts[3])

        if action_type == "marry":
            if current_user_id != user2_id:
                try:
                    await query.edit_message_text(text="Это предложение адресовано не вам!")
                except BadRequest:
                    await query.message.reply_text("Это предложение адресовано не вам!")
                return

            is_eligible, reason, markup = await check_command_eligibility(update, context)

            if not is_eligible:
                try:
                    await query.edit_message_text(
                        text=f"Вы не соответствуете условиям для принятия/отклонения предложения: {reason}",
                        parse_mode=ParseMode.HTML)
                except BadRequest:
                    await query.bot.send_message(
                        chat_id=current_user_id,
                        text=f"Вы не соответствуете условиям для принятия/отклонения предложения: {reason}",
                        parse_mode=ParseMode.HTML)
                return

            proposal = await asyncio.to_thread(get_pending_marriage_proposal, user1_id, user2_id)

            if not proposal:
                try:
                    await query.edit_message_text(text="Это предложение уже неактивно или истекло.")
                except BadRequest:
                    await query.bot.send_message(chat_id=current_user_id,
                                                 text="Это предложение уже неактивно или истекло.")
                return

            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, user1_id)
            target_info = await asyncio.to_thread(get_marriage_user_data_by_id, user2_id)

            if not initiator_info or not target_info:
                try:
                    await query.edit_message_text(text="Не удалось получить данные о пользователях.")
                except BadRequest:
                    await query.bot.send_message(chat_id=current_user_id,
                                                 text="Не удалось получить данные о пользователях.")
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
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text="К сожалению, один из вас уже состоит в браке.",
                                                     parse_mode=ParseMode.HTML)
                    await asyncio.to_thread(reject_marriage_proposal_db, proposal['id'])  # Reject to clear state
                    return

                if await asyncio.to_thread(accept_marriage_proposal_db, proposal['id'], user1_id, user2_id):
                    try:
                        await query.edit_message_text(text=f"Вы успешно венчались с {initiator_mention}!",
                                                      parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text=f"Вы успешно венчались с {initiator_mention}!",
                                                     parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(chat_id=proposal['chat_id'],
                                                       text=f"{target_mention} и {initiator_mention} успешно венчались!",
                                                       parse_mode=ParseMode.HTML)
                        # Уведомляем инициатора
                        await context.bot.send_message(chat_id=user1_id,
                                                       text=f"💍 Ваше предложение венчаться с {target_mention} было принято!",
                                                       parse_mode=ParseMode.HTML)
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
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text="💔 Произошла ошибка при принятии предложения. Пожалуйста, попробуйте еще раз.",
                                                     parse_mode=ParseMode.HTML)
            elif action == "no":
                if await asyncio.to_thread(reject_marriage_proposal_db, proposal['id']):
                    try:
                        await query.edit_message_text(
                            text=f"💔 Вы отклонили предложение венчаться от {initiator_mention}.",
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text=f"💔 Вы отклонили предложение венчаться от {initiator_mention}.",
                                                     parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(
                            chat_id=user1_id,
                            text=f"💔 {target_mention} отклонил(а) ваше предложение венчаться.",
                            parse_mode=ParseMode.HTML)
                    except Exception as e:
                        logger.warning(
                            f"💔 Не удалось отправить уведомление об отклонении инициатору {user1_id}: {e}",
                            exc_info=True)
                else:
                    try:
                        await query.edit_message_text(
                            text="💔 Произошла ошибка при отклонении предложения. Пожалуйста, попробуйте еще раз.",
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text="💔 Произошла ошибка при отклонении предложения. Пожалуйста, попробуйте еще раз.",
                                                     parse_mode=ParseMode.HTML)

        elif action_type == "divorce":
            if current_user_id != user1_id:
                try:
                    await query.edit_message_text(text="Не суй свой носик в чужие дела!")
                except BadRequest:
                    await query.bot.send_message(chat_id=current_user_id, text="Не суй свой носик в чужие дела!")
                return

            partner_id = user2_id

            initiator_info = await asyncio.to_thread(get_marriage_user_data_by_id, current_user_id)
            partner_info = await asyncio.to_thread(get_marriage_user_data_by_id, partner_id)

            if not initiator_info or not partner_info:
                try:
                    await query.edit_message_text(text="Не удалось получить данные о пользователях.")
                except BadRequest:
                    await query.bot.send_message(chat_id=current_user_id,
                                                 text="Не удалось получить данные о пользователях.")
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
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text=f"💔 Вы развелись с {partner_mention}. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери длительности брака.",
                                                     parse_mode=ParseMode.HTML)
                    try:
                        await context.bot.send_message(
                            chat_id=partner_id,
                            text=f"💔 Ваш брак с {initiator_mention} был расторгнут. У вас есть {REUNION_PERIOD_DAYS} дня для повторного венчания без потери длительности брака.",
                            parse_mode=ParseMode.HTML)
                    except Exception as e:
                        logger.warning(f"💔 Не удалось уведомить партнера {partner_id} о разводе: {e}",
                                       exc_info=True)
                else:
                    try:
                        await query.edit_message_text(
                            text="❤️‍🩹 Произошла ошибка при попытке развода. Пожалуйста, попробуйте еще раз",
                            parse_mode=ParseMode.HTML)
                    except BadRequest:
                        await query.bot.send_message(chat_id=current_user_id,
                                                     text="❤️‍🩹 Произошла ошибка при попытке развода. Пожалуйста, попробуйте еще раз",
                                                     parse_mode=ParseMode.HTML
                                                     )
            elif action == "cancel":
                try:
                    await query.edit_message_text(text="❤️‍🩹 Развод отменен", parse_mode=ParseMode.HTML)
                except BadRequest:
                    await query.bot.send_message(chat_id=current_user_id, text="❤️‍🩹 Развод отменен",
                                                 parse_mode=ParseMode.HTML)

    elif data == 'delete_message':
        try:
            await query.delete_message()
        except BadRequest as e:
            logger.warning(f"Failed to delete message: {e}")
        return

    # --- Обработка кнопок Лависки ---
    elif data == "show_love_is_menu":
        await show_love_is_menu(query, context)

    elif data == "back_to_notebook_menu":
        await edit_to_notebook_menu(query, context)

    elif data == "back_to_main_collection":
        await edit_to_love_is_menu(query, context)

    elif data == "show_collection":
        user_data_laviska = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        owned_card_ids = sorted([int(cid) for cid in user_data_laviska["cards"].keys()])
        if not owned_card_ids:
            await edit_to_love_is_menu(query, context)
            return

        user_data_laviska["current_collection_view_index"] = 0
        await asyncio.to_thread(update_user_data, current_user_id, user_data_laviska)
        await send_collection_card(query, user_data_laviska, owned_card_ids[0])

    elif data.startswith("view_card_"):
        parts = data.split("_")
        card_to_view_id = int(parts[2])

        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])
        if not owned_card_ids:
            await edit_to_love_is_menu(query, context)
            return

        current_index = owned_card_ids.index(card_to_view_id)
        user_data["current_collection_view_index"] = current_index
        await asyncio.to_thread(update_user_data, current_user_id, user_data)
        await send_collection_card(query, user_data, card_to_view_id)

    elif data.startswith("nav_card_"):
        direction = data.split("_")[2]

        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        owned_card_ids = sorted([int(cid) for cid in user_data["cards"].keys()])
        if not owned_card_ids:
            await edit_to_love_is_menu(query, context)
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

    elif data == "show_achievements":
        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        unique_count = len(user_data.get("cards", {}))
        achieved_ids = set(user_data.get("achievements", []))

        lines = ["🏆 Доступные достижения: \n"]
        for ach in ACHIEVEMENTS:
            if ach["id"] in achieved_ids:
                lines.append(
                    f"✅ {ach['name']} — получено ({ach['reward']['amount']} {('жетонов' if ach['reward']['type'] == 'spins' else 'фрагментов')})")
            else:
                lines.append(f"🃏 ▎ {ach['name']} — {unique_count}/{ach['threshold']}\n")

        lines.append("✨ Так держать! Не останавливайся! Кто знает, может в будущем это пригодится…")
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")]])
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption="\n".join(lines)),
                reply_markup=reply_markup)
        except BadRequest as e:
            logger.warning(f"Failed to show achievements media: {e}")
            try:
                await query.bot.send_photo(
                    chat_id=query.from_user.id,
                    photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                    caption="\n".join(lines),
                    reply_markup=reply_markup)
            except Exception as new_send_e:
                logger.error(f"Failed to send new photo: {new_send_e}")

    elif data == "buy_spins":
        user_data = await asyncio.to_thread(get_user_data, current_user_id, current_user_username)
        keyboard = [
            [InlineKeyboardButton(f"Обменять {SPIN_COST} 🧩 на жетон",
                                  callback_data="exchange_crystals_for_spin")],
            [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")], ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text_for_buy_spins = (
            f"🧧 Стоимость: {SPIN_COST} 🧩\n\n"
            f"У вас  {user_data['crystals']} 🧩 фрагментов.")
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(NOTEBOOK_MENU_IMAGE_PATH, "rb"),
                                      caption=message_text_for_buy_spins),
                reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Failed to buy_spins: {e}")

    elif data == "exchange_crystals_for_spin":
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
                    media=InputMediaPhoto(media=open(NOTEBOOK_MENU_IMAGE_PATH, "rb"), caption=message_text_success),
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"Exchange success error: {e}")
        else:
            await query.answer("Недостаточно фрагментов для покупки жетона!", show_alert=True)

    # --- Обработка кнопок Игрового Бота "Евангелие" ---
    elif data == 'send_papa':
        try:
            await query.message.reply_text(
                'Добро пожаловать в мир "Евангелия" — интерактивной игры бота ISSUE! 🪐\n\n'
                '▎Что вас ждет в "Евангелии"? \n\n'
                '1. ⛩️ Хождение на службу — Молитвы...\n\n'
                '📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫',
                parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка 'send_papa': {e}")

    elif data == 'show_commands':
        await send_command_list(update, context)

    elif data.startswith('gospel_top_'):
        parts = data.split('_')
        view = parts[2]
        scope = parts[4]
        page = int(parts[6]) if len(parts) > 6 else 1

        if scope == 'chat':
            target_chat_id = query.message.chat.id if query.message.chat.type in ['group',
                                                                                  'supergroup'] else GROUP_CHAT_ID
        else:
            target_chat_id = 0

        message_text, reply_markup = await _get_leaderboard_message(context, target_chat_id, view, scope, page)
        try:
            await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Leaderboard error: {e}")

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
    init_db()
    application = ApplicationBuilder().token(TOKEN).build()
    # 1. Сначала КОМАНДЫ (начинаются с /)
    application.add_handler(CommandHandler("start", unified_start_command))
    application.add_handler(CommandHandler("name", set_name))
    application.add_handler(CommandHandler("shop", shop))
    application.add_handler(CommandHandler("top", top_main_menu))
    application.add_handler(CommandHandler("premium", premium_info))
    application.add_handler(CommandHandler("account", profile))
    
    # 2. Потом специфичные ТЕКСТОВЫЕ команды (Regex)
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^аккаунт$"), profile))
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^регнуть$"), regnut_handler))
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^моба$"), mobba_handler))
    application.add_handler(MessageHandler(filters.Regex(r"^\d{9}\s\(\d{4}\)$"), id_detection_handler))
    # 3. Общий обработчик текста (RP-команды и прочее)
    # Важно: он должен быть НИЖЕ "моба" и "регнуть
    # 4. Обработчики платежей
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))
    # 5. CALLBACKS (Кнопки)
    # Сначала специфичные паттерны!
    
    # Регистрация нажатий кнопок магазина (pattern ловит все вызовы начинающиеся на buy_shop_)
    application.add_handler(CallbackQueryHandler(shop_callback_handler, pattern="^buy_shop_"))
    application.add_handler(CallbackQueryHandler(handle_moba_my_cards, pattern="^moba_my_cards$"))
    application.add_handler(CallbackQueryHandler(moba_show_cards_all, pattern="^moba_show_cards_all_"))
    application.add_handler(CallbackQueryHandler(back_to_profile_from_moba, pattern="^back_to_profile_from_moba$"))
    application.add_handler(CallbackQueryHandler(handle_bag, pattern="^bag$"))
    application.add_handler(CallbackQueryHandler(handle_moba_collections, pattern="^moba_show_collections$"))
    application.add_handler(CallbackQueryHandler(moba_view_collection_cards, pattern="^moba_view_col_"))
    application.add_handler(CallbackQueryHandler(moba_show_cards_by_rarity, pattern="^moba_show_cards_rarity_"))
    application.add_handler(CallbackQueryHandler(handle_moba_collections, pattern="^moba_collections_page_"))
    application.add_handler(CallbackQueryHandler(handle_moba_collections, pattern="^moba_collections$"))
    application.add_handler(CallbackQueryHandler(confirm_id_callback, pattern="^confirm_add_id$"))
    application.add_handler(CallbackQueryHandler(cancel_id_callback, pattern="^cancel_add_id$"))
    # ... остальные специфичные CallbackQueryHandler ...
    # В самом конце списка колбэков — универсальный (если он нужен)
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unified_text_message_handler))
    application.add_error_handler(error_handler)
    application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()


















