import io, asyncio
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
    Message, LabeledPrice, CallbackQuery
from telegram.constants import ChatAction, ParseMode
from datetime import datetime, timezone, timedelta
from collections import defaultdict, OrderedDict
from typing import Optional, Tuple, List, Dict
from telegram.helpers import mention_html
from psycopg2.extras import DictCursor
from telegram.error import BadRequest
from functools import wraps, partial
from dotenv import load_dotenv

_CALLBACK_LAST_TS: Dict[Tuple[int, str], float] = {}
DEBOUNCE_SECONDS = 2
load_dotenv()
DEFAULT_PROFILE_IMAGE = os.path.join(".", "default_avatar.jpg")

def normalize_collection_name(name: Optional[str]) -> str:
    if not name: return ""
    return name.strip().upper()

def decode_collection_short_token(token: str) -> str:
    return SHORT_TO_COLLECTION_MAP.get(token, token)
NOTEBOOK_MENU_CAPTION = (
    "─────── ⋆⋅☆⋅⋆ ───────\n📙Блокнот с картами 📙\n➖➖➖➖➖➖➖➖➖➖\n👤 Профиль: {username}\n🔖 ID: {user_id}\n➖➖➖➖➖➖➖➖➖➖\n🧧 Жетоны: {token_count}\n🧩 Фрагменты: {fragment_count}\n─────── ⋆⋅☆⋅⋆ ───────\n")

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не установлен в переменных окружения!")
TOP_COMMAND_COOLDOWN_SECONDS = 30

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения!")
COLLECTIONS_PER_PAGE = 5
GROUP_CHAT_ID: int = int(os.environ.get("GROUP_CHAT_ID", "-1002372051836"))  # Основной ID элитры
AQUATORIA_CHAT_ID: Optional[int] = int(
    os.environ.get("AQUATORIA_CHAT_ID", "-1003405511585"))  # ID другой группы, если есть
CHAT_ISSUE_USERNAME = "chat_issue"
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "issuemlbb")
CHAT_USERNAME = os.getenv("CHAT_USERNAME", "CHAT_ISSUE")
CHANNEL_ID = f"@{CHANNEL_USERNAME}"
CHAT_ID = f"@{CHAT_USERNAME}"
GROUP_USERNAME_PLAIN = os.environ.get("GROUP_USERNAME_PLAIN", "CHAT_ISSUE")
GROUP_CHAT_INVITE_LINK = os.environ.get("GROUP_CHAT_INVITE_LINK")
PHOTO_BASE_PATH = "."  # Относительный путь к папке с фотографиями
NUM_PHOTOS = 74
PAGE_SIZE = 15  # Количество записей на одной странице топа
COOLDOWN_SECONDS = 10800  # Задержка между командами "лав иска"
SPIN_COST = 200  # Стоимость крутки в кристаллах
SPIN_USED_COOLDOWN = 600  # 10 минут
REPEAT_CRYSTALS_BONUS = 80  # Кристаллы за повторную карточку
COLLECTION_MENU_IMAGE_PATH = os.path.join(PHOTO_BASE_PATH, "photo_2025-12-17_17-01-44.jpg")
NOTEBOOK_MENU_IMAGE_PATH = os.path.join(PHOTO_BASE_PATH, "photo_2025-12-17_17-03-14.jpg")
privetstvie = os.path.join(PHOTO_BASE_PATH, "photo_2026-06-09_19-22-09.jpg")
CACHED_CHANNEL_ID = None
CACHED_GROUP_ID = None
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK")  # Добавил переменную для инвайт-линка канала
NOTEBOOK_MENU_OWNERSHIP: Dict[Tuple[int, int], int] = {}
LIFETIME_PREMIUM_USER_IDS = {2123680656}
ADMIN_ID = 2123680656  # Ваш ID
SHOP_BOOSTER_DAILY_LIMIT = 2  # ежедневный лимит бустеров
SHOP_LUCK_WEEKLY_LIMIT = 5  # недельный лимит удачи (в примере 1/2)
SHOP_PROTECT_WEEKLY_LIMIT = 5  # недельный лимит защиты (в примере 2/4)
LAV_ISKA_REGEX = re.compile(r"^(лав иска)$", re.IGNORECASE)
MY_COLLECTION_REGEX = re.compile(r"^(блокнот)$", re.IGNORECASE)

ACHIEVEMENTS = [{"id": "ach_10", "name": "1. «Новичок»\nСобрал 10 уникальных карточек", "threshold": 10,
                 "reward": {"type": "spins", "amount": 5}},
                {"id": "ach_25", "name": "2. «Любитель»\nСобрал 25 уникальных карточек", "threshold": 25,
                 "reward": {"type": "spins", "amount": 5}},
                {"id": "ach_50", "name": "3. «Мастер»\nСобрал 50 уникальных карточек", "threshold": 50,
                 "reward": {"type": "spins", "amount": 10}},
                {"id": "ach_all", "name": "4. «Гуру»\nСобрал 74 уникальных карточек", "threshold": NUM_PHOTOS,
                 "reward": {"type": "crystals", "amount": 1000}}, ]

DIAMONDS_REWARD_BASE = {
    "regular card": 10,
    "rare card": 20,
    "exclusive card": 40,
    "epic card": 80,
    "collectible card": 150,
    "limited": 250
}

# Бонус к базе, если карта состоит в коллекции
COLLECTION_BONUS = 15
# Множитель за повторку
REPEAT_DIAMOND_MULTIPLIER = 5

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

COLLECTION_SHORT_MAP = {
    "KISHIN DENSETSU": "KD", "ATOMIC POP": "AP",
    "ATTACK ON TITAN": "AOT","NEOBEASTS": "NB",
    "SOUL VESSELS": "SV","EXORCIST": "EX",
    "MYSTIC MEOW": "MM", "M-WORLD": "MW",
    "SANRIO CHARASTERS": "SC", "CLOUD": "CL",
    "LIMITED": "LTD", "STUN": "ST",
    "THE ASPIRANTS": "ASP", "NARUTO": "NR",
    "KUNG FU PANDA": "KFP", "SAINTS SERIES": "SS",
    "VENOM": "VM", "MISTBENDERS": "MB",
    "HUNTERxHUNTER": "HXH", "COVENANT": "CV",
    "STAR WARS": "SW","LIGHTBORN": "LB",
    "JUJUTSU KAISEN": "JK","TRANSFORMERS": "TF",
    "LEGEND": "LG","SPARKLE": "SPK", " ": "NONE", }


SHORT_TO_COLLECTION_MAP = {v: k for k, v in COLLECTION_SHORT_MAP.items()}
async def safe_edit_message_text(query, text, reply_markup=None):
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info("Сообщение не изменено.")
        else:
            logger.error(f"Ошибка редактирования сообщения: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при редактировании: {e}")

def to_roman(num):
    if not isinstance(num, int) or not (0 < num < 4000):
        raise ValueError("Число должно быть целым от 1 до 3999")

    roman_map = OrderedDict()
    roman_map[1000] = "M"
    roman_map[900] = "CM"
    roman_map[500] = "D"
    roman_map[400] = "CD"
    roman_map[100] = "C"
    roman_map[90] = "XC"
    roman_map[50] = "L"
    roman_map[40] = "XL"
    roman_map[10] = "X"
    roman_map[9] = "IX"
    roman_map[5] = "V"
    roman_map[4] = "IV"
    roman_map[1] = "I"

    roman_numeral = ""
    for value, symbol in roman_map.items():
        while num >= value:
            roman_numeral += symbol
            num -= value
    return roman_numeral




async def safe_delete_message(query, context):
    try:
        await query.message.delete()
    except BadRequest as e:
        if "Message to delete not found" in str(e):
            logger.info("Сообщение уже удалено.")
        else:
            logger.error(f"Ошибка при удалении: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка удаления: {e}")

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

PACK_PRICES = {"1": 1100, "2": 1300, "3": 1600, "4": 2100, "5": 3000, "ltd": 5000,}

PACK_RARITIES_MAP = {
    "1": ["regular card"], "2": ["rare card"], "3": ["exclusive card"],
    "4": ["epic card"], "5": ["collectible card"], "ltd": ["LIMITED"],}

# Количество карт в каждом наборе
CARDS_PER_PACK = 3

PHOTO_DETAILS = {
    1: {"path": os.path.join(PHOTO_BASE_PATH, "1 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nрай!\n\n🔖…1!"},
    2: {"path": os.path.join(PHOTO_BASE_PATH, "2 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nкогда вместе!\n\n🔖…2! "},
    3: {"path": os.path.join(PHOTO_BASE_PATH, "3 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nуметь переглядываться!\n\n🔖…3! "},
    4: {"path": os.path.join(PHOTO_BASE_PATH, "4 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nбыть на коне!\n\n🔖…4! "},
    5: {"path": os.path.join(PHOTO_BASE_PATH, "5 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nпочувствовать легкое головокружение!\n\n🔖…5! "},
    6: {"path": os.path.join(PHOTO_BASE_PATH, "6 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nобнимашки!\n\n🔖…6! "},
    7: {"path": os.path.join(PHOTO_BASE_PATH, "7 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nне только сахар!\n\n🔖…7! "},
    8: {"path": os.path.join(PHOTO_BASE_PATH, "8 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nпонимать друг друга без слов!\n\n🔖…8! "},
    9: {"path": os.path.join(PHOTO_BASE_PATH, "9 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nуметь успокоить!\n\n🔖…9! "},
    10: {"path": os.path.join(PHOTO_BASE_PATH, "10 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nсуметь удержаться!\n\n🔖…10! "},
    11: {"path": os.path.join(PHOTO_BASE_PATH, "11 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nне дать себя запутать!\n\n🔖…11! "},
    12: {"path": os.path.join(PHOTO_BASE_PATH, "12 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nсуметь сохранить секретик!\n\n🔖…12! "},
    13: {"path": os.path.join(PHOTO_BASE_PATH, "13 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nпод прикрытием\n\n🔖…13! "},
    14: {"path": os.path.join(PHOTO_BASE_PATH, "14 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nкогда нам по пути!\n\n🔖…14! "},
    15: {"path": os.path.join(PHOTO_BASE_PATH, "15 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nпрорыв.\n\n🔖…15! "},
    16: {"path": os.path.join(PHOTO_BASE_PATH, "16 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nзагадывать желание\n\n🔖…16!  "},
    17: {"path": os.path.join(PHOTO_BASE_PATH, "17 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nлето круглый год!\n\n🔖…17! "},
    18: {"path": os.path.join(PHOTO_BASE_PATH, "18 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nромантика!\n\n🔖…18! "},
    19: {"path": os.path.join(PHOTO_BASE_PATH, "19 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nкогда жарко!\n\n🔖…19! "},
    20: {"path": os.path.join(PHOTO_BASE_PATH, "20 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nраскрываться!\n\n🔖…20! "},
    21: {"path": os.path.join(PHOTO_BASE_PATH, "21 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nвыполнять обещания\n\n🔖…21! "},
    22: {"path": os.path.join(PHOTO_BASE_PATH, "22 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nцирк вдвоем!\n\n🔖…22! "},
    23: {"path": os.path.join(PHOTO_BASE_PATH, "23 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nслышать друг друга!\n\n🔖…23! "},
    24: {"path": os.path.join(PHOTO_BASE_PATH, "24 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nсладость\n\n🔖…24! "},
    25: {"path": os.path.join(PHOTO_BASE_PATH, "25 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nне упустить волну!\n\n🔖…25! "},
    26: {"path": os.path.join(PHOTO_BASE_PATH, "26 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nсказать о важном!\n\n🔖…26! "},
    27: {"path": os.path.join(PHOTO_BASE_PATH, "27 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nискриться!\n\n🔖…27! "},
    28: {"path": os.path.join(PHOTO_BASE_PATH, "28 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nтолько мы вдвоём\n\n🔖…28! "},
    29: {"path": os.path.join(PHOTO_BASE_PATH, "29 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпервое прикосновение\n\n🔖…29! "},
    30: {"path": os.path.join(PHOTO_BASE_PATH, "30 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nвзять дело в свои руки\n\n🔖…30! "},
    31: {"path": os.path.join(PHOTO_BASE_PATH, "31 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда не важно какая погода\n\n🔖…31! "},
    32: {"path": os.path.join(PHOTO_BASE_PATH, "32 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nуметь прощать!\n\n🔖…32! "},
    33: {"path": os.path.join(PHOTO_BASE_PATH, "33 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nотметиться!\n\n🔖…33! "},
    34: {"path": os.path.join(PHOTO_BASE_PATH, "34 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпервый поцелуй\n\n🔖…34!"},
    35: {"path": os.path.join(PHOTO_BASE_PATH, "35 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда без интернета! \n\n🔖…35!"},
    36: {"path": os.path.join(PHOTO_BASE_PATH, "36 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nлегкое головокружение\n\n🔖…36!"},
    37: {"path": os.path.join(PHOTO_BASE_PATH, "37 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпозвонить просто так\n\n🔖…37!"},
    38: {"path": os.path.join(PHOTO_BASE_PATH, "38 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nвсё что нужно\n\n🔖…38!"},
    39: {"path": os.path.join(PHOTO_BASE_PATH, "39 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nто, что создаёшь ты\n\n🔖…39!"},
    40: {"path": os.path.join(PHOTO_BASE_PATH, "40 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nсвобода\n\n🔖…40!"},
    41: {"path": os.path.join(PHOTO_BASE_PATH, "41 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда пробежала искра!\n\n🔖…41!"},
    42: {"path": os.path.join(PHOTO_BASE_PATH, "42 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nизображать недотрогу \n\n🔖…42!"},
    43: {"path": os.path.join(PHOTO_BASE_PATH, "43 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nсварить ему борщ)\n\n🔖…43!"},
    44: {"path": os.path.join(PHOTO_BASE_PATH, "44 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпотрясать мир \n\n🔖…44!"},
    45: {"path": os.path.join(PHOTO_BASE_PATH, "45 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда он не ангел!\n\n🔖…45!"},
    46: {"path": os.path.join(PHOTO_BASE_PATH, "46 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпритягивать разных!\n\n🔖…46!"},
    47: {"path": os.path.join(PHOTO_BASE_PATH, "47 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nтепло внутри, когда холодно снаружи \n\n🔖…47!"},
    48: {"path": os.path.join(PHOTO_BASE_PATH, "48 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nделать покупки друг друга\n\n🔖…48!"},
    49: {"path": os.path.join(PHOTO_BASE_PATH, "49 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nнемного колкости\n\n🔖…49!"},
    50: {"path": os.path.join(PHOTO_BASE_PATH, "50 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда тянет магнитом \n\n🔖…50!"},
    51: {"path": os.path.join(PHOTO_BASE_PATH, "51 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nбыть на седьмом небе!\n\n🔖…51!"},
    52: {"path": os.path.join(PHOTO_BASE_PATH, "52 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nты и я\n\n🔖…52!"},
    53: {"path": os.path.join(PHOTO_BASE_PATH, "53 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда купил самое необходимое!\n\n🔖…53!"},
    54: {"path": os.path.join(PHOTO_BASE_PATH, "54 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкак первый день весны!\n\n🔖…54!"},
    55: {"path": os.path.join(PHOTO_BASE_PATH, "55 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпоздравить первым!\n\n🔖…55!"},
    56: {"path": os.path.join(PHOTO_BASE_PATH, "56 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nоставить след!\n\n🔖…56!"},
    57: {"path": os.path.join(PHOTO_BASE_PATH, "57 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nмикс чувств!\n\n🔖…57!"},
    58: {"path": os.path.join(PHOTO_BASE_PATH, "58 — копия.jpg"),"caption": "❤️‍🔥 LOVE IS…\nслучайные порывы!\n\n🔖…58!"},
    59: {"path": os.path.join(PHOTO_BASE_PATH, "59 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда мысли сходятся!\n\n🔖…59!"},
    60: {"path": os.path.join(PHOTO_BASE_PATH, "60 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпосильная ноша!\n\n🔖…60!"},
    61: {"path": os.path.join(PHOTO_BASE_PATH, "61 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nвыбрать свое сердце!\n\n🔖…61!"},
    62: {"path": os.path.join(PHOTO_BASE_PATH, "62 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nто, что требует заботы!\n\n🔖…62!"},
    63: {"path": os.path.join(PHOTO_BASE_PATH, "63 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nбессонные ночи!\n\n🔖…63!"},
    64: {"path": os.path.join(PHOTO_BASE_PATH, "64 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nбыть на вершине мира\n\n🔖…64!"},
    65: {"path": os.path.join(PHOTO_BASE_PATH, "65 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nисправлять ошибки!\n\n🔖…65!"},
    66: {"path": os.path.join(PHOTO_BASE_PATH, "66 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nлюбоваться друг другом!\n\n🔖…66!"},
    67: {"path": os.path.join(PHOTO_BASE_PATH, "67 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nдарить главное!\n\n🔖…67!"},
    68: {"path": os.path.join(PHOTO_BASE_PATH, "68 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nкогда совсем не холодно!\n\n🔖…68!"},
    69: {"path": os.path.join(PHOTO_BASE_PATH, "69 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nдобавить изюминку!\n\n🔖…69!"},
    70: {"path": os.path.join(PHOTO_BASE_PATH, "70 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nснится друг другу!\n\n🔖…70!"},
    71: {"path": os.path.join(PHOTO_BASE_PATH, "71 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nпикник на двоих!\n\n🔖…71!"},
    72: {"path": os.path.join(PHOTO_BASE_PATH, "72 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nдурачиться, как дети\n\n🔖…72!"},
    73: {"path": os.path.join(PHOTO_BASE_PATH, "73 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nдарить себя!\n\n🔖…73!"},
    74: {"path": os.path.join(PHOTO_BASE_PATH, "74 — копия.jpg"),"caption": "️‍❤️‍🔥 LOVE IS…\nгорячее сердце!\n\n🔖…74!"},
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
    "regular card": 30, "rare card": 24, "exclusive card": 19, "epic card": 14,
    "collectible card": 10, "LIMITED": 3}
PREMIUM_RARITY_CHANCES = {"regular card": 12, "rare card": 12, "exclusive card": 25,
                          "epic card": 20, "collectible card": 25, "LIMITED": 10}

CARDS = {
    1: {"name": "Angela", "collection": "KISHIN DENSETSU", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "1.jpg")},
    2: {"name": "Karrie", "collection": "KISHIN DENSETSU", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "2.jpg")},
    3: {"name": "Lancelot", "collection": "KISHIN DENSETSU", "points": 1500,"path": os.path.join(PHOTO_BASE_PATH, "3.jpg")},
    4: {"name": "Miya", "collection": "ATOMIC POP", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "4.jpg")},
    5: {"name": "Eudora", "collection": "ATOMIC POP", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "5.jpg")},
    6: {"name": "Yin", "collection": "ATTACK ON TITAN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "6.jpg")},
    7: {"name": "Martis", "collection": "ATTACK ON TITAN", "points": 1500,"path": os.path.join(PHOTO_BASE_PATH, "7.jpg")},
    8: {"name": "Fanny", "collection": "ATTACK ON TITAN", "points": 1500,"path": os.path.join(PHOTO_BASE_PATH, "8.jpg")},
    9: {"name": "Balmond", "path": os.path.join(PHOTO_BASE_PATH, "9.jpg")},
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
    23: {"name": "Silvanna", "collection": "MYSTIC MEOW", "points": 1000,"path": os.path.join(PHOTO_BASE_PATH, "23.jpg")},
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
    34: {"name": "Nana", "path": os.path.join(PHOTO_BASE_PATH, "34.jpg")},
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
    47: {"name": "Wanwan", "path": os.path.join(PHOTO_BASE_PATH, "47.jpg")},
    48: {"name": "Atlas", "path": os.path.join(PHOTO_BASE_PATH, "48.jpg")},
    49: {"name": "Bane", "path": os.path.join(PHOTO_BASE_PATH, "49.jpg")},
    50: {"name": "Chang'e", "collection": "THE ASPIRANTS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "50.jpg")},
    51: {"name": "Ruby", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "51.jpg")},
    52: {"name": "Fanny", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "52.jpg")},
    53: {"name": "Angela", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "53.jpg")},
    54: {"name": "Lesley", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "54.jpg")},
    55: {"name": "Layla", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "55.jpg")},
    56: {"name": "Guinevere", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "56.jpg")},
    57: {"name": "Vexana", "collection": "THE ASPIRANTS", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "57.jpg")},
    58: {"name": "Lukas", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "58.jpg")},
    59: {"name": "Hayabusa", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "59.jpg")},
    60: {"name": "Suyou", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "60.jpg")},
    61: {"name": "Kalea", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "61.jpg")},
    62: {"name": "Vale", "collection": "NARUTO", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "62.jpg")},
    63: {"name": "Chip", "path": os.path.join(PHOTO_BASE_PATH, "63.jpg")},
    64: {"name": "Rafaela", "path": os.path.join(PHOTO_BASE_PATH, "64.jpg")},
    65: {"name": "Thamu", "collection": "KUNG FU PANDA", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "65.jpg")},
    66: {"name": "Ling", "collection": "KUNG FU PANDA", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "66.jpg")},
    67: {"name": "Akai", "collection": "KUNG FU PANDA", "points": 1500,"path": os.path.join(PHOTO_BASE_PATH, "67.jpg")},
    68: {"name": "Eudura", "path": os.path.join(PHOTO_BASE_PATH, "68.jpg")},
    69: {"name": "Natalia", "path": os.path.join(PHOTO_BASE_PATH, "69.jpg")},
    70: {"name": "Valir", "collection": "SAINTS SERIES", "points": 1000, "path": os.path.join(PHOTO_BASE_PATH, "70.jpg")},
    71: {"name": "Chou", "collection": "SAINTS SERIES", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "71.jpg")},
    72: {"name": "Badang", "collection": "SAINTS SERIES", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "72.jpg")},
    73: {"name": "Hano", "path": os.path.join(PHOTO_BASE_PATH, "73.jpg")},
    74: {"name": "Helcurt", "path": os.path.join(PHOTO_BASE_PATH, "74.jpg")},
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
    85: {"name": "Aldous", "collection": "MISTBENDERS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "85.jpg")},
    86: {"name": "Julian", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "86.jpg")},
    87: {"name": "Dyrroth", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "87.jpg")},
    88: {"name": "Harith", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "88.jpg")},
    89: {"name": "Cecilion", "collection": "HUNTERxHUNTER", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "89.jpg")},
    90: {"name": "Benedetta", "collection": "COVENANT", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "90.jpg")},
    91: {"name": "Lesley", "collection": "COVENANT", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "91.jpg")},
    92: {"name": "Thamuz", "path": os.path.join(PHOTO_BASE_PATH, "92.jpg")},
    93: {"name": "Valentine", "path": os.path.join(PHOTO_BASE_PATH, "93.jpg")},
    94: {"name": "Kadita", "path": os.path.join(PHOTO_BASE_PATH, "94.jpg")},
    95: {"name": "Cyclops", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "95.jpg")},
    96: {"name": "Alucard", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "96.jpg")},
    97: {"name": "Argus", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "97.jpg")},
    98: {"name": "Kimmy", "collection": "STAR WARS", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "98.jpg")},
    99: {"name": "Obsisia", "path": os.path.join(PHOTO_BASE_PATH, "99.jpg")},
    100: {"name": "Fanny", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "100.jpg")},
    101: {"name": "Harith", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "101.jpg")},
    102: {"name": "Alucard", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "102.jpg")},
    103: {"name": "Granger", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "103.jpg")},
    104: {"name": "Tigreal", "collection": "LIGHTBORN", "points": 1500, "path": os.path.join(PHOTO_BASE_PATH, "104.jpg")},
    105: {"name": "Xavier", "collection": "JUJUTSU KAISEN", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "105.jpg")},
    106: {"name": "Julian", "collection": "JUJUTSU KAISEN", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "106.jpg")},
    107: {"name": "Yin", "collection": "JUJUTSU KAISEN", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "107.jpg")},
    108: {"name": "Melissa", "collection": "JUJUTSU KAISEN", "points": 1500,  "path": os.path.join(PHOTO_BASE_PATH, "108.jpg")},
    109: {"name": "Suyou", "path": os.path.join(PHOTO_BASE_PATH, "109.jpg")},
    110: {"name": "Granger", "collection": "TRANSFORMERS", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "110.jpg")},
    111: {"name": "Johnson", "collection": "TRANSFORMERS", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "111.jpg")},
    112: {"name": "X.Borg", "collection": "TRANSFORMERS", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "112.jpg")},
    113: {"name": "Roger", "collection": "TRANSFORMERS", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "113.jpg")},
    114: {"name": "Popol and Kupa", "collection": "TRANSFORMERS", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "114.jpg")},
    115: {"name": "Aldous", "collection": "TRANSFORMERS", "points": 1000,  "path": os.path.join(PHOTO_BASE_PATH, "115.jpg")},
    116: {"name": "Novaria", "path": os.path.join(PHOTO_BASE_PATH, "116.jpg")},
    117: {"name": "Barats", "path": os.path.join(PHOTO_BASE_PATH, "117.jpg")},
    118: {"name": "Phoveus", "path": os.path.join(PHOTO_BASE_PATH, "118.jpg")},
    119: {"name": "Aulus", "path": os.path.join(PHOTO_BASE_PATH, "119.jpg")},
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
    134: {"name": "Joy", "path": os.path.join(PHOTO_BASE_PATH, "134.jpg")},
    135: {"name": "Joy", "path": os.path.join(PHOTO_BASE_PATH, "135.jpg")},
    136: {"name": "Arlott", "path": os.path.join(PHOTO_BASE_PATH, "136.jpg")},
    137: {"name": "Ixia", "path": os.path.join(PHOTO_BASE_PATH, "137.jpg")},
    138: {"name": "Cici", "path": os.path.join(PHOTO_BASE_PATH, "138.jpg")},
    139: {"name": "Suyou", "path": os.path.join(PHOTO_BASE_PATH, "139.jpg")},
    140: {"name": "huxin", "path": os.path.join(PHOTO_BASE_PATH, "140.jpg")},
    141: {"name": "huxin", "path": os.path.join(PHOTO_BASE_PATH, "141.jpg")},
    142: {"name": "Kalea", "path": os.path.join(PHOTO_BASE_PATH, "142.jpg")},
    143: {"name": "Sora", "path": os.path.join(PHOTO_BASE_PATH, "143.jpg")},
    144: {"name": "Lukas", "path": os.path.join(PHOTO_BASE_PATH, "144.jpg")},
    145: {"name": "Novaria", "path": os.path.join(PHOTO_BASE_PATH, "145.jpg")},
    146: {"name": "Cici", "path": os.path.join(PHOTO_BASE_PATH, "146.jpg")},
    147: {"name": "Ixia", "path": os.path.join(PHOTO_BASE_PATH, "147.jpg")},
    148: {"name": "Melissa", "path": os.path.join(PHOTO_BASE_PATH, "148.jpg")},
    149: {"name": "Aanom", "path": os.path.join(PHOTO_BASE_PATH, "149.jpg")},
    150: {"name": "Edith", "path": os.path.join(PHOTO_BASE_PATH, "150.jpg")},
    151: {"name": "Aulus", "path": os.path.join(PHOTO_BASE_PATH, "151.jpg")},
    152: {"name": "Beatrix", "path": os.path.join(PHOTO_BASE_PATH, "152.jpg")},
    153: {"name": "Natan", "path": os.path.join(PHOTO_BASE_PATH, "153.jpg")},
    154: {"name": "Gloo", "path": os.path.join(PHOTO_BASE_PATH, "154.jpg")},
    155: {"name": "Gloo", "path": os.path.join(PHOTO_BASE_PATH, "155.jpg")},
    156: {"name": "Barats", "path": os.path.join(PHOTO_BASE_PATH, "156.jpg")},
    157: {"name": "Yu hong", "path": os.path.join(PHOTO_BASE_PATH, "157.jpg")},
    158: {"name": "Atlas", "path": os.path.join(PHOTO_BASE_PATH, "158.jpg")},
    159: {"name": "Fasha", "path": os.path.join(PHOTO_BASE_PATH, "159.jpg")},
    160: {"name": "Cecilion", "path": os.path.join(PHOTO_BASE_PATH, "160.jpg")},
    161: {"name": "Wanwan", "path": os.path.join(PHOTO_BASE_PATH, "161.jpg")},
    162: {"name": "Tigreal", "path": os.path.join(PHOTO_BASE_PATH, "162.jpg")},
    163: {"name": "Bruno", "path": os.path.join(PHOTO_BASE_PATH, "163.jpg")},
    164: {"name": "Clint", "path": os.path.join(PHOTO_BASE_PATH, "164.jpg")},
    165: {"name": "Harley", "path": os.path.join(PHOTO_BASE_PATH, "165.jpg")},
    166: {"name": "Diggie", "path": os.path.join(PHOTO_BASE_PATH, "166.jpg")},
    167: {"name": "Leomord", "path": os.path.join(PHOTO_BASE_PATH, "167.jpg")},
    168: {"name": "Hylos", "path": os.path.join(PHOTO_BASE_PATH, "168.jpg")},
    169: {"name": "Kimmy", "path": os.path.join(PHOTO_BASE_PATH, "169.jpg")},
    170: {"name": "Minsitthar", "path": os.path.join(PHOTO_BASE_PATH, "170.jpg")},
    171: {"name": "Faramis", "path": os.path.join(PHOTO_BASE_PATH, "171.jpg")},
    172: {"name": "Khufra", "path": os.path.join(PHOTO_BASE_PATH, "172.jpg")},
    173: {"name": "Terila", "path": os.path.join(PHOTO_BASE_PATH, "173.jpg")},
    174: {"name": "X.Borg", "path": os.path.join(PHOTO_BASE_PATH, "174.jpg")},
    175: {"name": "Ling", "path": os.path.join(PHOTO_BASE_PATH, "175.jpg")},
    176: {"name": "Terila", "path": os.path.join(PHOTO_BASE_PATH, "176.jpg")},
    177: {"name": "Baxia", "path": os.path.join(PHOTO_BASE_PATH, "177.jpg")},
    178: {"name": "Masha", "path": os.path.join(PHOTO_BASE_PATH, "178.jpg")},
    179: {"name": "Alice", "path": os.path.join(PHOTO_BASE_PATH, "179.jpg")},
    180: {"name": "Karina", "path": os.path.join(PHOTO_BASE_PATH, "180.jpg")},
    181: {"name": "Karina", "path": os.path.join(PHOTO_BASE_PATH, "181.jpg")},
    182: {"name": "Bane", "path": os.path.join(PHOTO_BASE_PATH, "182.jpg")},
    183: {"name": "Wanwan", "path": os.path.join(PHOTO_BASE_PATH, "183.jpg")},
    184: {"name": "ilong", "path": os.path.join(PHOTO_BASE_PATH, "184.jpg")},
    185: {"name": "Natalia", "path": os.path.join(PHOTO_BASE_PATH, "185.jpg")},
    186: {"name": "Minotaur", "path": os.path.join(PHOTO_BASE_PATH, "186.jpg")},
    187: {"name": "Freya", "path": os.path.join(PHOTO_BASE_PATH, "187.jpg")},
    188: {"name": "Kagura", "path": os.path.join(PHOTO_BASE_PATH, "188.jpg")},
    189: {"name": "Alpha", "path": os.path.join(PHOTO_BASE_PATH, "189.jpg")},
    190: {"name": "Hilda", "path": os.path.join(PHOTO_BASE_PATH, "190.jpg")},
    191: {"name": "Vexana", "path": os.path.join(PHOTO_BASE_PATH, "191.jpg")},
    192: {"name": "Karrie", "path": os.path.join(PHOTO_BASE_PATH, "192.jpg")},
    193: {"name": "Gatotkaca", "path": os.path.join(PHOTO_BASE_PATH, "193.jpg")},
    194: {"name": "Grock", "path": os.path.join(PHOTO_BASE_PATH, "194.jpg")},
    195: {"name": "Odette", "path": os.path.join(PHOTO_BASE_PATH, "195.jpg")},
    196: {"name": "Lancelot", "path": os.path.join(PHOTO_BASE_PATH, "196.jpg")},
    197: {"name": "hask", "path": os.path.join(PHOTO_BASE_PATH, "197.jpg")},
    198: {"name": "Helcurt", "path": os.path.join(PHOTO_BASE_PATH, "198.jpg")},
    199: {"name": "Jawhead", "path": os.path.join(PHOTO_BASE_PATH, "199.jpg")},
    200: {"name": "Martis", "path": os.path.join(PHOTO_BASE_PATH, "200.jpg")},
    201: {"name": "Uranus", "path": os.path.join(PHOTO_BASE_PATH, "201.jpg")},
    202: {"name": "Kaja", "path": os.path.join(PHOTO_BASE_PATH, "202.jpg")},
    203: {"name": "Claude", "path": os.path.join(PHOTO_BASE_PATH, "203.jpg")},
    204: {"name": "Valt", "path": os.path.join(PHOTO_BASE_PATH, "204.jpg")},
    205: {"name": "Kagura", "path": os.path.join(PHOTO_BASE_PATH, "205.jpg")},
    206: {"name": "Kimmy", "path": os.path.join(PHOTO_BASE_PATH, "206.jpg")},
    207: {"name": "Belerick", "path": os.path.join(PHOTO_BASE_PATH, "207.jpg")},
    208: {"name": "Minsitthar", "path": os.path.join(PHOTO_BASE_PATH, "208.jpg")},
    209: {"name": "Badang", "path": os.path.join(PHOTO_BASE_PATH, "209.jpg")},
    210: {"name": "Guinevere", "path": os.path.join(PHOTO_BASE_PATH, "210.jpg")},
    211: {"name": "Guinevere", "path": os.path.join(PHOTO_BASE_PATH, "211.jpg")},
    212: {"name": "Yve", "path": os.path.join(PHOTO_BASE_PATH, "212.jpg")},
    213: {"name": "Lylia", "path": os.path.join(PHOTO_BASE_PATH, "213.jpg")},
    214: {"name": "Sun", "path": os.path.join(PHOTO_BASE_PATH, "214.jpg")},
    215: {"name": "Kadita", "path": os.path.join(PHOTO_BASE_PATH, "215.jpg")},
    216: {"name": "Silvanna", "path": os.path.join(PHOTO_BASE_PATH, "216.jpg")},
    217: {"name": "Silvanna", "path": os.path.join(PHOTO_BASE_PATH, "217.jpg")},
    218: {"name": "Carmilla", "path": os.path.join(PHOTO_BASE_PATH, "218.jpg")},
    219: {"name": "Luo Yi", "path": os.path.join(PHOTO_BASE_PATH, "219.jpg")},
    220: {"name": "Luo Yi", "path": os.path.join(PHOTO_BASE_PATH, "220.jpg")},
    221: {"name": "Khaleed", "path": os.path.join(PHOTO_BASE_PATH, "221.jpg")},
    222: {"name": "Mathilda", "path": os.path.join(PHOTO_BASE_PATH, "222.jpg")},
    223: {"name": "Mathilda", "path": os.path.join(PHOTO_BASE_PATH, "223.jpg")},
    224: {"name": "Gusion", "path": os.path.join(PHOTO_BASE_PATH, "224.jpg")},
    225: {"name": "Xavier", "path": os.path.join(PHOTO_BASE_PATH, "225.jpg")},
    226: {"name": "Estes", "path": os.path.join(PHOTO_BASE_PATH, "226.jpg")},
    227: {"name": "Selena", "path": os.path.join(PHOTO_BASE_PATH, "227.jpg")},
    228: {"name": "Nolan", "path": os.path.join(PHOTO_BASE_PATH, "228.jpg")},
    229: {"name": "Nolan", "path": os.path.join(PHOTO_BASE_PATH, "229.jpg")},
    230: {"name": "Fanny", "path": os.path.join(PHOTO_BASE_PATH, "230.jpg")},
    231: {"name": "Lesley", "path": os.path.join(PHOTO_BASE_PATH, "231.jpg")},
    232: {"name": "Cecilion", "path": os.path.join(PHOTO_BASE_PATH, "232.jpg")},
    233: {"name": "Clint", "path": os.path.join(PHOTO_BASE_PATH, "233.jpg")},
    234: {"name": "Selena", "path": os.path.join(PHOTO_BASE_PATH, "234.jpg")},
    235: {"name": "Arlott", "path": os.path.join(PHOTO_BASE_PATH, "235.jpg")},
    236: {"name": "Moskov", "path": os.path.join(PHOTO_BASE_PATH, "236.jpg")},
    237: {"name": "Lapu-Lapu", "path": os.path.join(PHOTO_BASE_PATH, "237.jpg")},
    238: {"name": "Roger", "path": os.path.join(PHOTO_BASE_PATH, "238.jpg")},
    239: {"name": "Brody", "path": os.path.join(PHOTO_BASE_PATH, "239.jpg")},
    240: {"name": "Popol and Kupa", "path": os.path.join(PHOTO_BASE_PATH, "240.jpg")},
    241: {"name": "Beatrix", "path": os.path.join(PHOTO_BASE_PATH, "241.jpg")},
    242: {"name": "Valentina", "path": os.path.join(PHOTO_BASE_PATH, "242.jpg")},
    243: {"name": "Melissa", "path": os.path.join(PHOTO_BASE_PATH, "243.jpg")},
    244: {"name": "Natan", "path": os.path.join(PHOTO_BASE_PATH, "244.jpg")},
    245: {"name": "Edith", "path": os.path.join(PHOTO_BASE_PATH, "245.jpg")},
    246: {"name": "Ling", "path": os.path.join(PHOTO_BASE_PATH, "246.jpg")},
    247: {"name": "X.Borg", "path": os.path.join(PHOTO_BASE_PATH, "247.jpg")},
    248: {"name": "Aurora", "path": os.path.join(PHOTO_BASE_PATH, "248.jpg")},
    249: {"name": "Yi Sun-shin", "path": os.path.join(PHOTO_BASE_PATH, "249.jpg")},
    250: {"name": "Fanny", "path": os.path.join(PHOTO_BASE_PATH, "250.jpg")},
    251: {"name": "Cyclops", "path": os.path.join(PHOTO_BASE_PATH, "251.jpg")},
    252: {"name": "Floryn", "path": os.path.join(PHOTO_BASE_PATH, "252.jpg")},
    253: {"name": "Esmeralda", "path": os.path.join(PHOTO_BASE_PATH, "253.jpg")},
    254: {"name": "Khufra", "path": os.path.join(PHOTO_BASE_PATH, "254.jpg")},
    255: {"name": "Harith", "path": os.path.join(PHOTO_BASE_PATH, "255.jpg")},
    256: {"name": "Jawhead", "path": os.path.join(PHOTO_BASE_PATH, "256.jpg")},
    257: {"name": "Moskov", "path": os.path.join(PHOTO_BASE_PATH, "257.jpg")},
    258: {"name": "Lolita", "path": os.path.join(PHOTO_BASE_PATH, "258.jpg")},
    259: {"name": "Sun", "path": os.path.join(PHOTO_BASE_PATH, "259.jpg")},
    260: {"name": "Layla", "path": os.path.join(PHOTO_BASE_PATH, "260.jpg")},
    261: {"name": "Melissa", "collection": "SPARKLE", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "261.jpg")},
    262: {"name": "Fredrinn", "collection": "SPARKLE", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "262.jpg")},
    263: {"name": "Estes", "collection": "SPARKLE", "points": 800, "path": os.path.join(PHOTO_BASE_PATH, "263.jpg")},
    264: {"name": "Fasha", "path": os.path.join(PHOTO_BASE_PATH, "264.jpg")},
    265: {"name": "Karina", "path": os.path.join(PHOTO_BASE_PATH, "265.jpg")},
    266: {"name": "Fanny", "path": os.path.join(PHOTO_BASE_PATH, "266.jpg")},
    267: {"name": "Natalia", "path": os.path.join(PHOTO_BASE_PATH, "267.jpg")},
    268: {"name": "Layla", "path": os.path.join(PHOTO_BASE_PATH, "268.jpg")},
    269: {"name": "Guinevere", "path": os.path.join(PHOTO_BASE_PATH, "269.jpg")},
}

FIXED_CARD_RARITIES = {
    1: "collectible card", 2: "collectible card", 3: "collectible card", 4: "collectible card",
    5: "collectible card", 6: "collectible card", 7: "collectible card", 8: "collectible card",
    9: "regular card",
    10: "collectible card", 11: "collectible card", 12: "collectible card", 13: "collectible card",
    14: "collectible card", 15: "collectible card", 16: "collectible card", 17: "collectible card",
    18: "collectible card", 19: "collectible card", 20: "collectible card", 21: "collectible card",
    22: "collectible card", 23: "collectible card", 24: "collectible card", 25: "collectible card",
    26: "collectible card", 27: "collectible card", 28: "collectible card", 29: "collectible card",
    30: "collectible card", 31: "collectible card", 32: "collectible card", 33: "collectible card",
    34: "regular card",
    35: "LIMITED", 36: "LIMITED", 37: "LIMITED", 38: "LIMITED", 39: "LIMITED", 40: "LIMITED",
    41: "LIMITED", 42: "LIMITED", 43: "LIMITED",
    44: "collectible card", 45: "collectible card", 46: "collectible card",
    47: "regular card", 48: "regular card", 49: "regular card",
    50: "collectible card", 51: "collectible card", 52: "collectible card", 53: "collectible card",
    54: "collectible card", 55: "collectible card", 56: "collectible card", 57: "collectible card",
    58: "collectible card", 59: "collectible card", 60: "collectible card", 61: "collectible card",
    62: "collectible card",
    63: "regular card", 64: "regular card",
    65: "collectible card", 66: "collectible card", 67: "collectible card",
    68: "regular card", 69: "regular card",
    70: "collectible card", 71: "collectible card", 72: "collectible card",
    73: "regular card", 74: "regular card",
    75: "collectible card", 76: "collectible card",77: "collectible card", 78: "collectible card",
    79: "collectible card", 80: "collectible card",
    81: "LIMITED", 82: "LIMITED", 83: "LIMITED",
    84: "collectible card",
    85: "collectible card", 86: "collectible card", 87: "collectible card", 88: "collectible card",
    89: "collectible card", 90: "collectible card", 91: "collectible card",
    92: "regular card", 93: "regular card", 94: "regular card",
    95: "collectible card", 96: "collectible card",97: "collectible card", 98: "collectible card",
    99: "regular card",
    100: "collectible card", 101: "collectible card", 102: "collectible card", 103: "collectible card",
    104: "collectible card", 105: "collectible card", 106: "collectible card", 107: "collectible card",
    108: "collectible card",
    109: "regular card",
    110: "collectible card", 111: "collectible card", 112: "collectible card", 113: "collectible card",
    114: "collectible card", 115: "collectible card",
    116: "regular card", 117: "regular card", 118: "regular card", 119: "regular card",
    120: "collectible card", 121: "collectible card", 122: "collectible card", 123: "collectible card",
    124: "collectible card", 125: "collectible card", 126: "collectible card", 127: "collectible card",
    128: "collectible card", 129: "collectible card", 130: "collectible card", 131: "collectible card",
    132: "collectible card", 133: "collectible card",
    134: "regular card", 135: "regular card", 136: "regular card", 137: "regular card", 138: "regular card",
    139: "regular card", 140: "regular card", 141: "regular card", 142: "regular card", 143: "regular card",
    144: "regular card", 145: "regular card",
    146: "rare card", 147: "rare card", 148: "rare card", 149: "rare card", 150: "rare card", 151: "rare card",
    152: "rare card", 153: "rare card", 154: "rare card", 155: "rare card", 156: "rare card", 157: "rare card",
    158: "rare card", 159: "rare card", 160: "rare card", 161: "rare card", 162: "rare card", 163: "rare card",
    164: "rare card", 165: "rare card", 166: "rare card", 167: "rare card", 168: "rare card", 169: "rare card",
    170: "rare card", 171: "rare card", 172: "rare card", 173: "rare card", 174: "rare card", 175: "rare card",
    176: "rare card", 177: "rare card", 178: "rare card",
    179: "exclusive card", 180: "exclusive card", 181: "exclusive card", 182: "exclusive card", 183: "exclusive card",
    184: "exclusive card", 185: "exclusive card", 186: "exclusive card", 187: "exclusive card", 188: "exclusive card",
    189: "exclusive card", 190: "exclusive card", 191: "exclusive card", 192: "exclusive card", 193: "exclusive card",
    194: "exclusive card", 195: "exclusive card", 196: "exclusive card", 197: "exclusive card", 198: "exclusive card",
    199: "exclusive card", 200: "exclusive card", 201: "exclusive card", 202: "exclusive card", 203: "exclusive card",
    204: "exclusive card", 205: "exclusive card", 206: "exclusive card", 207: "exclusive card", 208: "exclusive card",
    209: "exclusive card", 210: "exclusive card", 211: "exclusive card", 212: "exclusive card", 213: "exclusive card",
    214: "exclusive card", 215: "exclusive card", 216: "exclusive card", 217: "exclusive card", 218: "exclusive card",
    219: "exclusive card", 220: "exclusive card", 221: "exclusive card", 222: "exclusive card", 223: "exclusive card",
    224: "exclusive card", 225: "exclusive card", 226: "exclusive card", 227: "exclusive card", 228: "exclusive card",
    229: "exclusive card",
    230: "epic card", 231: "epic card", 232: "epic card", 233: "epic card", 234: "epic card", 235: "epic card",
    236: "epic card", 237: "epic card", 238: "epic card", 239: "epic card", 240: "epic card", 241: "epic card",
    242: "epic card", 243: "epic card", 244: "epic card", 245: "epic card", 246: "epic card", 247: "epic card",
    248: "epic card", 249: "epic card", 250: "epic card", 251: "epic card", 252: "epic card", 253: "epic card",
    254: "epic card", 255: "epic card", 256: "epic card", 257: "epic card", 258: "epic card", 259: "epic card",
    260: "epic card",
    261: "collectible card", 262: "collectible card", 263: "collectible card",
    264: "rare card", 265: "rare card", 266: "rare card", 267: "rare card", 268: "rare card", 269: "rare card",}

season_data = {
    "start_date": datetime(2026, 6, 1),  # Год, Месяц, День начала сезона
    "season_number": 1
}

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
    " <b>Огромный пинг!</b> Купи уже наконец-то Wi-Fi ",
    " <b>Поражение.</b> Эпики в твоей команде — это приговор",
    " <b>Твой билд не сработал.</b> Попробуй в следующий раз",
    "Тормоз года!У твоего отца и то быстрее реакция!",
    "Эпик в крови! Можешь менять ник на 'Корм для врагов'!",
    "Купи телефон! Твой POCO скоро взорвется!",
    "Скрытопульный ТП от Лои на фантан врага!",
    "1% зарядки! Телефон вырубился в самый важный момент замеса!",
    "Мама забрала телефон, вы слили катку! Тебе же говорили — «сначала уроки!»"
]

async def delete_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщение: {e}")

#4.команда /reset_season для админа, позволяющая вручную сбросить сезон и обнулить звезды
async def manual_reset_season_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return

    if not context.args or context.args[0].lower() != "подтверждаю":
        await update.message.reply_text(
            "⚠️ <b>ВНИМАНИЕ! Вы собираетесь сбросить игровой сезон вручную.</b>\n\n"
            "Это действие сбросит текущие звезды (stars) и сезонную статистику у <b>ВСЕХ</b> игроков в базе данных в 0.\n"
            "<i>(Общие звезды за все время и рекорды останутся нетронутыми).</i>\n\n"
            "Если вы уверены, отправьте команду строго в таком виде:\n"
            "<code>/reset_season подтверждаю</code>",
            parse_mode=ParseMode.HTML)
        return

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Получаем текущие данные счетчиков
        cursor.execute("SELECT value FROM system_settings WHERE key = 'roman_season_counter';")
        roman_season_counter_str = cursor.fetchone()[0] if cursor.rowcount > 0 else '0'
        roman_season_counter = int(roman_season_counter_str)

        cursor.execute("SELECT value FROM system_settings WHERE key = 'current_active_display_season_id';")
        current_active_display_season_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None

        # 2. Сохраняем статистику текущих игроков в историю предыдущего сезона
        if current_active_display_season_id:
            cursor.execute("""
                INSERT INTO moba_season_history (user_id, season_id, total_games, wins, final_rank)
                SELECT user_id, %s, season_reg_total, season_reg_success, 
                       CASE WHEN stars > 0 THEN get_rank_info(stars)::text ELSE 'Игрок AFK в этом сезоне' END
                FROM moba_users
                ON CONFLICT DO NOTHING;
            """, (current_active_display_season_id,))

        # 3. Определяем новый ID для отображаемого сезона
        new_display_season_id = ""
        if current_active_display_season_id == "1/2 SEASON":
            new_display_season_id = to_roman(1) + " SEASON"
            roman_season_counter = 1
        elif current_active_display_season_id is None or roman_season_counter == 0:
            new_display_season_id = "1/2 SEASON"
            roman_season_counter = 0
        else:
            roman_season_counter += 1
            new_display_season_id = to_roman(roman_season_counter) + " SEASON"

        # 4. Сбрасываем текущие звезды и сезонную статистику игроков
        cursor.execute("UPDATE moba_users SET stars = 0, season_reg_total = 0, season_reg_success = 0;")

        # 5. Обновляем системные настройки
        now = datetime.now()
        manual_trigger_id = f"{now.year}_MANUAL_{now.strftime('%m%d_%H%M')}"
        cursor.execute("""
            INSERT INTO system_settings (key, value) 
            VALUES ('last_monthly_trigger_season', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """, (manual_trigger_id,)) # Для ручного сброса тоже обновляем триггер, чтобы не сработало дважды

        cursor.execute("""
            INSERT INTO system_settings (key, value) 
            VALUES ('roman_season_counter', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """, (str(roman_season_counter),))

        cursor.execute("""
            INSERT INTO system_settings (key, value) 
            VALUES ('current_active_display_season_id', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """, (new_display_season_id,))

        conn.commit()

        await update.message.reply_text(
            f"✅ <b>Игровой сезон успешно сброшен вручную!</b>\n\n"
            f"• Текущие звезды и сезонная статистика всех игроков обнулены.\n"
            f"• Новый сезон: <b>{new_display_season_id}</b>",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Администратор {user_id} вручную обнулил сезон. Установлен ID: {new_display_season_id}")

    except Exception as e:
        logger.error(f"Ошибка ручного сброса сезона: {e}", exc_info=True)
        if conn:
            conn.rollback()
        await update.message.reply_text("❌ Произошла ошибка при выполнении операции в базе данных.")
    finally:
        if conn:
            conn.close()
            
async def reset_all_cards_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return

    # Проверка на подтверждение
    if not context.args or context.args[0].lower() != "подтверждаю":
        await update.message.reply_text(
            "⚠️ <b>ВНИМАНИЕ! Вы собираетесь УДАЛИТЬ ВСЕ КАРТЫ у абсолютно ВСЕХ пользователей.</b>\n\n"
            "Это действие полностью очистит инвентарь каждого игрока в базе данных. "
            "Его нельзя будет отменить!\n\n"
            "Если вы абсолютно уверены, отправьте команду строго в таком виде:\n"
            "<code>/reset_all_cards подтверждаю</code>",
            parse_mode=ParseMode.HTML
        )
        return

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Удаляем все карты из инвентаря
        cursor.execute("TRUNCATE TABLE moba_inventory CASCADE;")

        # 2. Обнуляем очки points у пользователей, так как карт больше нет
        cursor.execute("UPDATE moba_users SET points = 0;")

        conn.commit()

        await update.message.reply_text(
            "✅ <b>База данных успешно очищена!</b>\n\n"
            "• Все карты всех пользователей стерты.\n"
            "• Рейтинговые очки (points) сброшены в 0.\n"
            "• Игроки могут начать собирать новые, чистые карты!",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Администратор {user_id} полностью очистил таблицу инвентаря moba_inventory.")

    except Exception as e:
        logger.error(f"Ошибка при полной очистке карт: {e}", exc_info=True)
        if conn:
            conn.rollback()
        await update.message.reply_text("❌ Произошла критическая ошибка при очистке таблиц базы данных.")
    finally:
        if conn:
            conn.close()

#3.втоматически проверяет наступление нового игрового сезона (Весна/Лето/Осень/Зима) и обнуляет звезды игроков
async def check_season_reset():
    now = datetime.now()

    # Определяем ID триггера на основе текущего месяца (реальный сезон, чтобы понять, когда сбрасывать)
    if now.month in [3, 4, 5]:
        monthly_trigger_id = f"{now.year}_SPRING"
        season_name_ru = "Весна 🌸"
    elif now.month in [6, 7, 8]:
        monthly_trigger_id = f"{now.year}_SUMMER"
        season_name_ru = "Лето ☀️"
    elif now.month in [9, 10, 11]:
        monthly_trigger_id = f"{now.year}_AUTUMN"
        season_name_ru = "Осень 🍂"
    else:
        winter_year = now.year if now.month == 12 else now.year - 1
        monthly_trigger_id = f"{winter_year}_WINTER"
        season_name_ru = "Зима ❄️"

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT value FROM system_settings WHERE key = 'last_monthly_trigger_season';")
        last_monthly_trigger_season = cursor.fetchone()[0] if cursor.rowcount > 0 else None

        cursor.execute("SELECT value FROM system_settings WHERE key = 'roman_season_counter';")
        roman_season_counter_str = cursor.fetchone()[0] if cursor.rowcount > 0 else '0'
        roman_season_counter = int(roman_season_counter_str)

        cursor.execute("SELECT value FROM system_settings WHERE key = 'current_active_display_season_id';")
        current_active_display_season_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None

        # Проверяем, изменился ли реальный сезон
        if last_monthly_trigger_season != monthly_trigger_id:
            logger.info(f"Обнаружено изменение реального сезона: {last_monthly_trigger_season} -> {monthly_trigger_id}. Запускаем сброс.")

            # --- Шаг 1: Сохраняем статистику текущих игроков в историю предыдущего сезона ---
            if current_active_display_season_id: # Если уже был какой-то сезон
                cursor.execute("""
                    INSERT INTO moba_season_history (user_id, season_id, total_games, wins, final_rank)
                    SELECT user_id, %s, season_reg_total, season_reg_success, 
                           CASE WHEN stars > 0 THEN get_rank_info(stars)::text ELSE 'Игрок AFK в этом сезоне' END
                    FROM moba_users
                    ON CONFLICT DO NOTHING;
                """, (current_active_display_season_id,)) # Используем старый display ID для истории
            
            # --- Шаг 2: Определяем новый ID для отображаемого сезона ---
            new_display_season_id = ""
            if current_active_display_season_id == "1/2 SEASON": # Если прошлый был 1/2, следующий I
                new_display_season_id = to_roman(1) + " SEASON"
                roman_season_counter = 1
            elif current_active_display_season_id is None or roman_season_counter == 0:
                # Первый запуск или первый реальный сезон после сброса (если не 1/2)
                new_display_season_id = "1/2 SEASON"
                roman_season_counter = 0 # Обнуляем счетчик, если вернулись к 1/2
            else:
                roman_season_counter += 1
                new_display_season_id = to_roman(roman_season_counter) + " SEASON"

            # --- Шаг 3: Сбрасываем текущие звезды и сезонную статистику игроков ---
            cursor.execute("UPDATE moba_users SET stars = 0, season_reg_total = 0, season_reg_success = 0;")
            
            # --- Шаг 4: Обновляем системные настройки ---
            cursor.execute("""
                INSERT INTO system_settings (key, value) 
                VALUES ('last_monthly_trigger_season', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
            """, (monthly_trigger_id,))

            cursor.execute("""
                INSERT INTO system_settings (key, value) 
                VALUES ('roman_season_counter', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
            """, (str(roman_season_counter),))

            cursor.execute("""
                INSERT INTO system_settings (key, value) 
                VALUES ('current_active_display_season_id', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
            """, (new_display_season_id,))

            conn.commit()
            logger.info(
                f"🏆 НАЧАЛСЯ НОВЫЙ ИГРОВОЙ СЕЗОН: {new_display_season_id} ({season_name_ru} {now.year})! Все звезды и сезонная статистика сброшены в 0.")
        else:
            logger.debug(f"Текущий реальный сезон {monthly_trigger_id} не изменился. Сброс не требуется.")

    except Exception as e:
        logger.error(f"Ошибка во время автоматического сброса сезона: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def generate_card_stats(rarity: str, card_info: dict, is_repeat: bool = False) -> dict:
    stats_range = RARITY_STATS.get(rarity, RARITY_STATS["regular card"])
    gained_bo = random.randint(stats_range["min_bo"], stats_range["max_bo"])
    gained_points = card_info.get("points")
    if gained_points is None:
        gained_points = stats_range["points"] 
    dia_reward = DIAMONDS_REWARD_BASE.get(rarity.lower(), 10)
    raw_collection_name = card_info.get("collection", "").strip()
    collection_name_lower = raw_collection_name.lower()
    excluded_collection_names = ["", "common", "обычная", "none"]
    is_real_collection = raw_collection_name and collection_name_lower not in excluded_collection_names
    if is_real_collection:
        dia_reward += COLLECTION_BONUS
    if is_repeat:
        dia_reward *= REPEAT_DIAMOND_MULTIPLIER
    return {
        "bo": gained_bo,
        "points": gained_points,
        "diamonds": dia_reward}


def is_recent_callback(user_id: int, key: str, window: float = DEBOUNCE_SECONDS) -> bool:
    now = time.time()
    current = _CALLBACK_LAST_TS.get((user_id, key), 0.0)
    if now - current < window:
        # обновим время, чтобы многократные нажатия продолжали отклоняться в этом окне
        _CALLBACK_LAST_TS[(user_id, key)] = now
        return True
    _CALLBACK_LAST_TS[(user_id, key)] = now
    return False


def check_menu_owner(func):
    @wraps(func)
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        query = update.callback_query
        if not query:
            return await func(update, context, *args, **kwargs)
        current_user_id = query.from_user.id
        message_id = query.message.message_id
        chat_id = query.message.chat_id
        owner_id = NOTEBOOK_MENU_OWNERSHIP.get((chat_id, message_id))
        if owner_id is None:
            pass
        elif owner_id != current_user_id:
            await query.answer("Это не ваше меню!!!", show_alert=True)
            return
        return await func(update, context, *args, **kwargs)

    return wrapper


def get_rank_info(stars):
    if stars <= 0:
        return "Игрок AFK в этом сезоне", ""  # Возвращаем 2 значения
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
            stars_in_rank = stars - current_threshold
            div_index = (stars_in_rank - 1) // stars_per_div
            div_number = divs - div_index
            stars_left = ((stars_in_rank - 1) % stars_per_div) + 1
            return f"{name} {div_number}", f"{stars_left}⭐️"
        current_threshold += rank_total_stars

    mythic_stars = stars - 112
    if mythic_stars < 25:
        return "Мифический", f"{mythic_stars}⭐️"
    elif mythic_stars < 50:
        return "Мифическая Честь", f"{mythic_stars}⭐️"
    elif mythic_stars < 100:
        return "Мифическая Слава", f"{mythic_stars}⭐️"
    else:
        return "Мифический Бессмертный", f"{mythic_stars}⭐️"


async def grant_premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # Проверяем, что команду вызывает именно админ
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды.")
        return

    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "ℹ️ <b>Выдача Premium статуса:</b>\n"
            "<code>/grant_prem &lt;ID_пользователя&gt; &lt;кол-во_дней/lifetime&gt;</code>\n\n"
            "<i>Примеры:</i>\n"
            "• <code>/grant_prem 123456789 30</code> — выдать на 30 дней\n"
            "• <code>/grant_prem 123456789 lifetime</code> — сделать премиум вечным",
            parse_mode=ParseMode.HTML)
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID пользователя должен состоять только из цифр.")
        return
    days_arg = context.args[1].lower() if len(context.args) > 1 else "30"
    # Загружаем/создаем профиль целевого пользователя в БД
    target_user = await asyncio.to_thread(get_moba_user, target_id)
    if not target_user:
        await update.message.reply_text("❌ Не удалось найти или создать профиль для этого ID.")
        return
    now = datetime.now(timezone.utc)
    if days_arg in ("lifetime", "вечный", "вечно"):
        # Устанавливаем дату окончания в далеком будущем (например, 2099 год)
        premium_until = datetime(2099, 1, 1, tzinfo=timezone.utc)
        display_duration = "навсегда (вечный) ♾️"
    else:
        try:
            days = int(days_arg)
            if days <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Количество дней должно быть целым положительным числом или 'lifetime'.")
            return
        # Если у пользователя уже есть активный премиум, продлеваем его. Если нет — считаем от текущего момента.
        current_prem = target_user.get("premium_until")
        if current_prem and current_prem > now:
            premium_until = current_prem + timedelta(days=days)
        else:
            premium_until = now + timedelta(days=days)
        display_duration = f"{days} дней (до {premium_until.strftime('%d.%m.%Y %H:%M')} UTC)"
    # Записываем изменения в БД
    target_user["premium_until"] = premium_until
    await asyncio.to_thread(save_moba_user, target_user)
    # Отвечаем админу в чате
    target_nickname = html.escape(target_user.get('nickname', 'моблер'))
    await update.message.reply_text(
        f"✅ <b>Премиум успешно выдан!</b>\n\n"
        f"• Пользователь: <code>{target_id}</code> (Ник: <i>{target_nickname}</i>)\n"
        f"• Срок действия: <b>{display_duration}</b>",
        parse_mode=ParseMode.HTML
    )

    # Пробуем отправить личное сообщение пользователю
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=f"🚀 <b>Вам выдан Premium статус!</b>\n\n"
                 f"Срок действия: <b>{display_duration}</b>.\n"
                 f"Проверьте свой профиль в /account! Спасибо, что играете с нами! ❤️",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление пользователю {target_id} в личку: {e}")


def get_mastery_info(reg_total):
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
            if i + 1 < len(levels):
                next_threshold = levels[i + 1][0]
            else:
                next_threshold = None  # Максимальный уровень
        else:
            break

    return current_title, next_threshold


async def regnut_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    if update.message.text.lower().strip() != "регнуть": return
    
    user = get_moba_user(update.effective_user.id)
    now = time.time()

    if now - user.get("last_reg_time", 0) < 1200:
        wait = int(1200 - (now - user["last_reg_time"]))
        await update.message.reply_text(
            f"⏳ <b>Поиск матча</b>\n<blockquote>Катку можно регнуть через {wait // 60} мин {wait % 60} сек</blockquote>",
            parse_mode=ParseMode.HTML)
        return
    
    user["last_reg_time"] = now

    win_chance = 100 if user["stars"] < 2 else (60 if user["stars"] < 38 else 50)
    win = random.randint(1, 100) <= win_chance
    coins = random.randint(42, 97)
    
    user["coins"] += coins
    user["reg_total"] = user.get("reg_total", 0) + 1
    user["season_reg_total"] = user.get("season_reg_total", 0) + 1

    if win:
        user["stars"] += 1
        user["reg_success"] = user.get("reg_success", 0) + 1
        user["season_reg_success"] = user.get("season_reg_success", 0) + 1
        user["stars_all_time"] += 1
        if user["stars"] > user["max_stars"]:
            user["max_stars"] = user["stars"]
        msg = random.choice(WIN_PHRASES)
        change = "<b>⚡️ VICTORY ! </b>"
        rank_change_text = "<b>Текущий ранг повышен!</b>"
    else:
        if user.get("protection_active", 0) > 0:
            user["protection_active"] -= 1
            msg = "🛡 Защита сохранила вашу звезду!"
            change = "<b>💢 DEFEAT ! </b>"
            rank_change_text = "Ранг сохранен!"
        else:
            if user["stars"] > 0:
                user["stars"] -= 1
            msg = random.choice(LOSE_PHRASES)
            change = "<b>💢 DEFEAT ! </b>"
            rank_change_text = "<b>Текущий ранг понижен!</b>"

    rank_name, star_info = get_rank_info(user["stars"])
    save_moba_user(user)

    res = (f"<b>{change} {msg}</b>\n\n"
           f"<blockquote>{rank_change_text}</blockquote>\n"
           f"<b><i>{rank_name} ({star_info})  💰 БО + {coins}! </i></b>\n")
    await update.message.reply_text(res, parse_mode=ParseMode.HTML)

async def id_detection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    pattern = r"^\d{8,10}\s*\(\d{4,5}\)$"
    if re.match(pattern, text):
        logger.info(f"ID обнаружен: {text} от пользователя {update.effective_user.id}")
        normalized_id = re.sub(r"\s*\(", " (", text)
        context.user_data['temp_mlbb_id'] = normalized_id
        keyboard = [
            [InlineKeyboardButton("✅ Добавить", callback_data="confirm_add_id"),
             InlineKeyboardButton("❌ Отмена", callback_data="cancel_add_id")]        ]
        await update.message.reply_text(
            f"<b>👾 GAME ID</b>\n<blockquote>Хотите добавить свой айди <code>{normalized_id}</code> в профиль?</blockquote>",
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=ParseMode.HTML        )
    else:
        logger.debug(f"Сообщение '{text}' не подошло под паттерн ID")
        pass 

async def confirm_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:  # Добавим проверку на существование query
        return
    await query.answer()  # Отвечаем на callback_query, чтобы убрать "часики" на кнопке
    user_id = query.from_user.id
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


async def get_user_chat_membership_status(user_id: int, chat_username: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not chat_username:
        return False
    try:
        chat_member = await context.bot.get_chat_member(f"@{chat_username}", user_id)
        return chat_member.status in ('member', 'creator', 'administrator')
    except Exception as e:
        logger.debug(f"Error checking chat membership for user {user_id} in @{chat_username}: {e}")
        return False


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
            cursor.execute("INSERT INTO moba_users (user_id) VALUES (%s) RETURNING *", (user_id,))
            user_data = cursor.fetchone()
            conn.commit()

        user_dict = dict(user_data)
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
        user_dict.setdefault('season_reg_total', 0)
        user_dict.setdefault('season_reg_success', 0)
        user_dict.setdefault('premium_until', None)
        user_dict['last_mobba_time'] = float(user_dict.get('last_mobba_time') or 0)
        user_dict['last_reg_time'] = float(user_dict.get('last_reg_time') or 0)
        user_dict.setdefault('protection_active', 0)
        user_dict.setdefault('luck_active', 0)
        user_dict.setdefault('pending_boosters', 0)

        user_dict['cards'] = get_user_inventory(user_id)
        return user_dict
    except Exception as e:
        logger.error(f"Ошибка БД в get_moba_user: {e}", exc_info=True)
        return None
    finally:
        if conn: conn.close()


def get_moba_leaderboard_paged(category: str, limit: int = 15, offset: int = 0, chat_id: Optional[int] = None) -> List[
    dict]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        join_clause = ""
        where_clause = ""
        params = []

        if chat_id is not None:
            join_clause = "JOIN moba_chat_activity mca ON u.user_id = mca.user_id"
            where_clause = "WHERE mca.chat_id = %s"
            params.append(chat_id)

        # Текстовое имя: никнейм из профиля или заглушка Игрок ID
        nickname_expr = "COALESCE(NULLIF(NULLIF(u.nickname, ''), 'моблер'), CONCAT('Игрок ', u.user_id))"

        if category == "points":
            # Фильтр: только игроки, у которых больше 0 очков
            points_where = f"WHERE u.points > 0" if chat_id is None else f"{where_clause} AND u.points > 0"
            sql = f"""
                SELECT {nickname_expr} AS nickname, u.points as val, u.premium_until, u.user_id
                FROM moba_users u
                {join_clause}
                {points_where}
                ORDER BY u.points DESC NULLS LAST, u.user_id ASC 
                LIMIT %s OFFSET %s
            """
        elif category == "cards":
            # Фильтр: используем INNER JOIN вместо LEFT JOIN.
            # Это автоматически оставит в выборке только тех, у кого есть хотя бы 1 карта!
            sql = f"""
                SELECT {nickname_expr} AS nickname, COUNT(i.id) as val, u.premium_until, u.user_id
                FROM moba_users u
                INNER JOIN moba_inventory i ON u.user_id = i.user_id
                {join_clause}
                {where_clause}
                GROUP BY u.user_id, u.premium_until, u.nickname
                ORDER BY val DESC NULLS LAST, u.user_id ASC
                LIMIT %s OFFSET %s
            """
        elif category == "stars_season":
            # Фильтр: только игроки, у которых больше 0 звезд в этом сезоне (не AFK)
            stars_where = f"WHERE u.stars > 0" if chat_id is None else f"{where_clause} AND u.stars > 0"
            sql = f"""
                SELECT {nickname_expr} AS nickname, u.stars as val, u.premium_until, u.user_id
                FROM moba_users u
                {join_clause}
                {stars_where}
                ORDER BY u.stars DESC NULLS LAST, u.user_id ASC
                LIMIT %s OFFSET %s
            """
        elif category == "stars_all":
            # Фильтр: только игроки, у которых больше 0 звезд за всё время
            all_stars_where = f"WHERE u.stars_all_time > 0" if chat_id is None else f"{where_clause} AND u.stars_all_time > 0"
            sql = f"""
                SELECT {nickname_expr} AS nickname, u.stars_all_time as val, u.premium_until, u.user_id
                FROM moba_users u
                {join_clause}
                {all_stars_where}
                ORDER BY u.stars_all_time DESC NULLS LAST, u.user_id ASC
                LIMIT %s OFFSET %s
            """
        else:
            return []

        params.extend([limit, offset])

        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        logger.info(f"DB returned {len(rows)} active rows for category {category} (Chat: {chat_id}).")
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Ошибка при получении глобального топа MOBA ({category}): {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


async def _format_moba_global_page(context, rows: List[dict], page: int, per_page: int, category_label: str):
    target_chat_username = f"@{CHAT_USERNAME}"  # @CHAT_SUNRISE

    lines = []
    for idx, row in enumerate(rows, start=1 + (page - 1) * per_page):
        uid = row.get('user_id')
        nickname = html.escape(row.get('nickname') or str(uid))
        val = row.get('val', 0)
        moon_emoji = ""
        if uid:
            try:
                # Проверяем статус пользователя в целевом чате
                member = await context.bot.get_chat_member(target_chat_username, uid)
                if member.status in ('member', 'creator', 'administrator'):
                    moon_emoji = "🌙"
            except Exception:
                # Если бот не в чате или ошибка доступа, смайлик не ставим
                moon_emoji = ""

        lines.append(f"{idx}. {nickname}{moon_emoji} — {val}")

    body = "\n".join(lines) if lines else "Пока нет данных."
    message = f"🏆 {category_label}\n\n{body}"
    return message


async def send_moba_global_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       category_token: str = "season", page: int = 1):
    per_page = 15
    offset = (page - 1) * per_page

    # Определяем категорию для БД
    if category_token == "season":
        db_cat = "stars_season"
        label = "Моба — Топ сезона (Звезды)"
    elif category_token == "all":
        db_cat = "stars_all"
        label = "Моба — Топ всех времен (Звезды)"
    elif category_token == "points":
        db_cat = "points"
        label = "Топ по очкам"
    elif category_token == "cards":
        db_cat = "cards"
        label = "Топ по картам"
    else:
        db_cat = "stars_season"
        label = "Рейтинг"

    # Получаем 15 записей
    records = await asyncio.to_thread(get_moba_leaderboard_paged, db_cat, per_page, offset)

    # Форматируем текст
    text = await _format_moba_global_page(context, records, page, per_page, label)

    keyboard = []
    # Навигация (Назад | Стр | Вперед)
    nav_row = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton("<< Назад", callback_data=f"moba_top_global_{category_token}_page_{page - 1}"))

    nav_row.append(InlineKeyboardButton(f"стр. {page}", callback_data="moba_top_ignore"))

    # Проверяем, есть ли данные на следующей странице, чтобы показать кнопку "Вперед"
    next_check = await asyncio.to_thread(get_moba_leaderboard_paged, db_cat, 1, offset + per_page)
    if next_check:
        nav_row.append(
            InlineKeyboardButton("Вперед >>", callback_data=f"moba_top_global_{category_token}_page_{page + 1}"))

    keyboard.append(nav_row)

    # Кнопки переключения категорий
    keyboard.append([
        InlineKeyboardButton("🌟 Сезон", callback_data="moba_top_global_season_page_1"),
        InlineKeyboardButton("🌍 Весь топ", callback_data="moba_top_global_all_page_1")
    ])
    keyboard.append([
        InlineKeyboardButton("✨ Очки", callback_data="moba_top_global_points_page_1"),
        InlineKeyboardButton("🃏 Карты", callback_data="moba_top_global_cards_page_1")
    ])
    keyboard.append([InlineKeyboardButton("< Назад в меню", callback_data="moba_top_cards_main")])

    kb = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def send_moba_chat_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🌟 Топ сезона", callback_data="moba_top_chat_season_page_1"),
         InlineKeyboardButton("🌍 Топ за все время", callback_data="moba_top_chat_all_page_1")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="delete_message")]
    ]
    text = "🏆 MOBA — рейтинг по этому чату\n\nВыберите тип рейтинга:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)


async def send_moba_chat_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE, category_token: str = "all"):
    db_cat = "stars_season" if category_token == "season" else "stars_all"
    label = "Рейтинг (сезон)" if category_token == "season" else "Рейтинг (за все время)"
    rows = await asyncio.to_thread(get_moba_leaderboard_paged, db_cat, 20, 0)  # top20 for chat view

    # Формат строк без глобальной звезды (поведение "всё остается таким же")
    body_lines = []
    now = datetime.now(timezone.utc)
    for i, row in enumerate(rows, start=1):
        is_prem = row.get("premium_until") and row["premium_until"] > now
        prem_icon = "🚀 " if is_prem else ""
        nickname = html.escape(row.get('nickname') or str(row.get('user_id')))
        val = row.get('val', 0)
        body_lines.append(f"{i}. {prem_icon}<b>{nickname}</b> — {val}")

    text = f"🏆 <b>{label}</b>\n\n" + ("\n".join(body_lines) if body_lines else "<i>Пока нет данных</i>")
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("< Назад", callback_data="top_main")]])
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except BadRequest:
            await context.bot.send_message(chat_id=update.callback_query.from_user.id, text=text, reply_markup=keyboard,
                                           parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


async def handle_moba_top_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if update.effective_chat.type == 'private':
        await update.message.reply_text(
            "⛩️ Эта команда доступна только в группах. Пожалуйста, используйте её в чате с игроками!",
            parse_mode=ParseMode.HTML
        )
        return

    txt = update.message.text.lower().strip()

    if txt in ("моба топ вся", "моба топвся"):
        await handle_moba_top_display(update, context, scope='global', page=1)
        return

    if txt == "моба топ":
        await handle_moba_top_display(update, context, scope='chat', page=1)
        return


async def moba_top_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("moba_top_"):
        parts = data.split('_')
        # parts[2] = scope (chat/global)
        # parts[4] = page number
        if len(parts) >= 5:
            scope = parts[2]
            try:
                page = int(parts[4])
            except ValueError:
                page = 1

            await handle_moba_top_display(update, context, scope=scope, page=page)
            return


async def _moba_send_filtered_card(query, context, cards: List[dict], index: int, back_cb: str = "moba_my_cards"):
    try:
        await query.answer()
    except Exception:
        pass

    try:
        base = (query.data or "moba_filtered").rsplit("_", 1)[0]
    except Exception:
        base = query.data or "moba_filtered"

    if is_recent_callback(query.from_user.id, base):
        return

    if not cards:
        try:
            if query.message and getattr(query.message, "photo", None):
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="У вас нет карт в этой категории."
                )
            else:
                await query.edit_message_text("У вас нет карт в этой категории.")
        except Exception:
            await context.bot.send_message(chat_id=query.from_user.id, text="У вас нет карт в этой категории.")
        return

    if index < 0:
        index = 0
    if index >= len(cards):
        index = len(cards) - 1
    card = cards[index]
    photo_path = card.get('image_path') or CARDS.get(card.get('card_id'), {}).get('path') or \
                 PHOTO_DETAILS.get(card.get('card_id'), {}).get('path')
    caption = _moba_card_caption(card, index, len(cards))

    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton("<", callback_data=f"{base}_{index - 1}"))
    nav.append(InlineKeyboardButton(f"{index + 1}/{len(cards)}", callback_data="moba_ignore"))
    if index < len(cards) - 1:
        nav.append(InlineKeyboardButton(">", callback_data=f"{base}_{index + 1}"))
    keyboard = [nav, [InlineKeyboardButton("< В коллекцию", callback_data=back_cb)]]

    try:
        if query.message and getattr(query.message, "photo", None):
            with open(photo_path, "rb") as ph:
                await query.edit_message_media(
                    InputMediaPhoto(media=ph, caption=caption, parse_mode=ParseMode.HTML),
                    reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            try:
                await query.message.delete()
            except Exception:
                pass
            with open(photo_path, "rb") as ph:
                await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=ph,
                    caption=caption,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML)
    except FileNotFoundError:
        logger.error(f"Photo not found for moba card: {photo_path}")
        try:
            if query.message and getattr(query.message, "photo", None):
                try:
                    await query.message.delete()
                except Exception:
                    pass
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=caption + "\n\n⚠️ (Фото не найдено на сервере)",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML)
            else:
                await query.edit_message_text(
                    text=caption + "\n\n⚠️ (Фото не найдено на сервере)",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML)
        except Exception:
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text=caption + "\n\n⚠️ (Фото не найдено на сервере)",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception("Ошибка при отправке отфильтрованной карты MOBA: %s", e)


def log_moba_chat_activity(user_id: int, chat_id: int):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gospel_chat_activity (user_id, chat_id, prayer_count, total_piety_score)
            VALUES (%s, %s, 0, 0)
            ON CONFLICT (user_id, chat_id) DO UPDATE SET
                prayer_count = gospel_chat_activity.prayer_count
        ''', (user_id, chat_id))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при логировании чат-активности MOBA для {user_id} в чате {chat_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()



def save_moba_user(user):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = '''
            UPDATE moba_users SET
                nickname = %s,
                game_id = %s,
                points = %s,
                diamonds = %s,
                coins = %s,
                stars = %s,
                max_stars = %s,
                stars_all_time = %s,
                reg_total = %s,
                reg_success = %s,
                season_reg_total = %s,
                season_reg_success = %s,
                premium_until = %s,
                last_mobba_time = %s,
                last_reg_time = %s,
                protection_active = %s,
                luck_active = %s,
                pending_boosters = %s,
                bought_booster_today = %s,
                bought_luck_week = %s,
                bought_protection_week = %s,
                last_daily_reset = %s,
                last_weekly_reset = %s
            WHERE user_id = %s
        '''
        params = (
            user.get('nickname', 'моблер'),
            user.get('game_id'),
            user.get('points', 0),
            user.get('diamonds', 0),
            user.get('coins', 0),
            user.get('stars', 0),
            user.get('max_stars', 0),
            user.get('stars_all_time', 0),
            user.get('reg_total', 0),
            user.get('reg_success', 0),
            user.get('season_reg_total', 0),
            user.get('season_reg_success', 0),
            user.get('premium_until'),
            float(user.get('last_mobba_time') or 0),
            float(user.get('last_reg_time') or 0),
            user.get('protection_active', 0),
            user.get('luck_active', 0),
            user.get('pending_boosters', 0),
            user.get('bought_booster_today', 0),
            user.get('bought_luck_week', 0),
            user.get('bought_protection_week', 0),
            user.get('last_daily_reset'),
            user.get('last_weekly_reset'),
            user['user_id']
        )
        cursor.execute(sql, params)
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"Ошибка сохранения пользователя {user.get('user_id')}: {e}", exc_info=True)
    finally:
        if conn: conn.close()


def add_card_to_inventory(user_id, card):
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
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT * FROM moba_inventory WHERE user_id = %s", (user_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

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

        await update.message.reply_text(f"<b>👾 Ник изменен на {new_name}</b>", parse_mode=ParseMode.HTML)
        logger.info(f"set_name: Ник пользователя {user_id} успешно изменен на '{new_name}'.")  # Используем logger
    else:
        await update.message.reply_text(
            "<b>👾 Придумай свой ник</b>\n<blockquote>Длина от 5 до 16 символов\nПример: /name помидорка</blockquote>",
            parse_mode=ParseMode.HTML)
        logger.warning(
            f"set_name: Попытка установить невалидный ник: '{new_name}' (длина: {len(new_name)}) для user_id: {user_id}")  # Используем logger


async def mobba_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    if update.message.text.lower().strip() != "моба": return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    register_moba_chat_activity(user_id, chat_id)

    user_id = update.effective_user.id
    user = await asyncio.to_thread(get_moba_user, user_id)
    if not user: return

    now = time.time()
    is_premium = user["premium_until"] and user["premium_until"] > datetime.now(timezone.utc)
    base_cooldown = 14400  # 4 часа (нормальное значение)
    if is_premium: base_cooldown *= 0.75

    time_passed = now - user["last_mobba_time"]
    used_item_text = ""

    if time_passed < base_cooldown:
        if user.get("pending_boosters", 0) > 0:
            user["pending_boosters"] -= 1
            user["last_mobba_time"] -= 7200  # Срезаем 2 часа
            await asyncio.to_thread(save_moba_user, user)
            # Перепроверяем кулдаун после бустера
            if (now - user["last_mobba_time"]) < base_cooldown:
                wait = int(base_cooldown - (now - user["last_mobba_time"]))
                await update.message.reply_text(f"<b>🃏 Вы уже получали карту</b>\n"
                                                f"<blockquote>Попробуйте через {wait // 3600} ч. {(wait % 3600) // 60} мин</blockquote>\n<b>⚡️Бустер сократил время на 2ч !</b>",
                                                parse_mode=ParseMode.HTML)
                return
            used_item_text = "⚡️ <b>Потрачен 1 бустер из сумки!</b>\n"
        else:
            wait = int(base_cooldown - time_passed)
            await update.message.reply_text(f"<b>🃏 Вы уже получали карту</b>\n"
                                            f"<blockquote>Попробуйте через {wait // 3600} ч. {(wait % 3600) // 60} мин</blockquote>",
                                            parse_mode=ParseMode.HTML)
            return

    # ПРИМЕНЕНИЕ УДАЧИ
    if user.get("luck_active", 0) > 0:
        user["luck_active"] -= 1
        used_item_text += "🍀 <b>Использована 1 удача! Шанс повышен.</b>\n"
        # Логика повышения шанса: если удача активна, мы будем роллить карту из пуска Rare+
        # Для простоты в этом коде: если удача активна, перевыбираем карту, пока она не станет выше regular
        card_id = random.choice(list(CARDS.keys()))
        for _ in range(5):  # 5 попыток найти крутую карту
            if FIXED_CARD_RARITIES.get(card_id) != "regular card": break
            card_id = random.choice(list(CARDS.keys()))
    else:
        card_id = random.choice(list(CARDS.keys()))

    if now - user["last_mobba_time"] < base_cooldown:
        wait = int(base_cooldown - (now - user["last_mobba_time"]))
        wait_text = (f"<b>🃏 Вы уже получали карту</b>\n"
                     f"<blockquote>Попробуйте через {wait // 3600} ч. {(wait % 3600) // 60} мин {wait % 60}</blockquote>")
        if is_premium:
            wait_text += f"\n<b>🚀 Premium сократил время на 25% !</b>"
        await update.message.reply_text(wait_text, parse_mode=ParseMode.HTML)
        return

    card_id = random.choice(list(CARDS.keys()))
    card_info = CARDS[card_id]
    rarity = FIXED_CARD_RARITIES.get(card_id, "regular card")
    raw_collection_name = card_info.get("collection", "").strip()
    collection_name_lower = raw_collection_name.lower()
    # Список названий, которые НЕ являются "настоящими" коллекциями для подсчета прогресса
    excluded_collection_names = ["", "common", "обычная", "none"]
    is_real_collection = raw_collection_name and collection_name_lower not in excluded_collection_names
    inventory = await asyncio.to_thread(get_user_inventory, user_id)
    is_repeat = any(c['card_id'] == card_id for c in inventory)
    dia_reward = DIAMONDS_REWARD_BASE.get(rarity.lower(), 10)

    if is_real_collection:
        dia_reward += COLLECTION_BONUS

    if is_repeat:
        dia_reward *= REPEAT_DIAMOND_MULTIPLIER
        msg_type = "<blockquote><b>Повторка! Алмазы Х5 !</b></blockquote>"
    else:
        if is_real_collection:
            total_in_col = sum(1 for c_id in CARDS if CARDS[c_id].get("collection", "").strip() == raw_collection_name)

            unique_owned_in_col = set(
                c['card_id'] for c in inventory if c.get('collection', "").strip() == raw_collection_name)
            current_progress = len(unique_owned_in_col) + 1
            if total_in_col > 0:
                msg_type = f"<blockquote><b>Карта {current_progress}/{total_in_col} из коллекции {raw_collection_name}!</b></blockquote>"
            else:
                msg_type = "<blockquote><b>Новая карта добавлена в коллекцию!</b></blockquote>"
        else:
            msg_type = "<blockquote><b>Новая карта добавлена в коллекцию!</b></blockquote>"
    # Параметры БО и Очков
    stats_range = RARITY_STATS.get(rarity, RARITY_STATS["regular card"])
    gained_bo = random.randint(stats_range["min_bo"], stats_range["max_bo"])
    gained_points = card_info.get("points", stats_range["points"])

    # Обновление данных
    user["last_mobba_time"] = now
    user["points"] += gained_points
    user["diamonds"] += dia_reward
    user["coins"] += gained_bo

    await asyncio.to_thread(save_moba_user, user)
    await asyncio.to_thread(add_card_to_inventory, user_id, {
        "card_id": card_id,
        "name": card_info["name"],
        "collection": raw_collection_name if raw_collection_name else " ",
        "rarity": rarity,
        "bo": gained_bo,
        "points": gained_points,
        "diamonds": dia_reward
    })
    display_collection_name = raw_collection_name if is_real_collection else "COMMON"
    # Формирование сообщения
    caption = (
            f"🃏<b> {card_info.get('collection', 'COMMON')} • {card_info['name']}</b>\n"
            f"<blockquote><b>+ {gained_points} ОЧКОВ !</b></blockquote>\n\n"
            f"✨ <b>Редкость • </b>{rarity}\n"
            f"💰 <b>БО •</b> {gained_bo}\n"
            f"💎 <b>Алмазы •</b> {dia_reward}" + (" [x5 🔥]" if is_repeat else "") + "\n"
                                                                                   f"\n{msg_type}"
    )

    try:
        with open(card_info["path"], 'rb') as photo:
            await update.message.reply_photo(photo, caption=caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при отправке фото карты: {e}")
        await update.message.reply_text(f"Вы получили карту: {card_info['name']}\n\n{caption}",
                                        parse_mode=ParseMode.HTML)


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
    user_id = update.effective_user.id
    user = await asyncio.to_thread(get_moba_user, user_id)

    if user is None:
        if update.message:
            await update.message.reply_text("Произошла ошибка загрузки профиля.")
        elif update.callback_query:
            await update.callback_query.answer("Произошла ошибка загрузки профиля.")
        return
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT value FROM system_settings WHERE key = 'current_active_display_season_id';")
    active_season_row = cursor.fetchone()
    current_season_display_id = active_season_row['value'] if active_season_row else "Не определен"
    conn.close()

    now = datetime.now(timezone.utc)
    now = datetime.now(timezone.utc)
    premium_until = user.get("premium_until")
    if premium_until and premium_until.tzinfo is None:
        premium_until = premium_until.replace(tzinfo=timezone.utc)
    if premium_until and premium_until > now:
        date_str = premium_until.strftime("%d.%m")
        prem_status = f"🚀 Обладатель Premium до {date_str}"
    else:
        prem_status = "Не обладает Premium"

    curr_rank, curr_stars = get_rank_info(user.get("stars", 0))
    max_rank, _ = get_rank_info(user.get("stars_all_time", 0))

    # Сезонные данные
    season_games = user.get("season_reg_total", 0)
    season_wins = user.get("season_reg_success", 0)
    season_winrate = (season_wins / season_games * 100) if season_games > 0 else 0.0

    total_card_count = len(user.get('cards', []))
    display_id = user.get('game_id') if user.get('game_id') else "Не добавлен"

    text = (
        f"Ценитель <b>MOBILE LEGENDS\n\n«{html.escape(user['nickname'])}»</b>\n"
        f"<blockquote><b>👾 GAME ID •</b> <i>{display_id}</i></blockquote>\n\n"
        f"<b></b> <i>{current_season_display_id}</i>\n"
        f"<b>🏆 Ранг •</b> <i>{curr_rank} ({curr_stars})</i>\n"
        f"<b>🎮 Игр в сезоне •</b> <i>{season_games}</i>\n"
        f"<b>🎗️ Win rate •</b> <i>{season_winrate:.1f}%</i>\n"
        f"<b>⚜️ Макс ранг •</b> <i>{max_rank}</i>\n\n"
        f"<b>✨ Очки •</b> <i>{user['points']}</i>\n"
        f"<b>💰 БО • </b><i>{user['coins']}</i>\n"
        f"<b>💎 Алмазы • </b><i>{user['diamonds']}</i>\n\n"
        f"<blockquote>{prem_status}</blockquote>"
    )

    keyboard = [
        [InlineKeyboardButton("🃏 Мои карты", callback_data="moba_my_cards"),
         InlineKeyboardButton("👝 Сумка", callback_data="bag")],
        [InlineKeyboardButton("Подробнее о игроке", callback_data="all_season_info")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Получаем аватарку пользователя
    photos = await update.effective_user.get_profile_photos(limit=1)
    photo_to_send = photos.photos[0][0].file_id if photos.photos else (
        DEFAULT_PROFILE_IMAGE if os.path.exists(DEFAULT_PROFILE_IMAGE) else None)

    # Кликнули на кнопку (CallbackQuery)
    if update.callback_query:
        query = update.callback_query
        try:
            await query.answer()
        except:
            pass

        # КЕЙС 1: В текущем сообщении есть фото — редактируем его медиа-часть
        if query.message.photo:
            try:
                if photo_to_send:
                    media = InputMediaPhoto(
                        media=photo_to_send if not (isinstance(photo_to_send, str) and os.path.exists(photo_to_send)) else open(photo_to_send, 'rb'),
                        caption=text,
                        parse_mode=ParseMode.HTML
                    )
                    await query.edit_message_media(media=media, reply_markup=reply_markup)
                else:
                    await safe_delete_message(query, context)
                    msg = await context.bot.send_message(chat_id=query.message.chat_id, text=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
                    NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    await safe_delete_message(query, context)
                    msg = await context.bot.send_photo(
                        chat_id=query.message.chat_id,
                        photo=photo_to_send if not (isinstance(photo_to_send, str) and os.path.exists(photo_to_send)) else open(photo_to_send, 'rb'),
                        caption=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
                    NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
        
        # КЕЙС 2: В текущем сообщении НЕТ фото (мы вернулись из текстового меню «Сумка»)
        else:
            if photo_to_send:
                # Удаляем текстовое сообщение и отправляем новое красивое сообщение с ФОТО
                await safe_delete_message(query, context)
                try:
                    msg = await context.bot.send_photo(
                        chat_id=query.message.chat_id,
                        photo=photo_to_send if not (isinstance(photo_to_send, str) and os.path.exists(photo_to_send)) else open(photo_to_send, 'rb'),
                        caption=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
                    NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
                except Exception as e:
                    logger.error(f"Ошибка восстановления фото при возврате: {e}")
                    await safe_edit_message_text(query, text, reply_markup)
            else:
                # Если аватарки нет вообще, просто меняем текст
                await safe_edit_message_text(query, text, reply_markup)

        NOTEBOOK_MENU_OWNERSHIP[(query.message.chat_id, query.message.message_id)] = user_id

    # Написали команду /account текстом
    else:
        try:
            if photo_to_send:
                if isinstance(photo_to_send, str) and os.path.exists(photo_to_send):
                    with open(photo_to_send, 'rb') as photo_file:
                        msg = await context.bot.send_photo(
                            chat_id=update.effective_chat.id,
                            photo=photo_file,
                            caption=text,
                            reply_markup=reply_markup,
                            parse_mode=ParseMode.HTML
                        )
                else:
                    msg = await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=photo_to_send,
                        caption=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
            else:
                msg = await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
            NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
        except Exception as e:
            logger.error(f"Ошибка при отправке текстового профиля: {e}")
            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
            
@check_menu_owner
async def handle_all_season_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass

    user_id = query.from_user.id
    user = await asyncio.to_thread(get_moba_user, user_id)

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)

    # Получаем текущий активный ID сезона
    cursor.execute("SELECT value FROM system_settings WHERE key = 'current_active_display_season_id';")
    active_season_row = cursor.fetchone()
    current_active_display_season_id = active_season_row['value'] if active_season_row else "—"

    # История сезонов
    cursor.execute("SELECT * FROM moba_season_history WHERE user_id = %s ORDER BY id DESC", (user_id,))
    history = cursor.fetchall()
    conn.close()

    # Общая игровая статистика
    total_all_games = user.get("reg_total", 0) if user else 0
    total_all_wins = user.get("reg_success", 0) if user else 0
    all_winrate = (total_all_wins / total_all_games * 100) if total_all_games > 0 else 0.0
    max_rank, _ = get_rank_info(user.get("stars_all_time", 0)) if user else ("—", "")

    # --- 1. ЛОГИКА ЕДИНОЙ КОЛЛЕКЦИИ «МОБЛА» (269 карт) ---
    TOTAL_MOBA_CARDS = 269
    user_inventory = user.get("cards", [])
    # Считаем только уникальные card_id, которые входят в диапазон Моблы (1-269)
    user_unique_moba_ids = set(r['card_id'] for r in user_inventory if r.get('card_id') and 1 <= r['card_id'] <= TOTAL_MOBA_CARDS)
    moba_owned_count = len(user_unique_moba_ids)

    # --- 2. ЛОГИКА КОЛЛЕКЦИИ «Love is…» (74 вкладыша) ---
    TOTAL_LOVE_CARDS = 74
    love_user_data = await asyncio.to_thread(get_user_data, user_id, user.get('nickname', 'моблер'))
    love_cards = love_user_data.get("cards", {}) if love_user_data else {}
    # Ключи в love_cards - это ID вкладышей ("1", "2" и т.д.). Считаем уникальные в диапазоне 1..74
    user_unique_love_ids = set(int(cid) for cid, qty in love_cards.items() if qty > 0 and cid.isdigit() and 1 <= int(cid) <= TOTAL_LOVE_CARDS)
    love_owned_count = len(user_unique_love_ids)

    # Распределение по спискам
    completed_list = []
    in_progress_list = []

    # Проверка "Мобла"
    if moba_owned_count >= TOTAL_MOBA_CARDS:
        completed_list.append(f" <b>Мобла</b> (Все {TOTAL_MOBA_CARDS} карт собраны!)")
    elif moba_owned_count > 0:
        in_progress_list.append(f" <b>Мобла</b> — {moba_owned_count}/{TOTAL_MOBA_CARDS}")

    # Проверка "Love is..."
    if love_owned_count >= TOTAL_LOVE_CARDS:
        completed_list.append(f"<b>Love is…</b> (Все {TOTAL_LOVE_CARDS} вкладышей собраны!)")
    elif love_owned_count > 0:
        in_progress_list.append(f" <b>Love is…</b> — {love_owned_count}/{TOTAL_LOVE_CARDS}")

    # --- ФОРМИРОВАНИЕ ТЕКСТА СООБЩЕНИЯ ---
    display_id = user.get('game_id') if user.get('game_id') else "Не добавлен"
    text = (        f"Ценитель <b>MOBILE LEGENDS\n\n«{html.escape(user['nickname'])}»</b>\n"
        f"<blockquote><b>👾 GAME ID •</b> <i>{display_id}</i></blockquote>\n\n")

    text += "🎗️ <b>Собранные коллекции:</b>\n"
    if completed_list:
        text += "\n".join(completed_list) + "\n\n"
    else:
        text += "<i><blockquote>Пока нет полностью собранных коллекций</blockquote></i>\n\n"

    # 2. Текущие коллекции
    text += "<b>Текущие коллекции:</b>\n"
    if in_progress_list:
        text += f"<blockquote>"
        text += "\n".join(in_progress_list) + "\n"
        text += f"</blockquote>"
    else:
        if not completed_list and moba_owned_count == 0 and love_owned_count == 0:
            text += "<i><blockquote>Вы еще не начали собирать коллекции</blockquote></i>\n"
        else:
            text += "<i><blockquote>Все коллекции собраны!</blockquote></i>\n"


    text += (
        f" \n<b>Игровая статистика:</b>\n"
        f"👾 Игры: {total_all_games}\n"
        f"🎗️ Винрейт: {all_winrate:.1f}%\n"
        f"⚜️ Макс ранг: {max_rank}\n"
    )

    
    text += f"\n<b>История сезонов:</b> \n"
    text += f"<b>{current_active_display_season_id}</b>)\n"
    text += f"<blockquote>"

    
    if not history:
        text += "<i>История прошлых сезонов пока пуста.</i>\n\n"
    else:
        for row in history:
            s_games = row['total_games'] or 0
            rank_val = row['final_rank']
            # Безопасно обрабатываем ранг, если он записан числом или текстом
            if isinstance(rank_val, int) or (isinstance(rank_val, str) and rank_val.isdigit()):
                rank_result = get_rank_info(int(rank_val))
                r_name = rank_result[0] if isinstance(rank_result, tuple) and len(rank_result) > 0 else str(rank_result)
            else:
                r_name = str(rank_val or "—")
            text += f"• <b>{row['season_id']}</b>: {s_games} игр (Ранг: {r_name})\n"
        text += "\n"
    text += f"</blockquote>"



    # 1. Собранные коллекции

    keyboard = [[InlineKeyboardButton("< Назад", callback_data="back_to_moba_profile")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Метод вывода на экран (удаление фото и отправка текста)
    if query.message.photo:
        await safe_delete_message(query, context)
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id, 
            text=text, 
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
    else:
        await safe_edit_message_text(query, text, reply_markup)

async def premium_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_admin = (user_id == ADMIN_ID)
    user = await asyncio.to_thread(get_moba_user, user_id)
    now = datetime.now(timezone.utc)
    premium_until = user.get("premium_until")
    if premium_until and premium_until.tzinfo is None:
        premium_until = premium_until.replace(tzinfo=timezone.utc)
    if premium_until and premium_until > now:
        date_str = premium_until.strftime("%d.%m")
        status_text = f"<blockquote>Действует до {date_str} </blockquote>"
    else:
        status_text = "<blockquote>Не активирован</blockquote>"
    text = (
        "🚀 <b>Premium</b>\n\n"
        f"{status_text}\n"
        "<blockquote>• 🔥 Шанс на особые карты увеличен на 10%\n"
        "• ⏳ Время получения следующей карты снижено на 25%\n"
        "• 💰 Выпадение БО увеличено на 20 %\n"
        "• 🚀 Значок в топе\n\n"
        "Срок действия при покупке • 30 дней</blockquote>")
    if is_admin:
        keyboard = [
            [InlineKeyboardButton("🚀 Активировать бесплатно (Создатель 🎁)", callback_data="admin_free_premium_30")]]
    else:
        invoice_link = await context.bot.create_invoice_link(
            title="Премиум",
            description="30 дней подписки",
            payload="premium_30",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice("Цена", 10)])
        keyboard = [[InlineKeyboardButton("🚀 Купить за 10 • ⭐️", url=invoice_link)]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

#6.возвращает текущее время сервера
def get_server_time():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def get_moba_user_rank(user_id, field, chat_id=None):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        if field == "stars_all":
            db_field = "stars_all_time"
        else:
            db_field = field  # stars, points и т.д.

        if field == "cards":
            # Текущее значение карт у пользователя
            cursor.execute("SELECT COUNT(id) AS cnt FROM moba_inventory WHERE user_id = %s", (user_id,))
            row = cursor.fetchone()
            current_val = row['cnt'] if row and row['cnt'] is not None else 0

            if current_val == 0:
                return "1000+"

            if chat_id is None:
                cursor.execute("""
                    SELECT COUNT(*) AS greater_cnt FROM (
                        SELECT user_id, COUNT(id) AS card_count
                        FROM moba_inventory
                        GROUP BY user_id
                    ) t
                    WHERE t.card_count > %s
                """, (current_val,))
                res = cursor.fetchone()
                greater = res['greater_cnt'] if res and res['greater_cnt'] is not None else 0
                return greater + 1
            else:
                cursor.execute("""
                    WITH my_count AS (
                        SELECT COUNT(id) AS cnt FROM moba_inventory WHERE user_id = %s
                    ), chat_counts AS (
                        SELECT mca.user_id,
                               COALESCE(t.card_count, 0) AS card_count
                                                       FROM moba_chat_activity mca
                        LEFT JOIN (
                            SELECT user_id, COUNT(id) AS card_count
                            FROM moba_inventory
                            GROUP BY user_id
                        ) t ON t.user_id = mca.user_id
                        WHERE mca.chat_id = %s
                    )
                    SELECT (SELECT COUNT(*) FROM chat_counts WHERE card_count > (SELECT cnt FROM my_count)) AS greater_cnt
                    FROM my_count
                """, (user_id, chat_id))
                res = cursor.fetchone()
                greater = res['greater_cnt'] if res and res['greater_cnt'] is not None else 0
                return greater + 1

        cursor.execute(f"SELECT {db_field} FROM moba_users WHERE user_id = %s", (user_id,))
        user_stat = cursor.fetchone()
        current_val = None
        if user_stat:
            # DictCursor: ключ - имя колонки
            current_val = user_stat.get(db_field)
        if current_val is None:
            current_val = 0

        if current_val == 0:
            return "1000+"

        if chat_id is None:
            # Глобальный ранг: учитываем tie-breaker по user_id для стабильности
            cursor.execute(f"""
                SELECT COUNT(u.user_id) AS cnt
                FROM moba_users u
                WHERE (u.{db_field} > %s OR (u.{db_field} = %s AND u.user_id < %s))
            """, (current_val, current_val, user_id))
            res = cursor.fetchone()
            cnt = res['cnt'] if res and res['cnt'] is not None else 0
            return cnt + 1
        else:
            # Локальный ранг: присоединяем moba_chat_activity и корректно ставим скобки
            cursor.execute(f"""
                SELECT COUNT(u.user_id) AS cnt
                FROM moba_users u
                JOIN moba_chat_activity mca ON u.user_id = mca.user_id
                WHERE mca.chat_id = %s
                  AND ((u.{db_field} > %s) OR (u.{db_field} = %s AND u.user_id < %s))
            """, (chat_id, current_val, current_val, user_id))
            res = cursor.fetchone()
            cnt = res['cnt'] if res and res['cnt'] is not None else 0
            return cnt + 1

    except Exception as e:
        logger.error(f"Error in get_moba_user_rank(user_id={user_id}, field={field}, chat_id={chat_id}): {e}",
                     exc_info=True)
        return "—"
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


async def handle_moba_top_display(update: Update, context: ContextTypes.DEFAULT_TYPE, scope: str, page: int):
    query = update.callback_query
    user_id = query.from_user.id if query else update.effective_user.id

    effective_chat = None
    if update.effective_chat:
        effective_chat = update.effective_chat
    elif query and getattr(query, "message", None) and getattr(query.message, "chat", None):
        effective_chat = query.message.chat

    if effective_chat and effective_chat.type == 'private' and scope == 'chat':
        text = " ❗️ <b>Это команда работает только в чатах </b>\n\n<blockquote>Для просмотра своего положения в топе изпользуйте «моба топ вся»</blockquote>"
        if query:
            await query.answer(text.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""),
                               show_alert=True)
        else:
            await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)
        return

    if scope == 'chat' and effective_chat is not None:
        chat_id_for_filter = effective_chat.id
    else:
        chat_id_for_filter = None

    filter_chat = chat_id_for_filter if scope == 'chat' else None

    target_chat_title = effective_chat.title if (
            scope == 'chat' and effective_chat and getattr(effective_chat, "title", None)) else "Все чаты"

    logger.info(f"MOBA TOP: User {user_id} requested top for scope={scope}, chat_id={filter_chat}, page={page}")
    now = datetime.now(timezone.utc)  # Получаем текущее время для проверки премиума
    if page == 1:
        top_cards = await asyncio.to_thread(get_moba_leaderboard_paged, "cards", 10, 0, chat_id=filter_chat)
        top_points = await asyncio.to_thread(get_moba_leaderboard_paged, "points", 10, 0, chat_id=filter_chat)
        rank_cards = await asyncio.to_thread(get_moba_user_rank, user_id, "cards", chat_id=filter_chat)
        rank_points = await asyncio.to_thread(get_moba_user_rank, user_id, "points", chat_id=filter_chat)
        title = f"🏆 MOBA. Cards {'\nРейтинг чата  • ' + target_chat_title if scope == 'chat' else '• Глобальный рейтинг'}"
        text = f"{title}\n\n"
        text += "👾 <b><i>ТОП 10 МОБЛЕРОВ ПО КАРТАМ:</i></b>\n"
        text += "<blockquote>"
        for i, r in enumerate(top_cards, 1):
            nickname_display = html.escape(r['nickname'] or f"Игрок {r['user_id']}")
            moon = await get_moon_status(r['user_id'], context, update.effective_chat.id)
            is_prem = r.get("premium_until") and r["premium_until"] > now
            prem_icon = " 🚀" if is_prem else ""
            text += f"{i}. {nickname_display} {moon} — {r['val']} шт.\n"
        text += "</blockquote>"
        text += f"<i>— Вы на {rank_cards} месте</i>\n\n"
        text += "👾<b><i> ТОП 10 МОБЛЕРОВ ПО ОЧКАМ:</i></b>\n"
        text += "<blockquote>"
        for i, r in enumerate(top_points, 1):
            nickname_display = html.escape(r['nickname'] or f"Игрок {r['user_id']}")
            moon = await get_moon_status(r['user_id'], context, update.effective_chat.id)
            is_prem = r.get("premium_until") and r["premium_until"] > now
            prem_icon = " 🚀" if is_prem else ""
            text += f"{i}. {nickname_display} {moon} — {r['val']}\n"
        text += "</blockquote>"
        text += f"<i>— Вы на {rank_points} месте</i>"
        text += "\n\n<blockquote>Для обновления топа используйте команды «моба»\nДля смены ника используйте /name ник»</blockquote>"
        keyboard = [
            [InlineKeyboardButton("⭐️ ТОП ПО РАНГУ", callback_data=f"moba_top_{scope}_page_2")],
            [InlineKeyboardButton("🗑 Удалить", callback_data="delete_message")]]

    elif page == 2:
        top_season = await asyncio.to_thread(get_moba_leaderboard_paged, "stars_season", 10, 0, chat_id=filter_chat)
        top_all = await asyncio.to_thread(get_moba_leaderboard_paged, "stars_all", 10, 0, chat_id=filter_chat)
        rank_s = await asyncio.to_thread(get_moba_user_rank, user_id, "stars", chat_id=filter_chat)
        rank_a = await asyncio.to_thread(get_moba_user_rank, user_id, "stars_all_time", chat_id=filter_chat)
        title = f"🏆 MOBA. Game {'\nРейтинг чата  • ' + target_chat_title if scope == 'chat' else '• Глобальный рейтинг'}"
        text = f"<b>{title}</b>\n\n"
        text += "<b>👾 ТОП 10 МОБЛЕРОВ ТЕКУЩЕГО СЕЗОНА:</b>\n"
        text += "<blockquote>"
        for i, r in enumerate(top_season, 1):
            nickname_display = html.escape(r['nickname'] or f"Игрок {r['user_id']}")
            moon = await get_moon_status(r['user_id'], context, update.effective_chat.id)
            rank_res = get_rank_info(r['val'])
            if isinstance(rank_res, (tuple, list)):
                rank_name = rank_res[0]
                star_info = rank_res[1] if len(rank_res) > 1 else ""
            else:
                rank_name = str(rank_res)
                star_info = ""
            is_prem = r.get("premium_until") and r["premium_until"] > now
            prem_icon = " 🚀" if is_prem else ""
            text += f"<code>{i}.</code> {nickname_display} {moon} — {rank_name} [{star_info}]\n"
        text += "</blockquote>"
        text += f"<i>— Вы на {rank_s} месте</i>\n\n"
        text += "<b>👾 ТОП 10 МОБЛЕРОВ ЗА ВСЕ ВРЕМЯ:</b>\n"
        text += "<blockquote>"
        for i, r in enumerate(top_all, 1):
            nickname_display = html.escape(r['nickname'] or f"Игрок {r['user_id']}")
            moon = await get_moon_status(r['user_id'], context, update.effective_chat.id)
            rank_res = get_rank_info(r['val'])
            if isinstance(rank_res, (tuple, list)):
                rank_name = rank_res[0]
                star_info = rank_res[1] if len(rank_res) > 1 else ""
            else:
                rank_name = str(rank_res)
                star_info = ""
            is_prem = r.get("premium_until") and r["premium_until"] > now
            prem_icon = " 🚀" if is_prem else ""
            text += f"<code>{i}.</code> {nickname_display} {moon} — {rank_name} [{star_info}]\n"
        text += "</blockquote>"
        text += f"<i>— Вы на {rank_a} месте</i>"
        text += "\n\n<blockquote>Для обновления топа используйте команду «регнуть\nДля смены ника используйте /name ник»</blockquote>"
        keyboard = [
            [InlineKeyboardButton("🃏 ТОП ПО КАРТАМ", callback_data=f"moba_top_{scope}_page_1")],
            [InlineKeyboardButton("🗑 Удалить", callback_data="delete_message")]]
    else:
        return await handle_moba_top_display(update, context, scope, 1)
    reply_markup = InlineKeyboardMarkup(keyboard)
    if query:
        try:
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except BadRequest:
            await context.bot.send_message(update.effective_chat.id, text, reply_markup=reply_markup,
                                           parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)


async def get_cards_for_pack(rarity):
    card_names = {
        "1": ["Обычная карта 1", "Обычная карта 2", "Обычная карта 3"],
        "2": ["Редкая карта 1", "Редкая карта 2", "Редкая карта 3"],
        "3": ["Эпическая карта 1", "Эпическая карта 2", "Эпическая карта 3"],
        "4": ["Легендарная карта 1", "Легендарная карта 2", "Легендарная карта 3"],
        "5": ["Мифическая карта 1", "Мифическая карта 2", "Мифическая карта 3"],
        "ltd": ["Эксклюзивная карта 1", "Эксклюзивная карта 2", "Эксклюзивная карта 3"]
    }
    return card_names.get(rarity, [])


async def check_shop_reset(user):
    now = datetime.now(timezone.utc)

    last_daily_reset = user.get('last_daily_reset')
    if not last_daily_reset or last_daily_reset.date() < now.date():
        user['bought_booster_today'] = 0
        user['last_daily_reset'] = now

    last_weekly_reset = user.get('last_weekly_reset')
    # 0 - это понедельник
    if not last_weekly_reset or (now.weekday() == 0 and last_weekly_reset.date() < now.date()):
        user['bought_luck_week'] = 0
        user['bought_protection_week'] = 0
        user['last_weekly_reset'] = now

    return user


async def create_shop_keyboard(user, bot):
    user_id = user['user_id']
    is_admin = (user_id == ADMIN_ID)
    if is_admin:
        premium_btn = InlineKeyboardButton("🚀 Premium (Бесплатно 🎁)", callback_data="admin_free_premium_30")
    else:
        premium_invoice_link = await bot.create_invoice_link(
            title="Премиум",
            description="30 дней подписки",
            payload="premium_30",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice("Цена", 10)])
        premium_btn = InlineKeyboardButton("🚀 Premium", url=premium_invoice_link)
    keyboard = [
        [InlineKeyboardButton("⚡️ Бустер", callback_data="booster_item"),
         InlineKeyboardButton("🍀 Удача", callback_data="luck_item"),
         InlineKeyboardButton("🛡 Защита", callback_data="protect_item")],
        [InlineKeyboardButton("💎 Алмазы", callback_data="diamond_item"),
         InlineKeyboardButton("💰 БО", callback_data="coins_item"),
         InlineKeyboardButton("🔖 Наборы", callback_data="shop_packs")],
        [premium_btn],
        [InlineKeyboardButton("❌ Закрыть", callback_data="delete_message")]]
    return keyboard


from datetime import datetime, timedelta, timezone


def _next_midnight_utc(now: datetime) -> datetime:
    if now.tzinfo is None or now.tzinfo.utcoffset(now) is None:
        now = now.replace(tzinfo=timezone.utc)

    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)


def _next_monday_utc(now: datetime) -> datetime:
    """Возвращает datetime следующего понедельника (00:00:00) UTC."""
    if now.tzinfo is None or now.tzinfo.utcoffset(now) is None:
        now = now.replace(tzinfo=timezone.utc)

    days_until_monday = (7 - now.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday = now.date() + timedelta(days=days_until_monday)
    return datetime.combine(next_monday, datetime.min.time(), tzinfo=timezone.utc)


def _format_timedelta_short(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    if total_seconds <= 0:
        return "0с"
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    parts = []
    if days > 0:
        parts.append(f"{days}д")
    if hours > 0 or (days > 0 and (minutes > 0 or seconds > 0)):
        parts.append(f"{hours}ч")
    if minutes > 0 or (days > 0 or hours > 0) and seconds > 0:
        parts.append(f"{minutes}м")
    if len(parts) > 3:
        return " ".join(parts[:3])

    return " ".join(parts)


async def get_moon_status(user_id, context, current_chat_id):
    if not CHAT_ISSUE_USERNAME:
        return ""

    try:
        chat_member = await context.bot.get_chat_member(f"@{CHAT_ISSUE_USERNAME}", user_id)
        if chat_member.status in ('member', 'creator', 'administrator'):
            return "🌙"
    except Exception as e:
        logger.debug(f"Ошибка проверки членства в @{CHAT_ISSUE_USERNAME} для {user_id}: {e}")
        pass

    return ""


async def shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await asyncio.to_thread(get_moba_user, user_id)
    user = await check_shop_reset(user)
    await asyncio.to_thread(save_moba_user, user)
    now = datetime.now(timezone.utc)  # Получаем текущее время для расчетов
    booster_count = user.get('bought_booster_today', 0)
    booster_limit = SHOP_BOOSTER_DAILY_LIMIT
    luck_count = user.get('bought_luck_week', 0)
    luck_limit = SHOP_LUCK_WEEKLY_LIMIT
    protect_count = user.get('bought_protection_week', 0)
    protect_limit = SHOP_PROTECT_WEEKLY_LIMIT
    next_daily = _next_midnight_utc(now)  # Предполагается, что эта функция определена
    time_to_daily = next_daily - now
    next_weekly = _next_monday_utc(now)  # Предполагается, что эта функция определена
    time_to_weekly = next_weekly - now
    time_str = now.strftime("%H:%M:%S")
    coins = user.get('coins', 0)
    diamonds = user.get('diamonds', 0)
    text = (
        f"<b>🛍 «Магазин»  </b>\n"
        f"<blockquote><b>💰БО • {coins} 💎 Алмазы • {diamonds}</b> </blockquote>\n\n"
        f"<b>Текущие лимиты:</b>\n "
        f"<b>Обновится через • {_format_timedelta_short(time_to_weekly)}</b> \n"  # Ежедневный сброс для бустера
        f"🍀Удача {luck_count}/{luck_limit} \n"
        f"🛡️Защита  {protect_count}/{protect_limit} \n\n"
        f"<b>Обновится через • {_format_timedelta_short(time_to_daily)}</b>  \n"  # Еженедельный сброс для удачи/защиты
        f"⚡️Бустер   {booster_count}/{booster_limit}\n\n"
        f"<blockquote>⌛️Глобальное обновление в магазине по понедельникам!</blockquote>\n"
        f" <b>Время сервера: {time_str} </b>\n"
    )
    keyboard = await create_shop_keyboard(user, context.bot)
    if update.callback_query:
        query = update.callback_query  #
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                      parse_mode=ParseMode.HTML)
        NOTEBOOK_MENU_OWNERSHIP[(query.message.chat_id, query.message.message_id)] = user_id
    else:
        msg = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                              parse_mode=ParseMode.HTML)
        NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id


async def handle_pack_purchase(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, user, pack_type: str):
    user_id = user['user_id']
    is_admin = (user_id == ADMIN_ID)  # Проверка на админа
    
    price = PACK_PRICES.get(pack_type)
    if not price:
        return "💢 <b>Неизвестный тип набора</b>" # Удалил "style", он не нужен здесь
    if not is_admin and user["diamonds"] < price:
        return f"<b>💢 Покупка не совершена</b>\n<blockquote>💎 Не хватает алмазов!</blockquote>"
    if not is_admin:
        user["diamonds"] -= price
    await asyncio.to_thread(save_moba_user, user)
    gained_cards_info = []
    
    # Получаем инвентарь пользователя ОДИН РАЗ до цикла
    inventory = await asyncio.to_thread(get_user_inventory, user_id)
    # Создаем сет из ID карт, которые УЖЕ БЫЛИ у пользователя до открытия этого пака
    initial_owned_card_ids = {c['card_id'] for c in inventory}
    # Создаем копию, которую будем обновлять внутри цикла для учета карт, выпавших в текущем паке
    current_pack_owned_card_ids = initial_owned_card_ids.copy()

    for _ in range(CARDS_PER_PACK):
        possible_card_ids = [
            card_id for card_id, rarity_name in FIXED_CARD_RARITIES.items()
            if rarity_name in PACK_RARITIES_MAP.get(pack_type, [])
        ]

        if not possible_card_ids:
            chosen_card_id = random.choice(list(CARDS.keys()))
        else:
            chosen_card_id = random.choice(possible_card_ids) # Первая попытка выбора
            
            # Флаг, который покажет, была ли карта изначально повторкой
            was_initially_repeat = chosen_card_id in current_pack_owned_card_ids
            
            # --- ЛОГИКА СНИЖЕНИЯ ШАНСА ПОВТОРКИ (40% реролл) ---
            if was_initially_repeat:
                if random.random() < 0.40:  # 40% шанс на замену повторки
                    # Ищем карты этой же редкости, которых у игрока еще НЕТ (с учетом выпавших в этом же паке)
                    unowned_of_this_rarity = [
                        cid for cid in possible_card_ids 
                        if cid not in current_pack_owned_card_ids
                    ]
                    if unowned_of_this_rarity:
                        # Успешный реролл! Заменяем повторку на уникальную карту
                        chosen_card_id = random.choice(unowned_of_this_rarity)
                        was_initially_repeat = False # Теперь эта карта не повторка
            # --------------------------------------------------

        # Определяем, является ли карта повторкой ПОСЛЕ всех рероллов
        # Это важно для финального сообщения и начисления алмазов
        is_repeat = chosen_card_id in current_pack_owned_card_ids # Проверка с учетом уже выпавших в этом паке

        # Добавляем выбранную (возможно, реролленную) карту в сет для следующих проверок в этом паке
        current_pack_owned_card_ids.add(chosen_card_id) 

        chosen_rarity = FIXED_CARD_RARITIES.get(chosen_card_id, "regular card")
        card_info = CARDS[chosen_card_id]
        card_stats = generate_card_stats(chosen_rarity, card_info, is_repeat=is_repeat)

        await asyncio.to_thread(add_card_to_inventory, user_id, {
            "card_id": chosen_card_id,
            "name": card_info["name"],
            "collection": card_info.get("collection", " "),
            "rarity": chosen_rarity,
            "bo": card_stats["bo"],
            "points": card_stats["points"],
            "diamonds": card_stats["diamonds"]
        })
        
        gained_cards_info.append({
            "name": card_info["name"],
            "rarity": chosen_rarity,
            "diamonds_gained": card_stats["diamonds"],
            "is_repeat": is_repeat # Добавляем флаг, чтобы использовать его в выводе
        })
        
        # Начисление алмазов за повторку, если это ДЕЙСТВИТЕЛЬНО повторка
        if is_repeat:
            user["diamonds"] += card_stats["diamonds"]
            await asyncio.to_thread(save_moba_user, user)

    result_message = f"<b>🧧 Набор приобретен!</b>\n\n"
    result_message += "Вы получили:\n"
    for card_data in gained_cards_info:
        # Теперь используем флаг is_repeat из card_data
        repeat_text = f" <i>Повторка +{card_data['diamonds_gained']} 💎</i>" if card_data['is_repeat'] else ""
        result_message += f"<blockquote>• <b>{card_data['name']}</b> ({card_data['rarity']}){repeat_text}</blockquote>\n"
        
    price_text = "0 💎 (Бесплатно для Создателя 🎁)" if is_admin else f"{price} 💎"
    result_message += f"\n<b>Списано: {price_text}</b>"
    return result_message
    
@check_menu_owner
async def shop_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    user = await asyncio.to_thread(get_moba_user, user_id)
    logger.info(f"Callback data received: {data} from user {user_id}")
    if data and data.startswith("admin_free_"):
        if user_id != ADMIN_ID:
            await query.answer("❌ Эта функция доступна только Создателю бота!", show_alert=True)
            return

        payload = data.replace("admin_free_", "")

        # 1. Бесплатные алмазы
        if payload.startswith("diamonds_"):
            try:
                amount = int(payload.split("_")[1])
                user["diamonds"] += amount
                await asyncio.to_thread(save_moba_user, user)
                await query.edit_message_text(
                    f"🎁 <b>Успешно зачислено!</b>\n\n"
                    f"Вы получили <b>{amount} 💎</b> за 0 ⭐️ (Привилегия Создателя).\n"
                    f"Ваш баланс: <b>{user['diamonds']} 💎</b>",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 В магазин", callback_data="back_to_shop")]]),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                await query.answer("Ошибка начисления", show_alert=True)

        # 2. Бесплатное БО
        elif payload.startswith("coins_"):
            try:
                amount = int(payload.split("_")[1])
                user["coins"] += amount
                await asyncio.to_thread(save_moba_user, user)
                await query.edit_message_text(
                    f"🎁 <b>Успешно зачислено!</b>\n\n"
                    f"Вы получили <b>{amount} 💰 БО</b> за 0 ⭐️ (Привилегия Создателя).\n"
                    f"Ваш баланс: <b>{user['coins']} БО</b>",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 В магазин", callback_data="back_to_shop")]]),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                await query.answer("Ошибка начисления", show_alert=True)

        # 3. Бесплатный премиум
        elif payload == "premium_30":
            current_time_utc = datetime.now(timezone.utc)
            if user.get("premium_until") and user["premium_until"] > current_time_utc:
                user["premium_until"] += timedelta(days=30)
            else:
                user["premium_until"] = current_time_utc + timedelta(days=30)

            await asyncio.to_thread(save_moba_user, user)
            await query.edit_message_text(
                "🚀 <b>Premium активирован бесплатно!</b>\n\n"
                "Срок вашего Premium статуса успешно продлен на 30 дней за 0 ⭐️.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 В магазин", callback_data="back_to_shop")]]),
                parse_mode=ParseMode.HTML
            )
        return
    # --- КОНЕЦ БЛОКА АДМИНИСТРАТОРА ---
    # --- БЛОК ПОДТВЕРЖДЕНИЯ ПОКУПКИ НАБОРОВ ---
    if data and data.startswith("confirm_pack_"):
        pack_type = data.split("_")[-1]
        
        # Словарь названий и цен для красивого вывода
        pack_info = {
            "1": {"name": "Regular pack (1★)", "price": 1100},
            "2": {"name": "Rare pack (2★)", "price": 1300},
            "3": {"name": "Exclusive pack (3★)", "price": 1600},
            "4": {"name": "Epic pack (4★)", "price": 2100},
            "5": {"name": "Collectible pack (5★)", "price": 3000},
            "ltd": {"name": "LIMITED pack (Эксклюзив)", "price": 5000}}
        info = pack_info.get(pack_type, {"name": "Неизвестный набор", "price": 0})
        is_admin = (user_id == ADMIN_ID)
        price_display = "0 💎 (Бесплатно для Создателя 🎁)" if is_admin else f"{info['price']} 💎"
        confirm_text = (
            f"<b>🛍 Подтверждение покупки набора</b>\n"
            f"<blockquote>• Набор: <b>{info['name']}</b></blockquote>\n"
            f"<blockquote>• Стоимость: <b>{price_display}</b></blockquote>\n"
            f"<i>Карты будут мгновенно добавлены в ваш инвентарь!</i>")
        
        kb = [[InlineKeyboardButton("Купить", callback_data=f"do_buy_pack_{pack_type}"),
             InlineKeyboardButton("Отмена", callback_data="shop_packs")]]
        await query.edit_message_text(confirm_text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        return
    # --- КОНЕЦ БЛОКА ПОДТВЕРЖДЕНИЯ НАБОРОВ ---
        # --- ОБРАБОТКА ФАКТИЧЕСКОЙ ПОКУПКИ НАБОРА ---
    if data and data.startswith("do_buy_pack_"):
        pack_type = data.split("_")[-1]
        result_message = await handle_pack_purchase(query, context, user, pack_type)

        keyboard_on_success = [[InlineKeyboardButton("🛍 В МАГАЗИН", callback_data="back_to_shop")]]
        await query.edit_message_text(
            text=result_message,
            reply_markup=InlineKeyboardMarkup(keyboard_on_success),
            parse_mode=ParseMode.HTML
        )
        return
    # --------------------------------------------

    user = await asyncio.to_thread(get_moba_user, user_id)
    user = await check_shop_reset(user)  # Обновляем лимиты магазина
    await asyncio.to_thread(save_moba_user, user)  # Сохраняем обновленные лимиты
    await query.answer()
    item_type = None
    if data == "booster_item":
        item_type = "booster"
        booster_limit = SHOP_BOOSTER_DAILY_LIMIT
        bought_booster_today = user.get("bought_booster_today", 0)

        text = (
            f"<b>⚡️Бустер [2500 бО ]</b>\n"
            f"<blockquote><b>MOBA.</b>Сокращает время ожидания карты на 2 часа. Суммируется с Premium</blockquote>\n"
            f"<b>Куплено сегодня {bought_booster_today}/{booster_limit}</b>"
        )
        keyboard = [
            [InlineKeyboardButton("Купить", callback_data=f"do_buy_{item_type}")],
            [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]
        ]
        await query.edit_message_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
        return
    if data == "luck_item":
        item_type = "luck"
        luck_limit = SHOP_LUCK_WEEKLY_LIMIT
        bought_luck_week = user.get("bought_luck_week", 0)

        text = (
            f"<b>🍀 Удача [5000 бО ]</b>\n"
            f"<blockquote><b>MOBA.</b> Повышает шанс выпадения карты редкости epic и выше на 10 %  </blockquote>\n"
            f"<b>Куплено на этой неделе {bought_luck_week}/{luck_limit}</b>")
        keyboard = [
            [InlineKeyboardButton("Купить", callback_data=f"do_buy_{item_type}")],
            [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]]
        await query.edit_message_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML)
        return

    if data == "protect_item":
        item_type = "protect"
        protect_limit = SHOP_PROTECT_WEEKLY_LIMIT
        bought_protection_week = user.get("bought_protection_week", 0)  # Исправлено на bought_protection_week

        text = (
            f"<b>🛡 Защита [5000 бО ]</b>\n"  # Исправлено на 🛡
            f"<blockquote><b>MOBA.</b> При проигрыше вы не потеряете звезду!</blockquote>\n"  # Исправлено описание
            f"<b>Куплено на этой неделе {bought_protection_week}/{protect_limit}</b>")
        keyboard = [
            [InlineKeyboardButton("Купить", callback_data=f"do_buy_{item_type}")],
            [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]]
        await query.edit_message_text(
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML)
        return

    if data == "coins_item":  # НОВЫЙ БЛОК ДЛЯ БО
        try:
            await buy_coins_menu(query, context, user)
        except BadRequest as e:
            logger.warning(f"Failed to edit coins_item menu for user {user_id}: {e}")
            text = ("💰 <b>Покупка БО за Звезды Telegram</b>\n"
                    f"<b>Время сервера: {datetime.now(timezone.utc).strftime('%H:%M:%S')}</b>\n\n"
                    "Нажмите на кнопку ниже, чтобы перейти к оплате:\n")
            coins_pack_1_link = await context.bot.create_invoice_link(
                title="100 БО", description="Игровые боевые очки", payload="coins_100",
                provider_token="", currency="XTR", prices=[LabeledPrice("Цена", 1)])
            coins_pack_2_link = await context.bot.create_invoice_link(
                title="500 БО", description="Игровые боевые очки", payload="coins_500",
                provider_token="", currency="XTR", prices=[LabeledPrice("Цена", 4)])
            kb = [
                [InlineKeyboardButton("100 БО (1 ⭐️)", url=coins_pack_1_link)],
                [InlineKeyboardButton("500 БО (4 ⭐️)", url=coins_pack_2_link)],
                [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]]
            await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(kb),
                                           parse_mode=ParseMode.HTML)
        return

    if data == "shop_packs":
        try:
            await shop_packs_diamonds(query, user)
        except BadRequest as e:
            logger.warning(f"Failed to edit shop_packs menu for user {user_id}: {e}")
            text = (
                "📦 <b>Магазин наборов карт</b>\n"
                "Карты выпадают случайным образом из соответствующей редкости и добавляются в инвентарь!\n\n"
                f"💎 Ваш баланс: {user['diamonds']}\n\n"
                "<b>Наборы за Алмазы:</b>\n"
                "1★ (3 шт) — 1800 💎\n"
                "2★ (3 шт) — 2300 💎\n"
                "3★ (3 шт) — 3400 💎\n"
                "4★ (3 шт) — 5700 💎\n"
                "5★ (3 шт) — 7500 💎\n"
                "LTD (3 шт) — 15000 💎 (Эксклюзивные карты)"
            )
            kb = [
                [InlineKeyboardButton("Regular\n1800💎", callback_data="buy_pack_1"),
                 InlineKeyboardButton("RARE", callback_data="buy_pack_2")],
                [InlineKeyboardButton("EXCLUSIVE", callback_data="buy_pack_3"),
                 InlineKeyboardButton("EPIC", callback_data="buy_pack_4")],
                [InlineKeyboardButton("COLLECTIBLE", callback_data="buy_pack_5"),
                 InlineKeyboardButton("LIMITED", callback_data="buy_pack_ltd")],
                [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]
            ]
            await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(kb),
                                           parse_mode=ParseMode.HTML)
        return

    if data == "diamond_item":
        try:
            # Вызов функции меню покупки алмазов
            await buy_diamonds_menu(query, context, user)
        except BadRequest as e:
            logger.warning(f"Failed to edit diamond_item menu for user {user_id}: {e}")
            text = (
                "💎 <b>Покупка Алмазов за Звезды Telegram</b>\n"
                f"<b>Время сервера: {datetime.now(timezone.utc).strftime('%H:%M:%S')}</b>\n\n"
                "Нажмите на кнопку ниже, чтобы перейти к оплате:\n"
            )
            diamond_pack_1_link = await context.bot.create_invoice_link(
                title="1000 Алмазов", description="Игровые алмазы", payload="diamonds_1000",
                provider_token="", currency="XTR", prices=[LabeledPrice("Цена", 5)])
            diamond_pack_2_link = await context.bot.create_invoice_link(
                title="5000 Алмазов", description="Игровые алмазы", payload="diamonds_5000",
                provider_token="", currency="XTR", prices=[LabeledPrice("Цена", 20)])
            kb = [
                [InlineKeyboardButton("1000 Алмазов (5 ⭐️)", url=diamond_pack_1_link)],
                [InlineKeyboardButton("5000 Алмазов (20 ⭐️)", url=diamond_pack_2_link)],
                [InlineKeyboardButton("< Назад", callback_data="back_to_shop")]
            ]
            await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(kb),
                                           parse_mode=ParseMode.HTML)
        return

    if data.startswith("confirm_buy_"):
        item_type = data.split("_")[2]  # item_type здесь гарантированно определен из callback_data
        price = 0
        currency = ""
        name = ""
        logger.info(f"DEBUG: Попытка покупки предмета: {item_type}") 
        if item_type == "booster":
            price = 10
            currency = "БО"
            name = "Бустер ⚡️"
        elif item_type == "protect":
            price = 15
            currency = "БО"
            name = "Защиту 🛡️"
        elif item_type == "diamond":  # Если алмазы можно купить за БО
            price = 50
            currency = "БО"
            name = "Алмазы 💎"
        return f"❌ Ошибка: предмет {item_type} не найден в списке магазина."
        price_safe = html.escape(str(price))
        currency_safe = html.escape(currency)
        name_safe = html.escape(name)
        confirm_text = (f"🛍️ Подтверждение покупки\n {name_safe} • Цена [💰{price_safe} {currency_safe}]")
        keyboard = [[InlineKeyboardButton("Купить", callback_data=f"do_buy_{item_type}")],
                    [InlineKeyboardButton("< Назад",
                                          callback_data=f"{item_type}_item")]]  # Возвращаем к деталям конкретного предмета
        try:
            await query.edit_message_text(confirm_text, reply_markup=InlineKeyboardMarkup(keyboard),
                                          parse_mode=ParseMode.HTML)
        except BadRequest as e:
            logger.warning(f"Failed to edit confirm_buy message for user {user_id}: {e}")
            await context.bot.send_message(chat_id=user_id, text=confirm_text,
                                           reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        return
    if data.startswith("do_buy_"):
        item_type = data.split("_")[2]
        result_message = await handle_shop_purchase(query, user, item_type)
        updated_user = await asyncio.to_thread(get_moba_user, user_id)
        final_text = (
            f"{result_message}\n"
        )
        keyboard_on_success = [[InlineKeyboardButton("🛍 В МАГАЗИН", callback_data="back_to_shop")]]
        try:
            await query.edit_message_text(
                text=final_text,
                reply_markup=InlineKeyboardMarkup(keyboard_on_success),
                parse_mode=ParseMode.HTML
            )
        except BadRequest:
            await context.bot.send_message(
                chat_id=user_id,
                text=final_text,
                reply_markup=InlineKeyboardMarkup(keyboard_on_success),
                parse_mode=ParseMode.HTML
            )
        return

    if data == "back_to_shop":
        await shop(update, context)
        return
    if data.startswith("buy_pack_"):
        pack_type = data.split("_")[-1]
        result_message = await handle_pack_purchase(query, context, user, pack_type)

        keyboard_on_success = [[InlineKeyboardButton("🛍 В МАГАЗИН", callback_data="back_to_shop")]]
        try:
            await query.edit_message_text(
                text=result_message,
                reply_markup=InlineKeyboardMarkup(keyboard_on_success),
                parse_mode=ParseMode.HTML
            )
        except BadRequest:
            await context.bot.send_message(
                chat_id=user_id,
                text=result_message,
                reply_markup=InlineKeyboardMarkup(keyboard_on_success),
                parse_mode=ParseMode.HTML
            )
        return

    await query.answer("Неизвестное действие.", show_alert=True)


async def buy_coins_menu(query, context: ContextTypes.DEFAULT_TYPE, user):
    user_id = query.from_user.id
    is_admin = (user_id == ADMIN_ID)  # Проверка на админа
    text = (
        "<b>💰 Магазин БО </b>\n"
        "<blockquote>Используются для полезных покупок. Защита, Удача, Бустер — приятные бонусы за БО!</blockquote>"
        f"<b>Баланс: 💰 {user.get('coins', 0)} БО</b>\n")
    packages = [
        (1000, 7),
        (5000, 34),
        (10000, 66),
        (15000, 98),
        (25000, 160),
        (50000, 300)]
    keyboard = []
    row = []
    for count, stars in packages:
        if is_admin:
            row.append(InlineKeyboardButton(f"{count} 💰 (Бесплатно 🎁)", callback_data=f"admin_free_coins_{count}"))
        else:
            link = await context.bot.create_invoice_link(
                title=f"{count} БО",
                description=f"Игровая валюта для MOBA бота",
                payload=f"coins_{count}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(f"{count} БО", stars)])
            row.append(InlineKeyboardButton(f"{count} 💰 ({stars} ⭐️)", url=link))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(" 🛍 В МАГАЗИН", callback_data="back_to_shop")])
    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except BadRequest:
        await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard),
                                       parse_mode=ParseMode.HTML)


async def edit_shop_message(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, user, now: datetime,
                            premium_invoice_link, bo_invoice_link):
    keyboard_markup = await create_shop_keyboard(user, context.bot)
    time_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
    booster_count = user.get('bought_booster_today', 0)
    now = datetime.now(timezone.utc)
    next_global = _next_monday_utc(now)  # Теперь это сработает
    booster_limit = SHOP_BOOSTER_DAILY_LIMIT
    time_to_global = next_global - now

    # Суточный ресет (для бустера) — до следующей полуночи UTC
    # Если у пользователя есть last_daily_reset, можно ориентироваться на него,
    # но общепринято — ежедневный ресет в 00:00 UTC следующего дня.
    next_daily = _next_midnight_utc(now)
    time_to_daily = next_daily - now

    # Недельный ресет для удачи/защиты — считаем до следующего понедельника 00:00 UTC
    next_weekly = _next_monday_utc(now)
    time_to_weekly = next_weekly - now

    coins = user.get('coins', 0)
    diamonds = user.get('diamonds', 0)

    luck_count = user.get('bought_luck_week', 0)
    luck_limit = SHOP_LUCK_WEEKLY_LIMIT

    protect_count = user.get('bought_protection_week', 0)
    protect_limit = SHOP_PROTECT_WEEKLY_LIMIT

    text = (
        f"<b>🛍 «Магазин»  </b>\n"
        f"<blockquote><b>💰БО • {coins} 💎 Алмазы • {diamonds}</b> </blockquote>\n\n"
        f"<b>Текущие лимиты:</b>\n "
        f"<b>Обновится через • {_format_timedelta_short(time_to_weekly)}</b> \n"  # Ежедневный сброс для бустера
        f"⚡️Бустер {booster_count}/{booster_limit}\n\n"
        f"<b>Обновится через • {_format_timedelta_short(time_to_daily)}</b>  \n"  # Еженедельный сброс для удачи/защиты
        f"🍀Удача {luck_count}/{luck_limit} \n"
        f"🛡️Защита {protect_count}/{protect_limit} \n\n"
        f"<blockquote>⌛️Глобальное обновление в магазине по понедельникам!</blockquote>\n"
        f" <b>Время сервера: {time_str} </b>\n"
    )

    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard_markup),
                                      parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Error editing message: {e}")


async def buy_diamonds_menu(query, context: ContextTypes.DEFAULT_TYPE, user):
    user_id = query.from_user.id
    is_admin = (user_id == ADMIN_ID)  # Проверка на админа
    text = (
        "<b>💎 Магазин алмазов </b>\n"
        "<blockquote>Используются для покупок наборов карт!</blockquote>"
        f"<b>Баланс: 💎 {user.get('diamonds', 0)} алмазов</b>\n")

    packages = [
        (50, 10),
        (100, 18),
        (200, 35),
        (400, 68),
        (600, 100),
        (1000, 165)]

    keyboard = []
    row = []
    for count, stars in packages:
        if is_admin:
            row.append(InlineKeyboardButton(f"{count} 💎 (Бесплатно 🎁)", callback_data=f"admin_free_diamonds_{count}"))
        else:
            link = await context.bot.create_invoice_link(
                title=f"{count} Алмазов",
                description=f"Игровая валюта для MOBA бота",
                payload=f"diamonds_{count}",
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(f"{count} 💎", stars)])
            row.append(InlineKeyboardButton(f"{count} 💎 ({stars} ⭐️)", url=link))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🛍 В МАГАЗИН", callback_data="back_to_shop")])
    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except BadRequest:
        await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard),
                                       parse_mode=ParseMode.HTML)


async def start_payment_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()


async def handle_pre_checkout_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    await query.answer(ok=True)


async def handle_shop_purchase(query, user, item_type):
    if item_type == "booster":
        price = 2500
        if user["coins"] < price: return "💢 Недостаточно БО"
        if user.get("bought_booster_today",
                    0) >= SHOP_BOOSTER_DAILY_LIMIT: return "<b>💢 Покупка не совершена</b>\n<blockquote>Лимит на сегодня исчерпан</blockquote>"
        user["coins"] -= price
        user["bought_booster_today"] += 1
        user["pending_boosters"] = user.get("pending_boosters", 0) + 1
        await asyncio.to_thread(save_moba_user, user)
        return f"<b>🛍️ Покупка успешна!</b>\n<blockquote>⚡️Бустер • [{user['pending_boosters']} шт] в сумке</blockquote><b>Списано : 💰 2500 БО</b>"
    elif item_type == "luck":
        price = 5000
        if user["coins"] < price: return "💢 Недостаточно БО"
        if user.get("bought_luck_week",
                    0) >= SHOP_LUCK_WEEKLY_LIMIT: return "<b>💢 Покупка не совершена</b>\n<blockquote>Лимит на неделю исчерпан</blockquote>"
        user["coins"] -= price
        user["bought_luck_week"] += 1
        user["luck_active"] = user.get("luck_active", 0) + 1
        await asyncio.to_thread(save_moba_user, user)
        return f"<b>🛍️ Покупка успешна!</b>\n<blockquote>🍀 Удача • [{user['luck_active']} шт] в сумке</blockquote><b>Списано : 💰 5000 БО</b>"
    elif item_type == "protect":
        price = 5000
        if user["coins"] < price: return "💢 Недостаточно БО"
        if user.get("bought_protection_week",
                    0) >= SHOP_PROTECT_WEEKLY_LIMIT: return "<b>💢 Покупка не совершена</b>\n<blockquote>Лимит на неделю исчерпан</blockquote>"
        user["coins"] -= price
        user["bought_protection_week"] += 1
        user["protection_active"] = user.get("protection_active", 0) + 1
        await asyncio.to_thread(save_moba_user, user)
        return f"<b>🛍️ Покупка успешна!</b>\n<blockquote>🛡️Защита • [{user['protection_active']} шт ]  в сумке</blockquote><b>Списано : 💰 5000 БО</b>"
    return "❌ Ошибка: предмет не найден."


async def shop_packs_diamonds(query, user):
    text = (
        "<b>🧧 Магазин наборов</b>\n"
        "<i>Каждый набор содержит 3 карточки определенной редкости. Шанс на повторки снижен на 40%</i>\n\n"
        "<b>Стоимость набора</b>"
        "<blockquote><b>💎1100   •  🃏 Regular pack</b></blockquote>\n"
        "<blockquote><b>💎1300   •  🃏 Rare pack</b></blockquote>\n"
        "<blockquote><b>💎1600   •  🃏 Exclusive pack</b></blockquote>\n"
        "<blockquote><b>💎2100   •  🃏 Epic pack</b></blockquote>\n"
        "<blockquote><b>💎3000   •  🃏 Collectible pack</b></blockquote>\n"
        "<blockquote><b>💎5000   •  🃏 LIMITED pack</b></blockquote>\n"
        f"<b>Текущий баланс • {user['diamonds']}💎</b>"
    )
    kb = [
        [InlineKeyboardButton("1100", callback_data="confirm_pack_1"),
         InlineKeyboardButton("1300", callback_data="confirm_pack_2"),
         InlineKeyboardButton("1600", callback_data="confirm_pack_3")],
        [InlineKeyboardButton("2100", callback_data="confirm_pack_4"),
         InlineKeyboardButton("3000", callback_data="confirm_pack_5"),
         InlineKeyboardButton("5000", callback_data="confirm_pack_ltd")],
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
        price = 10
    elif query.data == "shop_coins":
        title = "100 БО"
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
        prices=[LabeledPrice("Цена", price)])
    keyboard = [
        [InlineKeyboardButton(f"💳 Подтвердить оплату ({price} ⭐️)", url=invoice_link)],
        [InlineKeyboardButton("< Отмена", callback_query_handler="shop")] ]
    await query.edit_message_text(
        text=f"{title}\n\n{description}\n\nНажмите на кнопку ниже для перехода к оплате:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML)

@check_menu_owner
async def handle_bag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass

    user_id = query.from_user.id
    user = await asyncio.to_thread(get_moba_user, user_id)

    boosters = user.get('pending_boosters', 0)
    lucks = user.get('luck_active', 0)
    protects = user.get('protection_active', 0)

    items = []
    if boosters > 0: items.append(f"<blockquote>⚡️ Бустер: [ {boosters} шт ]</blockquote>")
    if lucks > 0: items.append(f"<blockquote>🍀 Удача: [ {lucks} шт ]</blockquote>")
    if protects > 0: items.append(f"<blockquote>🛡 Защита: [ {protects} шт ]</blockquote>")

    if not items:
        msg_text = "<b>👝 Сумка</b>\n<blockquote>Ваша сумка пока пуста</blockquote>\n<b>🛍  Магазин /shop</b>"
    else:
        msg_text = "<b>👝 Сумка</b>\n" + "\n".join(items) + "\n<b>🛍  Магазин /shop</b>"

    keyboard = [[InlineKeyboardButton("< Назад", callback_data="back_to_moba_profile")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query.message.photo:
        await safe_delete_message(query, context)
        msg = await context.bot.send_message(chat_id=query.message.chat_id, text=msg_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        NOTEBOOK_MENU_OWNERSHIP[(msg.chat_id, msg.message_id)] = user_id
    else:
        await safe_edit_message_text(query, msg_text, reply_markup)


async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    # Всегда отвечаем True для Stars
    await query.answer(ok=True)


async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment = update.message.successful_payment
    user_id = update.effective_user.id
    payload = payment.invoice_payload

    user = await asyncio.to_thread(get_moba_user, user_id)
    if user is None:
        logger.error(f"Successful payment for unknown user {user_id}. Payload: {payload}")
        await update.message.reply_text("❌ Произошла ошибка при начислении средств. Ваш профиль не найден. Пожалуйста, свяжитесь с администратором.")
        return

    # Обработка покупки алмазов (универсальная для любого количества)
    if payload.startswith("diamonds_"):
        try:
            amount = int(payload.split("_")[1])
            user["diamonds"] += amount
            await asyncio.to_thread(save_moba_user, user)
            await update.message.reply_text(
                f"✅ <b> Успешная оплата!</b> \n<blockquote>Вы получили {amount} 💎</blockquote>\n"
                f"Ваш текущий баланс: <b>{user['diamonds']} 💎</b>",
                parse_mode=ParseMode.HTML
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing diamond amount from payload '{payload}': {e}", exc_info=True)
            await update.message.reply_text("❌ Ошибка при начислении алмазов. Пожалуйста, свяжитесь с администратором.")

    elif payload == "premium_30":
        current_time_utc = datetime.now(timezone.utc)
        if user.get("premium_until") and user["premium_until"] > current_time_utc:
            user["premium_until"] += timedelta(days=30)
        else:
            user["premium_until"] = current_time_utc + timedelta(days=30)

        await asyncio.to_thread(save_moba_user, user)
        await update.message.reply_text("🚀 <b>Premium активирован на 30 дней!</b>", parse_mode=ParseMode.HTML)

    elif payload.startswith("coins_"): # <--- ИСПРАВЛЕНО: теперь проверяет, начинается ли payload с "coins_"
        try:
            amount = int(payload.split("_")[1]) # Извлекаем количество БО из payload (например, "coins_1000" -> 1000)
            user["coins"] += amount
            await asyncio.to_thread(save_moba_user, user)
            await update.message.reply_text(
                f"✅ <b> Успешная оплата!</b> \n<blockquote>Вы получили {amount} 💰 БО</blockquote>\n"
                f"Ваш текущий баланс: <b>{user['coins']} 💰 БО</b>",
                parse_mode=ParseMode.HTML
            )
        except (IndexError, ValueError) as e:
            logger.error(f"Error parsing coin amount from payload '{payload}': {e}", exc_info=True)
            await update.message.reply_text("❌ Ошибка при начислении БО. Пожалуйста, свяжитесь с администратором.")
    else:
        logger.warning(f"Unhandled successful payment payload: {payload} for user {user_id}")
        await update.message.reply_text(
            "✅ Оплата прошла успешно, но не удалось определить, что именно было куплено. "
            "Пожалуйста, свяжитесь с администратором, предоставив скриншот чека."
        )

async def show_specific_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    cat = "season"
    if data == "top_points":
        cat = "points"
    elif data == "top_cards":
        cat = "cards"
    elif data == "top_stars_season":
        cat = "season"
    elif data == "top_stars_all":
        cat = "all"
    await send_moba_global_leaderboard(update, context, category_token=cat, page=1)


@check_menu_owner
async def handle_moba_my_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if query:
        await query.answer()
        cb_base = (query.data or "moba_my_cards").rsplit("_", 1)[0]
        if is_recent_callback(user_id, cb_base):
            return

    # Получаем карты пользователя
    user_cards = await asyncio.to_thread(get_user_inventory, user_id)
    total_cards_count = len(user_cards)
    has_cards = total_cards_count > 0

    msg = None

    # Формируем текст и клавиатуру
    if not has_cards:
        msg_text = ("🃏 У тебя нет карт\n"
                    "<blockquote>Получи карту командой «моба»</blockquote>")
        keyboard = None
    else:
        msg_text = (f"🃏 Ваши карты\n"
                    f"<blockquote>Всего {total_cards_count}/269 карт</blockquote>")
        keyboard_layout = [
            [InlineKeyboardButton("❤️‍🔥 Коллекции", callback_data="moba_show_collections")],
            [InlineKeyboardButton("🪬 LIMITED", callback_data="moba_show_cards_rarity_LIMITED_0")],
            [InlineKeyboardButton("🃏 Все карты", callback_data="moba_show_cards_all_0")]
        ]
        keyboard = InlineKeyboardMarkup(keyboard_layout)

    if query:
        # Вызвано через КНОПКУ
        if query.message.photo:
            await query.message.delete()
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=msg_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            await query.edit_message_text(
                text=msg_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            msg = query.message
    else:
        # Вызвано через ТЕКСТОВУЮ КОМАНДУ «мои карты»
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=msg_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )

    # Записываем владельца меню блокнота
    if msg:
        NOTEBOOK_MENU_OWNERSHIP[(chat_id, msg.message_id)] = user_id


async def moba_get_sorted_user_cards_list(user_id: int) -> List[dict]:
    rows = get_user_inventory(user_id)  # возвращает list[dict] из БД
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
               f"<blockquote>Принесла вам  <b>{points}</b> очков!</blockquote>\n\n"
               f"✨ <b>Редкость</b> • <i>{rarity}</i>\n"
               f"💰<b> БО </b>•  <i>{bo}</i>\n\n"
               f"<blockquote>Карта из твоей коллекции! Помнишь как выбил ее?</blockquote>")
    return caption


@check_menu_owner
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
    photo_path = card.get('image_path') or CARDS.get(card.get('card_id'), {}).get('path') or PHOTO_DETAILS.get(
        card.get('card_id'), {}).get('path')
    caption = _moba_card_caption(card, index, len(cards))
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
        if query.message.photo:
            with open(photo_path, "rb") as ph:
                await query.edit_message_media(InputMediaPhoto(media=ph, caption=caption, parse_mode=ParseMode.HTML),
                                               reply_markup=InlineKeyboardMarkup(keyboard))
        else:
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
            await query.bot.send_message(chat_id=query.message.chat_id, text=caption + "\n\n(Фото не найдено)",
                                         reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except BadRequest as e:
        logger.warning(f"BadRequest in moba_show_cards_all: {e}", exc_info=True)
        try:
            with open(photo_path, "rb") as ph:
                await context.bot.send_photo(chat_id=query.message.chat_id, photo=ph, caption=caption,
                                             reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        except Exception as e2:
            logger.error(f"Failed to fallback send photo in moba_show_cards_all: {e2}", exc_info=True)
            await context.bot.send_message(chat_id=query.message.chat_id, text=caption, parse_mode=ParseMode.HTML)


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
            await query.edit_message_text("<b>🃏 У тебя нет карт</b>\n"
                                          "<blockquote>Получи карту командой «моба»</blockquote>",
                                          parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка при edit_message_text в handle_moba_collections (нет карт): {e}")
            await context.bot.send_message(chat_id=query.message.chat_id,
                                           text="<b>🃏 У тебя нет карт</b>\n<blockquote>Получи карту командой «моба»</blockquote>",
                                           parse_mode=ParseMode.HTML)
        return

    excluded_names = {"", " ", "z", "common", "обычная", "none", "common card", "regular card"}

    collections_data = {}
    for r in rows:
        col = r.get('collection')
        if not col:
            continue

        col_stripped = col.strip()
        col_lower = col_stripped.lower()

        # Игнорируем технические заглушки
        if not col_stripped or col_lower in excluded_names:
            continue

        # Подсчитываем, сколько реальных карт этой коллекции зарегистрировано в игре
        total_in_col = sum(1 for cid, cdata in CARDS.items() if (cdata.get('collection') or "").strip() == col_stripped)

        # Если в игре нет такой коллекции (0 карт), то это не коллекционная карта, игнорируем её в этом меню
        if total_in_col == 0:
            continue

        collections_data.setdefault(col_stripped, set()).add(r.get('card_id'))

    sorted_collection_names = sorted(list(collections_data.keys()))

    if not sorted_collection_names:
        text = (
            "<b>❤️‍🔥 Ваши коллекции</b>\n\n"
            "<blockquote>У вас пока нет карт, принадлежащих к тематическим коллекциям.\n\n"
            "Все имеющиеся у вас обычные карты можно посмотреть в разделе <b>«Все карты»</b>!</blockquote>"
        )
        keyboard = [[InlineKeyboardButton("< Назад к картам", callback_data="moba_my_cards")]]
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
        return

    total_collections = len(sorted_collection_names)
    total_pages = (total_collections + COLLECTIONS_PER_PAGE - 1) // COLLECTIONS_PER_PAGE
    start_index = current_page * COLLECTIONS_PER_PAGE
    end_index = min(start_index + COLLECTIONS_PER_PAGE, total_collections)
    collections_on_page = sorted_collection_names[start_index:end_index]

    keyboard = []
    for col_name in collections_on_page:
        ids = collections_data[col_name]
        total_in_col = sum(1 for cid, cdata in CARDS.items() if (cdata.get('collection') or "").strip() == col_name)
        owned_unique = len(ids)
        btn_text = f"{col_name} ({owned_unique}/{total_in_col})"
        short_token = COLLECTION_SHORT_MAP.get(col_name, col_name)
        callback_data_for_button = f"moba_view_col_{short_token}_0"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data_for_button)])

    pagination_buttons = []
    if current_page > 0:
        pagination_buttons.append(
            InlineKeyboardButton("< Назад", callback_data=f"moba_collections_page_{current_page - 1}"))
    if total_pages > 1:
        pagination_buttons.append(
            InlineKeyboardButton(f"{current_page + 1}/{total_pages}", callback_data="ignore_me"))
    if current_page < total_pages - 1:
        pagination_buttons.append(
            InlineKeyboardButton("Вперед >", callback_data=f"moba_collections_page_{current_page + 1}"))

    if pagination_buttons:
        keyboard.append(pagination_buttons)

    keyboard.append([InlineKeyboardButton("< Назад к картам", callback_data="moba_my_cards")])
    text = "❤️‍🔥 <b>Ваши коллекции</b>\n<blockquote>Выберите коллекцию для просмотра</blockquote>"
    if total_pages > 1:
        text += f"\n<i>Страница {current_page + 1} из {total_pages}</i>"

    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        try:
            await query.message.delete()
        except:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML)


async def moba_view_collection_cards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    prefix = "moba_view_col_"
    if not query.data.startswith(prefix):
        try:
            await query.answer("Неверный формат callback", show_alert=True)
        except Exception:
            pass
        return
    rest = query.data[len(prefix):]
    try:
        safe_enc, idx_str = rest.rsplit("_", 1)
        idx = int(idx_str)
    except Exception:
        safe_enc = rest
        idx = 0
    collection_name = decode_collection_short_token(safe_enc)
    norm_target = normalize_collection_name(collection_name)
    rows = await asyncio.to_thread(get_user_inventory, query.from_user.id)
    filtered = [r for r in rows if normalize_collection_name(r.get('collection')) == norm_target]
    if not filtered:
        try:
            await query.answer("У вас пока нет карт в этой коллекции.", show_alert=True)
        except Exception:
            pass
        return
    await _moba_send_filtered_card(query, context, filtered, idx, back_cb="moba_show_collections")


async def top_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = ""
    keyboard_buttons = []
    if query.data == "top_category_cards":
        text = "🏆 <b>Рейтинг коллекционеров</b>"
        keyboard_buttons = [
            [InlineKeyboardButton("✨ По очкам", callback_data="top_points"),
             InlineKeyboardButton("🃏 По количеству карт", callback_data="top_cards")],
            [InlineKeyboardButton("< Назад", callback_data="top_main")]]
    elif query.data == "top_category_game":
        text = "🏆 <b>Рейтинг игроков (Ранг)</b>"
        keyboard_buttons = [
            [InlineKeyboardButton("🌟 Топ сезона", callback_data="top_stars_season"),
             InlineKeyboardButton("🌍 За все время", callback_data="top_stars_all")],
            [InlineKeyboardButton("< Назад", callback_data="top_main")]]
    if not text:
        return
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        logger.warning(f"Failed to edit top_category_callback: {e}. Sending new message.")
        try:
            await context.bot.send_message(chat_id=query.from_user.id, text=text, reply_markup=reply_markup,
                                           parse_mode=ParseMode.HTML)
        except Exception as send_e:
            logger.error(f"Critical error in top_category_callback: {send_e}")


@check_menu_owner
async def moba_show_cards_by_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    parts = query.data.split("_")
    try:
        rarity = parts[4]  # LIMITED
        index = int(parts[5])  # 0
    except (IndexError, ValueError):
        rarity = "LIMITED"
        index = 0
    rows = await asyncio.to_thread(get_user_inventory, user_id)
    filtered = [r for r in rows if (r.get('rarity') or "").upper() == rarity.upper()]
    if not filtered:
        text = (
            f"🪬 <b>Карты редкости {rarity}</b>\n\n"
            f"<blockquote>У вас пока нет ни одной карты этой редкости.\n\n"
            f"Вы можете выбить их с помощью команды «<code>моба</code>» или "
            f"приобрести соответствующий набор в магазине «<code>/shop</code>»!</blockquote>")
        keyboard = [[InlineKeyboardButton("< Назад к картам", callback_data="moba_my_cards")]]
        try:
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        except Exception:
            try:
                await query.message.delete()
            except Exception:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML)
        return
    await _moba_send_filtered_card(query, context, filtered, index, back_cb="moba_my_cards")



def access_required(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        return await func(update, context, *args, **kwargs)

    return wrapper

#7.красиво форматирует время
async def format_duration(start_date_obj: datetime) -> str:
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


#1.создает и возвращает подключение к базе данных PostgreSQL
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Error as e:
        logger.error(f"Ошибка подключения к базе данных PostgreSQL: {e}", exc_info=True)
        raise

#2.инициализирует базу данных при старте: создает все таблицы (пользователи, инвентарь, браки, муты, евангелие) и добавляет недостающие колонки.
def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Создаем базовые таблицы пользователей

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
                season_reg_total INTEGER DEFAULT 0,
                season_reg_success INTEGER DEFAULT 0,
                premium_until TIMESTAMP WITH TIME ZONE,
                last_mobba_time DOUBLE PRECISION DEFAULT 0,
                last_reg_time DOUBLE PRECISION DEFAULT 0,
                protection_active INTEGER DEFAULT 0,
                luck_active INTEGER DEFAULT 0,
                pending_boosters INTEGER DEFAULT 0,
                bought_booster_today INTEGER DEFAULT 0,
                bought_luck_week INTEGER DEFAULT 0,
                bought_protection_week INTEGER DEFAULT 0,
                last_daily_reset TIMESTAMP WITH TIME ZONE,
                last_weekly_reset TIMESTAMP WITH TIME ZONE
            );
        """)

        cursor.execute("ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS season_reg_total INTEGER DEFAULT 0;")
        cursor.execute("ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS season_reg_success INTEGER DEFAULT 0;")

        
        cursor.execute("""
        ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS season_reg_total INTEGER DEFAULT 0;
        ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS season_reg_success INTEGER DEFAULT 0;
        
            CREATE TABLE IF NOT EXISTS moba_season_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                season_id TEXT,
                total_games INTEGER,
                wins INTEGER,
                final_rank TEXT
            );
        """)


        # Добавляем поля для отслеживания римских сезонов
        cursor.execute("""
            INSERT INTO system_settings (key, value) 
            VALUES ('roman_season_counter', '0')
            ON CONFLICT (key) DO NOTHING;
        """)
        cursor.execute("""
            INSERT INTO system_settings (key, value) 
            VALUES ('current_active_display_season_id', '1/2 SEASON')
            ON CONFLICT (key) DO NOTHING;
        """)
        cursor.execute("""
            INSERT INTO system_settings (key, value) 
            VALUES ('last_monthly_trigger_season', '')
            ON CONFLICT (key) DO NOTHING;
        """)

        conn.commit()
        logger.info("База данных успешно проинициализирована без дубликатов.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moba_season_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                season_id TEXT,
                total_games INTEGER,
                wins INTEGER,
                final_rank TEXT
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moba_inventory (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                card_id INTEGER,
                card_name TEXT,
                collection TEXT,
                rarity TEXT,
                bo INTEGER,
                points INTEGER,
                diamonds INTEGER
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS global_banned_users (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                banned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS moba_chat_activity (
                chat_id BIGINT,
                user_id BIGINT,
                last_activity TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, user_id)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pref_permissions (
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                PRIMARY KEY (chat_id, user_id)
            );
        """)

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

        # 2. Таблицы для Евангелие
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
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gospel_users_piety ON gospel_users (total_piety_score DESC);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gospel_users_prayers ON gospel_users (prayer_count DESC);")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gospel_chat_activity (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                prayer_count INTEGER DEFAULT 0,
                total_piety_score REAL DEFAULT 0,
                PRIMARY KEY (user_id, chat_id)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_gospel_chat_activity_chat_id ON gospel_chat_activity (chat_id);")

        # 3. Дополнительные таблицы (Браки, Лависки, Муты)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS laviska_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                data JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_laviska_users_username ON laviska_users (username);")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marriage_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                updated_at TIMESTAMP WITH TIME ZONE,
                last_message_in_group_at TIMESTAMP WITH TIME ZONE NULL
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_marriage_users_username ON marriage_users (LOWER(username));")

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

        # 4. Проверяем и добавляем новые колонки в moba_users, если их нет
        cursor.execute("""
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS luck_active INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS protection_active INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS last_daily_reset TIMESTAMP WITH TIME ZONE;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS last_weekly_reset TIMESTAMP WITH TIME ZONE;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS shop_last_reset TIMESTAMP WITH TIME ZONE DEFAULT NOW();
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS bought_booster_today INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS bought_luck_week INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS bought_protection_week INTEGER DEFAULT 0;
            ALTER TABLE moba_users ADD COLUMN IF NOT EXISTS pending_boosters INTEGER DEFAULT 0;
        """)

        # 5. Перенос старых данных (корректировка звезд)
        cursor.execute("""
            UPDATE moba_users 
            SET stars_all_time = stars 
            WHERE stars_all_time = 0 OR stars_all_time IS NULL;
        """)

        # 6. Триггер для обновления времени активности
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_last_activity_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
               NEW.last_activity = CURRENT_TIMESTAMP;
               RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_moba_chat_activity_timestamp') THEN
                    CREATE TRIGGER update_moba_chat_activity_timestamp
                    BEFORE UPDATE ON moba_chat_activity
                    FOR EACH ROW
                    EXECUTE FUNCTION update_last_activity_timestamp();
                END IF;
            END
            $$;
        """)

        conn.commit()
        logger.info("База данных успешно проинициализирована без дубликатов.")

    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


def register_moba_chat_activity(user_id, chat_id):
    if not chat_id or chat_id > 0:  # Не регистрируем в личке (chat_id > 0 для лички обычно)
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO moba_chat_activity (chat_id, user_id, last_activity)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (chat_id, user_id) DO UPDATE
                SET last_activity = CURRENT_TIMESTAMP;
            """
            cursor.execute(sql, (chat_id, user_id))
            conn.commit()
    finally:
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
        return {}
    finally:
        if conn:
            conn.close()


def update_piety_and_prayer_db_chat(user_id: int, chat_id: int, gained_piety: float):
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
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)
        order_clause = "gu.prayer_count DESC" if sort_by == 'prayers' else "gu.total_piety_score DESC"
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
                                 cursed_until: Optional[datetime], gospel_found: bool, first_name_cached: str,
                                 username_cached: Optional[str]):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''UPDATE gospel_users SET prayer_count = %s, total_piety_score = %s, last_prayer_time = %s, cursed_until = %s, gospel_found = %s, first_name_cached = %s, username_cached = %s WHERE user_id = %s''',
            (prayer_count, total_piety_score, last_prayer_time, cursed_until, gospel_found, first_name_cached,
             username_cached, user_id))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при обновлении данных пользователя {user_id} в gospel_game.db: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


async def find_gospel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)
    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)
    if user_data and user_data['gospel_found']:
        await update.message.reply_text("Вы уже нашли Евангелие. Отправляйтесь на службу!")
        return
    if not user_data:
        await asyncio.to_thread(add_gospel_game_user, user_id, user.first_name, user.username)
        user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)
        if not user_data:
            await update.message.reply_text("Ошибка инициализации данных. Попробуйте позже.")
            return
    last_prayer_time_obj = user_data['last_prayer_time'] if user_data.get('last_prayer_time') else None
    cursed_until_obj = user_data['cursed_until'] if user_data.get('cursed_until') else None
    await asyncio.to_thread(update_gospel_game_user_data, user_id,
                            user_data['prayer_count'],
                            user_data['total_piety_score'],
                            last_prayer_time_obj,
                            cursed_until_obj,
                            True,  # Gospel found
                            user.first_name, user.username)
    await update.message.reply_text(
        "Успех! ✨\nВаши реликвии у вас в руках!\n\nВам открылась возможность:\n⛩️ «мольба» — ходить на службу\n📜«Евангелие» — смотреть свои Евангелие\n📃 «Топ Евангелий» — и следить за вашими успехами!\nЖелаем удачи! 🍀")


async def prayer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    user_id = user.id
    chat_id = update.effective_chat.id
    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)
    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)
    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫")
        return
    current_time = datetime.now(timezone.utc)
    cursed_until = user_data['cursed_until']
    if cursed_until and current_time < cursed_until:
        remaining_time = cursed_until - current_time
        hours = int(remaining_time.total_seconds() // 3600)
        minutes = int((remaining_time.total_seconds() % 3600) // 60)
        await update.message.reply_text(
            f'У вас бесноватость 👹\n<blockquote>📿 Вы не сможете молиться еще {hours} часа(ов), {minutes} минут(ы). </blockquote> ',
            parse_mode=ParseMode.HTML)
        return
    is_friday = current_time.weekday() == 4
    is_early_morning = (21 <= current_time.hour < 1)
    if (is_friday or is_early_morning) and random.random() < 0.08:
        cursed_until_new = current_time + timedelta(hours=8)
        await asyncio.to_thread(update_curse_db, user_id, cursed_until_new)
        await update.message.reply_text(
            "У вас бесноватость 👹. Похоже вашу мольбу услышал кое-кто….другой\n<blockquote>📿 Вы не сможете молиться сутки.</blockquote>",
            parse_mode=ParseMode.HTML)
        return
    last_prayer_time = user_data['last_prayer_time']
    if last_prayer_time and current_time < last_prayer_time + timedelta(hours=1):
        remaining_time = (last_prayer_time + timedelta(hours=1)) - current_time
        minutes = int(remaining_time.total_seconds() // 60)
        seconds = int(remaining_time.total_seconds() % 60)
        await update.message.reply_text(
            f'...Похоже никто не слышит вашей мольбы\n<blockquote>📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы).</blockquote>',
            parse_mode=ParseMode.HTML)
        return
    gained_piety = round(random.uniform(1, 20) / 2, 1)
    await asyncio.to_thread(update_piety_and_prayer_db, user_id, gained_piety, current_time)
    if update.effective_chat.type in ['group', 'supergroup']:
        await asyncio.to_thread(update_piety_and_prayer_db_chat, user_id, chat_id, gained_piety)
    await update.message.reply_text(
        f'⛩️ Ваши мольбы были услышаны! \n<blockquote>✨ Набожность +{gained_piety}</blockquote>',
        parse_mode=ParseMode.HTML)


async def _get_leaderboard_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, view: str, scope: str,
                                   page: int = 1) -> Tuple[
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
        cached_first_name = row['first_name_cached']
        cached_username = row['username_cached']
        rank = start_index + rank_offset + 1
        display_text = cached_first_name or (f"@{cached_username}" if cached_username else f"ID: {uid}")
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
                    InlineKeyboardButton("< Назад", callback_data=f"gospel_top_{view}_scope_global_page_{page - 1}"))
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
    chat_id = update.effective_chat.id

    await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)
    user_data = await asyncio.to_thread(get_gospel_game_user_data, user_id)

    if not user_data or not user_data['gospel_found']:
        await update.message.reply_text(
            "⛩️ Для того чтоб просмотреть топ, вам нужно найти важные реликвии — книги Евангелие \n\n"
            "Возможно если вы взовете к помощи, вы обязательно ее получите \n\n"
            "📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫"
        )
        return
    scope = 'chat'
    if update.effective_chat.type == 'private':
        scope = 'global'
    message_text, reply_markup = await _get_leaderboard_message(context, chat_id, 'prayers', scope, 1)

    try:
        await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения топа Евангелий: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка при получении топа. Пожалуйста, попробуйте еще раз.")


async def check_and_award_achievements(update_or_user_id, context: ContextTypes.DEFAULT_TYPE, user_data: dict):
    if isinstance(update_or_user_id, Update):
        user_id = update_or_user_id.effective_user.id
    else:
        user_id = int(update_or_user_id)

    async def send_notification(text):
        if isinstance(update_or_user_id, Update) and update_or_user_id.message:
            try:
                await update_or_user_id.message.reply_text(text, parse_mode=ParseMode.HTML)
                return
            except Exception:
                pass
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
        await asyncio.to_thread(update_user_data, user_id, user_data)
        for text in newly_awarded:
            await send_notification(text)



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
        await update.message.reply_text(
            f"⏳ <b>Вы уже получали карту</b>\n<blockquote> Получить карту можно через {' '.join(parts)}</blockquote>",
            parse_mode=ParseMode.HTML)
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
            chosen_card_id = int(random.choice(list(owned_card_ids_set))) if owned_card_ids_set else random.choice(
                range(1, NUM_PHOTOS + 1))
            user_data["crystals"] += REPEAT_CRYSTALS_BONUS
            await update.message.reply_text(
                f"У вас уже есть все карточки! Вы потратили жетон, вам начислены {REPEAT_CRYSTALS_BONUS} 🧩 фрагментов. Следующую команду можно написать через 10 минут.")
    else:
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
        if not owned_card_ids_set:  # Проверяем, что это действительно первая карточка
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

    await check_and_award_achievements(update, context, user_data)

    await asyncio.to_thread(update_user_data, user_id, user_data)


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


async def show_love_is_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    is_command = bool(update.message)
    query = update.callback_query

    if query:
        await query.answer()

    username = update.effective_user.username or update.effective_user.first_name or str(user_id)
    user_data = await asyncio.to_thread(get_user_data, user_id, username)
    total_owned_cards = len(user_data.get("cards", {}))
    # first_card_iso = user_data.get("first_card_date") # Эта переменная не используется

    keyboard = [
        [InlineKeyboardButton(f"❤️‍🔥 Мои карты {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements"),
         InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")],
        [InlineKeyboardButton("< Назад в профиль", callback_data="back_to_moba_profile")]
        # Добавим кнопку назад для удобства
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"─────── ⋆⋅☆⋅⋆ ───────\n"
        f"<b>КОЛЛЕКЦИЯ «❤️‍🔥 LOVE IS…»</b>\n"  # Сделаем заголовок жирным
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🃏 Карты: {total_owned_cards}\n"
        f"🧧 Жетоны: {user_data.get('spins', 0)}\n"
        f"🧩 Фрагменты: {user_data.get('crystals', 0)}\n"
        f"─────── ⋆⋅☆⋅⋆ ───────\n"
    )

    try:
        if is_command or not query.message.photo:  # Проверяем, есть ли уже фото в сообщении для редактирования
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=open(COLLECTION_MENU_IMAGE_PATH, "rb"),
                caption=message_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
            if is_command and update.effective_message.text == "блокнот":  # Проверяем, что это была именно команда "блокнот"
                try:
                    await update.effective_message.delete()
                except Exception as del_e:
                    logger.warning(f"Не удалось удалить команду 'блокнот': {del_e}")
        else:
            await query.edit_message_media(
                media=InputMediaPhoto(media=open(COLLECTION_MENU_IMAGE_PATH, "rb"), caption=message_text,
                                      parse_mode=ParseMode.HTML),
                reply_markup=reply_markup
            )
    except BadRequest as e:
        logger.warning(
            f"show_love_is_menu: edit/send photo failed (likely no photo in original msg or new msg attempt): {e}. Sending text.",
            exc_info=True)
        if is_command:  # Если это команда, отправляем новое текстовое сообщение
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup,
                                           parse_mode=ParseMode.HTML)
            if update.effective_message.text == "блокнот":
                try:
                    await update.effective_message.delete()  # Удаляем команду, если она была
                except Exception:
                    pass
        else:  # Если это кнопка, пытаемся отредактировать сообщение текстом
            try:
                await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            except Exception as e_text:
                logger.warning(f"show_love_is_menu: edit_message_text fallback failed: {e_text}. Sending new text msg.",
                               exc_info=True)
                await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup,
                                               parse_mode=ParseMode.HTML)
    except FileNotFoundError as fnf:
        logger.error(f"show_love_is_menu: COLLECTION_MENU_IMAGE_PATH не найден: {fnf}", exc_info=True)
        # Отправляем текстовую версию, если изображение не найдено
        if is_command:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup,
                                           parse_mode=ParseMode.HTML)
            if update.effective_message.text == "блокнот":
                try:
                    await update.effective_message.delete()
                except Exception:
                    pass
        else:
            try:
                await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            except Exception:
                await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup,
                                               parse_mode=ParseMode.HTML)
    except Exception as unexpected:
        logger.exception(f"show_love_is_menu: непредвиденная ошибка: {unexpected}")
        # Аварийное уведомление
        try:
            await context.bot.send_message(chat_id=chat_id,
                                           text="Произошла ошибка при отображении коллекции. Попробуйте ещё раз.",
                                           parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("show_love_is_menu: не удалось отправить сообщение об ошибке.")


logger = logging.getLogger(__name__)


async def edit_to_love_is_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        # На всякий случай: если вызвали не как callback (маловероятно)
        return
    await query.answer()

    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name or str(user_id)

    # Получаем данные пользователя из БД (blocking) в отдельном потоке
    user_data = await asyncio.to_thread(get_user_data, user_id, username)
    total_owned_cards = len(user_data.get("cards", {}))
    # first_card_iso = user_data.get("first_card_date") # Эта переменная здесь не используется

    # Кнопки меню
    keyboard = [
        [InlineKeyboardButton(f"❤️‍🔥 Мои карты {total_owned_cards}/{NUM_PHOTOS}", callback_data="show_collection")],
        [InlineKeyboardButton("🌙 Достижения", callback_data="show_achievements"),
         InlineKeyboardButton("🧧 Жетоны", callback_data="buy_spins")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        f"─────── ⋆⋅☆⋅⋆ ───────\n"
        f"КОЛЛЕКЦИЯ «❤️‍🔥 LOVE IS…»\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🃏 Карты: {total_owned_cards}\n"
        f"🧧 Жетоны: {user_data.get('spins', 0)}\n"
        f"🧩 Фрагменты: {user_data.get('crystals', 0)}\n"
        f"─────── ⋆⋅☆⋅⋆ ───────\n"
    )

    try:
        with open(COLLECTION_MENU_IMAGE_PATH, "rb") as photo_file:
            await query.edit_message_media(
                media=InputMediaPhoto(media=photo_file, caption=message_text, parse_mode=ParseMode.HTML),
                reply_markup=reply_markup
            )
    except FileNotFoundError:
        logger.error(f"COLLECTION_MENU_IMAGE_PATH не найден: {COLLECTION_MENU_IMAGE_PATH}", exc_info=True)
        # Отправляем текстовую версию, если фото не найдено
        try:
            await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        except BadRequest:
            # Если не удалось отредактировать сообщение (например, старое сообщение удалено),
            # пытаемся отправить новое.
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=message_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        except Exception as send_e:
            logger.exception(f"edit_to_love_is_menu: не удалось отправить текстовое сообщение: {send_e}")
    except BadRequest as e:
        logger.warning(
            f"edit_to_love_is_menu: edit_message_media failed (BadRequest): {e}. Falling back to send_photo/send_message.",
            exc_info=True)
        # Если редактирование не удалось (например, сообщение не ваше, или тип медиа не позволяет),
        # пытаемся отправить новое фото или текстовое сообщение.
        try:
            with open(COLLECTION_MENU_IMAGE_PATH, "rb") as photo_file:
                await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=photo_file,
                    caption=message_text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
        except FileNotFoundError:
            logger.error(f"COLLECTION_MENU_IMAGE_PATH не найден для fallback: {COLLECTION_MENU_IMAGE_PATH}",
                         exc_info=True)
            await context.bot.send_message(chat_id=query.message.chat_id, text=message_text, reply_markup=reply_markup,
                                           parse_mode=ParseMode.HTML)
        except Exception as send_e:
            logger.exception(f"edit_to_love_is_menu: не удалось отправить fallback фото или сообщение: {send_e}")
    except Exception as unexpected:
        logger.exception(f"edit_to_love_is_menu: непредвиденная ошибка: {unexpected}")
        try:
            await context.bot.send_message(chat_id=query.message.chat_id,
                                           text="Произошла ошибка при возврате в меню коллекции. Попробуйте ещё раз.")
        except Exception:
            logger.exception("edit_to_love_is_menu: не удалось отправить сообщение об ошибке.")


logger = logging.getLogger(__name__)


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


async def unified_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)
    chat_url = GROUP_CHAT_INVITE_LINK if GROUP_CHAT_INVITE_LINK else f'https://t.me/{GROUP_USERNAME_PLAIN}'
    keyboard = [[InlineKeyboardButton(f'Чат 💬', url='https://t.me/MobileLegend_chat_mobla'),
                 InlineKeyboardButton('Добавить в группу', url='https://t.me/@ElytraMLbot?startgroup=join')],
                [InlineKeyboardButton('Обновления', url='https://teletype.in/@meonimaw/3Qzuw4zfbwL'),
                 InlineKeyboardButton('Команды ⚙️', callback_data='show_commands')], ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_name = user.username or user.first_name or 'друг'
    message_text = (
        f'<b>Привет {user_name}!</b>\n''<blockquote>Это развлекательный бот\nФункционал постоянно пополняется, следи за обновлениями!</blockquote>')
    try:
        if os.path.exists(NOTEBOOK_MENU_IMAGE_PATH):
            data = await asyncio.to_thread(lambda: open(privetstvie, "rb").read())
            bio = io.BytesIO(data)
            bio.name = os.path.basename(privetstvie)
            bio.seek(0)
            await update.effective_message.reply_photo(
                photo=bio,
                caption=message_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup)
        else:
            logger.error(f"Collection menu image not found: {privetstvie}")
            await update.effective_message.reply_text(
                message_text + "\n\n(Ошибка: фоновая картинка коллекции не найдена)",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup)
    except Exception as e:
        logger.exception(f"Error sending collection menu photo: {e}")
        await update.effective_message.reply_text(
            message_text + f"\n\n(Ошибка при отправке фоновой картинки: {e})",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup)


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
        await asyncio.to_thread(add_gospel_game_user, user.id, user.first_name, user.username)
        await asyncio.to_thread(update_gospel_game_user_cached_data, user.id, user.first_name, user.username)

        if message_text_lower == "блокнот":
            await show_love_is_menu(update, context)
            return

        if LAV_ISKA_REGEX.match(message_text_lower):
            await lav_iska(update, context)
            return

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



async def send_command_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    command_list = """⚙️ Список команд:
<blockquote>👾 MOBA
/account, аккаунт — профиль моблера; коллекция; сумка
/premium — покупка премиума
/name ник — установка ника 
123456789 (1234) — добавление айди в профиль 
моба — получение карточки
регнуть — сыграть катку
моба топ — рейтинг игроков в чате
моба топ вся — рейтинг игроков </blockquote>
<blockquote>❤️‍🔥Love is…
лав иска — получение вкладыша 
блокнот — коллекция;  обменник жетонов</blockquote>
<blockquote>⛩ EVANGELIE 
мольба — поход на службу
топ евангелий — топ игроков 
евангелие — просмотр успехов</blockquote>
"""
    query = update.callback_query
    if query:
        await query.answer()
        if query.message and getattr(query.message, "photo", None):
            try:
                await query.message.delete()
            except Exception:
                pass
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=command_list,
                parse_mode=ParseMode.HTML)
        else:
            try:
                await query.edit_message_text(command_list, parse_mode=ParseMode.HTML)
            except BadRequest as e:
                if "Message is not modified" in str(e):
                    return
                logger.warning(f"Failed to edit command list message: {e}. Sending new one.")
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=command_list,
                    parse_mode=ParseMode.HTML)
    else:
        await update.effective_message.reply_text(command_list, parse_mode=ParseMode.HTML)


@check_menu_owner
async def unified_button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    current_user_id = query.from_user.id
    current_user_first_name = query.from_user.first_name
    current_user_username = query.from_user.username
    # --- БЛОК БЕСПЛАТНЫХ ПОКУПОК ДЛЯ АДМИНИСТРАТОРА ---
    if data and data.startswith("admin_free_"):
        if current_user_id != ADMIN_ID:
            await query.answer("❌ Эта функция доступна только Создателю бота!", show_alert=True)
            return

        payload = data.replace("admin_free_", "")
        user = await asyncio.to_thread(get_moba_user, current_user_id)

        # 1. Бесплатные алмазы
        if payload.startswith("diamonds_"):
            try:
                amount = int(payload.split("_")[1])
                user["diamonds"] += amount
                await asyncio.to_thread(save_moba_user, user)
                await query.edit_message_text(
                    f"🎁 <b>Успешно зачислено!</b>\n\n"
                    f"Вы получили <b>{amount} 💎</b> за 0 ⭐️ (Привилегия Создателя).\n"
                    f"Ваш баланс: <b>{user['diamonds']} 💎</b>",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 В магазин", callback_data="back_to_shop")]]),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                await query.answer("Ошибка начисления", show_alert=True)

        # 2. Бесплатное БО
        elif payload.startswith("coins_"):
            try:
                amount = int(payload.split("_")[1])
                user["coins"] += amount
                await asyncio.to_thread(save_moba_user, user)
                await query.edit_message_text(
                    f"🎁 <b>Успешно зачислено!</b>\n\n"
                    f"Вы получили <b>{amount} 💰 БО</b> за 0 ⭐️ (Привилегия Создателя).\n"
                    f"Ваш баланс: <b>{user['coins']} БО</b>",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 В магазин", callback_data="back_to_shop")]]),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                await query.answer("Ошибка начисления", show_alert=True)

        # 3. Бесплатный премиум
        elif payload == "premium_30":
            current_time_utc = datetime.now(timezone.utc)
            if user.get("premium_until") and user["premium_until"] > current_time_utc:
                user["premium_until"] += timedelta(days=30)
            else:
                user["premium_until"] = current_time_utc + timedelta(days=30)

            await asyncio.to_thread(save_moba_user, user)
            await query.edit_message_text(
                "🚀 <b>Premium активирован бесплатно!</b>\n\n"
                "Срок вашего Premium статуса успешно продлен на 30 дней за 0 ⭐️.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛍 В магазин", callback_data="back_to_shop")]]),
                parse_mode=ParseMode.HTML
            )
        return
    # --- КОНЕЦ БЛОКА АДМИНИСТРАТОРА ---

    await asyncio.to_thread(update_gospel_game_user_cached_data, current_user_id, current_user_first_name,
                            current_user_username)
    if data and (
            data.startswith("buy_shop_") or data.startswith("do_buy_") or data == "back_to_shop" or data.startswith(
        "buy_pack_") or data.endswith("_item") or data == "shop_packs"):
        await query.answer()
        return


    if data == "back_to_moba_profile":
        return
    elif data == "show_love_is_menu":
        await show_love_is_menu(query, context)
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
                [InlineKeyboardButton("Вернуться в коллекцию", callback_data="back_to_main_collection")],]
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


async def _format_moba_top_section(context, rows: List[dict], category_label: str, position_message: str):
    lines = []
    for idx, row in enumerate(rows, start=1):
        uid = row.get('user_id')
        nickname = html.escape(row.get('nickname') or str(row.get('user_id')))
        val = row.get('val', 0)

        # Проверка на Луну
        moon_emoji = ""
        if uid and CHAT_ISSUE_USERNAME:
            is_in_issue_chat = await get_user_chat_membership_status(uid, CHAT_ISSUE_USERNAME, context)
            if is_in_issue_chat:
                moon_emoji = " 🌙"

        lines.append(f"<code>{idx}.</code> <b>{nickname}</b>{moon_emoji} — <b>{val}</b>")

    body = "\n".join(lines) if lines else "<i>Данные отсутствуют</i>"
    return f"🏆 <b>{category_label}</b>\n\n{body}\n\n{position_message}"


async def send_moba_top_data(update: Update, context: ContextTypes.DEFAULT_TYPE,
                             top_sections_data: Dict[str, Tuple[List[dict], str, str]],
                             additional_buttons: List[List[InlineKeyboardButton]] = None,
                             current_scope: str = "chat"):

    message_parts = []
    keyboard_rows = []

    for category_token, (rows, label, pos_message) in top_sections_data.items():
        message_parts.append(await _format_moba_top_section(context, rows, label, pos_message))
    full_message_text = "\n\n".join(message_parts)
    if "reg_leaderboard" in top_sections_data:
        keyboard_rows.append(
            [InlineKeyboardButton("📈 Топ по регнуть", callback_data="moba_top_reg_leaderboard_page_1")])

    if len(top_sections_data) > 1:
        cat_buttons = []
        if "cards" in top_sections_data and "points" in top_sections_data:
            cat_buttons.append(InlineKeyboardButton("🃏 Карты", callback_data="moba_top_chat_page_1"))
            cat_buttons.append(InlineKeyboardButton("💰 Очки", callback_data="moba_top_points_chat_page_1"))
            keyboard_rows.append(cat_buttons)
        if "season_stars" in top_sections_data and "all_stars" in top_sections_data:
            cat_buttons = []
            cat_buttons.append(InlineKeyboardButton("🌟 Сезон", callback_data="moba_top_season_page_1"))
            cat_buttons.append(InlineKeyboardButton("🌍 Все время", callback_data="moba_top_all_page_1"))
            keyboard_rows.append(cat_buttons)

    keyboard_rows.append([InlineKeyboardButton("< Назад", callback_data="top_main")])
    if additional_buttons:
        keyboard_rows.extend(additional_buttons)
    reply_markup = InlineKeyboardMarkup(keyboard_rows)
    if update.callback_query:
        await update.callback_query.edit_message_text(full_message_text, reply_markup=reply_markup,
                                                      parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(full_message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)


async def _get_moba_top_data_for_message(context, chat_id: int, scope: str, category: str, page: int = 1) -> Tuple[
    List[dict], str, str]:
    per_page = 10  # Топ-10
    offset = (page - 1) * per_page
    db_category = ""
    label = ""

    if category == "cards":
        db_category = "cards"
        label = "Топ по картам"
    elif category == "points":
        db_category = "points"
        label = "Топ по очкам"
    elif category == "season_stars":
        db_category = "stars_season"
        label = "Топ ранга (Сезон)"
    elif category == "all_stars":
        db_category = "stars_all"
        label = "Топ ранга (Все время)"
    elif category == "reg_leaderboard":
        # Этот случай будет обрабатываться отдельно, т.к. он имеет под-разделы
        pass  # Просто возвращаем пустые данные
    else:
        # По умолчанию, если ничего не указано, показываем топ по картам
        db_category = "cards"
        label = "Топ по картам"

    if db_category:
        rows = await asyncio.to_thread(get_moba_leaderboard_paged, db_category, per_page, offset)
        position_message = "Вы на {rank} месте."
        return rows, label, position_message
    return [], "", ""


async def handle_moba_top_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    txt = update.message.text.lower().strip()
    scope = 'chat'
    if txt in ("моба топ вся", "моба топвся"):
        scope = 'global'
    await handle_moba_top_display(update, context, scope=scope, page=1)


async def moba_top_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if not data.startswith("moba_top_switch_"):
        return

    await query.answer()
    parts = data.split("_")
    section = parts[3]  # reg или cards
    is_global = parts[4] == "glob"
    await render_moba_top(update, context, is_global=is_global, section="cards" if section == "cards" else "reg")


async def handle_reg_leaderboard_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id if query.message and query.message.chat else GROUP_CHAT_ID  # Получаем chat_id

    # Получаем данные для двух секций
    season_stars_data = await _get_moba_top_data_for_message(context, chat_id, "chat", "season_stars")
    all_stars_data = await _get_moba_top_data_for_message(context, chat_id, "chat", "all_stars")

    sections_to_display = {
        "season_stars": season_stars_data,
        "all_stars": all_stars_data
    }

    # Кнопка "Назад"
    additional_buttons = [[InlineKeyboardButton("< Назад", callback_data="moba_top_chat_page_1")]]

    await send_moba_top_data(update, context, sections_to_display, additional_buttons=additional_buttons,
                             current_scope="chat")

async def all_season_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    try:
        await query.answer()
    except Exception:
        pass

    user_id = query.from_user.id

    try:
        user = await asyncio.to_thread(get_moba_user, user_id)

        conn = get_db_connection()
        try:
            cursor = conn.cursor(cursor_factory=DictCursor)
            cursor.execute("""
                SELECT season_id, total_games, wins, final_rank
                FROM moba_season_history
                WHERE user_id = %s
                ORDER BY id DESC
            """, (user_id,))
            history = cursor.fetchall()
        finally:
            conn.close()

        total_games = int(user.get("reg_total") or 0)
        total_wins = int(user.get("reg_success") or 0)
        winrate = total_wins / total_games * 100 if total_games else 0

        text = (
            "📊 <b>Вся информация</b>\n\n"
            "📈 <b>За всё время:</b>\n"
            f"• Игр: {total_games}\n"
            f"• Побед: {total_wins}\n"
            f"• Winrate: {winrate:.1f}%\n\n"
            "📜 <b>История сезонов:</b>\n"
        )

        if history:
            for row in history:
                games = int(row["total_games"] or 0)
                wins = int(row["wins"] or 0)
                wr = wins / games * 100 if games else 0
                text += (
                    f"• {row['season_id']}: "
                    f"{games} игр, побед {wins}, "
                    f"Winrate {wr:.1f}%, "
                    f"Ранг: {row['final_rank'] or '—'}\n"
                )
        else:
            text += "История прошлых сезонов пока пуста.\n"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "< Назад в профиль",
                callback_data="back_to_moba_profile"
            )]
        ])

        # Для сообщения с фотографией нельзя использовать edit_message_text
        if query.message and query.message.photo:
            await query.message.delete()
            new_message = await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            new_message = await query.edit_message_text(
                text=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )

        NOTEBOOK_MENU_OWNERSHIP[
            (new_message.chat_id, new_message.message_id)
        ] = user_id

    except Exception as e:
        logger.exception("Ошибка кнопки all_season_info")

        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"❌ Ошибка открытия статистики:\n<code>{html.escape(str(e))}</code>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f'Update "{update}" вызвал ошибку "{context.error}"', exc_info=True)
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Упс, произошла ошибка. Возможно бот сейчас на тех обслуживании, если проблема не устраняется спустя время сообщи об этом админу чата",
                parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке пользователю: {e}", exc_info=True)


def main():
    init_db()
    application = ApplicationBuilder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", unified_start_command))
    application.add_handler(CommandHandler("name", set_name))
    application.add_handler(CommandHandler("shop", shop))
    application.add_handler(CommandHandler("premium", premium_info))
    application.add_handler(CommandHandler("reset_all_cards", reset_all_cards_command))
    application.add_handler(CommandHandler("account", profile))
    application.add_handler(CommandHandler("reset_season", manual_reset_season_command))
    application.add_handler(CommandHandler("grant_prem", grant_premium_command))  # <-- ВСТАВИТЬ ЭТУ СТРОКУ
    # Привязываем команду "блокнот" к show_love_is_menu
    application.add_handler(CallbackQueryHandler(handle_all_season_info, pattern=r"^all_season_info$"), group=0)
    application.add_handler(CallbackQueryHandler(shop_callback_handler, pattern="^(buy_shop_|do_buy_|back_to_shop|booster_item|luck_item|protect_item|diamond_item|coins_item|shop_packs|confirm_buy_|buy_pack_|confirm_pack_|do_buy_pack_)"))
    application.add_handler(CallbackQueryHandler(show_love_is_menu, pattern="^show_love_is_menu$"))
    application.add_handler(CallbackQueryHandler(delete_message_callback, pattern="^delete_message$"))
    application.add_handler(CallbackQueryHandler(moba_top_callback, pattern=r"^moba_top_(chat|global)_page_\d+$"))
    application.add_handler(CallbackQueryHandler(profile, pattern="^back_to_moba_profile$"))
    application.add_handler(CallbackQueryHandler(moba_top_callback_handler, pattern="^moba_top_switch_"))
    application.add_handler(CallbackQueryHandler(moba_top_callback, pattern=r"^moba_top_"))
    application.add_handler(CallbackQueryHandler(top_category_callback, pattern="^top_category_"))
    application.add_handler(CallbackQueryHandler(show_specific_top, pattern="^top_(points|cards|stars_season|stars_all)$"))
    application.add_handler(CallbackQueryHandler(handle_moba_my_cards, pattern="^moba_my_cards$"))
    application.add_handler(CallbackQueryHandler(moba_show_cards_all, pattern="^moba_show_cards_all_"))
    application.add_handler(CallbackQueryHandler(handle_bag, pattern="^bag$"))
    application.add_handler(CallbackQueryHandler(handle_moba_collections, pattern="^moba_show_collections$"))
    application.add_handler(CallbackQueryHandler(moba_view_collection_cards, pattern="^moba_view_col_"))
    application.add_handler(CallbackQueryHandler(handle_moba_collections, pattern="^moba_collections_page_"))
    application.add_handler(CallbackQueryHandler(handle_moba_collections, pattern="^moba_collections$"))
    application.add_handler(CallbackQueryHandler(confirm_id_callback, pattern="^confirm_add_id$"))
    application.add_handler(CallbackQueryHandler(cancel_id_callback, pattern="^cancel_add_id$"))
    application.add_handler(CallbackQueryHandler(top_category_callback, pattern="^top_category_"))
    application.add_handler(CallbackQueryHandler(edit_to_love_is_menu, pattern="^back_to_main_collection$"))
    application.add_handler(CallbackQueryHandler(send_command_list, pattern="^show_commands$"))
    application.add_handler(CallbackQueryHandler(send_collection_card, pattern="^view_card_"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^nav_card_"))  # Для навигации по картам
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^show_achievements$"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^buy_spins$"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^exchange_crystals_for_spin$"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^send_papa$"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^gospel_top_"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^ignore_page_num$"))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler, pattern="^delete_message$"))
    application.add_handler(CallbackQueryHandler(handle_moba_my_cards, pattern="^moba_my_cards$"))
    application.add_handler(CallbackQueryHandler(moba_show_cards_by_rarity, pattern="^moba_show_cards_rarity_"))
    application.add_handler(CallbackQueryHandler(handle_moba_my_cards, pattern="^moba_my_cards$"))
    application.add_handler(CallbackQueryHandler(shop_callback_handler))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler,pattern=r"^(?!all_season_info$).+"),group=1)


    application.add_handler(MessageHandler(filters.Regex(r"(?i)^аккаунт$"), profile))
    application.add_handler(MessageHandler(filters.SuccessfulPayment(), successful_payment_callback))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r"(?i)^моба топ( вся)?$")), handle_moba_top_message))
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^регнуть$"), regnut_handler))
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^моба$"), mobba_handler))
    application.add_handler(MessageHandler(filters.Regex(r"^\d{9}\s\(\d{4}\)$"), id_detection_handler))
    application.add_handler(MessageHandler(filters.Regex(re.compile(r"^(мои карты)$", re.IGNORECASE)), handle_moba_my_cards))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unified_text_message_handler))
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(CallbackQueryHandler(unified_button_callback_handler))

    # 6. Обработчик ошибок
    application.add_error_handler(error_handler)

    application.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
