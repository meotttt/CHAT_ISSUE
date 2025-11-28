import asyncio
import logging
import random
import re
import sqlite3
from datetime import datetime, timedelta

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    ChatPermissions,
)
from telegram.helpers import mention_html
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    filters,
)

# ========== Конфиг ==========
ADMIN_ID = 2123680656  # если надо, можешь менять
TOKEN = "8086930010:AAH1elkRFf6497_Ls9-XnZrUeIh_rWyMF5c"  # Замените на ваш токен

# ========== Логирование ==========
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== БД: общая для банов/мутов ==========
MUTED_DB = "baza.sql"        # для muted_users и banned_users (как в твоём старом коде)
GOSPEL_DB = "gospel_game.db"  # для игры


def init_databases():
    # baza.sql: muted_users, banned_users
    conn = sqlite3.connect(MUTED_DB, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS muted_users (
               user_id INTEGER,
               chat_id INTEGER,
               mute_until INTEGER,
               PRIMARY KEY(user_id, chat_id)
           )"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS banned_users (
               user_id INTEGER,
               chat_id INTEGER,
               PRIMARY KEY(user_id, chat_id)
           )"""
    )
    conn.commit()
    conn.close()

    # gospel_game.db: users
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS users (
               user_id INTEGER PRIMARY KEY,
               prayer_count INTEGER DEFAULT 0,
               total_piety_score REAL DEFAULT 0,
               last_prayer_time TEXT,
               initialized INTEGER DEFAULT 0,
               possession_of_demon TEXT
           )"""
    )
    # для игрового аккаунта +акк
    cur.execute(
        """CREATE TABLE IF NOT EXISTS game_users (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               user_id INTEGER UNIQUE,
               name TEXT UNIQUE,
               password TEXT
           )"""
    )
    conn.commit()
    conn.close()


# ========== Утилиты для БД ==========
def insert_mute(user_id: int, chat_id: int, until_ts: int):
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO muted_users (user_id, chat_id, mute_until) VALUES (?, ?, ?)",
        (user_id, chat_id, until_ts),
    )
    conn.commit()
    conn.close()


def remove_mute(user_id: int, chat_id: int):
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute("DELETE FROM muted_users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
    conn.commit()
    conn.close()


def get_expired_mutes():
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    now_ts = int(datetime.now().timestamp())
    cur.execute("SELECT user_id, chat_id FROM muted_users WHERE mute_until <= ?", (now_ts,))
    rows = cur.fetchall()
    conn.close()
    return rows


def insert_ban(user_id: int, chat_id: int):
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO banned_users (user_id, chat_id) VALUES (?, ?)", (user_id, chat_id))
    conn.commit()
    conn.close()


def remove_ban(user_id: int, chat_id: int):
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute("DELETE FROM banned_users WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
    conn.commit()
    conn.close()


# ========== Функции для игры (gospel_game.db) ==========
def register_user_in_game(user_id: int):
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if cur.fetchone() is None:
        cur.execute("INSERT INTO users (user_id, initialized) VALUES (?, ?)", (user_id, 0))
        conn.commit()
    conn.close()


def set_initialized(user_id: int, value: int = 1):
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute("UPDATE users SET initialized = ? WHERE user_id = ?", (value, user_id))
    conn.commit()
    conn.close()


def get_user_data_game(user_id: int):
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute("SELECT prayer_count, total_piety_score, last_prayer_time, initialized, possession_of_demon FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def update_user_game(user_id: int, prayer_count: int, total_piety_score: float, last_prayer_time: str or None, possession_of_demon: str or None):
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute(
        """UPDATE users SET prayer_count = ?, total_piety_score = ?, last_prayer_time = ?, possession_of_demon = ? WHERE user_id = ?""",
        (prayer_count, total_piety_score, last_prayer_time, possession_of_demon, user_id),
    )
    conn.commit()
    conn.close()


# ========== Разбор длительности мута ==========
def parse_duration(tokens):
    """
    Разбирает список токенов типа ['1', 'ч', '30', 'мин'] или ['1h', '30m'] и возвращает секунды.
    Если не получилось — возвращает None.
    """
    text = " ".join(tokens).lower()
    # Поддержка форматов: "1h 30m", "1ч 30м", "1 час 30 минут", "1 30" (некорректный)
    seconds = 0
    # найдем все пары (число + единица) или единицы в формате 1h/30m
    pattern = re.compile(r"(\d+)\s*(час(?:ов)?|часа|ч|h|минут(?:ы)?|минуту|мин|m|s|сек(?:унд)?|секунд(?:ы)?)", re.IGNORECASE)
    for m in pattern.finditer(text):
        n = int(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith(("час", "ч", "h")):
            seconds += n * 3600
        elif unit.startswith(("мин", "m")):
            seconds += n * 60
        elif unit.startswith(("с", "сек")):
            seconds += n
    # Также поддерживаем формат "1h" и "30m" без пробела
    compact = re.findall(r"(\d+)(h|m|s)", text)
    for n, u in compact:
        n = int(n)
        if u == "h":
            seconds += n * 3600
        elif u == "m":
            seconds += n * 60
        elif u == "s":
            seconds += n
    return seconds if seconds > 0 else None


# ========== Хендлеры бота ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton('Вступить в чат 💬', url='https://t.me/CHAT_ISSUE')],
        [InlineKeyboardButton('Новогоднее голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
        [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user = update.effective_user
    name = user.username or user.first_name or 'друг'
    await update.message.reply_text(
        f'Привет, {name}! 🪐\nЭто бот чата 𝙄𝙎𝙎𝙐𝙀 \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!',
        reply_markup=reply_markup,
    )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    await query.message.reply_text(
        'Добро пожаловать в мир "Евангелия" — интерактивной игры бота ISSUE! 🪐\n\n▎Что вас ждет в "Евангелии"? \n\n1. ⛩️ Хождение на службу — Молитвы: Каждый раз, когда вы молитесь, вы не просто выполняете рутинное действие — вы получаете повышения своей набожности\n\n2. ✨ Система Набожности: Ваши молитвы влияют на вашу духовную силу. Чем больше вы молитесь, тем выше ваша набожность. Станьте одним из самых набожных игроков!\n\n3. 📃 Соревнования и Достижения: Вы можете видеть, кто из игроков находится на вершине таблицы лидеров! Сравните свои достижения с друзьями и стремитесь занять первое место в рейтингах молитв и набожности.\n\n4. 👹 Неожиданные Повороты: Будьте готовы к неожиданным событиям! У вас есть шанс столкнуться с "бесноватостью".\n\nПоговаривают что стоит молиться аккуратнее с 00:00 до 04:00 и быть предельно осторожным в пятницу!\n\n─────── ⋆⋅☆⋅⋆ ───────\n\n⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие\n\nВозможно если вы взовете к помощи, вы обязательно ее получите\n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫'
    )


# ---------- МУТ ----------
async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.chat.type not in ['group', 'supergroup']:
        await msg.reply_text("Эта команда работает только в группе.")
        return

    # Проверка прав вызывающего
    member = await context.bot.get_chat_member(msg.chat.id, msg.from_user.id)
    if member.status not in ['administrator', 'creator']:
        await msg.reply_text("У вас нет прав для выполнения этой команды.")
        return

    # Должен быть ответ на сообщение
    if not msg.reply_to_message:
        await msg.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого вы хотите замучить.")
        return

    target = msg.reply_to_message.from_user
    chat_id = msg.chat.id
    # Парсим длительность
    parts = msg.text.split()[1:]  # все после "мут"
    duration = parse_duration(parts) if parts else None
    if duration is None:
        duration = 3600  # по умолчанию 1 час

    until_dt = datetime.now() + timedelta(seconds=duration)
    until_ts = int(until_dt.timestamp())

    # Ограничиваем
    permissions = ChatPermissions(
        can_send_messages=False,
        can_send_media_messages=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False,
        can_change_info=False,
        can_invite_users=False,
        can_pin_messages=False,
    )
    try:
        await context.bot.restrict_chat_member(chat_id, target.id, permissions=permissions, until_date=until_dt)
    except Exception as e:
        logger.exception("Ошибка при наложении мута")
        await msg.reply_text("Не удалось замутить пользователя (проверьте права бота).")
        return

    insert_mute(target.id, chat_id, until_ts)
    hours = duration // 3600
    minutes = (duration % 3600) // 60
    await msg.reply_html(f"Пользователь {mention_html(target.id, target.first_name)} замучен на {hours} часов и {minutes} минут.")


# Функция размутить (команда)
async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.chat.type not in ['group', 'supergroup']:
        await msg.reply_text("Эта команда работает только в группе.")
        return

    member = await context.bot.get_chat_member(msg.chat.id, msg.from_user.id)
    if member.status not in ['administrator', 'creator']:
        await msg.reply_text("У вас нет прав для выполнения этой команды.")
        return

    if not msg.reply_to_message:
        await msg.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого вы хотите размучить.")
        return

    target = msg.reply_to_message.from_user
    chat_id = msg.chat.id
    permissions = ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_change_info=False,
        can_invite_users=True,
        can_pin_messages=True,
    )
    try:
        await context.bot.restrict_chat_member(chat_id, target.id, permissions=permissions)
    except Exception as e:
        logger.exception("Ошибка при размуте")
        await msg.reply_text("Не удалось размутить пользователя (проверьте права бота).")
        return

    remove_mute(target.id, chat_id)
    await msg.reply_html(f"Пользователь {mention_html(target.id, target.first_name)} размучен.")


# ---------- БАН ----------
async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.chat.type not in ['group', 'supergroup']:
        await msg.reply_text("Эта команда работает только в группе.")
        return
    member = await context.bot.get_chat_member(msg.chat.id, msg.from_user.id)
    if member.status not in ['administrator', 'creator']:
        await msg.reply_text("У вас нет прав для выполнения этой команды.")
        return
    if not msg.reply_to_message:
        await msg.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого вы хотите забанить.")
        return
    target = msg.reply_to_message.from_user
    chat_id = msg.chat.id
    try:
        await context.bot.ban_chat_member(chat_id, target.id)
    except Exception as e:
        logger.exception("Ошибка при бане")
        await msg.reply_text("Не удалось забанить пользователя (проверьте права бота).")
        return
    insert_ban(target.id, chat_id)
    await msg.reply_html(f"Пользователь {mention_html(target.id, target.first_name)} ЗАБАНЕН")


# ---------- РАЗБАН ----------
async def cmd_unban_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.chat.type not in ['group', 'supergroup']:
        await msg.reply_text("Эта команда работает только в группе.")
        return
    member = await context.bot.get_chat_member(msg.chat.id, msg.from_user.id)
    if member.status not in ['administrator', 'creator']:
        await msg.reply_text("У вас нет прав для выполнения этой команды.")
        return
    if not msg.reply_to_message:
        await msg.reply_text("Пожалуйста, ответьте на сообщение пользователя, которого вы хотите разбанить.")
        return

    target = msg.reply_to_message.from_user
    chat_id = msg.chat.id

    # Проверяем в БД
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute("SELECT * FROM banned_users WHERE user_id = ? AND chat_id = ?", (target.id, chat_id))
    banned_user = cur.fetchone()
    conn.close()

    if banned_user:
        try:
            await context.bot.unban_chat_member(chat_id, target.id)
        except Exception as e:
            logger.exception("Ошибка при разбане")
            await msg.reply_text("Не удалось разбанить пользователя (проверьте права бота).")
            return
        remove_ban(target.id, chat_id)
        await msg.reply_html(f"Пользователь {mention_html(target.id, target.first_name)} РАЗБАНЕН и может снова присоединиться к группе")
        try:
            invite_link = await context.bot.export_chat_invite_link(chat_id)
            await context.bot.send_message(target.id, f"Вы были разблокированы в чате {msg.chat.title}! Мы рады видеть вас снова! Присоединяйтесь по ссылке: {invite_link}")
        except Exception:
            # не обязательно, если нельзя отправить личное сообщение
            pass
    else:
        await msg.reply_text("Этот пользователь не был забанен.")


# ---------- Реакция на фото ----------
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("нихуевое фото братан")


# ---------- Информационные команды и +акк (регистрация в игре) ----------
AKK_NAME, AKK_PASS = range(2)


async def cmd_issuе_word(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Обработка слова "иссуе" (в старом коде было и в общих сообщениях)
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton('Вступить в чат 💬', url='https://t.me/CHAT_ISSUE')],
            [InlineKeyboardButton('Новогоднее голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
            [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa')],
        ]
    )
    await update.message.reply_text(f'Привет, {update.message.from_user.username}! 🪐\nЭто бот чата 𝙄𝙎𝙎𝙐𝙀 \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!', reply_markup=markup)


async def cmd_myinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f'Ваш ID: {update.message.from_user.id}')


async def cmd_iss_belka(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Ожидается, что файл qq.jpg присутствует в рабочей папке
    try:
        with open('qq.jpg', 'rb') as f:
            await context.bot.send_photo(update.message.chat.id, f, 'Вот твоя белочка!')
    except FileNotFoundError:
        await update.message.reply_text('Файл qq.jpg не найден на сервере.')


# ---------- +акк: разговор для создания аккаунта в game_users ----------
async def start_add_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Добавьте свой аккаунт в игру evangelie \nВведите свой будущий ник:')
    return AKK_NAME


async def akk_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    user_id = update.message.from_user.id
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute('SELECT * FROM game_users WHERE name = ?', (name,))
    if cur.fetchone():
        await update.message.reply_text('Этот ник уже занят. Пожалуйста, выберите другой.')
        conn.close()
        return AKK_NAME  # повторим ввод
    # временно сохраняем ник в user_data
    context.user_data['new_game_name'] = name
    await update.message.reply_text('Введите пароль для аккаунта:')
    conn.close()
    return AKK_PASS


async def akk_get_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    user_id = update.message.from_user.id
    name = context.user_data.get('new_game_name')
    if not name:
        await update.message.reply_text('Что-то пошло не так. Попробуйте заново: +акк')
        return ConversationHandler.END
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    try:
        cur.execute('INSERT INTO game_users (user_id, name, password) VALUES (?, ?, ?)', (user_id, name, password))
        conn.commit()
    except sqlite3.IntegrityError:
        await update.message.reply_text('Не удалось создать аккаунт (ник занят).')
        conn.close()
        return ConversationHandler.END
    conn.close()
    await update.message.reply_text('Твой аккаунт успешно добавлен в игру!')
    # можно добавить кнопку "Все игроки"
    await update.message.reply_text('Нажми /players чтобы увидеть игроков (если нужно).')
    return ConversationHandler.END


async def cmd_list_players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(MUTED_DB)
    cur = conn.cursor()
    cur.execute('SELECT name FROM game_users')
    rows = cur.fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text('Нет игроков.')
        return
    info = "Игроки:\n" + "\n".join([f"- {r[0]}" for r in rows])
    await update.message.reply_text(info)


# ---------- ИГРА: найти евангелие, молитва, евангелие, топ евангелий ----------
async def cmd_find_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    register_user_in_game(user_id)
    set_initialized(user_id, 1)
    await update.message.reply_text("Успех! ✨\nВаши реликвии у вас в руках!\n\nВам открылась возможность:\n⛩️ «мольба» — ходить на службу\n📜«Евангелие» — смотреть свои Евангелие\n📃 «Топ Евангелий» — и следить за вашими успехами!\nЖелаем удачи! 🍀")


async def cmd_prayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    register_user_in_game(user_id)
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute('SELECT initialized, possession_of_demon, last_prayer_time, prayer_count, total_piety_score FROM users WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    if not row:
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫")
        conn.close()
        return

    initialized, possession_of_demon, last_prayer_time_str, prayer_count, total_piety_score = row

    if initialized == 0:
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫")
        conn.close()
        return

    now = datetime.now()

    # проверяем possession_of_demon
    if possession_of_demon:
        try:
            demon_dt = datetime.strptime(possession_of_demon, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                demon_dt = datetime.strptime(possession_of_demon, "%Y-%m-%d %H:%M:%S")
            except Exception:
                demon_dt = None
        if demon_dt and demon_dt > now:
            remaining = demon_dt - now
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            await update.message.reply_text(f'У вас бесноватость 👹\n📿 Вы не сможете молится еще {hours} часа(ов), {minutes} минут(ы) ')
            conn.close()
            return

    # особая логика генерации "бесноватости"
    # как в старом коде: проверяем пятницу (weekday==4) и часы 0-3, шанс 0.1
    if now.weekday() == 4 and (0 <= now.hour < 4):
        if random.random() < 0.1:
            possession_until = now + timedelta(days=1)
            cur.execute("UPDATE users SET possession_of_demon = ? WHERE user_id = ?", (possession_until.strftime("%Y-%m-%d %H:%M:%S.%f"), user_id))
            conn.commit()
            await update.message.reply_text("У вас бесноватость 👹\nПохоже вашу мольбу услышал кое-кто….другой\n\n📿 Вы не сможете молиться сутки")
            conn.close()
            return

    # получаем время последней молитвы
    last_prayer = None
    if last_prayer_time_str:
        try:
            last_prayer = datetime.strptime(last_prayer_time_str, "%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            try:
                last_prayer = datetime.strptime(last_prayer_time_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                last_prayer = None

    if last_prayer and now < last_prayer + timedelta(hours=1):
        remaining = (last_prayer + timedelta(hours=1)) - now
        remaining_seconds = int(remaining.total_seconds())
        minutes = remaining_seconds // 60
        seconds = remaining_seconds % 60
        await update.message.reply_text(f'…..Похоже никто не слышит вашей мольбы\n📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы)')
        conn.close()
        return

    # генерируем набожность
    piety_score = round(random.uniform(1, 20) / 2, 1)  # 1..10 с шагом 0.5
    prayer_count = (prayer_count or 0) + 1
    total_piety_score = (total_piety_score or 0) + piety_score
    cur.execute("UPDATE users SET last_prayer_time = ?, prayer_count = ?, total_piety_score = ? WHERE user_id = ?",
                (now.strftime("%Y-%m-%d %H:%M:%S.%f"), prayer_count, total_piety_score, user_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(f'⛩️ Ваши мольбы были услышаны! \n✨ Набожность +{piety_score}\nНа следующую службу можно будет выйти через час 📿')


async def cmd_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    register_user_in_game(user_id)
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute('SELECT initialized FROM users WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    if not row or row[0] == 0:
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        conn.close()
        return
    cur.execute('SELECT prayer_count, total_piety_score FROM users WHERE user_id = ?', (user_id,))
    data = cur.fetchone()
    conn.close()
    if data:
        prayer_count, total_piety_score = data
        await update.message.reply_text(f'📜 Ваше евангелие:\n\nМолитвы — {prayer_count}📿\nНабожность — {total_piety_score:.1f} ✨')
    else:
        await update.message.reply_text('Пользователь не найден.')


async def cmd_top_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    register_user_in_game(user_id)
    conn = sqlite3.connect(GOSPEL_DB)
    cur = conn.cursor()
    cur.execute('SELECT initialized FROM users WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    if not row or row[0] == 0:
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        conn.close()
        return
    try:
        cur.execute('SELECT user_id, prayer_count FROM users ORDER BY prayer_count DESC')
        prayer_leaderboard = cur.fetchall()
        cur.execute('SELECT user_id, total_piety_score FROM users ORDER BY total_piety_score DESC')
        piety_leaderboard = cur.fetchall()
    except sqlite3.Error as e:
        await update.message.reply_text(f'Ошибка при доступе к базе данных: {e}')
        conn.close()
        return
    conn.close()

    leaderboard_msg = "Топ Евангелий:\n⛩️ Услышанные молитвы:\n"
    # Чтобы не падать, получаем имя пользователя через get_chat, но оборачиваем в try
    for rank, (uid, count) in enumerate(prayer_leaderboard, start=1):
        try:
            user = await context.bot.get_chat(uid)
            name = user.first_name or str(uid)
        except Exception:
            name = str(uid)
        leaderboard_msg += f"{rank}.  {name}: {count} молитв\n"

    leaderboard_msg += "\n✨<b>Набожность:</b>\n"
    for rank, (uid, score) in enumerate(piety_leaderboard, start=1):
        try:
            user = await context.bot.get_chat(uid)
            name = user.first_name or str(uid)
        except Exception:
            name = str(uid)
        leaderboard_msg += f"{rank}.  {name}: {score:.1f} набожности\n"
    await update.message.reply_text(leaderboard_msg, parse_mode='HTML')


# ---------- Универсальный обработчик текстовых сообщений ----------
async def catch_all_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.lower().strip()

    # команды-перехватчики
    if text.startswith('мут'):
        await cmd_mute(update, context)
        return
    if text == 'размут':
        await cmd_unmute(update, context)
        return
    if text.startswith('исразбан'):
        await cmd_unban_custom(update, context)
        return
    # слова как в старом коде
    if text == 'иссуе':
        await cmd_issuе_word(update, context)
        return
    if text == 'моя инфа':
        await cmd_myinfo(update, context)
        return
    if text == 'исс белку':
        await cmd_iss_belka(update, context)
        return
    if text == '+акк':
        return await start_add_account(update, context)
    # Игра:
    if 'найти евангелие' in text:
        await cmd_find_gospel(update, context)
        return
    if 'мольба' in text or 'молитва' in text:
        await cmd_prayer(update, context)
        return
    if text == 'евангелие':
        await cmd_gospel(update, context)
        return
    if 'топ евангелий' in text:
        await cmd_top_gospel(update, context)
        return


# ========== Фоновая задача для автоматического размутывания ==========
async def unmute_monitor(app):
    while True:
        try:
            expired = get_expired_mutes()
            for user_id, chat_id in expired:
                try:
                    permissions = ChatPermissions(
                        can_send_messages=True,
                        can_send_media_messages=True,
                        can_send_polls=True,
                        can_send_other_messages=True,
                        can_add_web_page_previews=True,
                        can_change_info=False,
                        can_invite_users=True,
                        can_pin_messages=True,
                    )
                    await app.bot.restrict_chat_member(chat_id, user_id, permissions=permissions)
                    remove_mute(user_id, chat_id)
                    try:
                        await app.bot.send_message(chat_id, f"Пользователь {user_id} был размучен автоматически.")
                    except Exception:
                        pass
                except Exception:
                    logger.exception("Не удалось автоматически размутить пользователя")
        except Exception:
            logger.exception("Ошибка в unmute_monitor")
        await asyncio.sleep(30)  # проверяем каждые 30 секунд


# ========== Основная точка запуска ==========
def main():
    init_databases()
    app = ApplicationBuilder().token(TOKEN).build()

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback_handler, pattern="^send_papa$"))

    # Фото
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    # Универсальный текстовый обработчик (включает игровые команды и модерацию по ключевым словам)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, catch_all_text))

    # Conversation для +акк
    conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r'^\+акк$'), start_add_account)],
        states={
            AKK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, akk_get_name)],
            AKK_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, akk_get_pass)],
        },
        fallbacks=[],
        per_user=True,
    )
    app.add_handler(conv)

    # Доп: команда для списка игроков
    app.add_handler(CommandHandler("players", cmd_list_players))

    # Команды модерации как alias (на случай, если кто вызовет их прямо)
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("unban", cmd_unban_custom))

    # Запускаем фоновую задачу на размуты
    async def run():
        # стартуем фоновую задачу
        task = asyncio.create_task(unmute_monitor(app))
        await app.run_polling()

        # если polling закончится — отменим таск
        task.cancel()

    asyncio.run(run())


if __name__ == "__main__":
    main()
