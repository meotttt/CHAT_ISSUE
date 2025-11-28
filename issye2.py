import time
import threading
import random
from telebot import types
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import sqlite3
import telebot
from datetime import datetime, timedelta, time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
import logging
from telegram.ext.filters import REPLY
from telegram.helpers import mention_html


ADMIN_ID = '2123680656'
TOKEN ="8086930010:AAH1elkRFf6497_Ls9-XnZrUeIh_rWyMF5c"
bot = telebot.TeleBot(TOKEN)
name = None

# Создание базы данных и таблиц МУТ И БАН
def init_db():
    conn = sqlite3.connect('baza.sql', detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS muted_users (user_id INTEGER PRIMARY KEY, chat_id INTEGER, mute_until INTEGER) ''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS banned_users (user_id INTEGER PRIMARY KEY, chat_id INTEGER)''')
    conn.commit()
    conn.close()
init_db()

# КОМАНДЫ ЧЕРЕЗ СЛЕШ
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton('Вступить в чат 💬', url='https://t.me/CHAT_ISSUE')],
        [InlineKeyboardButton('Новогоднее голосование 🌲', url='https://t.me/ISSUEhappynewyearbot')],
        [InlineKeyboardButton('𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄', callback_data='send_papa')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user = update.effective_user
    name = user.username or user.first_name or 'друг'
    await update.message.reply_text(f'Привет, {name}! 🪐\nЭто бот чата 𝙄𝙎𝙎𝙐𝙀 \nТут ты сможешь поиграть в 𝐄𝐕𝐀𝐍𝐆𝐄𝐋𝐈𝐄, принять участие в новогоднем голосовании, а так же получить всю необходимую помощь!', reply_markup=reply_markup)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text('Добро пожаловать в мир "Евангелия" — интерактивной игры бота ISSUE! 🪐\n\n▎Что вас ждет в "Евангелии"? \n\n1. ⛩️ Хождение на службу — Молитвы: Каждый раз, когда вы молитесь, вы не просто выполняете рутинное действие — вы получаете повышения своей набожности\n\n2. ✨ Система Набожности: Ваши молитвы влияют на вашу духовную силу. Чем больше вы молитесь, тем выше ваша набожность. Станьте одним из самых набожных игроков!\n\n3. 📃 Соревнования и Достижения: Вы можете видеть, кто из игроков находится на вершине таблицы лидеров! Сравните свои достижения с друзьями и стремитесь занять первое место в рейтингах молитв и набожности.\n\n4. 👹 Неожиданные Повороты: Будьте готовы к неожиданным событиям! У вас есть шанс столкнуться с "бесноватостью".\n\nПоговаривают что стоит молиться аккуратнее с 00:00 до 04:00 и быть предельно осторожным в пятницу!\n\n─────── ⋆⋅☆⋅⋆ ───────\n\n⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие\n\nВозможно если вы взовете к помощи, вы обязательно ее получите\n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫')

#МУT
def mute_timer(chat_id, user_id, duration):
# Ждем указанное время в секундах
    threading.Timer(duration, unmute_user_after_timer, args=(chat_id, user_id)).start()
def unmute_user_after_timer(chat_id, user_id):
# Снимаем мут с пользователя
    bot.restrict_chat_member(chat_id, user_id, can_send_messages=True, can_send_media_messages=True, can_send_other_messages=True, can_add_web_page_previews=True, can_pin_messages=True)
# Удаляем информацию из базы данных
    conn = sqlite3.connect('baza.sql')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM muted_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
    conn.commit()
    conn.close()
# Уведомляем о размуте
    bot.send_message(chat_id, f"Пользователь {user_id} был размучен автоматически.")
@bot.message_handler(func=lambda message: message.text.lower().startswith('мут'))
def mute_user(message):
    if message.chat.type in ['group', 'supergroup']:
        chat_member = bot.get_chat_member(message.chat.id, message.from_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")
            return
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            chat_id = message.chat.id
# Получаем время мута из сообщения (например, "мут 1h 30m")
            tokens = message.text.split()[1:] # Берем все части после "мут"
            duration = 0
            if len(tokens) == 0:
# Если не указана длительность, устанавливаем по умолчанию 1 час
                duration = 3600
            else:
                i = 0
                while i < len(tokens):
                    tok = tokens[i]
                    if tok.isdigit():
                        n = int(tok)
                        unit = tokens[i + 1] if i + 1 < len(tokens) else ''
                        if unit.startswith('час') or unit in ('ч', 'h'):
                            duration += n * 3600
                            i += 2
                            continue
                        if unit.startswith('мин') or unit in ('м', 'min', 'm'):
                            duration += n * 60
                            i += 2
                            continue
                    else:
                        i += 1  # Если токен не число, просто переходим к следующему
                if duration <= 0:
                    bot.send_message(chat_id, "Неверный формат времени. Пожалуйста, укажите длительность.")
                    return
# Замучиваем пользователя
            bot.restrict_chat_member(chat_id, user_id,
                                     can_send_messages=False,
                                     can_send_media_messages=False,
                                     can_send_other_messages=False,
                                     can_add_web_page_previews=False,
                                     can_pin_messages=False)
# Вносим информацию в базу данных
            conn = sqlite3.connect('baza.sql')
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO muted_users (user_id, chat_id) VALUES (?, ?)', (user_id, chat_id))
            conn.commit()
            conn.close()
            user = message.reply_to_message.from_user
            chat_id = message.chat.id
            bot.send_message(chat_id, f"Пользователь {mention_html(user.id, user.first_name)} замучен на {duration // 3600} часов и {duration % 3600 // 60} минут.", parse_mode='HTML')
# Запускаем таймер для автоматического размучивания
            mute_timer(chat_id, user_id, duration)
        else:
            bot.send_message(message.chat.id,
                             "Пожалуйста, ответьте на сообщение пользователя, которого вы хотите замучить.")
#РАЗМУT
@bot.message_handler(func=lambda message: message.text.lower() == 'размут')
def unmute_user(message):
    if message.chat.type in ['group', 'supergroup']:
        chat_member = bot.get_chat_member(message.chat.id, message.from_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")
            return
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            chat_id = message.chat.id
# Снимаем мут с пользователя
            bot.restrict_chat_member(chat_id, user_id, can_send_messages=True, can_send_media_messages=True, can_send_other_messages=True, can_add_web_page_previews=True, can_pin_messages=True)
# Удаляем информацию из базы данных
            conn = sqlite3.connect('baza.sql')
            cursor = conn.cursor()
            cursor.execute('DELETE FROM banned_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
            conn.commit()
            conn.close()
            user = message.reply_to_message.from_user
            chat_id = message.chat.id
            bot.send_message(chat_id, f"Пользователь {mention_html(user.id, user.first_name)} размучен.", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id,"Пожалуйста, ответьте на сообщение пользователя, которого вы хотите размучить.")

# РЕАКЦИЯ НА ФОТО
@bot.message_handler(content_types=['photo'])
def get_photo(message):
    bot.reply_to(message, 'нихуевое фото братан')

# БАН
def ban_user(message):
    if message.chat.type in ['group', 'supergroup']:
        chat_member = bot.get_chat_member(message.chat.id, message.from_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")
            return
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            chat_id = message.chat.id

            # Ограничиваем возможности пользователя
            bot.kick_chat_member(chat_id, user_id)
            # Вносим информацию в базу данных
            conn = sqlite3.connect('baza.sql')
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO banned_users (user_id, chat_id) VALUES (?, ?)', (user_id, chat_id))
            conn.commit()
            conn.close()
            user = message.reply_to_message.from_user
            chat_id = message.chat.id
            bot.send_message(chat_id,
                             f"Пользователь {mention_html(user.id, user.first_name)} ЗАБАНЕН", parse_mode='HTML')
        else:
            bot.send_message(message.chat.id,
                             "Пожалуйста, ответьте на сообщение пользователя, которого вы хотите забанить.")

#РАЗБАН
@bot.message_handler(func=lambda message: message.text.lower().startswith('исразбан'))
def unban_user(message):
    if message.chat.type in ['group', 'supergroup']:
        chat_member = bot.get_chat_member(message.chat.id, message.from_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            bot.send_message(message.chat.id, "У вас нет прав для выполнения этой команды.")
            return

        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            chat_id = message.chat.id
            # Проверяем, есть ли пользователь в базе данных
            conn = sqlite3.connect('baza.sql')
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM banned_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
            banned_user = cursor.fetchone()
            conn.close()
            # Разрешаем пользователю снова присоединиться к группе
            if banned_user:
                bot.unban_chat_member(chat_id, user_id)
            # Удаляем информацию из базы данных
                conn = sqlite3.connect('baza.sql')
                cursor = conn.cursor()
                cursor.execute('DELETE FROM banned_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
                conn.commit()
                conn.close()

                user = message.reply_to_message.from_user
                bot.send_message(chat_id,f"Пользователь {mention_html(user.id, user.first_name)} РАЗБАНЕН и может снова присоединиться к группе", parse_mode='HTML')

                invite_link = bot.export_chat_invite_link(chat_id)

                # Замените на правильную ссылку на ваш чат
                bot.send_message(user.id,
                             f"Вы были разблокированы в чате {message.chat.title}! Мы рады видеть вас снова! "
                             f"Присоединяйтесь по ссылке: {invite_link}")

            else:
                bot.send_message(chat_id,"Этот пользователь не был забанен.")
        else:
            bot.send_message(message.chat.id,
                         "Пожалуйста, ответьте на сообщение пользователя, которого вы хотите разбанить.")

# КОМАНДЫ ОТ СЛОВА
@bot.message_handler()
def info(message):
    if message.text.lower() == '+акк':
        conn = sqlite3.connect('baza.sql')
        cur = conn.cursor()

        # Создаем таблицу, если она не существует
        cur.execute('''CREATE TABLE IF NOT EXISTS game_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            name VARCHAR(50) UNIQUE,
            password VARCHAR(50)
        )''')
        conn.commit()

        def ask_name(msg):
            name = msg.text.strip()

            # Проверка на занятого пользователя
            cur.execute('SELECT * FROM game_users WHERE user_id = ?', (msg.from_user.id,))
            if cur.fetchone():
                bot.send_message(msg.chat.id, 'У вас уже есть аккаунт.')
                return

            # Проверка на занятой ник
            cur.execute('SELECT * FROM game_users WHERE name = ?', (name,))
            if cur.fetchone():
                bot.send_message(msg.chat.id, 'Этот ник уже занят, напиши другой.')
                bot.register_next_step_handler(msg, ask_name)
                return

            # Ник свободен → спрашиваем пароль
            bot.send_message(msg.chat.id, 'Теперь введи пароль:')
            bot.register_next_step_handler(msg, ask_pass, name)

        def ask_pass(msg, name):
            password = msg.text.strip()

            cur.execute(
                'INSERT INTO game_users (user_id, name, password) VALUES (?, ?, ?)',
                (msg.from_user.id, name, password)
            )
            conn.commit()
            cur.close()
            conn.close()

            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(telebot.types.InlineKeyboardButton('Все игроки', callback_data='game_users'))
            bot.send_message(msg.chat.id, 'Твой аккаунт успешно добавлен!', reply_markup=markup)

        # Запуск первого шага
        bot.send_message(message.chat.id, 'Введите ваш будущий ник:')
        bot.register_next_step_handler(message, ask_name)

@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    conn = sqlite3.connect('baza.sql')
    cur = conn.cursor()

    cur.execute('select * from game_users')
    game_users = cur.fetchall()

    info = ''
    for el in game_users:
        info += f'Игрок: {el[1]}\n'

    cur.close()
    conn.close()

    bot.send_message(call.message.chat.id, info)









# ИГРА
# Создаем Базу данных
def create_db():
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users ( user_id INTEGER PRIMARY KEY, prayer_count INTEGER DEFAULT 0, piety_score REAL DEFAULT 0, last_prayer_time DATETIME,initialized BOOLEAN NOT NULL DEFAULT 0,cursed_until DATETIME)''')
    conn.commit()
    conn.close()

# Инициализация базы данных
create_db()
def init_db():
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
#Добавляем новый столбец, если он отсутствует
    try:
        cursor.execute('ALTER TABLE users ADD COLUMN initialized BOOLEAN NOT NULL DEFAULT 0')
    except sqlite3.OperationalError:
        # Если столбец уже существует, игнорируем ошибку
        pass
    conn.commit()
    conn.close()
#def add_demon_column():
    #conn = sqlite3.connect('gospel_game.db')  # Замените на имя вашей базы данных
    #cursor = conn.cursor()
     #Добавление столбца, если он отсутствует
    #try:
        #cursor.execute("ALTER TABLE users ADD COLUMN demon INTEGER DEFAULT 0;")
        #print("Столбец 'demon' успешно добавлен.")
    #except sqlite3.OperationalError as e:
        #if "duplicate column name" in str(e):
            #print("Столбец 'demon' уже существует.")
        #else:
            #print(f"Ошибка при добавлении столбца: {e}")

   #conn.commit()
    #conn.close()

# Функция для добавления пользователя в базу данных
def add_user(user_id):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO users (user_id, initialized) VALUES (?, ?)', (user_id, 0))
    conn.commit()
    conn.close()

# Функция для обновления статуса пользователя
def update_user_initialized(user_id):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET initialized = ? WHERE user_id = ?', (1, user_id))
    conn.commit()
    conn.close()

# Функция для получения данных пользователя
def get_user_data(user_id):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    conn.close()
    return user_data

def register_user(user_id):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()

    # Проверяем, существует ли пользователь
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if user is None:
        # Если пользователь не найден, добавляем его с initialized = False
        cursor.execute('INSERT INTO users (user_id, initialized) VALUES (?, ?)', (user_id, False))
        conn.commit()
    conn.close()

def initialize_user(user_id):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    # Устанавливаем initialized в True
    cursor.execute('UPDATE users SET initialized = ? WHERE user_id = ?', (True, user_id))
    conn.commit()
    conn.close()

async def check_gospel_found(user_id, update):
    def check_gospel_found(user_id):
        user_data = get_user_data(user_id)
        if user_data is None:
            # Если пользователь не найден, добавляем его
            add_user(user_id)
            print("Вы зарегистрированы! Теперь скажите 'найти евангелие', чтобы продолжить.")
            return False
        if user_data[1] == 0:  # Проверяем, нашел ли пользователь евангелие
            print("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫")
            return False
        return True

def get_user_data(user_id):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    if user_data is None:
        cursor.execute('INSERT INTO users (user_id) VALUES (?)', (user_id,))
        conn.commit()
        user_data = (user_id, 0, 0.0, None, None)
    conn.close()
    return user_data

# Функция для обновления данных пользователя
def update_user_data(user_id, prayer_count, total_piety_score, last_prayer_time, cursed_until):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('''UPDATE users SET prayer_count = ?, total_piety_score = ?, last_prayer_time = ?, cursed_until = ? WHERE user_id = ?''', (prayer_count, total_piety_score, last_prayer_time, cursed_until, user_id))
    conn.commit()
    conn.close()

# Обработка сообщения "найти евангелие"
async def find_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    # Проверяем, существует ли пользователь
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if user is None:
        # Если пользователь не существует, добавляем его
        cursor.execute('INSERT INTO users (user_id, initialized) VALUES (?, ?)', (user_id, False))
        conn.commit()
    # Логика поиска евангелия...
    # После успешного поиска обновляем значение initialized
    cursor.execute('UPDATE users SET initialized = ? WHERE user_id = ?', (True, user_id))
    conn.commit()
    await update.message.reply_text("Успех! ✨\nВаши реликвии у вас в руках!\n\nВам открылась возможность:\n⛩️ «мольба» — ходить на службу\n📜«Евангелие» — смотреть свои Евангелие\n📃 «Топ Евангелий» — и следить за вашими успехами!\nЖелаем удачи! 🍀")
    conn.close()
#СТООООООППППП
#async def check_initialization(update: Update, context: CallbackContext):
    #user_id = update.message.from_user.id
    #conn = sqlite3.connect('users.db')
    #cursor = conn.cursor()

    #cursor.execute('SELECT initialized FROM users WHERE user_id = ?', (user_id,))
    #user = cursor.fetchone()

    #if user is None or not user[0]:
        #await update.message.reply_text("Сначала выполните команду 'найти евангелие'.")
        #return False

    #return True

# НЕТ ОТВЕТА НА МОЛЬБА

# Обработка сообщения "молитва"
# Обработка сообщения "мольба"
async def prayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    try:
        # Проверяем, существует ли пользователь в базе данных
        cursor.execute('SELECT initialized, possession_of_demon FROM users WHERE user_id = ?', (user_id,))
        user_status = cursor.fetchone()

        # Проверка на существование пользователя и инициализацию
        if user_status is None:
            await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫")
            return
        # Проверяем, инициализирован ли пользователь
        initialized, possession_of_demon = user_status
        if initialized == 0:
            await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫")
            return
        current_time = datetime.now()

        # Получаем время бесноватости
        if possession_of_demon is not None:
            # Проверяем тип possession_of_demon
            if isinstance(possession_of_demon, str):
                try:
                    remaining_time = datetime.strptime(possession_of_demon, '%Y-%m-%d %H:%M:%S.%f') - current_time
                except ValueError:
                    remaining_time = timedelta(seconds=0)  # Установить значение по умолчанию
            elif isinstance(possession_of_demon, int):
                # Если possession_of_demon - это int, обрабатываем это как отсутствие бесноватости
                remaining_time = timedelta(seconds=0)
            else:
                remaining_time = timedelta(seconds=0)  # Обработка других типов

            if remaining_time.total_seconds() > 0:
                hours = int(remaining_time.total_seconds() // 3600)
                minutes = int((remaining_time.total_seconds() % 3600) // 60)  # Остаток от часов
                seconds = int(remaining_time.total_seconds() % 60)
                await update.message.reply_text(f'У вас бесноватость 👹\n📿 Вы не сможете молится еще {hours} часа(ов), {minutes} минут(ы) ')
                return

        # Логика генерации "бесноватости"
        if current_time.weekday() == 4 and (0 <= current_time.hour < 4):  # Вторник с 00:00 до 23:59
            if random.random() < 0.1:  # 99% шанс на бесноватость
                possession_of_demon = current_time + timedelta(days=1)  # Бесноватость длится сутки
                cursor.execute('UPDATE users SET possession_of_demon = ? WHERE user_id = ?', (possession_of_demon.strftime('%Y-%m-%d %H:%M:%S.%f'), user_id))
                conn.commit()
                await update.message.reply_text("У вас бесноватость 👹\nПохоже вашу мольбу услышал кое-кто….другой\n\n📿 Вы не сможете молиться сутки")
                return

        # Получаем время последней молитвы
        cursor.execute('SELECT last_prayer_time, prayer_count, total_piety_score FROM users WHERE user_id = ?',(user_id,))
        user_data = cursor.fetchone()
        if user_data is not None:
            last_prayer_time_str, prayer_count, total_piety_score = user_data
            last_prayer_time = datetime.strptime(last_prayer_time_str,
                                                 '%Y-%m-%d %H:%M:%S.%f') if last_prayer_time_str else None
        else:
            last_prayer_time = None
            prayer_count = 0
            total_piety_score = 0

        current_time = datetime.now()
        # Проверяем, прошло ли больше часа с последней молитвы
        if last_prayer_time is not None and current_time < last_prayer_time + timedelta(hours=1):
            remaining_time = (last_prayer_time + timedelta(hours=1)) - current_time
            remaining_seconds = int(remaining_time.total_seconds())
            minutes = remaining_seconds // 60
            seconds = remaining_seconds % 60
            await update.message.reply_text( f'…..Похоже никто не слышит вашей мольбы\n📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы)')
            return

        # Логика молитвы...
        piety_score = round(random.uniform(1, 20) / 2, 1)  # Генерируем случайное число от 1 до 10 с шагом 0.5
        # Увеличиваем счетчик молитв и обновляем общую набожность
        prayer_count += 1
        total_piety_score += piety_score
        # Сохраняем обновленные данные пользователя в базе данных
        cursor.execute('UPDATE users SET last_prayer_time = ?, prayer_count = ?, total_piety_score = ? WHERE user_id = ?',(current_time.strftime('%Y-%m-%d %H:%M:%S.%f'), prayer_count, total_piety_score, user_id))
        conn.commit()
        await update.message.reply_text(f'⛩️ Ваши мольбы были услышаны! \n✨ Набожность +{piety_score}\nНа следующую службу можно будет выйти через час 📿')
        logging.basicConfig(filename='app.log', level=logging.ERROR)
    finally:
        conn.close()  # Закрываем соединение с базой данных

# Обработка сообщения "евангелие"
async def gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    # Проверяем, зарегистрирован ли пользователь
    cursor.execute('SELECT initialized FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if user is None:
        # Если пользователь не найден, можно предложить регистрацию
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        conn.close()  # Закрываем соединение с базой данных
        return
    if not user[0]:  # Если пользователь не инициализирован
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        conn.close()  # Закрываем соединение с базой данных
        return
    # Пользователь зарегистрирован и инициализирован, получаем его данные
    cursor.execute('SELECT prayer_count, total_piety_score FROM users WHERE user_id = ?', (user_id,))
    user_data = cursor.fetchone()
    # Проверка, что данные были найдены
    if user_data:
        prayer_count = user_data[0]
        total_piety_score = user_data[1]
        # Отправка информации о значениях евангелия
        await update.message.reply_text(
            f'📜 Ваше евангелие:\n\nМолитвы — {prayer_count}📿\nНабожность — {total_piety_score:.1f} ✨')
    else:
        await update.message.reply_text('Пользователь не найден.')
    # Закрытие соединения с базой данных
    conn.close()

# Обработка сообщения "ТОП евангелие"
async def top_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    user_id = update.message.from_user.id
    # Проверяем, зарегистрирован ли пользователь
    cursor.execute('SELECT initialized FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if user is None or not user[0]:  # Если пользователь не найден или не инициализирован
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важные реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        return

    # Топ по количеству молитв
    try:
        cursor.execute('SELECT user_id, prayer_count FROM users ORDER BY prayer_count DESC')
        prayer_leaderboard = cursor.fetchall()

     # Топ по набожности
        cursor.execute('SELECT user_id, total_piety_score FROM users ORDER BY total_piety_score DESC')
        piety_leaderboard = cursor.fetchall()
        if not piety_leaderboard:
            await update.message.reply_text('Нет данных о набожности.')
            return
    except sqlite3.Error as e:
        await update.message.reply_text(f'Ошибка при доступе к базе данных: {e}')
        conn.close()
        return
    finally:
        conn.close()
    leaderboard_msg = "Топ Евангелий:\n⛩️ Услышанные молитвы:\n"
    for rank, (user_id, count) in enumerate(prayer_leaderboard, start=1):    # Получите объект пользователя по user_id
        user = await context.bot.get_chat(user_id)  # Получаем объект пользователя
        leaderboard_msg += f"{rank}.  {user.first_name}: {count} молитв\n"
    # Для HTML
    leaderboard_msg += "\n✨<b>Набожность:</b>\n"  # Переносим заголовок здесь
    for rank, (user_id, score) in enumerate(piety_leaderboard, start=1):    # Получите объект пользователя по user_id
        user = await context.bot.get_chat(user_id)  # Получаем объект пользователя
        leaderboard_msg += f"{rank}.  {user.first_name}: {score:.1f} набожности\n"
    await update.message.reply_text(leaderboard_msg, parse_mode='HTML')  # Для HTML

# Обработка любого текстового сообщения
async def handle_message(update, context):
    if update.message and update.message.text:
        text = update.message.text.lower()
        # Обработка текста сообщения
    else:
        print("Получено обновление без текстового сообщения.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower()
    if update.message is None:
        return  # Игнорируем обновления, которые не содержат сообщения

    if "найти евангелие" in text:
        await find_gospel(update, context)
    elif "мольба" in text:
        await prayer(update, context)
    elif "евангелие" in text:
        await gospel(update, context)
    elif "топ евангелий" in text:
        await top_gospel(update, context)

def main():
    application = ApplicationBuilder().token("8086930010:AAH1elkRFf6497_Ls9-XnZrUeIh_rWyMF5c").build()
    #add_demon_column()  # Добавление нового столбца, если он отсутствует

    # Обработчик текстовых сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.run_polling()

if __name__ == '__main__':
    main()

class baza:
    def __init__(self, db_file):
        self.connection = sqlite3.connect(db_file)
        self.cursor = self.connection.cursor()

    def examination(self, user_id):
        with self.connection:
            res = self.cursor.execute('select * from users where id = ?', (user_id,)).fetchall()
            return bool(len(res))

    def add(self, user_id):
        with self.connection:
            return self.connection.execute("INSERT INTO users ('user_id') VALUES (?)", (user_id,))

    def mute (self, user_id):
        with self.connection:
            user = self.connection.execute("SELECT id FROM users where id = ?", (user_id,)).fetchall()
            return int(user[2]) >= int(time.time())

    def add_mute(self, user_id, mute_time):
        with self.connection:
            return self.connection.execute("UPDATE users SET mute_time = ? WHERE id = ?", (int(time.time()) +mute_time, user_id))

bot.polling(non_stop=True)

