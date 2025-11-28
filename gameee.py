import random
import sqlite3
import telebot
from datetime import datetime, timedelta
from telegram import Update
import logging
from telegram.helpers import mention_html
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CallbackContext

# Создаем базу данных
def create_db():
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            prayer_count INTEGER DEFAULT 0,
            piety_score REAL DEFAULT 0,
            last_prayer_time DATETIME,
            initialized BOOLEAN NOT NULL DEFAULT 0,
            cursed_until DATETIME)''')
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


def add_demon_column():
    conn = sqlite3.connect('gospel_game.db')  # Замените на имя вашей базы данных
    cursor = conn.cursor()

     #Добавление столбца, если он отсутствует
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN demon INTEGER DEFAULT 0;")
        print("Столбец 'demon' успешно добавлен.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("Столбец 'demon' уже существует.")
        else:
            print(f"Ошибка при добавлении столбца: {e}")

    conn.commit()
    conn.close()


# Функция для получения данных пользователя







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
            print("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
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
    cursor.execute('''
        UPDATE users SET prayer_count = ?, total_piety_score = ?, last_prayer_time = ?, cursed_until = ?
        WHERE user_id = ?
    ''', (prayer_count, total_piety_score, last_prayer_time, cursed_until, user_id))
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
            await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
            return
        # Проверяем, инициализирован ли пользователь
        initialized, possession_of_demon = user_status
        if initialized == 0:
            await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
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
                await update.message.reply_text(
                    f'У вас бесноватость 👹\n📿 Вы не сможете молится еще {hours} часа(ов), {minutes} минут(ы) ')
                return

        # Логика генерации "бесноватости"
        if current_time.weekday() == 4 and (0 <= current_time.hour < 4):  # Вторник с 00:00 до 23:59
            if random.random() < 0.1:  # 99% шанс на бесноватость
                possession_of_demon = current_time + timedelta(days=1)  # Бесноватость длится сутки
                cursor.execute('UPDATE users SET possession_of_demon = ? WHERE user_id = ?',
                               (possession_of_demon.strftime('%Y-%m-%d %H:%M:%S.%f'), user_id))
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
            await update.message.reply_text(
                f'…..Похоже никто не слышит вашей мольбы\n📿 Попробуйте прийти на службу через {minutes} минут(ы) и {seconds} секунд(ы)')
            return

        # Логика молитвы...
        piety_score = round(random.uniform(1, 20) / 2, 1)  # Генерируем случайное число от 1 до 10 с шагом 0.5
        # Увеличиваем счетчик молитв и обновляем общую набожность
        prayer_count += 1
        total_piety_score += piety_score
        # Сохраняем обновленные данные пользователя в базе данных
        cursor.execute(
            'UPDATE users SET last_prayer_time = ?, prayer_count = ?, total_piety_score = ? WHERE user_id = ?',
            (current_time.strftime('%Y-%m-%d %H:%M:%S.%f'), prayer_count, total_piety_score, user_id))
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
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        conn.close()  # Закрываем соединение с базой данных
        return
    if not user[0]:  # Если пользователь не инициализирован
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвии — книги Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
        conn.close()  # Закрываем соединение с базой данных
        return
    # Пользователь зарегистрирован и инициализирован, получаем его данные
    user_data = get_user_data(user_id)
    # Проверяем, что данные пользователя были успешно получены
    if user_data is None:
        await update.message.reply_text("Ошибка при получении данных пользователя.")
        conn.close()  # Закрываем соединение с базой данных
        return
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


    # Отправляем информацию о значениях евангелия
    await update.message.reply_text(f'📜 Ваше евангелие:\n\nМолитвы — {prayer_count}📿\nНабожность — {total_piety_score:.1f} ✨')
    conn.close()
async def top_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('gospel_game.db')
    cursor = conn.cursor()
    user_id = update.message.from_user.id
    # Проверяем, зарегистрирован ли пользователь
    cursor.execute('SELECT initialized FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()
    if user is None or not user[0]:  # Если пользователь не найден или не инициализирован
        await update.message.reply_text("⛩️ Для того чтоб ходить на службу вам нужно найти важную реликвию — книгу Евангелие \n\nВозможно если вы взовете к помощи, вы обязательно ее получите \n\n📜 «Найти Евангелие» — кто знает, может так у вас получится…🤫.")
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
    add_demon_column()  # Добавление нового столбца, если он отсутствует

    # Обработчик текстовых сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.run_polling()

if __name__ == '__main__':
    main()
bot.polling()