# --- Конфигурация ---
import logging
import sqlite3
from datetime import datetime
from typing import Optional # <<< Добавьте эту строку

from telegram import Update, User
from telegram.ext import Application, ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters








# !!! ЗАМЕНИТЕ НА ВАШ ТОКЕН БОТА !!!
TOKEN = "8599757452:AAF9X0oj_9-YNQLONWWIMj-b47Ki6s49zwY"

# !!! ЗАМЕНИТЕ НА ID ВАШЕЙ ГРУППЫ !!!
# Этот ID вы получите, как описано в Шаге 1.
# Если вы хотите сохранять пользователей из ЛЮБОГО чата, установите GROUP_CHAT_ID = None
GROUP_CHAT_ID: Optional[int] = -1002372051836  # Пример: -1001234567890
GROUP_USERNAME = "@CHAT_ISSUE"  # Имя группы (для информационных сообщений)

DATABASE_NAME = "POLZOVATEL.db"  # Имя файла базы данных SQLite

# --- Настройка логирования ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# --- Функции для работы с базой данных ---

def init_db():
    """Инициализирует базу данных, создавая таблицу 'users', если она не существует."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            first_name TEXT,
            last_name TEXT,
            updated_at TEXT
        )
    """)
    conn.commit()
    conn.close()
    logger.info("База данных 'users.db' инициализирована.")


def save_user_data(user: User):
    """Сохраняет или обновляет данные пользователя в базе данных."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    current_time = datetime.now().isoformat()

    try:
        cursor.execute("""
            INSERT INTO users (user_id, username, first_name, last_name, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = ?,
                first_name = ?,
                last_name = ?,
                updated_at = ?
        """, (
            user.id, user.username, user.first_name, user.last_name, current_time,
            user.username, user.first_name, user.last_name, current_time
        ))
        conn.commit()
        logger.info(f"Данные пользователя {user.id} (@{user.username or 'NoUsername'}) сохранены/обновлены.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных пользователя {user.id}: {e}")
    finally:
        conn.close()


def get_user_id_from_username_db(username: str) -> Optional[int]:
    """Получает user_id по username из базы данных."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (username,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


# --- Обработчики команд и сообщений ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает команду /start."""
    user = update.effective_user
    if user:
        save_user_data(user)  # Сохраняем данные пользователя, который начал диалог с ботом
        await update.message.reply_html(
            f"Привет, {user.mention_html()}! Я бот для сохранения данных пользователей. "
            "Отправьте `/get_chat_id`, чтобы узнать ID текущего чата."
        )


async def get_chat_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает команду /get_chat_id и выводит ID текущего чата."""
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = update.effective_chat.title if chat_type != 'private' else 'Личный чат'

    response = (
        f"ID этого чата: `{chat_id}`\n"
        f"Тип чата: `{chat_type}`\n"
        f"Название чата: `{chat_title}`"
    )
    await update.message.reply_text(response, parse_mode="Markdown")


async def echo_and_save_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает все сообщения, сохраняет данные пользователя, если сообщение
    пришло из целевой группы.
    """
    chat_id = update.effective_chat.id
    user = update.effective_user

    if user and not user.is_bot:  # Игнорируем сообщения от ботов
        # Если GROUP_CHAT_ID установлен, проверяем, соответствует ли chat_id
        if GROUP_CHAT_ID is None or chat_id == GROUP_CHAT_ID:
            save_user_data(user)
            logger.info(
                f"Сообщение от пользователя {user.id} (@{user.username or 'NoUsername'}) в чате {chat_id} обработано и данные сохранены.")
        else:
            logger.debug(f"Сообщение от пользователя {user.id} в чате {chat_id} (не целевой чат) проигнорировано.")

    # Можно добавить логику для ответа на сообщения, если это необходимо
    # await update.message.reply_text(update.message.text)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирует ошибки, вызванные обработчиками."""
    logger.warning(f'Update "{update}" вызвал ошибку "{context.error}"')


# --- Основная функция запуска бота ---

def main():
    """Запускает бота."""
    init_db()  # Инициализация базы данных при старте бота

    application = ApplicationBuilder().token(TOKEN).build()

    # Регистрируем обработчики команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("get_chat_id", get_chat_id_command))

    # Регистрируем обработчик для всех сообщений (кроме команд)
    # filters.TEXT & ~filters.COMMAND - обрабатывает текстовые сообщения, которые не являются командами
    # filters.ALL - обрабатывает вообще все, включая фото, видео, стикеры и т.д.
    # Для сохранения пользователя достаточно filters.ALL, так как update.effective_user всегда доступен.
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, echo_and_save_user))

    # Регистрируем обработчик ошибок
    application.add_error_handler(error_handler)



    logger.info("Бот запущен. Ожидание сообщений...")
    application.run_polling()


if __name__ == '__main__':
    main()
