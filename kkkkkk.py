import sqlite3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import logging





# Создание базы данных
def init_db():
    conn = sqlite3.connect('marriage.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            married_to INTEGER
        )
    ''')
    conn.commit()
    conn.close()


# Получение user_id по username
async def get_user_id_from_username(username):
    conn = sqlite3.connect('marriage.db')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


# Проверка, женат ли пользователь
async def is_married(user_id):
    conn = sqlite3.connect('marriage.db')
    cursor = conn.cursor()
    cursor.execute("SELECT married_to FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] is not None if result else False


# Венчание пользователей
async def marry_users(first_user_id, second_user_id):
    conn = sqlite3.connect('marriage.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET married_to = ? WHERE user_id = ?", (second_user_id, first_user_id))
    cursor.execute("UPDATE users SET married_to = ? WHERE user_id = ?", (first_user_id, second_user_id))
    conn.commit()
    conn.close()


# Развод пользователя
async def divorce(user_id):
    conn = sqlite3.connect('marriage.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET married_to = NULL WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


# Команда венчаться
async def venchatsya(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    message_text = update.message.text
    parts = message_text.split()

    if len(parts) != 2 or not parts[1].startswith('@'):
        await context.bot.send_message(chat_id=chat_id, text="Используйте команду в формате: Венчаться @username")
        return

    second_username = parts[1][1:].strip()
    second_user_id = await get_user_id_from_username(second_username)

    if second_user_id is None:
        await context.bot.send_message(chat_id=chat_id, text="Пользователь не найден.")
        return

    first_user_id = update.effective_user.id
    if await is_married(first_user_id):
        await context.bot.send_message(chat_id=chat_id, text="Вы уже находитесь в браке.")
        return

    # Логика отправки сообщения с предложением венчаться
    keyboard = [[
        InlineKeyboardButton("Да", callback_data=f"yes_{first_user_id}"),
        InlineKeyboardButton("Нет", callback_data=f"no_{first_user_id}")
    ]]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(chat_id=second_user_id,
                                   text=f"Вам предложил венчаться пользователь @{update.effective_user.username}.",
                                   reply_markup=reply_markup)

    await context.bot.send_message(chat_id=chat_id,
                                   text=f"Вы отправили предложение венчаться пользователю @{second_username}.")


# Обработка нажатий кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data.split('_')
    response = data[0]
    first_user_id = int(data[1])

    if response == "yes":
        await marry_users(first_user_id, query.from_user.id)
        await query.message.reply_text(f"Вы успешно венчались с @{query.from_user.username}.")
    elif response == "no":
        await context.bot.send_message(chat_id=first_user_id, text=f"Ваш запрос отклонили.")


# Список всех браков
async def brakosochetanie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('marriage.db')
    cursor = conn.cursor()
    cursor.execute("SELECT u1.username, u2.username FROM users u1 JOIN users u2 ON u1.married_to = u2.user_id")
    marriages = cursor.fetchall()

    if not marriages:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Нет зарегистрированных браков.")
        conn.close()  # Закрываем соединение перед выходом из функции
        return

    marriage_list = "\n".join([f"@{m[0]} и @{m[1]}" for m in marriages])
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Список всех браков:\n" + marriage_list)

    conn.close()  # Закрываем соединение после завершения работы с базой данных


# Команда развестись
async def razvodytsya(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await is_married(user_id):
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Вы не находитесь в браке.")
        return

    await divorce(user_id)
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Вы успешно развелись.")


# Обработка сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text
    if message_text.startswith("Венчаться"):
        await venchatsya(update, context)
    elif message_text.startswith("бракосочетания"):
        await brakosochetanie(update, context)
    elif message_text.startswith("развестись"):
        await razvodytsya(update, context)


# Основная функция запуска бота
def main():
    init_db()  # Инициализация базы данных
    application = ApplicationBuilder().token('8599757452:AAF9X0oj_9-YNQLONWWIMj-b47Ki6s49zwY').build()

    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'^Венчаться'), handle_message))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'^бракосочетания'), handle_message))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'^развестись'), handle_message))

    application.add_handler(CallbackQueryHandler(button))

    application.run_polling()  # блокирует поток и корректно управляет loop


if __name__ == '__main__':
    main()

