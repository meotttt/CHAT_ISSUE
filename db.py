from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes


async def find_gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вы выбрали команду найти евангелие.")


async def prayer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вы выбрали команду мольба.")


async def gospel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вы выбрали команду евангелие.")


async def top_gospels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вы выбрали команду топ евангелия.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower()

    if text == "найти евангелие":
        await find_gospel(update, context)
    elif text == "мольба":
        await prayer(update, context)
    elif text == "евангелие":
        await gospel(update, context)
    elif text == "топ евангелия":
        await top_gospels(update, context)
    else:
        await update.message.reply_text("Неизвестная команда. Пожалуйста, попробуйте снова.")


if __name__ == "__main__":
    application = ApplicationBuilder().token("8599757452:AAGsJ3tPbcE4-8euVBmc1krMaLIzsszJlGk").build()

    # Регистрация обработчиков команд
    application.add_handler(
        CommandHandler("start", lambda update, context: update.message.reply_text("Привет! Введите команду.")))

    # Регистрация обработчика текстовых сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.run_polling()
f.connection.execute("UPDATE users SET mute_time = ? WHERE id = ?", (int(time.time()) +mute_time, user_







                     id)






































#игра
# Создаем или подключаемся к базе данных



conn = sqlite3.connect('game.db')
c = conn.cursor()

# Создаем таблицу пользователей, если она не существует
c.execute('''CREATE TABLE IF NOT EXISTS users ( user_id INTEGER PRIMARY KEY, prayers INTEGER DEFAULT 0, piety REAL DEFAULT 0, last_prayer DATETIME DEFAULT NULL, cursed_until DATETIME DEFAULT NULL ) ''')
conn.commit()


async def find_gospel(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    c.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
    conn.commit()
    await update.message.reply_text("Вы можете молиться! Используйте команду 'мольба'.")

async def prayer(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    # Получаем данные пользователя
    c.execute('SELECT prayers, piety, last_prayer, cursed_until FROM users WHERE user_id = ?', (user_id,))
    user_data = c.fetchone()

    if user_data is None:
        await update.message.reply_text("Сначала используйте 'Найти евангелие'.")
        return

    prayers, piety, last_prayer, cursed_until = user_data
    now = datetime.now()

    # Проверяем на "бесноватость"
    if cursed_until and now < cursed_until:
        remaining_time = cursed_until - now
        await update.message.reply_text(f"Вы уже недавно молились. Осталось времени: {remaining_time.seconds // 3600} часов и {(remaining_time.seconds // 60) % 60} минут.")
        return

    # Проверяем время последней молитвы
    if last_prayer and now < last_prayer + timedelta(hours=1):
        remaining_time = last_prayer + timedelta(hours=1) - now
        await update.message.reply_text(f"Вы уже недавно молились. Осталось времени: {remaining_time.seconds // 3600} часов и {(remaining_time.seconds // 60) % 60} минут.")
        return

    # Обновляем данные пользователя
    last_prayer = now
    prayers += 1

    # Генерируем набожность
    piety_gain = round(random.uniform(1, 20) / 2, 1)
    piety += piety_gain

    # Проверяем на "бесноватость"
    if random.random() < 0.03:  # 3% шанс
        cursed_until = now + timedelta(days=1)
        await update.message.reply_text("Ваши молитвы вызвали бесноватость! Теперь вы можете молиться только через сутки.")
    else:
        cursed_until = None
        await update.message.reply_text("Мольба удалась!")

    # Сохраняем обновленные данные в БД
    c.execute('''UPDATE users SET prayers = ?, piety = ?, last_prayer = ?, cursed_until = ? WHERE user_id = ? ''', (prayers, piety, last_prayer, cursed_until, user_id))
    conn.commit()


async def gospel(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    c.execute('SELECT prayers, piety FROM users WHERE user_id = ?', (user_id,))
    user_data = c.fetchone()

    if user_data is None:
        await update.message.reply_text("Сначала используйте 'Найти евангелие'.")
        return

    prayers, piety = user_data
    await update.message.reply_text(f"Ваши мольбы - {prayers}\nВаша набожность - {piety}")


async def top_gospels(update: Update, context: CallbackContext) -> None:
    # Топ по молитвам
    c.execute('SELECT user_id, prayers FROM users ORDER BY prayers DESC')
    top_prayers = c.fetchall()

    # Топ по набожности
    c.execute('SELECT user_id, piety FROM users ORDER BY piety DESC')
    top_piety = c.fetchall()

    top_prayers_message = "Топ молитв:\n"
    for user in top_prayers:
        top_prayers_message += f"Пользователь {user[0]}: {user[1]} мольб\n"

    top_piety_message = "Топ набожности:\n"
    for user in top_piety:
        top_piety_message += f"Пользователь {user[0]}: {user[1]} набожности\n"

        await update.message.reply_text(top_prayers_message + "\n" + top_piety_message)





application = ApplicationBuilder().token(TOKEN).build()
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

application.add_handler(CommandHandler("EVA", find_gospel))
application.add_handler(CommandHandler("MOLBA", prayer))
application.add_handler(CommandHandler("EVA", gospel))
application.add_handler(CommandHandler("TOP_MOLBA", top_gospels))

if __name__ == '__main__':
    application.run_polling()










