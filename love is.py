import random
import time
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Message, CallbackQuery
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes

# Список фотографий и уникальных подписей
photos = [
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\1.jpg","Подпись для фото 1"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\2.jpg", "Подпись для фото 2"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\3.jpg", "Подпись для фото 3"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\4.jpg", "Подпись для фото 4"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\5.jpg", "Подпись для фото 5"),
]



# Хранение состояния пользователей
user_data = {}
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data['collection'] = []  # Инициализация коллекции
    context.user_data['current_index'] = 0  # Инициализация текущего индекса
    await update.message.reply_text("Добро пожаловать! Используйте команду 'лависка' для получения карточек.")

async def handle_laviska(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    current_time = datetime.now()

    # Проверка времени последнего запроса
    if user_id in user_data:
        last_request_time = user_data[user_id]['last_request_time']
        if current_time < last_request_time + timedelta(seconds=3):
            remaining_time = (last_request_time + timedelta(seconds=3) - current_time).seconds
            await update.message.reply_text(
                f"Вы уже получали лависку. Повторите через {remaining_time // 3600}ч {remaining_time % 3600 // 60}м.")
            return

    # Генерация случайного фото и подписи
    photo, caption = random.choice(photos)
    user_data[user_id] = {
        'last_request_time': current_time,
        'collection': [],
        'current_index': 0,
    }

    # Инициализация user_data для нового пользователя
    if user_id not in user_data:
        user_data[user_id] = {
            'last_request_time': current_time,
            'collection': [],
            'current_index': 0,
        }


    # Добавление карточки в коллекцию
    if user_id not in user_data:
        user_data[user_id] = {
            'last_request_time': current_time,
            'collection': [],
            'current_index': 0,
        }

    user_data[user_id]['collection'].append((photo, caption))  # Добавляем карточку в коллекцию

    # Обновляем время последнего запроса
    user_data[user_id]['last_request_time'] = current_time

    await update.message.reply_photo(photo=photo, caption=caption)


async def handle_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем наличие callback_query
    if update.callback_query:
        user_id = update.callback_query.from_user.id  # Получаем user_id из callback_query
        await update.callback_query.answer()  # Подтверждаем получение callback'а

        # Обрабатываем коллекцию
        if user_id in user_data and user_data[user_id]['collection']:
            current_index = user_data[user_id]['current_index']
            collection = user_data[user_id]['collection']
            photo, caption = collection[current_index]
            await update.callback_query.message.reply_photo(photo=photo, caption=caption)
            # Обновляем индекс для следующего фото
            user_data[user_id]['current_index'] = (current_index + 1) % len(collection)
        else:
            await update.callback_query.message.reply_text("Ваша коллекция пуста.")
    else:
        # Если это текстовое сообщение, вы можете добавить логику для обработки текстовых сообщений
        # Например:
        user_id = update.effective_user.id  # Получаем user_id из текстового сообщения
        if user_id in user_data and user_data[user_id]['collection']:
            current_index = user_data[user_id]['current_index']
            collection = user_data[user_id]['collection']
            photo, caption = collection[current_index]
            await update.message.reply_photo(photo=photo, caption=caption)
            # Обновляем индекс для следующего фото
            user_data[user_id]['current_index'] = (current_index + 1) % len(collection)
        else:
            await update.message.reply_text("Ваша коллекция пуста.")


async def send_card(query: CallbackQuery, user_id: int):
    current_index = user_data[user_id]['current_index']
    collection = user_data[user_id]['collection']
    
    if current_index < 0 or current_index >= len(collection):
        await query.answer("Недопустимый индекс.", show_alert=True)
        return

    photo, caption = collection[current_index]
    await query.message.reply_photo(photo=photo, caption=caption)

    if current_index >= len(user_data[user_id]['collection']):
        current_index = len(user_data[user_id]['collection']) - 1

    current_photo, current_caption = user_data[user_id]['collection'][current_index]
    await query.message.reply_photo(photo=photo, caption=caption)
    keyboard = [
        [InlineKeyboardButton("Показать предыдущую карточку", callback_data='prev')],
        [InlineKeyboardButton("Выйти в мою коллекцию", callback_data='exit')],
        [InlineKeyboardButton("Показать следующую карточку", callback_data='next')]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Обновляем сообщение с новой карточкой
    await query.edit_message_media(InputMediaPhoto(media=current_photo, caption=current_caption))
    await query.edit_message_reply_markup(reply_markup=reply_markup)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    if user_id not in user_data or not user_data[user_id]['collection']:
        await query.message.reply_text(
            "Ваша коллекция пуста. Пожалуйста, используйте текст 'лависка' для получения карточек.")
        return

    current_index = user_data[user_id].get('current_index', 0)
    print(f"Current index before update: {current_index}")

    if query.data == 'prev':
        if current_index > 0:
            current_index -= 1
            user_data[user_id]['current_index'] = current_index  # Обновляем индекс
            print(f"Moved to previous index: {current_index}")
        else:
            await query.answer("Это первая карточка.", show_alert=True)
            return

    elif query.data == 'next':
        if current_index < len(user_data[user_id]['collection']) - 1:
            current_index += 1
            user_data[user_id]['current_index'] = current_index  # Обновляем индекс
            print(f"Moved to next index: {current_index}")
        else:
            await query.answer("Это последняя карточка.", show_alert=True)
            return

    elif query.data == 'exit':
        await query.message.reply_text("Вы вышли в свою коллекцию.")
        return

    # Обновляем текущую карточку и клавиатуру
    await send_card(query, user_id)  # Передаем query вместо update



if __name__ == '__main__':
    application = ApplicationBuilder().token('8599757452:AAF9X0oj_9-YNQLONWWIMj-b47Ki6s49zwY').build()

    application.add_handler(CommandHandler("start", start))

    # Обработчик текстовых сообщений для "лависка"
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'(?i)лависка'), handle_laviska))

    # Обработчик текстовых сообщений для "коллекция"
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'(?i)коллекция'), handle_collection))

    application.add_handler(CallbackQueryHandler(button_handler))

    application.run_polling()

