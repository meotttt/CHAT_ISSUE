import random
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import requests


# Данные для карточек
photos = [
    (f"https://ltdfoto.ru/image/neJzhr{i}.jpg", f"Подпись для карточки {i}") for i in range(1, 11)
]
# Проверка доступности всех изображений
def check_photos(photos):
    available_photos = []
    for url, caption in photos:
        try:
            response = requests.head(url)
            if response.status_code == 200:
                available_photos.append((url, caption))
            else:
                print(f"Изображение недоступно: {url} (статус: {response.status_code})")
        except Exception as e:
            print(f"Ошибка при проверке {url}: {e}")
    return available_photos

# Проверка фотографий
available_photos = check_photos(photos)

# Теперь вы можете использовать available_photos вместо photos в вашем коде

# Данные для карточек
photos = [
    (f"https://yapx.ru/album/cOVzw{i}.jpg", f"Подпись для карточки {i}") for i in range(1, 11)
]

# Хранение данных пользователей
user_data = {}
last_request_time = {}


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Используй команды: 'иссуе лав', 'моя коллекция'.")


async def issue_lav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    current_time = time.time()

    # Проверка времени между запросами
    if user_id in last_request_time:
        elapsed_time = current_time - last_request_time[user_id]
        if elapsed_time < 5:
            remaining_time = 5 - elapsed_time
            await update.message.reply_text(f"Вы уже получали лависку. Повторите через {int(remaining_time)} секунд.")
            return

    # Обновление времени последнего запроса
    last_request_time[user_id] = current_time

    # Выбор случайной карточки с шансом на повтор
    if user_id not in user_data:
        user_data[user_id] = {'collection': [], 'seen': set()}

    if len(user_data[user_id]['collection']) < len(photos) or random.random() < 0.3:
        # Возможен повтор
        while True:
            photo_index = random.randint(0, len(photos) - 1)
            if photo_index not in user_data[user_id]['seen']:
                break
        user_data[user_id]['seen'].add(photo_index)
    else:
        # Новая карточка
        photo_index = random.choice([i for i in range(len(photos)) if i not in user_data[user_id]['seen']])
        user_data[user_id]['seen'].add(photo_index)

    # Добавление карточки в коллекцию
    user_data[user_id]['collection'].append(photo_index)

    photo_url, caption = photos[photo_index]
    await update.message.reply_photo(photo=photo_url, caption=caption)


async def my_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    collection_size = len(user_data.get(user_id, {}).get('collection', []))

    keyboard = [[InlineKeyboardButton(f"Лависки {collection_size}/74", callback_data='show_collection')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text("Ваши коллекции:", reply_markup=reply_markup)


async def show_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    query.answer()

    user_id = query.from_user.id
    collection = user_data.get(user_id, {}).get('collection', [])

    if not collection:
        await query.message.reply_text("Ваша коллекция пуста.")
        return

    current_index = 0  # Начинаем с первой карточки
    await send_card(query, user_id, current_index)


async def send_card(query, user_id, index):
    collection = user_data[user_id]['collection']

    if index < 0 or index >= len(collection):
        await query.message.reply_text("Недопустимый индекс.")
        return

    photo_index = collection[index]
    photo_url, caption = photos[photo_index]

    keyboard = [
        [InlineKeyboardButton("Предыдущая карточка", callback_data=f'prev_{index}'),
         InlineKeyboardButton("Следующая карточка", callback_data=f'next_{index}')],
        [InlineKeyboardButton("Выйти в коллекцию", callback_data='show_collection')]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_photo(photo=photo_url, caption=caption, reply_markup=reply_markup)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    query.answer()

    user_id = query.from_user.id

    if query.data.startswith('next_'):
        index = int(query.data.split('_')[1]) + 1
        await send_card(query, user_id, index)

    elif query.data.startswith('prev_'):
        index = int(query.data.split('_')[1]) - 1
        await send_card(query, user_id, index)

    elif query.data == 'show_collection':
        await my_collection(update, context)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower()

    if text == "иссуе лав":
        await issue_lav(update, context)
    elif text == "моя коллекция":
        await my_collection(update, context)


def main():
    application = ApplicationBuilder().token('8599757452:AAF9X0oj_9-YNQLONWWIMj-b47Ki6s49zwY').build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button_handler))

    application.run_polling()


if __name__ == '__main__':
    main()
