import random
import time
import os
from datetime import datetime, timedelta
from collections import defaultdict
from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, CallbackQueryHandler, filters

# Список фотографий и подписей
photos = [
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\1.jpg","Подпись для фото 1"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\2.jpg", "Подпись для фото 2"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\3.jpg", "Подпись для фото 3"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\4.jpg", "Подпись для фото 4"),
    (r"C:\Users\anana\PycharmProjects\PythonProject2\photo\5.jpg", "Подпись для фото 5"),]

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
            await update.message.reply_text( f"Вы уже получали лависку. Повторите через {remaining_time // 3600}ч {remaining_time % 3600 // 60}м.")
            return
    else:
        # Инициализация user_data для нового пользователя
        user_data[user_id] = { 'last_request_time': current_time, 'collection': [], 'current_index': 0, 'photo_count': 0}
    # Генерация случайного фото и подписи
    photo, caption = random.choice(photos)
    # Добавление карточки в коллекцию
    user_data[user_id]['collection'].append((photo, caption))  # Добавляем карточку в коллекцию
    user_data[user_id]['photo_count'] += 1  # Увеличиваем счетчик фото
    # Обновляем время последнего запроса
    user_data[user_id]['last_request_time'] = current_time
    await update.message.reply_photo(photo=photo, caption=caption)

async def my_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # Проверяем, есть ли данные для пользователя
    if user_id not in user_data:
        await update.message.reply_text("Ваша коллекция пуста.")
        return
    # Получаем количество фото, используя get() для избежания KeyError
    count = user_data[user_id].get('photo_count', 0)
    keyboard = [[InlineKeyboardButton(f'Лависки {count}/{len(photos)}', callback_data='show_card')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # Отправляем сообщение и сохраняем его ID
    message = await update.message.reply_text("Ваши коллекции:", reply_markup=reply_markup)
    # Сохраняем ID сообщения в user_data для дальнейшего редактирования
    user_data[user_id]['message_id'] = message.message_id

async def show_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # Проверяем, есть ли данные для пользователя
    if user_id not in user_data or not user_data[user_id]['collection']:
        await update.message.reply_text("Ваша коллекция пуста.")
        return
    current_index = user_data[user_id]['current_index']
    photo = user_data[user_id]['collection'][current_index]
    caption = photo[1]
    count = user_data[user_id]['collection'].count(photo)
    keyboard = [
        [InlineKeyboardButton('Показать предыдущую карточку', callback_data='prev_card'),
         InlineKeyboardButton('Выйти в мою коллекцию', callback_data='my_collection'),
         InlineKeyboardButton('Показать следующую карточку', callback_data='next_card')] ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.answer()  # Обработка нажатия кнопки
    await update.callback_query.message.reply_photo(photo=photo[0], caption=f"{caption}\nКоличество: {count}",
                                                    reply_markup=reply_markup)

#не обновляется клава при нажатии на кнопки выходит новое соо


async def next_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_data:
        user_data[user_id]['current_index'] += 1
        if user_data[user_id]['current_index'] >= len(user_data[user_id]['collection']):
            user_data[user_id]['current_index'] = 0  # Зацикливание на коллекции
        await show_card(update, context)
    else:
        await update.message.reply_text("Ваша коллекция пуста.")

async def prev_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_data:
        user_data[user_id]['current_index'] -= 1
        if user_data[user_id]['current_index'] < 0:
            user_data[user_id]['current_index'] = len(user_data[user_id]['collection']) - 1  # Зацикливание на коллекции
        await show_card(update, context)
    else:
        await update.message.reply_text("Ваша коллекция пуста.")

def main():
    application = ApplicationBuilder().token("8599757452:AAF9X0oj_9-YNQLONWWIMj-b47Ki6s49zwY").build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'(?i)иссуе лав'), handle_laviska))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'(?i)моя коллекция'), my_collection))
    # Добавьте обработчики колбэков
    application.add_handler(CallbackQueryHandler(next_card, pattern='next_card'))
    application.add_handler(CallbackQueryHandler(prev_card, pattern='prev_card'))
    application.add_handler(CallbackQueryHandler(show_card, pattern='show_card'))
    application.add_handler(CallbackQueryHandler(my_collection, pattern='my_collection'))
    application.run_polling()

if __name__ == '__main__':
    main()

