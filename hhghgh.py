import sqlite3
import threading
import re
from telebot import TeleBot

bot = TeleBot('8086930010:AAH1elkRFf6497_Ls9-XnZrUeIh_rWyMF5c')


def mute_timer(chat_id, user_id, duration):
    # Ждем указанное время в секундах
    threading.Timer(duration, unmute_user_after_timer, args=(chat_id, user_id)).start()


def unmute_user_after_timer(chat_id, user_id):
    # Снимаем мут с пользователя
    bot.restrict_chat_member(chat_id, user_id,
                             can_send_messages=True,
                             can_send_media_messages=True,
                             can_send_other_messages=True,
                             can_add_web_page_previews=True,
                             can_pin_messages=True
                             )

    # Удаляем информацию из базы данных
    conn = sqlite3.connect('baza.sql')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM banned_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
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
            tokens = message.text.split()[1:]  # Берем все части после "мут"
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
            cursor.execute('INSERT OR REPLACE INTO banned_users (user_id, chat_id) VALUES (?, ?)', (user_id, chat_id))
            conn.commit()
            conn.close()

            bot.send_message(chat_id,f"Пользователь {message.reply_to_message.from_user.username} замучен на {duration // 3600} часов и {duration % 3600 // 60} минут.")

            # Запускаем таймер для автоматического размутирования
            mute_timer(chat_id, user_id, duration)
        else:
            bot.send_message(message.chat.id,
                             "Пожалуйста, ответьте на сообщение пользователя, которого вы хотите замучить.")




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
            bot.restrict_chat_member(chat_id, user_id,
                                     can_send_messages=True,
                                     can_send_media_messages=True,
                                     can_send_other_messages=True,
                                     can_add_web_page_previews=True,
                                     can_pin_messages=True
                                     )

            # Удаляем информацию из базы данных
            conn = sqlite3.connect('baza.sql')
            cursor = conn.cursor()
            cursor.execute('DELETE FROM banned_users WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
            conn.commit()
            conn.close()

            bot.send_message(chat_id, f"Пользователь {message.reply_to_message.from_user.username} размучен.")
        else:
            bot.send_message(message.chat.id,
                             "Пожалуйста, ответьте на сообщение пользователя, которого вы хотите размучить.")

bot.polling(non_stop=True)