import sqlite3
import time
name = None





conn = sqlite3.connect('baza.sql')
    cur = conn.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS users (id int auto_increment primary key, name varchar(50), pass varchar(50))')
    conn.commit()
    cur.close()
    conn.close()

    bot.send_message(message.chat.id, 'ща тя зарегаем давай имя')
    bot.register_next_step_handler(message, user_name)

def user_name(message):
    global name
    name = message.text.strip()
    bot.send_message(message.chat.id, 'ща тя зарегаем давай пароль')
    bot.register_next_step_handler(message, user_pass)

def user_pass(message):
    password = message.text.strip()

    conn = sqlite3.connect('baza.sql')
    cur = conn.cursor()

    cur.execute(f'INSERT INTO users (name, pass) VALUES ("%s","%s")' % (name, password))
    conn.commit()
    cur.close()
    conn.close()

    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton('зАРЕГЕСТРИРОВАННЫЕ ПОЛЬЗОВАТЕЛИ', callback_data='users'))
    bot.send_message(message.chat.id, 'Пользователь зарегестрирован!', reply_markup=markup )

@bot.callback_query_handler(func=lambda call: True)
def callback(call):
    conn = sqlite3.connect('baza.sql')
    cur = conn.cursor()

    cur.execute('select * from users')
    users = cur.fetchall()

    info = ''
    for el in users:
        info += f'Имя: {el[1]}, пароль: {el[2]}\n'

    cur.close()
    conn.close()




    @bot.message_handler(commands=['mute'])
    def mute_user(message):
        if message.chat.type in ['group', 'supergroup']:
            user_id = message.reply_to_message.from_user.id
            chat_id = message.chat.id

            # Замучиваем пользователя
            bot.restrict_chat_member(chat_id, user_id, can_send_messages=False)

            # Вносим информацию в базу данных
            conn = sqlite3.connect('baza.sql')
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO banned_users (user_id, chat_id) VALUES (?, ?)', (user_id, chat_id))
            conn.commit()
            conn.close()
            bot.send_message(chat_id, f"Пользователь {message.reply_to_message.from_user.username} замучен.")