import config
from aiogram import Bot, types, executor, Dispatcher



ADMIN_ID = '2123680656'
bot = Bot(token=config.token)
dp = Dispatcher(bot)

@db.message_handler()
async def echobot(message: types.Message):
    await message.answer(message.text)

if __name__ == '__main__':
    executor.start_polling(db)








@bot.message_handler(commands=['mute'])
async def mute(message: types.Message):
    if str(message.from_user.id) == help.ADMIN_ID:
        if not message.reply_to_message:
            await message.reply('👀 Пользователь не указан \nИзпользуйте команду ответом на нужное сообщение')
        return
    mute_sec = int(message.text[6:])
    db.add_mute(message.from_user.id, mute_sec)
    await message.delete()
    await message.reply_to_message.reply(f"👀 Пользователь был замучен на {mute_sec} секунд")

@bot.message_handler(content_types=['text'])
async def filter_mes(message: types.Message):
    if message.text.lower() in words:
        await message.delete()

    if not bot.examination(message.from_user.id):
        bot.add(message.from_user.id)

    if not bot.mute(message.from_user.id):
        print("/")
    else:
        await message.delete()
    asyncio.run(filter_mes())


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

    def mute (self, user_id, mute_time):
        with self.connection:
            user = self.connection.execute("SELECT id FROM users where id = ?", (user_id,)).fetchall()
            return int(user[2]) >= int(time.time())

    def add_mute(self, user_id, mute_time):
        with self.connection:
            return self.connection.execute("UPDATE users SET mute_time = ? WHERE id = ?", (int(time.time()) +mute_time, user_id))





async def info(message):
    if message.text.lower() == 'мут':
        if str(message.from_user.id) == help.ADMIN_ID:
            if not message.reply_to_message:
                await message.reply('👀 Пользователь не указан \nИзпользуйте команду ответом на нужное сообщение')
            return
        mute_sec = int(message.text[6:])
        bot.add_mute(message.from_user.id, mute_sec)
        await message.delete()
        await message.reply_to_message.reply(f"👀 Пользователь был замучен на {mute_sec} секунд")
