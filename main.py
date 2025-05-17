import logging
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import aiohttp

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота
BOT_TOKEN = '7757580544:AAHMXO0sgFFvNJMIDksbxqc9zYHrNNGo-rA'

# Клавиатура с кнопками
reply_keyboard = [
    ["/fact", "/help"],
    ["/start", "/stop"]
]
markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)

async def get_random_fact():
    url = 'https://uselessfacts.jsph.pl/random.json?language=en'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('text')
                return 'Не удалось получить факт 😞 (ошибка API)'
    except Exception as e:
        logger.error(f"Error fetching fact: {e}")
        return 'Не удалось получить факт 😞 (ошибка соединения)'

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'Привет! Я бот с интересными фактами. Выбери действие:',
        reply_markup=markup
    )

async def fact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    fact_text = await get_random_fact()
    await update.message.reply_text(fact_text, reply_markup=markup)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Доступные команды:\n"
        "/start - начать работу\n"
        "/fact - случайный факт\n"
        "/help - помощь",
        reply_markup=markup
    )

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "До свидания! Нажмите /start чтобы начать снова.",
        reply_markup=ReplyKeyboardMarkup([["/start"]], resize_keyboard=True)
    )

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("fact", fact))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stop", stop))

    logger.info("Бот запущен...")
    app.run_polling()

if __name__ == '__main__':
    main()