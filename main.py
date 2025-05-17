import logging
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from database import Database


DB_CONFIG = {
        "dbname": "interesnich@localhost",
        "user": "postgres",
        "password": "123",
        "host": "localhost",
        "port": "5432"
    }

db = Database(**DB_CONFIG)


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = '7757580544:AAHMXO0sgFFvNJMIDksbxqc9zYHrNNGo-rA'

reply_keyboard = [['/help']]
markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добро пожаловать! Нажмите /help, чтобы узнать, что я умею.",
        reply_markup=markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Отправтье запрос, а я что-нибудь найду.",
        reply_markup=markup
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    # TODO: распарсить запрос, кинуть запрос в нейронку
    result = text[::-1]
    await update.message.reply_text(result, reply_markup=markup)

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
    )

    logger.info("Бот запущен...")
    app.run_polling()

if __name__ == '__main__':
    main()
