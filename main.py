import json
import logging
import textwrap

import requests
from telegram import Update, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from ai_request_processor import AiRequestProcessor
from database_query_parser import DbQueryParser
from db_response_parser import DbResponseParser
from database import Database

DB_CONFIG = {
    "dbname": "interesich",
    "user": "cock_userr",
    "password": "ifconfigroute-3n",
    "host": "51.250.112.217",
    "port": "5432"
}

db = Database(**DB_CONFIG)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Убедитесь, что Rasa NLU сервер запущен на этом адресе и порту
ai_request_processor = AiRequestProcessor(base_url="http://localhost:5005/model/parse")

BOT_TOKEN = '7757580544:AAHMXO0sgFFvNJMIDksbxqc9zYHrNNGo-rA'  # Ваш токен

reply_keyboard = [['/help']]
markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Добро пожаловать! Я помогу вам найти информацию о сотрудниках, мероприятиях, задачах и днях рождения. Нажмите /help, чтобы узнать больше.",
        reply_markup=markup
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
Вы можете спросить меня:
- "Найди Иванова Петра"
- "Какие мероприятия завтра?"
- "У кого день рождения в июне?"
- "Мои задачи на сегодня"
- "Свободен ли я завтра в 10?"

Просто напишите ваш вопрос в свободной форме!
    """
    await update.message.reply_text(
        textwrap.dedent(help_text),
        reply_markup=markup
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    logger.info(f"Получен запрос от пользователя: {text}")
    try:
        ai_response = ai_request_processor.process_query(text)
        logger.info(f"Ответ от NLU: {json.dumps(ai_response, ensure_ascii=False, indent=2)}")

        query_data = DbQueryParser.parse(ai_response)

        sql_query = None
        query_params = None

        if isinstance(query_data, tuple):
            sql_query, query_params = query_data
        else:  # Для обратной совместимости со старыми методами, возвращающими только строку
            sql_query = query_data
            query_params = None

        logger.info(f"Сформирован SQL: {sql_query} с параметрами: {query_params}")

        db_result = db.execute_query(query=sql_query, params=query_params, fetch=True)

        if not db_result:  # Проверяем, если результат пустой или None
            message = "По вашему запросу ничего не найдено."
            logger.info("БД не вернула результатов.")
        else:
            logger.info(f"Результат из БД: {db_result}")
            message = DbResponseParser.parse_into_message(db_result)

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при обращении к Rasa NLU API: {e}")
        message = "Извините, не удалось связаться с сервисом распознавания. Попробуйте позже."
    except ValueError as e:  # Для ошибок парсинга или отсутствия данных
        logger.warning(f"Ошибка обработки запроса: {e}")
        message = f"Не удалось обработать ваш запрос: {e}"
    except Exception as e:
        logger.exception(f"Произошла непредвиденная ошибка: {e}")  # Логируем полный стектрейс
        message = "Произошла внутренняя ошибка. Пожалуйста, попробуйте позже."

    await update.message.reply_text(message, reply_markup=markup, parse_mode=ParseMode.HTML)


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