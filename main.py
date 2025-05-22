import os
import json
import logging
import textwrap

from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher

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
from database import Database
from database_query_parser import DbQueryParser
from db_response_parser import DbResponseParser
from text_normalizer import lemmatize_entity_value

DB_CONFIG_EXAMPLE = {
    "dbname": "interesich",
    "user": "cock_userr",
    "password": "ifconfigroute-3n",
    "host": "51.250.112.217",
    "port": "5432"
}

db = Database(**DB_CONFIG_EXAMPLE)

allowed_telegram_ids = db.execute_query(f"select * from \"Authentication\"", fetch=True)
allowed_telegram_ids = [i[0] for i in allowed_telegram_ids]

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

ai_request_processor = AiRequestProcessor(base_url="http://localhost:5005/model/parse")

BOT_TOKEN = os.environ.get("BOT_TOKEN")

reply_keyboard = [['/help']]
markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    await update.message.reply_text(
        f"""
Добро пожаловать! Я помогу вам найти информацию о сотрудниках, мероприятиях, задачах и днях рождения. 
Нажмите /help, чтобы узнать больше.

{"Вы авторизованы как сотрудник" if user_id in allowed_telegram_ids else "Вы авторизованы как гость."}
""",
        reply_markup=markup
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
Вы можете спросить меня:
- "Найди Волкова Андрея"
- "Какие мероприятия на неделе?"
- "У кого день рождения в июне?"
- "Задачи Алексея"

Просто напишите ваш вопрос в свободной форме!
    """
    await update.message.reply_text(
        textwrap.dedent(help_text),
        reply_markup=markup
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    original_text = update.message.text
    db.ensure_connection()

    logger.info(f"Получен оригинальный запрос от пользователя: {original_text}")

    try:
        ai_response = ai_request_processor.process_query(original_text)
        intent_name = ai_response.get("intent", {}).get("name")
        if intent_name == 'greet':
            await update.message.reply_text("Привет! Задай вопрос и я что-нибудь найду.", reply_markup=markup,
                                            parse_mode=ParseMode.HTML)
            return
        if intent_name == 'goodbye':
            await update.message.reply_text("Пока! Очень жаль с тобой прощаться...", reply_markup=markup,
                                            parse_mode=ParseMode.HTML)
            return
        if intent_name == 'affirm':
            await update.message.reply_text("Ты что-то подтвердил.", reply_markup=markup, parse_mode=ParseMode.HTML)
            return
        if intent_name == 'deny':
            await update.message.reply_text("Ты что-то отклонил.", reply_markup=markup, parse_mode=ParseMode.HTML)
            return

        logger.info(
            f"Ответ от NLU (на основе оригинального текста): {json.dumps(ai_response, ensure_ascii=False, indent=2)}"
        )

        entities_from_nlu = ai_response.get('entities', [])
        entity_types_to_lemmatize_values = {
            "department",
            "skill",
            "event_category",
            "birthday_specifier",
            "task_status",
            "task_priority",
            "task_tag",
            "name"
        }

        processed_entities_for_parser = []
        if entities_from_nlu:
            for entity_data in entities_from_nlu:
                entity_type = entity_data.get('entity')
                original_value = entity_data.get('value')
                current_entity = entity_data.copy()

                if entity_type in entity_types_to_lemmatize_values and original_value:
                    lemmatized_val = lemmatize_entity_value(original_value)
                    if lemmatized_val != original_value:
                        logger.info(
                            f"Лемматизация значения сущности типа '{entity_type}': '{original_value}' -> '{lemmatized_val}'")
                    current_entity['value'] = lemmatized_val

                processed_entities_for_parser.append(current_entity)

        payload_for_db_parser = {
            "text": ai_response.get("text"),
            "intent": ai_response.get("intent"),
            "entities": processed_entities_for_parser,
            "intent_ranking": ai_response.get("intent_ranking"),
            "response_selector": ai_response.get("response_selector")
        }

        query_data = DbQueryParser.parse(payload_for_db_parser)

        if isinstance(query_data, tuple):
            sql_query, query_params = query_data
        else:
            sql_query = query_data
            query_params = None

        logger.info(f"Сформирован SQL: {sql_query} с параметрами: {query_params}")

        db_result = db.execute_query(query=sql_query, params=query_params, fetch=True)

        if not db_result:
            message = "По вашему запросу ничего не найдено."
            logger.info("БД не вернула результатов.")
        else:
            logger.info(f"Результат из БД: {db_result}")
            message = DbResponseParser.parse_into_message(db_result)

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при обращении к Rasa NLU API: {e}")
        message = "Извините, не удалось связаться с сервисом распознавания. Попробуйте позже."
    except ValueError as e:
        logger.warning(f"Ошибка обработки запроса: {e}")
        message = f"Не удалось обработать ваш запрос: {e}"
    except Exception as e:
        logger.exception(f"Произошла непредвиденная ошибка: {e}")
        message = "Произошла внутренняя ошибка. Пожалуйста, попробуйте позже."

    # await update.message.reply_text(str(sql_query), reply_markup=markup, parse_mode=ParseMode.HTML)
    await update.message.reply_text(message, reply_markup=markup, parse_mode=ParseMode.HTML)


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Бот запущен...")
    app.run_polling()


if __name__ == '__main__':
    main()
