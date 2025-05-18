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
from text_normalizer import lemmatize_entity_value  # <--- ИЗМЕНИЛИ ИМПОРТ

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

ai_request_processor = AiRequestProcessor(base_url="http://localhost:5005/model/parse")

BOT_TOKEN = '7947739921:AAG5dl3g0nLRIftfQJYSq4vrZcwoWS805ks'

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
    original_text = update.message.text
    logger.info(f"Получен оригинальный запрос от пользователя: {original_text}")

    try:
        # Шаг 1: Отправляем ОРИГИНАЛЬНЫЙ текст в Rasa NLU
        ai_response = ai_request_processor.process_query(original_text)
        logger.info(
            f"Ответ от NLU (на основе оригинального текста): {json.dumps(ai_response, ensure_ascii=False, indent=2)}")

        # Шаг 2: Лемматизация значений ВЫБРАННЫХ сущностей перед передачей в DbQueryParser
        # Это делается, если EntitySynonymMapper в Rasa не покрывает все случаи или
        # если DbQueryParser ожидает строго лемматизированные значения для определенных полей.

        entities_from_nlu = ai_response.get('entities', [])

        # Список типов сущностей, значения которых мы хотим лемматизировать.
        # ВАЖНО: Тщательно выберите, какие сущности лемматизировать.
        # Имена, названия проектов, даты, числовые значения обычно НЕ лемматизируют.
        # EntitySynonymMapper в Rasa — предпочтительный способ канонизации.
        # Эта лемматизация здесь — как дополнительный слой, если он действительно нужен.
        entity_types_to_lemmatize_values = {
            "department",  # "отдела разработки" -> "отдел разработка"
            "skill",  # "на Python" -> "на python" (если EntitySynonymMapper не настроен на каноническую форму "python")
            "event_category",  # "корпоративные тренинги" -> "корпоративный тренинг"
            "birthday_specifier", # Например, "в июне". Логика парсинга дат в DbQueryParser может быть чувствительна. Осторожно!
            "task_status",        # Обычно это уже ключевые слова "невыполненные", "в процессе"
            "task_priority",      # "высокий", "низкий"
            "task_tag",
            "name"
        }

        processed_entities_for_parser = []
        if entities_from_nlu:
            for entity_data in entities_from_nlu:
                entity_type = entity_data.get('entity')
                original_value = entity_data.get('value')

                # Копируем сущность, чтобы не изменять оригинальный ai_response напрямую, если он используется где-то еще
                current_entity = entity_data.copy()

                if entity_type in entity_types_to_lemmatize_values and original_value:
                    lemmatized_val = lemmatize_entity_value(original_value)
                    if lemmatized_val != original_value:  # Логируем, если было изменение
                        logger.info(
                            f"Лемматизация значения сущности типа '{entity_type}': '{original_value}' -> '{lemmatized_val}'")
                    current_entity['value'] = lemmatized_val

                processed_entities_for_parser.append(current_entity)

        # Создаем новый объект для DbQueryParser, чтобы передать обработанные сущности
        # и сохранить оригинальную структуру ответа NLU
        payload_for_db_parser = {
            "text": ai_response.get("text"),  # Это оригинальный текст пользователя
            "intent": ai_response.get("intent"),
            "entities": processed_entities_for_parser,  # Здесь лемматизированные значения для выбранных типов
            "intent_ranking": ai_response.get("intent_ranking"),
            "response_selector": ai_response.get("response_selector")
            # Можно добавить 'original_user_text': original_text, если нужно где-то дальше
        }
        # --- КОНЕЦ: Лемматизация значений сущностей ---

        query_data = DbQueryParser.parse(payload_for_db_parser)

        sql_query = None
        query_params = None

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
    await update.message.reply_text(str(sql_query), reply_markup=markup, parse_mode=ParseMode.HTML)
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