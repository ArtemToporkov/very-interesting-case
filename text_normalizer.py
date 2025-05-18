import pymorphy3
import string

morph = pymorphy3.MorphAnalyzer()


def lemmatize_entity_value(text_value: str) -> str:
    """
    Лемматизирует значение сущности (фразу):
    1. Приводит к нижнему регистру.
    2. Удаляет пунктуацию.
    3. Токенизирует.
    4. Лемматизирует каждое слово.
    Возвращает строку с лемматизированными словами, разделенными пробелами.
    Стоп-слова НЕ удаляются.
    """
    if not text_value:
        return ""

    # 1. Приведение к нижнему регистру
    text_lower = text_value.lower()

    # 2. Удаление пунктуации (можно сделать опциональным, если пунктуация важна для каких-то значений)
    # Для большинства значений сущностей, передаваемых в SQL, удаление пунктуации безопасно или полезно.
    translator = str.maketrans('', '', string.punctuation)
    text_without_punctuation = text_lower.translate(translator)

    # 3. Токенизация
    words = text_without_punctuation.split()

    lemmatized_words = []
    for word in words:
        if not word.strip():  # Пропускаем пустые слова
            continue

        parsed_word = morph.parse(word)[0]  # Берем первый (наиболее вероятный) разбор
        normal_form = parsed_word.normal_form
        lemmatized_words.append(normal_form)

    return " ".join(lemmatized_words)


if __name__ == '__main__':
    # Примеры использования
    test_values = [
        "отдела разработки",
        "корпоративные тренинги",
        "на Python",
        "в июне",
        "Иванова Петра",  # Имена лучше не лемматизировать так агрессивно
        "Проект Альфа",  # Названия проектов тоже
        "20 мая",  # Даты
        "на этой неделе",
        "Мои невыполненные задачи",  # Для примера, хотя это скорее фраза из запроса
        "завтра в 10 утра"
    ]

    for value in test_values:
        lemmatized = lemmatize_entity_value(value)
        print(f"Оригинал: '{value}'")
        print(f"Лемматизировано: '{lemmatized}'")
        print("-" * 30)

    print(f"Оригинал: 'IT отдел'")  # Пример, где EntitySynonymMapper Rasa может быть лучше
    print(f"Лемматизировано: '{lemmatize_entity_value('IT отдел')}'")  # -> it отдел