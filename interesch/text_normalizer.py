import string
import pymorphy3

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

    text_lower = text_value.lower()
    translator = str.maketrans('', '', string.punctuation)
    text_without_punctuation = text_lower.translate(translator)
    words = text_without_punctuation.split()

    lemmatized_words = []
    for word in words:
        if not word.strip():
            continue

        parsed_word = morph.parse(word)[0]
        normal_form = parsed_word.normal_form
        lemmatized_words.append(normal_form)

    return " ".join(lemmatized_words)


if __name__ == '__main__':
    test_values = [
        "отдела разработки",
        "корпоративные тренинги",
        "на Python",
        "в июне",
        "Иванова Петра",
        "Проект Альфа",
        "20 мая",
        "на этой неделе",
        "Мои невыполненные задачи",
        "завтра в 10 утра"
    ]

    for value in test_values:
        lemmatized = lemmatize_entity_value(value)
        print(f"Оригинал: '{value}'")
        print(f"Лемматизировано: '{lemmatized}'")
        print("-" * 30)

    print(f"Оригинал: 'IT отдел'")
    print(f"Лемматизировано: '{lemmatize_entity_value('IT отдел')}'")
