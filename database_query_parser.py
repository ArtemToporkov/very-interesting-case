import datetime
from psycopg2 import sql

# Словарь для преобразования названий месяцев и их форм в номера
MONTH_NAME_TO_NUMBER = {
    "январь": 1, "января": 1, "январе": 1,
    "февраль": 2, "февраля": 2, "феврале": 2,
    "март": 3, "марта": 3, "марте": 3,
    "апрель": 4, "апреля": 4, "апреле": 4,
    "май": 5, "мая": 5, "мае": 5,
    "июнь": 6, "июня": 6, "июне": 6,
    "июль": 7, "июля": 7, "июле": 7,
    "август": 8, "августа": 8, "августе": 8,
    "сентябрь": 9, "сентября": 9, "сентябре": 9,
    "октябрь": 10, "октября": 10, "октябре": 10,
    "ноябрь": 11, "ноября": 11, "ноябре": 11,
    "декабрь": 12, "декабря": 12, "декабре": 12,
}


class DbQueryParser:
    @staticmethod
    def parse(data: dict):  # -> Union[str, Tuple[sql.Composed, list]]: # Python 3.9 needs Tuple from typing
        intent_name = data.get("intent", {}).get("name")
        if not intent_name:
            raise ValueError("Intent name is missing in NLU data")

        match intent_name:
            case "search_person":
                return DbQueryParser.search_person(data)
            case "search_event":
                return DbQueryParser.search_event(data)  # This still needs get_date and parameterization
            case "find_birthday":
                return DbQueryParser.find_birthday(data)
            case "check_task":
                return DbQueryParser.check_task(data)
            # Добавьте сюда другие интенты, если они будут

        raise ValueError(f"Неизвестный интент: {intent_name}")

    @staticmethod
    def search_person(data: dict) -> str:
        # ЭТОТ МЕТОД ВСЕ ЕЩЕ ИСПОЛЬЗУЕТ ФОРМАТИРОВАНИЕ СТРОК - РЕКОМЕНДУЕТСЯ РЕФАКТОРИНГ
        for entity in data.get('entities', []):
            if entity.get('entity') == 'name':
                # SQL-инъекция возможна здесь!
                return f'''
                    SELECT 
                        'PersonInfo',
                        emp."Surname", 
                        emp."Name", 
                        emp."Father", 
                        emp."Birthday", 
                        emp."FirstDay", 
                        lng."Name" as "LanguageName", 
                        rnk."Status" as "RankStatus",
                        prj."Name" as "ProjectName",
                        dprt."Name" as "DepartmentName",
                        emp."Contacts"
                    FROM "Employees" as emp
                    LEFT JOIN "Languages" as lng ON lng."Language_Id"=emp."LanguageId"
                    LEFT JOIN "Rank" as rnk ON rnk."Rank_Id" = emp."RankId"
                    LEFT JOIN "Project" as prj ON prj."Project_Id"=emp."ProjectId"
                    LEFT JOIN "Department" as dprt ON dprt."Department_Id"=emp."DepartmentId"
                    WHERE emp."Name" ILIKE '%{entity['value']}%' OR emp."Surname" ILIKE '%{entity['value']}%'
                    LIMIT 1;
                    '''
        # Возвращаем пустую строку или специфический запрос, если имя не найдено,
        # чтобы избежать ошибки в main.py, если интент search_person, но нет имени.
        # Либо можно выбросить исключение, которое будет обработано в main.py.
        # Для консистентности с find_birthday, лучше выбрасывать исключение, если нет ключевых данных.
        raise ValueError("Сущность 'name' не найдена для search_person")

    @staticmethod
    def search_event(data: dict) -> str:
        # ЭТОТ МЕТОД ТРЕБУЕТ get_date() И ПАРАМЕТРИЗАЦИИ
        entities: list = data.get('entities', [])
        keywords = DbQueryParser._entities_to_dict(entities)

        where_clauses = []
        if 'event_name' in keywords:
            # SQL-инъекция возможна здесь!
            where_clauses.append(f"""ev."Name" ILIKE '%{keywords['event_name'][0]}%'""")
        if 'date' in keywords:
            # Здесь должна быть логика get_date и корректное форматирование даты
            # Пока что это заглушка, предполагающая, что get_date вернет строку YYYY-MM-DD
            # date_str = DbQueryParser.get_date_placeholder(keywords['date'][0])
            # where_clauses.append(f"""ev."Begin" >= '{date_str}'""") # Пример
            where_clauses.append("1=1")  # Заглушка, чтобы запрос был валидным

        # if 'location' in keywords: ...
        # if 'event_category' in keywords: ...

        query_str = """
            SELECT 'EventInfo', ev."Name", ev."Begin", ev."Duration", cat."Name" as "CategoryName"
            FROM "Events" as ev
            LEFT JOIN "Categories" as cat ON cat."Category_Id" = ev."CategoryId"
        """
        if where_clauses:
            query_str += " WHERE " + " AND ".join(where_clauses)
        query_str += " LIMIT 5;"

        print(f"[DEBUG] search_event SQL (needs proper date handling & parameterization): {query_str}")
        return query_str

    @staticmethod
    def _get_month_day_from_specifier(specifier_value: str) -> tuple[int | None, int | None, str | None]:
        """ Пытается извлечь месяц и день из текстового спецификатора.
            Возвращает (месяц, день,特殊SQLусловие)
        """
        specifier_lower = specifier_value.lower()

        # "сегодня", "завтра"
        if specifier_lower == "сегодня":
            return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE, 'MM-DD')"
        if specifier_lower == "завтра":
            return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE + INTERVAL '1 day', 'MM-DD')"

        # "на этой неделе" - сложно для годовой повторяемости, лучше на уровне приложения или более сложный SQL
        # "в этом месяце"
        if specifier_lower == "в этом месяце":
            return datetime.date.today().month, None, None

        # "в <месяц>"
        for month_name, month_number in MONTH_NAME_TO_NUMBER.items():
            if month_name in specifier_lower:
                # Попробуем найти число (день) перед или после месяца
                parts = specifier_lower.replace(month_name, "").strip().split()
                day = None
                for part in parts:
                    if part.isdigit():
                        day = int(part)
                        break
                return month_number, day, None

        # Простые сезоны (примерная реализация)
        if "зимой" in specifier_lower or "зима" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (12, 1, 2)"
        if "весной" in specifier_lower or "весна" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (3, 4, 5)"
        if "летом" in specifier_lower or "лето" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (6, 7, 8)"
        if "осенью" in specifier_lower or "осень" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (9, 10, 11)"

        return None, None, None  # Не удалось распознать

    @staticmethod
    def find_birthday(data: dict):  # -> Tuple[sql.Composed, list]
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))

        select_fields = [
            sql.SQL('emp."Surname"'),
            sql.SQL('emp."Name"'),
            sql.SQL('emp."Father"'),
            sql.SQL('emp."Birthday"'),
            sql.SQL('dprt."Name"').AS('department_name'),
            # sql.SQL('prj."Name"').AS('project_name') # Если нужно будет фильтровать/отображать проект
        ]
        from_table = sql.SQL('FROM "Employees" as emp')
        joins = [
            sql.SQL('LEFT JOIN "Department" as dprt ON dprt."Department_Id" = emp."DepartmentId"'),
            # sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = emp."ProjectId"')
        ]
        where_clauses = []
        params = []

        # Обработка birthday_specifier и date
        # Приоритет отдается birthday_specifier, если есть оба
        date_entity_values = entities.get('date', [])
        birthday_specifiers = entities.get('birthday_specifier',
                                           date_entity_values)  # Используем date, если нет specifier

        processed_date_condition = False
        if birthday_specifiers:
            # Берем первый попавшийся спецификатор/дату для простоты
            # В более сложной системе можно было бы обрабатывать диапазоны и т.д.
            specifier_val = birthday_specifiers[0]
            month, day, special_sql = DbQueryParser._get_month_day_from_specifier(specifier_val)
            if special_sql:
                where_clauses.append(sql.SQL(special_sql))
                processed_date_condition = True
            elif month:
                where_clauses.append(sql.SQL('EXTRACT(MONTH FROM emp."Birthday") = %s'))
                params.append(month)
                if day:
                    where_clauses.append(sql.SQL('EXTRACT(DAY FROM emp."Birthday") = %s'))
                    params.append(day)
                processed_date_condition = True

        # Если ни один спецификатор/дата не обработан, и есть общие запросы без даты
        # (например, "дни рождения в отделе маркетинга" - это на весь год), то условие по дате не добавляем.
        # Если интент find_birthday, но нет даты, это может быть запрос на ДР конкретного человека.
        # if not processed_date_condition and not entities.get('name'):
        #     raise ValueError("Не указан период или дата для поиска дней рождения.")

        if 'department' in entities:
            # Проверяем, есть ли уже JOIN для Department, чтобы не дублировать
            if not any('dprt ON' in str(j) for j in joins):  # Простая проверка
                joins.append(sql.SQL('LEFT JOIN "Department" as dprt ON dprt."Department_Id" = emp."DepartmentId"'))
            where_clauses.append(sql.SQL('dprt."Name" ILIKE %s'))
            params.append(f"%{entities['department'][0]}%")

        # if 'project' in entities:
        #     if not any('prj ON' in str(j) for j in joins):
        #         joins.append(sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = emp."ProjectId"'))
        #     where_clauses.append(sql.SQL('prj."Name" ILIKE %s'))
        #     params.append(f"%{entities['project'][0]}%")

        if 'name' in entities:
            name_val = entities['name'][0]
            # Если имя содержит пробел, скорее всего это Имя + Фамилия или наоборот
            if ' ' in name_val:
                parts = name_val.split(' ', 1)
                where_clauses.append(sql.SQL(
                    '((emp."Name" ILIKE %s AND emp."Surname" ILIKE %s) OR (emp."Name" ILIKE %s AND emp."Surname" ILIKE %s))'
                ))
                params.extend([f"%{parts[0]}%", f"%{parts[1]}%", f"%{parts[1]}%", f"%{parts[0]}%"])
            else:  # Иначе ищем по имени или фамилии
                where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
                params.extend([f"%{name_val}%", f"%{name_val}%"])

        if 'age_older_than' in entities:
            try:
                age = int(entities['age_older_than'][0])
                where_clauses.append(sql.SQL('date_part(\'year\', age(emp."Birthday")) > %s'))
                params.append(age)
            except ValueError:
                print(f"Warning: could not parse age_older_than value: {entities['age_older_than'][0]}")

        if 'age_younger_than' in entities:
            try:
                age = int(entities['age_younger_than'][0])
                where_clauses.append(sql.SQL('date_part(\'year\', age(emp."Birthday")) < %s'))
                params.append(age)
            except ValueError:
                print(f"Warning: could not parse age_younger_than value: {entities['age_younger_than'][0]}")

        if not where_clauses:  # Если нет условий, то запрос может вернуть слишком много.
            raise ValueError("Недостаточно критериев для поиска дней рождения.")

        query_parts = [sql.SQL("SELECT 'BirthdayList',")]
        query_parts.extend(select_fields)
        query_parts.append(from_table)
        query_parts.extend(joins)

        query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL(
            'ORDER BY EXTRACT(MONTH FROM emp."Birthday"), EXTRACT(DAY FROM emp."Birthday"), emp."Surname", emp."Name" LIMIT 10'))

        final_query = sql.SQL(' ').join(query_parts)
        return final_query, params

    @staticmethod
    def check_task(data: dict):  # -> Tuple[sql.Composed, list]
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))

        select_fields = [
            sql.SQL('tsk."Name"').AS('task_name'),
            sql.SQL('tsk."Description"'),
            sql.SQL('tsk."Deadline"'),
            sql.SQL('tsk."Status"'),
            sql.SQL('tsk."Priority"'),
            sql.SQL('emp_assignee."Name"').AS('assignee_name'),
            sql.SQL('emp_assignee."Surname"').AS('assignee_surname'),
            sql.SQL('prj."Name"').AS('project_name')
        ]
        from_table = sql.SQL('FROM "Tasks" as tsk')  # Предполагаем, что таблица Tasks
        joins = [
            sql.SQL('LEFT JOIN "Employees" as emp_assignee ON emp_assignee."Employee_Id" = tsk."AssigneeId"'),
            # Предполагаем AssigneeId
            sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = tsk."ProjectId"')  # Предполагаем ProjectId
        ]
        where_clauses = []
        params = []

        # Если есть "мои", ищем задачи текущего пользователя (потребуется ID пользователя, здесь не доступен)
        # Пока что, если есть name, ищем по нему. Если "мои", это нужно обработать на более высоком уровне
        # или передать user_id в этот парсер.
        if 'name' in entities:
            name_val = entities['name'][0]
            if name_val.lower() in ["мои", "меня", "я", "мне"]:  # Обработка "моих" задач
                # Здесь нужна логика для получения ID текущего пользователя.
                # Для примера, поставим заглушку или пропустим, если ID не доступен.
                # where_clauses.append(sql.SQL('tsk."AssigneeId" = %s'))
                # params.append(current_user_id) # current_user_id должен быть передан
                print("Warning: 'мои' задачи требуют ID текущего пользователя, который здесь не доступен.")
            else:  # Поиск по имени сотрудника
                if ' ' in name_val:
                    parts = name_val.split(' ', 1)
                    where_clauses.append(sql.SQL(
                        '((emp_assignee."Name" ILIKE %s AND emp_assignee."Surname" ILIKE %s) OR (emp_assignee."Name" ILIKE %s AND emp_assignee."Surname" ILIKE %s))'
                    ))
                    params.extend([f"%{parts[0]}%", f"%{parts[1]}%", f"%{parts[1]}%", f"%{parts[0]}%"])
                else:
                    where_clauses.append(sql.SQL('(emp_assignee."Name" ILIKE %s OR emp_assignee."Surname" ILIKE %s)'))
                    params.extend([f"%{name_val}%", f"%{name_val}%"])

        if 'project' in entities:
            where_clauses.append(sql.SQL('prj."Name" ILIKE %s'))
            params.append(f"%{entities['project'][0]}%")

        if 'date' in entities:  # дедлайн
            # Нужна более умная обработка дат ("на сегодня", "на эту неделю")
            # Для примера, если дата в формате YYYY-MM-DD
            try:
                # Предположим, что NLU возвращает дату в каком-то формате, который можно преобразовать
                # Это очень упрощенно. Duckling бы дал структурированную дату.
                date_str = entities['date'][0]  # Например, "сегодня"
                if date_str == "сегодня":
                    where_clauses.append(sql.SQL('tsk."Deadline"::date = CURRENT_DATE'))
                elif date_str == "на этот месяц":
                    where_clauses.append(sql.SQL(
                        'EXTRACT(MONTH FROM tsk."Deadline") = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM tsk."Deadline") = EXTRACT(YEAR FROM CURRENT_DATE)'))

                # Добавить другие варианты обработки date
            except Exception as e:
                print(f"Error parsing date for task: {e}")

        if 'task_status' in entities:
            where_clauses.append(sql.SQL('tsk."Status" ILIKE %s'))
            params.append(f"%{entities['task_status'][0]}%")

        if 'task_priority' in entities:
            # Может потребоваться маппинг ("высокий" -> High)
            where_clauses.append(sql.SQL('tsk."Priority" ILIKE %s'))
            params.append(f"%{entities['task_priority'][0]}%")

        if 'task_tag' in entities:
            # Если теги в отдельной таблице, нужен JOIN
            # Пока предположим, что тег это текстовое поле в Tasks
            where_clauses.append(sql.SQL('tsk."Tags" ILIKE %s'))  # Предполагаем поле "Tags"
            params.append(f"%{entities['task_tag'][0]}%")

        if 'task_name' in entities:
            where_clauses.append(sql.SQL('tsk."Name" ILIKE %s'))
            params.append(f"%{entities['task_name'][0]}%")

        if not where_clauses:
            raise ValueError("Недостаточно критериев для поиска задач.")

        query_parts = [sql.SQL("SELECT 'TaskList',")]
        query_parts.extend(select_fields)
        query_parts.append(from_table)
        query_parts.extend(joins)

        query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL('ORDER BY tsk."Deadline", tsk."Priority" DESC LIMIT 10'))

        final_query = sql.SQL(' ').join(query_parts)
        return final_query, params

    @staticmethod
    def _entities_to_dict(entities: list) -> dict:
        result = {}
        if not entities:  # Проверка, что entities не None и не пустой
            return result
        for entity in entities:
            entity_type = entity.get("entity")
            entity_value = entity.get("value")
            if entity_type and entity_value is not None:  # Проверка, что ключи существуют
                if entity_type not in result:
                    result[entity_type] = [entity_value]
                else:
                    result[entity_type].append(entity_value)
        return result