# Файл: database_query_parser.py
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
    def parse(data: dict):
        intent_name = data.get("intent", {}).get("name")
        if not intent_name:
            raise ValueError("Intent name is missing in NLU data")

        match intent_name:
            case "search_person":
                # Этот метод все еще возвращает строку, хорошо бы переделать
                query_str = DbQueryParser.search_person_str(data)
                return query_str, None  # Возвращаем как кортеж для единообразия
            case "search_event":
                return DbQueryParser.search_event(data)  # ИЗМЕНЕНО
            case "find_birthday":
                return DbQueryParser.find_birthday(data)
            case "check_task":
                return DbQueryParser.check_task(data)
        raise ValueError(f"Неизвестный интент: {intent_name}")

    @staticmethod
    def search_person_str(data: dict) -> str:
        for entity in data.get('entities', []):
            if entity.get('entity') == 'name':
                # ОСТОРОЖНО: SQL-инъекция! Этот метод должен быть переписан с использованием параметризации.
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
        raise ValueError("Сущность 'name' не найдена для search_person")

    @staticmethod
    def _parse_event_date_entity(date_entity_value: str) -> tuple[
        datetime.date | None, datetime.date | None, str | None]:
        """
        Парсит значение сущности 'date' для событий.
        Возвращает (start_date, end_date, special_condition_sql)
        special_condition_sql используется для "сегодня", "завтра".
        end_date используется для диапазонов типа "на этой неделе", "в этом месяце".
        """
        date_lower = date_entity_value.lower()
        today = datetime.date.today()

        if date_lower == "сегодня":
            return today, None, sql.SQL('ev."Begin"::date = CURRENT_DATE')
        if date_lower == "завтра":
            tomorrow = today + datetime.timedelta(days=1)
            return tomorrow, None, sql.SQL('ev."Begin"::date = CURRENT_DATE + INTERVAL \'1 day\'')

        if "на эт" in date_lower and "недел" in date_lower:  # "на этой неделе"
            start_of_week = today - datetime.timedelta(days=today.weekday())
            end_of_week = start_of_week + datetime.timedelta(days=6)
            return start_of_week, end_of_week, None

        if "в эт" in date_lower and "месяц" in date_lower:  # "в этом месяце"
            start_of_month = today.replace(day=1)
            next_month = start_of_month.replace(month=start_of_month.month % 12 + 1, day=1)
            if start_of_month.month == 12:  # Обработка декабря
                next_month = start_of_month.replace(year=start_of_month.year + 1, month=1, day=1)
            end_of_month = next_month - datetime.timedelta(days=1)
            return start_of_month, end_of_month, None

        # Попытка распарсить месяц типа "в июне"
        for month_name, month_number in MONTH_NAME_TO_NUMBER.items():
            if month_name in date_lower:
                year_to_use = today.year  # По умолчанию текущий год
                # Можно добавить логику для "в июне следующего года" если нужно
                start_of_month = datetime.date(year_to_use, month_number, 1)
                next_month_day_one = start_of_month.replace(month=start_of_month.month % 12 + 1, day=1)
                if start_of_month.month == 12:
                    next_month_day_one = start_of_month.replace(year=start_of_month.year + 1, month=1, day=1)
                end_of_month = next_month_day_one - datetime.timedelta(days=1)
                return start_of_month, end_of_month, None

        # Попытка распарсить конкретную дату "DD.MM.YYYY" или "DD MMMM"
        # Это очень упрощенный парсер, для более надежного нужен Duckling или regex
        try:
            # Попытка "25 мая", "10 июня"
            parts = date_lower.split()
            if len(parts) == 2 and parts[0].isdigit():
                day = int(parts[0])
                month_str = parts[1]
                month_num = None
                for mn, mnum in MONTH_NAME_TO_NUMBER.items():
                    if mn.startswith(month_str):  # Ищем по началу слова
                        month_num = mnum
                        break
                if month_num:
                    parsed_date = datetime.date(today.year, month_num, day)
                    return parsed_date, None, None  # Только начальная дата, без диапазона
        except (ValueError, IndexError):
            pass

        try:
            # Попытка "DD.MM.YYYY" или "DD.MM"
            # Это место требует более надежного парсинга дат
            parsed_date = None
            if '.' in date_entity_value:
                date_parts = date_entity_value.split('.')
                if len(date_parts) == 2:  # DD.MM
                    parsed_date = datetime.datetime.strptime(f"{date_parts[0]}.{date_parts[1]}.{today.year}",
                                                             "%d.%m.%Y").date()
                elif len(date_parts) == 3:  # DD.MM.YYYY
                    parsed_date = datetime.datetime.strptime(date_entity_value, "%d.%m.%Y").date()
            if parsed_date:
                return parsed_date, None, None  # Только начальная дата
        except ValueError:
            print(f"Warning: Could not parse date string '{date_entity_value}' for event date.")

        return None, None, None

    @staticmethod
    def search_event(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        select_fields = [
            sql.SQL('ev."Name" AS event_name'),
            sql.SQL('ev."Begin" AS event_begin'),
            sql.SQL('ev."End" AS event_end'),  # <--- ИЗМЕНЕНО: выбираем End вместо Duration
            sql.SQL('cat."Name" AS category_name'),
            sql.SQL('ev."Description" AS event_description'),  # Предполагаем, что полное имя "Description"
            sql.SQL('emp."Name" AS organizer_name'),
            sql.SQL('emp."Surname" AS organizer_surname')
        ]
        from_table = sql.SQL('FROM "Event" AS ev')
        joins = [
            sql.SQL('LEFT JOIN "Categories" AS cat ON cat."Category_Id" = ev."CategoryId"'),
            sql.SQL('LEFT JOIN "Employees" AS emp ON emp."Employee_Id" = ev."EmployeeId"')
        ]
        where_clauses = []
        params = []

        if 'event_name' in entities:
            where_clauses.append(sql.SQL('ev."Name" ILIKE %s'))
            params.append(f"%{entities['event_name'][0]}%")

        if 'event_category' in entities:
            where_clauses.append(sql.SQL('cat."Name" ILIKE %s'))
            params.append(f"%{entities['event_category'][0]}%")

        if 'organizer' in entities:
            name_val = entities['organizer'][0]
            if ' ' in name_val:
                parts = name_val.split(' ', 1)
                where_clauses.append(sql.SQL(
                    '((emp."Name" ILIKE %s AND emp."Surname" ILIKE %s) OR (emp."Name" ILIKE %s AND emp."Surname" ILIKE %s))'
                ))
                params.extend([f"%{parts[0]}%", f"%{parts[1]}%", f"%{parts[1]}%", f"%{parts[0]}%"])
            else:
                where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
                params.extend([f"%{name_val}%", f"%{name_val}%"])

        if 'date' in entities:
            date_val = entities['date'][0]
            start_date, end_date, special_sql_cond = DbQueryParser._parse_event_date_entity(date_val)

            if special_sql_cond:  # Для "сегодня", "завтра" - применяется к Begin
                where_clauses.append(special_sql_cond)
            elif start_date and end_date:  # Диапазон дат - проверяем, что событие ПЕРЕСЕКАЕТСЯ с диапазоном
                # Событие пересекается с [S, E] если (ev.Begin <= E) AND (ev.End IS NULL OR ev.End >= S)
                # Если у события нет End, считаем, что оно длится до конца start_date или бессрочно в этот день.
                # Для простоты, если есть диапазон, ищем события, начинающиеся в этом диапазоне.
                # Более точное пересечение:
                # where_clauses.append(sql.SQL('(ev."Begin"::date <= %s AND (ev."End" IS NULL OR ev."End"::date >= %s))'))
                # params.extend([end_date, start_date])
                # Пока оставим упрощенный поиск по дате начала:
                where_clauses.append(sql.SQL('ev."Begin"::date >= %s AND ev."Begin"::date <= %s'))
                params.extend([start_date, end_date])
            elif start_date:
                where_clauses.append(sql.SQL('ev."Begin"::date = %s'))
                params.append(start_date)

        if 'location' in entities:
            print(
                f"Warning: Event location filtering is not directly supported by 'Event' table schema. Entity: {entities['location'][0]}")

        if not where_clauses and not (
                'date' in entities and DbQueryParser._parse_event_date_entity(entities['date'][0])[
            2]):
            print(
                "Info: No specific criteria for event search. Will attempt to fetch upcoming events or based on default ordering.")

        query_parts = [
            sql.SQL("SELECT"),
            sql.SQL("'EventList',"),
            sql.SQL(", ").join(select_fields),
            from_table
        ]
        query_parts.extend(joins)
        if where_clauses:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))

        query_parts.append(sql.SQL('ORDER BY ev."Begin" ASC LIMIT 10'))

        final_query = sql.SQL(' ').join(query_parts)
        return final_query, params

    @staticmethod
    def _get_month_day_from_specifier(specifier_value: str) -> tuple[int | None, int | None, str | None]:
        specifier_lower = specifier_value.lower()
        if specifier_lower == "сегодня":
            return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE, 'MM-DD')"
        if specifier_lower == "завтра":
            return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE + INTERVAL '1 day', 'MM-DD')"
        if specifier_lower == "в этом месяце":
            return datetime.date.today().month, None, None
        for month_name, month_number in MONTH_NAME_TO_NUMBER.items():
            if month_name in specifier_lower:
                parts = specifier_lower.replace(month_name, "").strip().split()
                day = None
                for part in parts:
                    if part.isdigit():
                        day = int(part)
                        break
                return month_number, day, None
        if "зимой" in specifier_lower or "зима" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (12, 1, 2)"
        if "весной" in specifier_lower or "весна" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (3, 4, 5)"
        if "летом" in specifier_lower or "лето" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (6, 7, 8)"
        if "осенью" in specifier_lower or "осень" in specifier_lower:
            return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (9, 10, 11)"
        return None, None, None

    @staticmethod
    def find_birthday(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        select_fields = [
            sql.SQL('emp."Surname"'),
            sql.SQL('emp."Name"'),
            sql.SQL('emp."Father"'),
            sql.SQL('emp."Birthday"'),
            sql.SQL('dprt."Name" AS department_name'),
        ]
        from_table = sql.SQL('FROM "Employees" as emp')
        joins = [
            sql.SQL('LEFT JOIN "Department" as dprt ON dprt."Department_Id" = emp."DepartmentId"'),
        ]
        where_clauses = []
        params = []
        date_entity_values = entities.get('date', [])
        birthday_specifiers = entities.get('birthday_specifier', date_entity_values)
        if birthday_specifiers:
            specifier_val = birthday_specifiers[0]
            month, day, special_sql = DbQueryParser._get_month_day_from_specifier(specifier_val)
            if special_sql:
                where_clauses.append(sql.SQL(special_sql))
            elif month:
                where_clauses.append(sql.SQL('EXTRACT(MONTH FROM emp."Birthday") = %s'))
                params.append(month)
                if day:
                    where_clauses.append(sql.SQL('EXTRACT(DAY FROM emp."Birthday") = %s'))
                    params.append(day)
        if 'department' in entities:
            if not any('dprt ON' in str(j) for j in joins):
                joins.append(sql.SQL('LEFT JOIN "Department" as dprt ON dprt."Department_Id" = emp."DepartmentId"'))
            where_clauses.append(sql.SQL('dprt."Name" ILIKE %s'))
            params.append(f"%{entities['department'][0]}%")
        if 'name' in entities:
            name_val = entities['name'][0]
            if ' ' in name_val:
                parts = name_val.split(' ', 1)
                where_clauses.append(sql.SQL(
                    '((emp."Name" ILIKE %s AND emp."Surname" ILIKE %s) OR (emp."Name" ILIKE %s AND emp."Surname" ILIKE %s))'
                ))
                params.extend([f"%{parts[0]}%", f"%{parts[1]}%", f"%{parts[1]}%", f"%{parts[0]}%"])
            else:
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
        if not where_clauses:
            raise ValueError("Недостаточно критериев для поиска дней рождения.")

        query_parts = [
            sql.SQL("SELECT"),
            sql.SQL("'BirthdayList',"),
            sql.SQL(", ").join(select_fields),
            from_table
        ]
        query_parts.extend(joins)
        if where_clauses:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL(
            'ORDER BY EXTRACT(MONTH FROM emp."Birthday"), EXTRACT(DAY FROM emp."Birthday"), emp."Surname", emp."Name" LIMIT 10'))
        final_query = sql.SQL(' ').join(query_parts)
        return final_query, params

    @staticmethod
    def check_task(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        select_fields = [
            sql.SQL('tsk."Name" AS task_name'),
            sql.SQL('tsk."Description" AS task_description'),  # <--- ИСПРАВЛЕНО (предполагая полное имя "Description")
            sql.SQL('tsk."Begin" AS task_deadline'),
            sql.SQL('emp_assignee."Name" AS assignee_name'),
            sql.SQL('emp_assignee."Surname" AS assignee_surname'),
            sql.SQL('prj."Name" AS project_name')
        ]
        # ... (остальная часть метода check_task без изменений, если имя поля Description)
        # ...
        from_table = sql.SQL('FROM "Task" as tsk')
        joins = [
            sql.SQL('LEFT JOIN "Employees" as emp_assignee ON emp_assignee."Employee_Id" = tsk."EmployeeId"'),
            sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = emp_assignee."ProjectId"')
        ]
        where_clauses = []
        params = []
        if 'name' in entities:
            name_val = entities['name'][0]
            if name_val.lower() in ["мои", "меня", "я", "мне"]:
                print("Warning: 'мои' задачи требуют ID текущего пользователя, который здесь не доступен.")
            else:
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
            if not any('prj ON prj."Project_Id" = emp_assignee."ProjectId"' in str(j) for j in joins):
                joins.append(sql.SQL(
                    'LEFT JOIN "Project" as prj_task_filter ON prj_task_filter."Project_Id" = emp_assignee."ProjectId"'))
                where_clauses.append(sql.SQL('prj_task_filter."Name" ILIKE %s'))
            else:
                where_clauses.append(sql.SQL('prj."Name" ILIKE %s'))
            params.append(f"%{entities['project'][0]}%")
        if 'date' in entities:
            date_str = entities['date'][0]
            if date_str.lower() == "сегодня":
                where_clauses.append(sql.SQL('tsk."Begin"::date = CURRENT_DATE'))
            elif date_str.lower() == "завтра":
                where_clauses.append(sql.SQL('tsk."Begin"::date = CURRENT_DATE + INTERVAL \'1 day\''))
            elif "на эт" in date_str.lower() and "недел" in date_str.lower():
                where_clauses.append(sql.SQL(
                    'tsk."Begin"::date >= date_trunc(\'week\', CURRENT_DATE) AND tsk."Begin"::date < date_trunc(\'week\', CURRENT_DATE) + INTERVAL \'1 week\''))
            else:
                try:
                    parsed_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                    where_clauses.append(sql.SQL('tsk."Begin"::date = %s'))
                    params.append(parsed_date)
                except ValueError:
                    print(f"Warning: Could not parse date string '{date_str}' for task deadline.")
        if 'task_status' in entities: print(
            f"Warning: Task status filtering not supported by DB schema. Entity: {entities['task_status'][0]}")
        if 'task_priority' in entities: print(
            f"Warning: Task priority filtering not supported by DB schema. Entity: {entities['task_priority'][0]}")
        if 'task_tag' in entities: print(
            f"Warning: Task tag filtering not supported by DB schema. Entity: {entities['task_tag'][0]}")
        if 'task_name' in entities:
            where_clauses.append(sql.SQL('tsk."Name" ILIKE %s'))
            params.append(f"%{entities['task_name'][0]}%")
        if not where_clauses:
            raise ValueError("Недостаточно критериев для поиска задач.")

        query_parts = [
            sql.SQL("SELECT"),
            sql.SQL("'TaskList',"),
            sql.SQL(", ").join(select_fields),
            from_table
        ]
        query_parts.extend(joins)
        if where_clauses:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL('ORDER BY tsk."Begin" ASC NULLS LAST LIMIT 10'))
        final_query = sql.SQL(' ').join(query_parts)
        return final_query, params

    @staticmethod
    def _entities_to_dict(entities: list) -> dict:
        result = {}
        if not entities:
            return result
        for entity in entities:
            entity_type = entity.get("entity")
            entity_value = entity.get("value")
            if entity_type and entity_value is not None:
                if entity_type not in result:
                    result[entity_type] = [entity_value]
                else:
                    result[entity_type].append(entity_value)
        return result