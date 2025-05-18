# Файл: database_query_parser.py
# Содержимое:
# Файл: database_query_parser.py
import datetime
from psycopg2 import sql
import logging  # Добавлено для логирования

# Настройка логгера (если еще не настроен глобально)
logger = logging.getLogger(__name__)
# Пример базовой настройки, если необходимо:
# if not logger.hasHandlers():
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


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
            logger.warning(f"Warning: Could not parse date string '{date_entity_value}' for event date.")

        return None, None, None

    @staticmethod
    def search_event(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        select_fields = [
            sql.SQL('ev."Name" AS event_name'),
            sql.SQL('ev."Begin" AS event_begin'),
            sql.SQL('ev."End" AS event_end'),  # <--- ИЗМЕНЕНО: выбираем End вместо Duration
            sql.SQL('cat."Name" AS category_name'),
            sql.SQL('ev."Description" AS event_description'),
            sql.SQL('emp."Name" AS organizer_name'),
            sql.SQL('emp."Surname" AS organizer_surname')
        ]
        # Логирование выбранных полей для диагностики
        # logger.info(f"DEBUG search_event: select_fields: {[str(f) for f in select_fields]}")

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

            if special_sql_cond:
                where_clauses.append(special_sql_cond)
            elif start_date and end_date:
                where_clauses.append(sql.SQL('ev."Begin"::date >= %s AND ev."Begin"::date <= %s'))
                params.extend([start_date, end_date])
            elif start_date:
                where_clauses.append(sql.SQL('ev."Begin"::date = %s'))
                params.append(start_date)

        if 'location' in entities:
            logger.warning(
                f"Warning: Event location filtering is not directly supported by 'Event' table schema. Entity: {entities['location'][0]}")

        if not where_clauses and not (
                'date' in entities and DbQueryParser._parse_event_date_entity(entities['date'][0])[
            2]):  # Проверка, что если нет явных фильтров, то нет и спец.условий по дате типа "сегодня"
            logger.info(
                "Info: No specific criteria for event search. Will attempt to fetch upcoming events or based on default ordering.")
            # Можно не добавлять WHERE, если нет условий, чтобы не было пустого "WHERE "

        query_parts = [
            sql.SQL("SELECT"),
            sql.SQL("'EventList',"),
            sql.SQL(", ").join(select_fields),
            from_table
        ]
        query_parts.extend(joins)
        if where_clauses:  # Только если есть условия, добавляем WHERE
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))

        query_parts.append(sql.SQL('ORDER BY ev."Begin" ASC LIMIT 10'))

        final_query = sql.SQL(' ').join(query_parts)
        # logger.info(f"DEBUG search_event: final_query: {final_query.as_string(None)}") # Для отладки строки запроса
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
                logger.warning(f"Warning: could not parse age_older_than value: {entities['age_older_than'][0]}")
        if 'age_younger_than' in entities:
            try:
                age = int(entities['age_younger_than'][0])
                where_clauses.append(sql.SQL('date_part(\'year\', age(emp."Birthday")) < %s'))
                params.append(age)
            except ValueError:
                logger.warning(f"Warning: could not parse age_younger_than value: {entities['age_younger_than'][0]}")
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
            sql.SQL('tsk."Description" AS task_description'),
            sql.SQL('tsk."Begin" AS task_deadline'),
            # В схеме Task.Begin - это дедлайн? Или это Task.Deadline? Проверьте имя поля.
            sql.SQL('emp_assignee."Name" AS assignee_name'),
            sql.SQL('emp_assignee."Surname" AS assignee_surname'),
            sql.SQL('prj."Name" AS project_name')
        ]
        from_table = sql.SQL('FROM "Task" as tsk')  # Предполагаем таблицу "Task"
        joins = [
            sql.SQL('LEFT JOIN "Employees" as emp_assignee ON emp_assignee."Employee_Id" = tsk."EmployeeId"'),
            # EmployeeId в Task
            sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = emp_assignee."ProjectId"')
            # ProjectId в Employees? Или в Task? В схеме Task.ProjectId нет.
            # Если задачи привязаны к проекту напрямую, то JOIN Project к Task.
            # Если задачи привязаны к сотруднику, а сотрудник к проекту, то как сейчас.
            # Судя по схеме, Task.EmployeeId есть, а ProjectId у Task нет. Значит, текущий JOIN для проекта задачи через сотрудника.
        ]
        where_clauses = []
        params = []
        if 'name' in entities:  # Имя исполнителя
            name_val = entities['name'][0]
            if name_val.lower() in ["мои", "меня", "я", "мне"]:
                logger.warning("Warning: 'мои' задачи требуют ID текущего пользователя, который здесь не доступен.")
                # raise ValueError("Поиск 'моих' задач пока не реализован.") # Или так
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

        # Фильтр по проекту:
        # Если Task связан с Project напрямую через Task.ProjectId, то JOIN должен быть `LEFT JOIN "Project" as prj ON prj."Project_Id" = tsk."ProjectId"`
        # И условие `prj."Name" ILIKE %s`.
        # Текущая логика фильтрует по проекту сотрудника, которому назначена задача.
        if 'project' in entities:
            # Эта логика дублирования JOIN-а для проекта кажется избыточной, если prj уже есть.
            # Проверяем, есть ли уже JOIN к Project, связанный с задачами (tsk) или сотрудником (emp_assignee)
            project_join_exists = any(
                ('prj ON prj."Project_Id" = emp_assignee."ProjectId"' in str(j)) or
                ('prj ON prj."Project_Id" = tsk."ProjectId"' in str(j))  # Если бы Task.ProjectId существовал
                for j in joins
            )
            if not project_join_exists:
                # Если нет JOIN для проекта, добавляем его (предполагая связь через сотрудника)
                joins.append(sql.SQL(
                    'LEFT JOIN "Project" AS prj_task_filter ON prj_task_filter."Project_Id" = emp_assignee."ProjectId"'))
                where_clauses.append(sql.SQL('prj_task_filter."Name" ILIKE %s'))
            else:  # JOIN уже есть (назван prj), используем его
                where_clauses.append(sql.SQL('prj."Name" ILIKE %s'))
            params.append(f"%{entities['project'][0]}%")

        if 'date' in entities:  # Дедлайн задачи (предполагается поле Task.Begin или Task.Deadline)
            date_str = entities['date'][0]
            # Используем поле "Begin" из таблицы Task, как указано в select_fields для task_deadline
            # Схема показывает Task.Begin (TIMESTAMPTZ), Task.Duration (INTERVAL). Если есть Task.Deadline, лучше использовать его.
            # Предположим, что Task.Begin - это дедлайн или дата начала, по которой можно фильтровать.
            if date_str.lower() == "сегодня":
                where_clauses.append(sql.SQL('tsk."Begin"::date = CURRENT_DATE'))
            elif date_str.lower() == "завтра":
                where_clauses.append(sql.SQL('tsk."Begin"::date = CURRENT_DATE + INTERVAL \'1 day\''))
            elif "на эт" in date_str.lower() and "недел" in date_str.lower():  # "на этой неделе"
                where_clauses.append(sql.SQL(
                    'tsk."Begin"::date >= date_trunc(\'week\', CURRENT_DATE) AND tsk."Begin"::date < date_trunc(\'week\', CURRENT_DATE) + INTERVAL \'1 week\''))
            else:
                try:
                    # Попытка распарсить дату формата DD.MM.YYYY
                    parsed_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                    where_clauses.append(sql.SQL('tsk."Begin"::date = %s'))
                    params.append(parsed_date)
                except ValueError:
                    logger.warning(f"Warning: Could not parse date string '{date_str}' for task deadline.")

        # Поля Status, Priority, Tags не видны в схеме для таблицы "Task".
        # Если они существуют, то фильтрация ниже корректна. Иначе - будут ошибки.
        # Предположим, что они есть.
        if 'task_status' in entities:
            logger.warning(
                f"Warning: Task status filtering relies on Task.Status column. Entity: {entities['task_status'][0]}")
            where_clauses.append(sql.SQL('tsk."Status" ILIKE %s'))  # Предполагаем Task.Status
            params.append(f"%{entities['task_status'][0]}%")
        if 'task_priority' in entities:
            logger.warning(
                f"Warning: Task priority filtering relies on Task.Priority column. Entity: {entities['task_priority'][0]}")
            where_clauses.append(sql.SQL('tsk."Priority" ILIKE %s'))  # Предполагаем Task.Priority
            params.append(f"%{entities['task_priority'][0]}%")
        if 'task_tag' in entities:
            logger.warning(f"Warning: Task tag filtering relies on Task.Tags column. Entity: {entities['task_tag'][0]}")
            where_clauses.append(sql.SQL('tsk."Tags" ILIKE %s'))  # Предполагаем Task.Tags (или JOIN на таблицу тегов)
            params.append(f"%{entities['task_tag'][0]}%")

        if 'task_name' in entities:
            where_clauses.append(sql.SQL('tsk."Name" ILIKE %s'))
            params.append(f"%{entities['task_name'][0]}%")

        if not where_clauses:
            # Если нет критериев, возможно, стоит вернуть ошибку или последние N задач
            # текущего пользователя (если это возможно определить).
            # Пока что, как и для ДР, требуем критерии.
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

        # Сортировка: NULLS LAST для tsk."Begin" чтобы задачи без дедлайна были в конце
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