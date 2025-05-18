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
                # Рекомендуется переделать на возврат (query, params)
                query_str = DbQueryParser.search_person_str(data)
                return query_str, None # Возвращаем как кортеж для единообразия
            case "search_event":
                # Рекомендуется переделать на возврат (query, params)
                query_str = DbQueryParser.search_event_str(data)
                return query_str, None # Возвращаем как кортеж для единообразия
            case "find_birthday":
                return DbQueryParser.find_birthday(data)
            case "check_task":
                return DbQueryParser.check_task(data)
        raise ValueError(f"Неизвестный интент: {intent_name}")

    @staticmethod
    def search_person_str(data: dict) -> str: # Переименовано для ясности, что возвращает строку
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
    def search_event_str(data: dict) -> str: # Переименовано для ясности
        entities: list = data.get('entities', [])
        keywords = DbQueryParser._entities_to_dict(entities)
        where_clauses = []
        params = [] # Этот метод должен быть переделан для использования params

        # ОСТОРОЖНО: SQL-инъекция! Этот метод должен быть переписан с использованием параметризации.
        if 'event_name' in keywords:
            where_clauses.append(f"""ev."Name" ILIKE '%{keywords['event_name'][0]}%'""")
        if 'date' in keywords:
            where_clauses.append("1=1") # Заглушка

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
            sql.SQL("'BirthdayList',"),  # Маркер типа результата, с последующей запятой
            sql.SQL(", ").join(select_fields), # Динамические поля, объединенные запятыми
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
        # Поля, которые мы выбираем. Убедитесь, что они соответствуют схеме и ожиданиям db_response_parser
        select_fields = [
            sql.SQL('tsk."Name" AS task_name'),
            sql.SQL('tsk."Descri..." AS task_description'), # В схеме "Descri...", предполагаем Description
            sql.SQL('tsk."Begin" AS task_deadline'), # В схеме "Begin" (TIMESTAMPTZ), используем как дедлайн
            sql.SQL('emp_assignee."Name" AS assignee_name'),
            sql.SQL('emp_assignee."Surname" AS assignee_surname'),
            sql.SQL('prj."Name" AS project_name')
            # Поля Status, Priority, Tags отсутствуют в таблице Task по схеме
        ]
        from_table = sql.SQL('FROM "Task" as tsk')
        joins = [
            sql.SQL('LEFT JOIN "Employees" as emp_assignee ON emp_assignee."Employee_Id" = tsk."EmployeeId"'),
            sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = emp_assignee."ProjectId"') # Проект задачи через проект исполнителя
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
            if not any('prj ON prj."Project_Id" = emp_assignee."ProjectId"' in str(j) for j in joins): # Проверка, чтобы не дублировать join для проекта
                 joins.append(sql.SQL('LEFT JOIN "Project" as prj_task_filter ON prj_task_filter."Project_Id" = emp_assignee."ProjectId"')) # Используем другой алиас, если уже есть prj
                 where_clauses.append(sql.SQL('prj_task_filter."Name" ILIKE %s'))
            else:
                 where_clauses.append(sql.SQL('prj."Name" ILIKE %s'))
            params.append(f"%{entities['project'][0]}%")
        if 'date' in entities:
            date_str = entities['date'][0]
            if date_str.lower() == "сегодня":
                where_clauses.append(sql.SQL('tsk."Begin"::date = CURRENT_DATE')) # Используем tsk."Begin" как дедлайн
            elif date_str.lower() == "завтра":
                 where_clauses.append(sql.SQL('tsk."Begin"::date = CURRENT_DATE + INTERVAL \'1 day\''))
            elif "на эт" in date_str.lower() and "недел" in date_str.lower():
                 where_clauses.append(sql.SQL('tsk."Begin"::date >= date_trunc(\'week\', CURRENT_DATE) AND tsk."Begin"::date < date_trunc(\'week\', CURRENT_DATE) + INTERVAL \'1 week\''))
            else:
                try:
                    parsed_date = datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
                    where_clauses.append(sql.SQL('tsk."Begin"::date = %s'))
                    params.append(parsed_date)
                except ValueError:
                    print(f"Warning: Could not parse date string '{date_str}' for task deadline.")
        # Фильтрация по task_status, task_priority, task_tag - пропускается, т.к. полей нет в Task
        if 'task_status' in entities: print(f"Warning: Task status filtering not supported by DB schema. Entity: {entities['task_status'][0]}")
        if 'task_priority' in entities: print(f"Warning: Task priority filtering not supported by DB schema. Entity: {entities['task_priority'][0]}")
        if 'task_tag' in entities: print(f"Warning: Task tag filtering not supported by DB schema. Entity: {entities['task_tag'][0]}")
        if 'task_name' in entities:
            where_clauses.append(sql.SQL('tsk."Name" ILIKE %s'))
            params.append(f"%{entities['task_name'][0]}%")
        if not where_clauses:
            raise ValueError("Недостаточно критериев для поиска задач.")

        query_parts = [
            sql.SQL("SELECT"),
            sql.SQL("'TaskList',"), # Маркер типа результата, с последующей запятой
            sql.SQL(", ").join(select_fields), # Динамические поля, объединенные запятыми
            from_table
        ]
        query_parts.extend(joins)
        if where_clauses:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL('ORDER BY tsk."Begin" ASC NULLS LAST LIMIT 10')) # Сортируем по tsk."Begin"
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