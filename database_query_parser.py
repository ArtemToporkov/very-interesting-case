# Файл: database_query_parser.py
# Содержимое:
import datetime
from psycopg2 import sql
import logging

logger = logging.getLogger(__name__)

MONTH_NAME_TO_NUMBER = {
    "январь": 1, "января": 1, "январе": 1, "февраль": 2, "февраля": 2, "феврале": 2,
    "март": 3, "марта": 3, "марте": 3, "апрель": 4, "апреля": 4, "апреле": 4,
    "май": 5, "мая": 5, "мае": 5, "июнь": 6, "июня": 6, "июне": 6,
    "июль": 7, "июля": 7, "июле": 7, "август": 8, "августа": 8, "августе": 8,
    "сентябрь": 9, "сентября": 9, "сентябре": 9, "октябрь": 10, "октября": 10, "октябре": 10,
    "ноябрь": 11, "ноября": 11, "ноябре": 11, "декабрь": 12, "декабря": 12, "декабре": 12,
}


class DbQueryParser:
    @staticmethod
    def _get_quarter_dates(year: int, quarter: int) -> tuple[datetime.date, datetime.date]:
        if quarter == 1: return datetime.date(year, 1, 1), datetime.date(year, 3, 31)
        if quarter == 2: return datetime.date(year, 4, 1), datetime.date(year, 6, 30)
        if quarter == 3: return datetime.date(year, 7, 1), datetime.date(year, 9, 30)
        if quarter == 4: return datetime.date(year, 10, 1), datetime.date(year, 12, 31)
        raise ValueError("Invalid quarter number")

    @staticmethod
    def _get_current_quarter_info(date_obj: datetime.date) -> tuple[int, int]:
        month = date_obj.month
        if 1 <= month <= 3: return 1, date_obj.year
        if 4 <= month <= 6: return 2, date_obj.year
        if 7 <= month <= 9: return 3, date_obj.year
        return 4, date_obj.year

    @staticmethod
    def parse(data: dict):
        intent_name = data.get("intent", {}).get("name")
        if not intent_name:
            raise ValueError("Intent name is missing in NLU data")

        match intent_name:
            case "search_person":
                return DbQueryParser.search_person(data)
            case "search_event":
                return DbQueryParser.search_event(data)
            case "find_birthday":
                return DbQueryParser.find_birthday(data)
            case "check_task":
                return DbQueryParser.check_task(data)
        logger.warning(f"Intent '{intent_name}' is not configured for SQL query generation in DbQueryParser.")
        raise ValueError(f"Неизвестный или нецелевой интент для SQL-парсера: {intent_name}")

    @staticmethod
    def _split_name_surname(name_entity_value: str) -> tuple[str | None, str | None]:
        parts = name_entity_value.strip().split()
        if len(parts) == 1:
            return parts[0], parts[0]
        if len(parts) == 2:
            return parts[0], parts[1]
        if len(parts) > 2:
            logger.warning(f"Name entity '{name_entity_value}' has more than 2 parts. Using first two for search.")
            return parts[0], parts[1]
        return None, None

    @staticmethod
    def search_person(data: dict):
        entities_dict = DbQueryParser._entities_to_dict(data.get('entities', []))
        name_val = entities_dict.get('name', [None])[0]

        if not name_val:
            raise ValueError("Сущность 'name' не найдена для search_person")

        # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
        select_fields = [
            sql.SQL('emp."Surname"'), sql.SQL('emp."Name"'), sql.SQL('emp."Father"'),
            sql.SQL('emp."Birthday"'), sql.SQL('emp."FirstDay"'),
            sql.SQL('lng."Name" AS "LanguageName"'),  # AS используется внутри строки SQL
            sql.SQL('rnk."Status" AS "RankStatus"'),
            sql.SQL('prj."Name" AS "ProjectName"'),
            sql.SQL('dprt."Name" AS "DepartmentName"'),
            sql.SQL('emp."Contacts"')
        ]
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        from_table = sql.SQL('FROM "Employees" AS emp')
        joins = [
            sql.SQL('LEFT JOIN "Languages" AS lng ON lng."Language_Id" = emp."LanguageId"'),
            sql.SQL('LEFT JOIN "Rank" AS rnk ON rnk."Rank_Id" = emp."RankId"'),
            sql.SQL('LEFT JOIN "Project" AS prj ON prj."Project_Id" = emp."ProjectId"'),
            sql.SQL('LEFT JOIN "Department" AS dprt ON dprt."Department_Id" = emp."DepartmentId"')
        ]

        where_clauses = []
        params = []

        part1, part2 = DbQueryParser._split_name_surname(name_val)

        if part1 and part2:
            if part1 == part2:
                where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
                params.extend([f"%{part1}%", f"%{part1}%"])
            else:
                where_clauses.append(sql.SQL(
                    '((emp."Name" ILIKE %s AND emp."Surname" ILIKE %s) OR (emp."Name" ILIKE %s AND emp."Surname" ILIKE %s))'
                ))
                params.extend([f"%{part1}%", f"%{part2}%", f"%{part2}%", f"%{part1}%"])
        elif part1:
            where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
            params.extend([f"%{part1}%", f"%{part1}%"])
        else:
            raise ValueError("Не удалось обработать значение имени для поиска.")

        query_parts = [sql.SQL("SELECT 'PersonInfo',"), sql.SQL(", ").join(select_fields), from_table]
        query_parts.extend(joins)
        if where_clauses:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL("LIMIT 1"))

        final_query = sql.SQL(" ").join(query_parts)
        return final_query, params

    @staticmethod
    def _parse_relative_date_entity(date_entity_value: str) -> tuple[
        datetime.date | None, datetime.date | None, str | None]:
        date_lower = date_entity_value.lower()
        today = datetime.date.today()
        if date_lower == "сегодня": return None, None, "{col}::date = CURRENT_DATE"
        if date_lower == "завтра": return None, None, "{col}::date = CURRENT_DATE + INTERVAL '1 day'"
        if date_lower == "послезавтра": return None, None, "{col}::date = CURRENT_DATE + INTERVAL '2 days'"
        if date_lower == "вчера": return None, None, "{col}::date = CURRENT_DATE - INTERVAL '1 day'"
        if date_lower == "позавчера": return None, None, "{col}::date = CURRENT_DATE - INTERVAL '2 days'"
        if "зимой" in date_lower or "зима" in date_lower: return None, None, "EXTRACT(MONTH FROM {col}::date) IN (12, 1, 2)"
        if "весной" in date_lower or "весна" in date_lower: return None, None, "EXTRACT(MONTH FROM {col}::date) IN (3, 4, 5)"
        if "летом" in date_lower or "лето" in date_lower: return None, None, "EXTRACT(MONTH FROM {col}::date) IN (6, 7, 8)"
        if "осенью" in date_lower or "осень" in date_lower: return None, None, "EXTRACT(MONTH FROM {col}::date) IN (9, 10, 11)"
        if "на эт" in date_lower and "недел" in date_lower:
            start_of_week = today - datetime.timedelta(days=today.weekday())
            return start_of_week, start_of_week + datetime.timedelta(days=6), None
        if "на след" in date_lower and "недел" in date_lower:
            start_of_next_week = today - datetime.timedelta(days=today.weekday()) + datetime.timedelta(days=7)
            return start_of_next_week, start_of_next_week + datetime.timedelta(days=6), None
        if "на прошл" in date_lower and "недел" in date_lower:
            start_of_last_week = today - datetime.timedelta(days=today.weekday()) - datetime.timedelta(days=7)
            return start_of_last_week, start_of_last_week + datetime.timedelta(days=6), None
        if "в эт" in date_lower and "месяц" in date_lower:
            start_of_month = today.replace(day=1)
            return start_of_month, (start_of_month + datetime.timedelta(days=35)).replace(day=1) - datetime.timedelta(
                days=1), None
        if "в след" in date_lower and "месяц" in date_lower:
            next_month_first_day = (today.replace(day=1) + datetime.timedelta(days=35)).replace(day=1)
            return next_month_first_day, (next_month_first_day + datetime.timedelta(days=35)).replace(
                day=1) - datetime.timedelta(days=1), None
        if "в прошл" in date_lower and "месяц" in date_lower:
            last_month_last_day = today.replace(day=1) - datetime.timedelta(days=1)
            return last_month_last_day.replace(day=1), last_month_last_day, None
        current_quarter, current_year_for_quarter = DbQueryParser._get_current_quarter_info(today)
        if "в эт" in date_lower and "квартал" in date_lower:
            return DbQueryParser._get_quarter_dates(current_year_for_quarter, current_quarter)[0], \
            DbQueryParser._get_quarter_dates(current_year_for_quarter, current_quarter)[1], None
        if "в след" in date_lower and "квартал" in date_lower:
            next_q, next_y = (current_quarter % 4 + 1, current_year_for_quarter) if current_quarter < 4 else (1,
                                                                                                              current_year_for_quarter + 1)
            return DbQueryParser._get_quarter_dates(next_y, next_q)[0], \
            DbQueryParser._get_quarter_dates(next_y, next_q)[1], None
        if "в прошл" in date_lower and "квартал" in date_lower:
            prev_q, prev_y = ((current_quarter - 2 + 4) % 4 + 1, current_year_for_quarter) if current_quarter > 1 else (
                4, current_year_for_quarter - 1)
            return DbQueryParser._get_quarter_dates(prev_y, prev_q)[0], \
            DbQueryParser._get_quarter_dates(prev_y, prev_q)[1], None
        if "в эт" in date_lower and "год" in date_lower: return datetime.date(today.year, 1, 1), datetime.date(
            today.year, 12, 31), None
        if "в след" in date_lower and "год" in date_lower: return datetime.date(today.year + 1, 1, 1), datetime.date(
            today.year + 1, 12, 31), None
        if "в прошл" in date_lower and "год" in date_lower: return datetime.date(today.year - 1, 1, 1), datetime.date(
            today.year - 1, 12, 31), None
        for month_name_key, month_number_val in MONTH_NAME_TO_NUMBER.items():
            if month_name_key in date_lower:
                year_to_use = today.year
                day_in_month_str = ''.join(filter(str.isdigit, date_lower.replace(month_name_key, "")))
                day_in_month = int(day_in_month_str) if day_in_month_str else None
                if day_in_month:
                    try:
                        return datetime.date(year_to_use, month_number_val, day_in_month), None, None
                    except ValueError:
                        logger.warning(
                            f"Invalid day {day_in_month} for month {month_number_val} in '{date_entity_value}'")
                start_of_month = datetime.date(year_to_use, month_number_val, 1)
                return start_of_month, (start_of_month + datetime.timedelta(days=35)).replace(
                    day=1) - datetime.timedelta(days=1), None
        try:
            if '.' in date_entity_value:
                parts = date_entity_value.split('.')
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    return datetime.datetime.strptime(f"{parts[0]}.{parts[1]}.{today.year}",
                                                      "%d.%m.%Y").date(), None, None
                if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit():
                    return datetime.datetime.strptime(date_entity_value, "%d.%m.%Y").date(), None, None
        except ValueError:
            logger.warning(f"Could not parse '{date_entity_value}' as DD.MM or DD.MM.YYYY.")
        logger.warning(f"Could not parse date '{date_entity_value}' with any known format.")
        return None, None, None

    @staticmethod
    def search_event(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        select_fields = [
            sql.SQL('ev."Name" AS event_name'), sql.SQL('ev."Begin" AS event_begin'),
            sql.SQL('ev."End" AS event_end'), sql.SQL('cat."Name" AS category_name'),
            sql.SQL('ev."Description" AS event_description'), sql.SQL('emp."Name" AS organizer_name'),
            sql.SQL('emp."Surname" AS organizer_surname')
        ]
        from_table = sql.SQL('FROM "Event" AS ev')
        joins = [
            sql.SQL('LEFT JOIN "Categories" AS cat ON cat."Category_Id" = ev."CategoryId"'),
            sql.SQL('LEFT JOIN "Employees" AS emp ON emp."Employee_Id" = ev."EmployeeId"')
        ]
        where_clauses = []
        params = []
        event_date_col_sql = sql.SQL('ev."Begin"')

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
                    '((emp."Name" ILIKE %s AND emp."Surname" ILIKE %s) OR (emp."Name" ILIKE %s AND emp."Surname" ILIKE %s))'))
                params.extend([f"%{parts[0]}%", f"%{parts[1]}%", f"%{parts[1]}%", f"%{parts[0]}%"])
            else:
                where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
                params.extend([f"%{name_val}%", f"%{name_val}%"])

        if 'date' in entities:
            date_val = entities['date'][0]
            start_dt, end_dt, sql_template = DbQueryParser._parse_relative_date_entity(date_val)
            if sql_template:
                where_clauses.append(sql.SQL(sql_template.format(col=event_date_col_sql)))
            elif start_dt and end_dt:
                where_clauses.append(sql.SQL('{col}::date >= %s AND {col}::date <= %s').format(col=event_date_col_sql))
                params.extend([start_dt, end_dt])
            elif start_dt:
                where_clauses.append(sql.SQL('{col}::date = %s').format(col=event_date_col_sql))
                params.append(start_dt)

        if 'location' in entities:
            logger.warning(f"Event location filtering not supported. Entity: {entities['location'][0]}")

        query_parts = [sql.SQL("SELECT 'EventList',"), sql.SQL(", ").join(select_fields), from_table]
        query_parts.extend(joins)
        if where_clauses:
            query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL('ORDER BY ev."Begin" ASC LIMIT 10'))
        return sql.SQL(' ').join(query_parts), params

    @staticmethod
    def _get_month_day_from_specifier(specifier_value: str) -> tuple[int | None, int | None, str | None]:
        specifier_lower = specifier_value.lower()
        if specifier_lower == "сегодня": return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE, 'MM-DD')"
        if specifier_lower == "завтра": return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE + INTERVAL '1 day', 'MM-DD')"
        if specifier_lower == "вчера": return None, None, "TO_CHAR(emp.\"Birthday\", 'MM-DD') = TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'MM-DD')"
        if specifier_lower == "в этом месяце": return datetime.date.today().month, None, None
        for month_name, month_number in MONTH_NAME_TO_NUMBER.items():
            if month_name in specifier_lower:
                day_str = ''.join(filter(str.isdigit, specifier_lower.replace(month_name, "")))
                day = int(day_str) if day_str else None
                return month_number, day, None
        if "зимой" in specifier_lower or "зима" in specifier_lower: return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (12, 1, 2)"
        if "весной" in specifier_lower or "весна" in specifier_lower: return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (3, 4, 5)"
        if "летом" in specifier_lower or "лето" in specifier_lower: return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (6, 7, 8)"
        if "осенью" in specifier_lower or "осень" in specifier_lower: return None, None, "EXTRACT(MONTH FROM emp.\"Birthday\") IN (9, 10, 11)"
        return None, None, None

    @staticmethod
    def find_birthday(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        select_fields = [
            sql.SQL('emp."Surname"'), sql.SQL('emp."Name"'), sql.SQL('emp."Father"'),
            sql.SQL('emp."Birthday"'), sql.SQL('dprt."Name" AS "department_name"'),  # ИСПРАВЛЕНО: псевдоним в кавычках
        ]
        from_table = sql.SQL('FROM "Employees" as emp')
        joins = [sql.SQL('LEFT JOIN "Department" as dprt ON dprt."Department_Id" = emp."DepartmentId"')]
        where_clauses = []
        params = []
        date_entity_values = entities.get('date', [])
        birthday_specifiers = entities.get('birthday_specifier', date_entity_values)

        if birthday_specifiers:
            specifier_val = birthday_specifiers[0]
            month, day, special_sql_str = DbQueryParser._get_month_day_from_specifier(specifier_val)
            if special_sql_str:
                where_clauses.append(sql.SQL(special_sql_str))
            elif month:
                where_clauses.append(sql.SQL('EXTRACT(MONTH FROM emp."Birthday") = %s'))
                params.append(month)
                if day:
                    where_clauses.append(sql.SQL('EXTRACT(DAY FROM emp."Birthday") = %s'))
                    params.append(day)

        if 'department' in entities:
            if not any('dprt."Department_Id" = emp."DepartmentId"' in str(j) for j in joins):
                joins.append(sql.SQL('LEFT JOIN "Department" as dprt ON dprt."Department_Id" = emp."DepartmentId"'))
            where_clauses.append(sql.SQL('dprt."Name" ILIKE %s'))
            params.append(f"%{entities['department'][0]}%")

        if 'name' in entities:
            name_val = entities['name'][0]
            part1, part2 = DbQueryParser._split_name_surname(name_val)
            if part1 and part2:
                if part1 == part2:
                    where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
                    params.extend([f"%{part1}%", f"%{part1}%"])
                else:
                    where_clauses.append(sql.SQL(
                        '((emp."Name" ILIKE %s AND emp."Surname" ILIKE %s) OR (emp."Name" ILIKE %s AND emp."Surname" ILIKE %s))'
                    ))
                    params.extend([f"%{part1}%", f"%{part2}%", f"%{part2}%", f"%{part1}%"])
            elif part1:
                where_clauses.append(sql.SQL('(emp."Name" ILIKE %s OR emp."Surname" ILIKE %s)'))
                params.extend([f"%{part1}%", f"%{part1}%"])

        if 'age_older_than' in entities:
            try:
                age = int(entities['age_older_than'][0]); where_clauses.append(
                    sql.SQL('date_part(\'year\', age(emp."Birthday")) > %s')); params.append(age)
            except ValueError:
                logger.warning(f"Could not parse age_older_than: {entities['age_older_than'][0]}")
        if 'age_younger_than' in entities:
            try:
                age = int(entities['age_younger_than'][0]); where_clauses.append(
                    sql.SQL('date_part(\'year\', age(emp."Birthday")) < %s')); params.append(age)
            except ValueError:
                logger.warning(f"Could not parse age_younger_than: {entities['age_younger_than'][0]}")

        if not where_clauses: raise ValueError("Недостаточно критериев для поиска дней рождения.")
        query_parts = [sql.SQL("SELECT 'BirthdayList',"), sql.SQL(", ").join(select_fields), from_table]
        query_parts.extend(joins)
        query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL(
            'ORDER BY EXTRACT(MONTH FROM emp."Birthday"), EXTRACT(DAY FROM emp."Birthday"), emp."Surname", emp."Name" LIMIT 10'))
        return sql.SQL(' ').join(query_parts), params

    @staticmethod
    def check_task(data: dict):
        entities = DbQueryParser._entities_to_dict(data.get('entities', []))
        # --- НАЧАЛО ИСПРАВЛЕНИЯ ПСЕВДОНИМОВ ---
        select_fields = [
            sql.SQL('tsk."Name" AS "task_name"'),
            sql.SQL('tsk."Description" AS "task_description"'),  # Предполагается, что Description есть
            sql.SQL('tsk."Begin" AS "task_deadline"'),
            sql.SQL('emp_assignee."Name" AS "assignee_name"'),
            sql.SQL('emp_assignee."Surname" AS "assignee_surname"'),
            sql.SQL('prj."Name" AS "project_name"')
        ]
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ПСЕВДОНИМОВ ---
        from_table = sql.SQL('FROM "Task" as tsk')
        joins = [
            sql.SQL('LEFT JOIN "Employees" as emp_assignee ON emp_assignee."Employee_Id" = tsk."EmployeeId"'),
            sql.SQL('LEFT JOIN "Project" as prj ON prj."Project_Id" = emp_assignee."ProjectId"')
        ]
        where_clauses = []
        params = []
        task_date_col_sql = sql.SQL('tsk."Begin"')

        if 'name' in entities:
            name_val = entities['name'][0]
            if name_val.lower() in ["мои", "меня", "я", "мне"]:
                logger.warning("'мои' задачи требуют ID пользователя.")
            else:
                part1, part2 = DbQueryParser._split_name_surname(name_val)
                if part1 and part2:
                    if part1 == part2:
                        where_clauses.append(
                            sql.SQL('(emp_assignee."Name" ILIKE %s OR emp_assignee."Surname" ILIKE %s)'))
                        params.extend([f"%{part1}%", f"%{part1}%"])
                    else:
                        where_clauses.append(sql.SQL(
                            '((emp_assignee."Name" ILIKE %s AND emp_assignee."Surname" ILIKE %s) OR (emp_assignee."Name" ILIKE %s AND emp_assignee."Surname" ILIKE %s))'
                        ))
                        params.extend([f"%{part1}%", f"%{part2}%", f"%{part2}%", f"%{part1}%"])
                elif part1:
                    where_clauses.append(sql.SQL('(emp_assignee."Name" ILIKE %s OR emp_assignee."Surname" ILIKE %s)'))
                    params.extend([f"%{part1}%", f"%{part1}%"])

        if 'project' in entities:
            project_join_exists = any('prj."Project_Id" = emp_assignee."ProjectId"' in str(j) for j in joins)
            if not project_join_exists:
                joins.append(
                    sql.SQL('LEFT JOIN "Project" AS prj_filter ON prj_filter."Project_Id" = emp_assignee."ProjectId"'))
                where_clauses.append(sql.SQL('prj_filter."Name" ILIKE %s'))
            else:
                where_clauses.append(sql.SQL('prj."Name" ILIKE %s'))
            params.append(f"%{entities['project'][0]}%")

        if 'date' in entities:
            date_val = entities['date'][0].lower()
            start_dt, end_dt, sql_template = DbQueryParser._parse_relative_date_entity(date_val)
            if sql_template:
                where_clauses.append(sql.SQL(sql_template.format(col=task_date_col_sql)))
            elif start_dt and end_dt:
                where_clauses.append(sql.SQL('{col}::date >= %s AND {col}::date <= %s').format(col=task_date_col_sql))
                params.extend([start_dt, end_dt])
            elif start_dt:
                where_clauses.append(sql.SQL('{col}::date = %s').format(col=task_date_col_sql))
                params.append(start_dt)
            else:
                logger.warning(f"Could not parse date '{date_val}' for task.")

        if 'task_status' in entities:
            logger.warning(f"Task status filtering relies on Task.Status. Entity: {entities['task_status'][0]}")
            where_clauses.append(sql.SQL('tsk."Status" ILIKE %s'))
            params.append(f"%{entities['task_status'][0]}%")
        if 'task_priority' in entities:
            logger.warning(f"Task priority filtering relies on Task.Priority. Entity: {entities['task_priority'][0]}")
            where_clauses.append(sql.SQL('tsk."Priority" ILIKE %s'))
            params.append(f"%{entities['task_priority'][0]}%")
        if 'task_tag' in entities:
            logger.warning(f"Task tag filtering relies on Task.Tags. Entity: {entities['task_tag'][0]}")
            where_clauses.append(sql.SQL('tsk."Tags" ILIKE %s'))
            params.append(f"%{entities['task_tag'][0]}%")
        if 'task_name' in entities:
            where_clauses.append(sql.SQL('tsk."Name" ILIKE %s'))
            params.append(f"%{entities['task_name'][0]}%")

        if not where_clauses: raise ValueError("Недостаточно критериев для поиска задач.")
        query_parts = [sql.SQL("SELECT 'TaskList',"), sql.SQL(", ").join(select_fields), from_table]
        query_parts.extend(joins)
        query_parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses))
        query_parts.append(sql.SQL('ORDER BY tsk."Begin" ASC NULLS LAST LIMIT 10'))
        return sql.SQL(' ').join(query_parts), params

    @staticmethod
    def _entities_to_dict(entities: list) -> dict:
        result = {}
        if not entities: return result
        for entity in entities:
            entity_type, entity_value = entity.get("entity"), entity.get("value")
            if entity_type and entity_value is not None:
                result.setdefault(entity_type, []).append(entity_value)
        return result