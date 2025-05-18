# Файл: db_response_parser.py
import textwrap
import datetime


class DbResponseParser:
    @staticmethod
    def parse_into_message(data_list: list) -> str:
        if not data_list or not data_list[0]:
            return "По вашему запросу ничего не найдено."
        first_record = data_list[0]
        result_type = first_record[0]

        match result_type:
            case "PersonInfo":
                return DbResponseParser.parse_person(first_record)
            case "BirthdayList":
                return DbResponseParser.parse_birthday_results(data_list)
            case "TaskList":
                return DbResponseParser.parse_task_results(data_list)
            case "EventList":  # ИЗМЕНЕНО
                return DbResponseParser.parse_event_results(data_list)
            case _:
                return f"Неизвестный тип результата для отображения: {result_type}"

    @staticmethod
    def parse_person(data_row: tuple) -> str:
        contacts = data_row[10] if len(data_row) > 10 and data_row[10] else {}
        phone = contacts.get("phone")
        email = contacts.get("email")
        birthday_str = data_row[4].strftime("%d.%m.%Y") if data_row[4] else "-"
        firstday_str = data_row[5].strftime("%d.%m.%Y") if data_row[5] else "-"
        return textwrap.dedent(f'''
                Нашёл первое совпадение:

                <b>{data_row[1] or ""} {data_row[2] or ""} {data_row[3] or ""}</b>
                <b>День рождения:</b> {birthday_str}
                <b>Вступил в должность:</b> {firstday_str}
                <b>Пишет на:</b> {data_row[6] or "-"}
                <b>Грейд:</b> {data_row[7] or "-"}
                <b>Сейчас работает над проектом:</b> {data_row[8] or "-"}
                <b>Состоит в отделе:</b> {data_row[9] or "-"}
                {f"<b>Почта:</b> {email}" if email else "<b>Почта:</b> -"}
                {f"<b>Номер телефона:</b> {phone}" if phone else "<b>Номер телефона:</b> -"}
            ''').strip()

    @staticmethod
    def parse_birthday_results(data_list: list) -> str:
        if not data_list:
            return "Дни рождения по вашим критериям не найдены."
        messages = ["<b>Найдены следующие дни рождения:</b>"]
        for row in data_list:
            surname = row[1] or ""
            name = row[2] or ""
            father = row[3] or ""
            birthday_date = row[4]
            department_name = row[5] or "Не указан"
            birthday_str = birthday_date.strftime("%d.%m") if isinstance(birthday_date, (
            datetime.date, datetime.datetime)) else "Дата не указана"
            person_info = f"{surname} {name} {father}".strip()
            messages.append(f"- {person_info} ({birthday_str}), Отдел: {department_name}")
        if len(messages) == 1:
            return "Дни рождения по вашим критериям не найдены."
        return "\n".join(messages)

    @staticmethod
    def parse_task_results(data_list: list) -> str:
        if not data_list:
            return "Задачи по вашим критериям не найдены."
        messages = ["<b>Найдены следующие задачи:</b>"]
        for row in data_list:
            task_name_val = row[1] or "Без названия"
            description_val = row[2] or "Нет описания"
            deadline_val = row[3].strftime("%d.%m.%Y %H:%M") if isinstance(row[3], (
            datetime.date, datetime.datetime)) else "Нет даты"
            status_val = "-"
            priority_val = "-"
            assignee_name_val = row[4] or ""
            assignee_surname_val = row[5] or ""
            project_name_val = row[6] or "Без проекта"
            assignee_full_name = f"{assignee_surname_val} {assignee_name_val}".strip()
            task_info = f"<b>{task_name_val}</b> (Проект: {project_name_val})"
            task_info += f"\n  <i>Описание:</i> {description_val}"
            task_info += f"\n  Исполнитель: {assignee_full_name if assignee_full_name else '-'}"
            task_info += f"\n  Дата/Дедлайн: {deadline_val}, Статус: {status_val}, Приоритет: {priority_val}"
            messages.append(task_info)
        if len(messages) == 1:
            return "Задачи по вашим критериям не найдены."
        return "\n\n".join(messages)

    @staticmethod
    def parse_event_results(data_list: list) -> str:
        if not data_list:
            return "Мероприятия по вашим критериям не найдены."

        messages = ["<b>Найдены следующие мероприятия:</b>"]
        for row in data_list:
            # Индексы соответствуют select_fields в search_event:
            # row[0] - 'EventList' (маркер)
            # row[1] - event_name
            # row[2] - event_begin (TIMESTAMPTZ)
            # row[3] - event_end (TIMESTAMPTZ или None) <--- ИЗМЕНЕНО
            # row[4] - category_name
            # row[5] - event_description (если полное имя "Description")
            # row[6] - organizer_name
            # row[7] - organizer_surname

            event_name = row[1] or "Без названия"
            event_begin_dt = row[2]  # datetime.datetime object
            event_end_dt = row[3]  # datetime.datetime object or None <--- ПОЛУЧАЕМ event_end
            category_name = row[4] or "Не указана"
            description = row[5] or "Нет описания"  # Убедитесь, что имя поля в БД - "Description"
            organizer_name = row[6] or ""
            organizer_surname = row[7] or ""

            organizer_full_name = f"{organizer_surname} {organizer_name}".strip()
            if not organizer_full_name:
                organizer_full_name = "Не указан"

            begin_str = event_begin_dt.strftime("%d.%m.%Y в %H:%M") if event_begin_dt else "Время начала не указано"
            end_str = event_end_dt.strftime(
                "%d.%m.%Y в %H:%M") if event_end_dt else ""  # Время окончания может отсутствовать

            duration_str = ""
            # Вычисляем длительность, если есть и начало, и конец
            if event_begin_dt and event_end_dt and event_end_dt > event_begin_dt:
                event_duration_td = event_end_dt - event_begin_dt
                hours, remainder = divmod(event_duration_td.total_seconds(), 3600)
                minutes, _ = divmod(remainder, 60)
                if hours > 0 and minutes > 0:
                    duration_str = f"{int(hours)} ч {int(minutes)} мин"
                elif hours > 0:
                    duration_str = f"{int(hours)} ч"
                elif minutes > 0:
                    duration_str = f"{int(minutes)} мин"
                elif event_duration_td.total_seconds() > 0:  # Если длительность < 1 минуты, но > 0
                    duration_str = f"{int(event_duration_td.total_seconds())} сек"

            event_info = f"<b>{event_name}</b> (Категория: {category_name})"
            event_info += f"\n  <i>Начало:</i> {begin_str}"
            if end_str and not duration_str:  # Если есть время окончания, но длительность не вычислена (например, end = begin)
                event_info += f"\n  <i>Окончание:</i> {end_str}"
            elif duration_str:
                event_info += f", <i>Длительность:</i> {duration_str}"

            event_info += f"\n  <i>Организатор:</i> {organizer_full_name}"
            if description and description != "Нет описания":
                event_info += f"\n  <i>Описание:</i> {textwrap.shorten(description, width=100, placeholder='...')}"

            messages.append(event_info)

        if len(messages) == 1:
            return "Мероприятия по вашим критериям не найдены."
        return "\n\n".join(messages)