import textwrap
import datetime


class DbResponseParser:
    @staticmethod
    def parse_into_message(data_list: list) -> str:
        if not data_list or not data_list[0]:  # Проверка на пустой результат
            return "По вашему запросу ничего не найдено."

        # data_list это список записей (кортежей/DictRow) из БД
        # Первая запись содержит тип результата в первом элементе
        first_record = data_list[0]
        result_type = first_record[0]  # Первый столбец должен быть типом результата

        match result_type:
            case "PersonInfo":
                # Для PersonInfo, мы ожидаем одну запись в data_list, так как LIMIT 1
                return DbResponseParser.parse_person(first_record)  # Передаем одну запись
            case "BirthdayList":
                return DbResponseParser.parse_birthday_results(data_list)
            case "TaskList":
                return DbResponseParser.parse_task_results(data_list)
            # Добавьте сюда обработку других типов результатов
            case _:
                return f"Неизвестный тип результата для отображения: {result_type}"

    @staticmethod
    def parse_person(data_row: tuple) -> str:  # data_row это одна запись (кортеж/DictRow)
        # data_row[0] это 'PersonInfo', начинаем с data_row[1]
        contacts = data_row[10] if len(data_row) > 10 and data_row[10] else {}
        phone = contacts.get("phone")
        email = contacts.get("email")

        # Проверка на None перед форматированием дат
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

        # data_list[0][0] будет 'BirthdayList', его пропускаем
        # Начинаем с data_list[0][1] для первой записи, если она одна
        # или итерируемся по data_list, если их несколько

        messages = ["<b>Найдены следующие дни рождения:</b>"]
        for row in data_list:
            # row[0] это 'BirthdayList', фактические данные начинаются с row[1]
            # surname, name, father, birthday, department_name
            surname = row[1] or ""
            name = row[2] or ""
            father = row[3] or ""  # Отчество
            birthday_date = row[4]
            department_name = row[5] or "Не указан"

            birthday_str = birthday_date.strftime("%d.%m") if isinstance(birthday_date, (
            datetime.date, datetime.datetime)) else "Дата не указана"

            person_info = f"{surname} {name} {father}".strip()
            messages.append(f"- {person_info} ({birthday_str}), Отдел: {department_name}")

        if len(messages) == 1:  # Только заголовок
            return "Дни рождения по вашим критериям не найдены."
        return "\n".join(messages)

    @staticmethod
    def parse_task_results(data_list: list) -> str:
        if not data_list:
            return "Задачи по вашим критериям не найдены."

        messages = ["<b>Найдены следующие задачи:</b>"]
        for row in data_list:
            # task_name, description, deadline, status, priority, assignee_name, assignee_surname, project_name
            task_name_val = row[1] or "Без названия"
            # description_val = row[2] or "" # Описание может быть длинным
            deadline_val = row[3].strftime("%d.%m.%Y") if isinstance(row[3], (
            datetime.date, datetime.datetime)) else "Нет дедлайна"
            status_val = row[4] or "-"
            priority_val = row[5] or "-"
            assignee_name_val = row[6] or ""
            assignee_surname_val = row[7] or ""
            project_name_val = row[8] or "Без проекта"

            assignee_full_name = f"{assignee_surname_val} {assignee_name_val}".strip()

            task_info = f"<b>{task_name_val}</b> (Проект: {project_name_val})"
            task_info += f"\n  Исполнитель: {assignee_full_name if assignee_full_name else '-'}"
            task_info += f"\n  Дедлайн: {deadline_val}, Статус: {status_val}, Приоритет: {priority_val}"
            messages.append(task_info)

        if len(messages) == 1:  # Только заголовок
            return "Задачи по вашим критериям не найдены."
        return "\n\n".join(messages)