import textwrap


class DbResponseParser:
    @staticmethod
    def parse_into_message(data: list) -> str:
        match data[0]:
            case "PersonInfo":
                return DbResponseParser.parse_person(data)

    @staticmethod
    def parse_person(data: list) -> str:
        contacts = data[10]
        phone = None
        email = None
        if "email" in contacts.keys():
            email = contacts["email"]
        if "phone" in contacts.keys():
            phone = contacts["phone"]
        return textwrap.dedent(f'''
                Нашёл первое совпадение:
                
                <b>{data[1]} {data[2]} {data[3]}</b>
                <b>День рождения:</b> {data[4].strftime("%d.%m.%Y")}
                <b>Вступил в должность:</b> {data[5].strftime("%d.%m.%Y")}
                <b>Пишет на:</b> {data[6]}
                <b>Грейд:</b> {data[7]}
                <b>Сейчас работает над проектом:</b> {data[8]}
                <b>Состоит в отделе:</b> {data[9]}
                {f"<b>Почта:</b> {email}" if email else f"<b>Почта:</b> -"}
                {f"<b>Номер телефона:</b> {phone}" if phone else f"<b>Номер телефона:</b> -"}
            ''').strip()
