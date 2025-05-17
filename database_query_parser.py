import datetime

MONTHS = ["january", "february", "march", "april", "may", "june","july", "august", "september","october", "november", "december"]
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
DAY_EXTENTIONS = ["rd", "th", "st", "nd"]
class DbQueryParser:
    @staticmethod
    def parse(data: dict) -> str:
        # TODO: сделать парсинг в запрос для БД

        return ''

    @staticmethod
    def search_person(data: dict) -> str:
        for entity in data['entities']:
            if entity['entity'] == 'name':
                return f"""
                    select birthday from names
                    where name={entity['value']}
                    """
                # TODO: заменить names на таблицу сотрудников, откуда изъять день рождения
        raise Exception
    
    @staticmethod
    def _parse_search_event(entities: list) -> str:
        keywords = DbQueryParser._entities_to_dict(entities)
        if 'event_name' in keywords:
            event_name = keywords['event_name']
        if 'date' in keywords:
            date = DbQueryParser.get_date(keywords['date'])


        event_name_contidion = f"""event_name = {event_name}"""
        date_contidion = f"""date > {date.strftime('%Y-%m-%d')}"""
        result = f"""select *
        from events
        where 
        {event_name_contidion} and
        {date_contidion}"""
        return result

    @staticmethod
    def _parse_find_birthday(entities: list) -> str:
        result = ""
        for entity in entities:
            if entity['name'] == 'date':
                result = f"""select *
                           from employees
                           where birthday = {entity['value']}"""
                break
        return result

    @staticmethod
    def _parse_check_task(entities: list) -> str:
        result = ""
        for entity in entities:
            if entity['name'] == 'name':
                result = f"""select * 
                            from tasks
                            where employee_name = {entity['value']}"""
        return result


    @staticmethod
    def _entities_to_dict(entities: list) -> dict:
        result = {}
        for entity in entities:
            if entity["entity"] not in result:
                result[entity["entity"]] = [entity["value"]]
            else:
                result[entity["entity"]].append(entity["value"])
        return result
