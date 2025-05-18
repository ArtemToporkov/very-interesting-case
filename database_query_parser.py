import datetime

MONTHS = ["january", "february", "march", "april", "may", "june","july", "august", "september","october", "november", "december"]
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
DAY_EXTENTIONS = ["rd", "th", "st", "nd"]


class DbQueryParser:
    @staticmethod
    def parse(data: dict) -> str:
        match data["intent"]["name"]:
            case "search_person":
                return DbQueryParser.search_person(data)
            case "search_event":
                return DbQueryParser.search_event(data)
            case "find_birthday":
                return DbQueryParser.find_birthday(data)
            case "check_task":
                return DbQueryParser.check_task(data)

        return ''

    @staticmethod
    def search_person(data: dict) -> str:

        for entity in data['entities']:
            if entity['entity'] == 'name':
                return f'''
                    select 
                        'PersonInfo',
                        emp."Surname", 
                        emp."Name", 
                        emp."Father", 
                        emp."Birthday", 
                        emp."FirstDay", 
                        lng."Name", 
                        rnk."Status",
                        prj."Name",
                        dprt."Name",
                        emp."Contacts"
                    from "Employees" as emp
                    join "Classes" as cl on cl."Class_Id" = emp."ClassId" 
                    join "Languages" as lng on lng."Language_Id"=emp."LanguageId"
                    join "Rank" as rnk on rnk."Rank_Id" = emp."RankId"
                    join "Project" as prj on prj."Project_Id"=emp."ProjectId"
                    join "Department" as dprt on dprt."Department_Id"=emp."DepartmentId"
                    where emp."Name"='{entity['value']}'
                    limit 1;
                    '''
        raise Exception("Данные не обнаружены")

    @staticmethod
    def search_event(data: dict) -> str:
        entities: list = data['entities']
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
        return result # TODO: куда-то делся метод get_date, вернуть

    @staticmethod
    def find_birthday(data: dict) -> str:
        entities = data['entities']
        for entity in entities:
            if entity['entity'] == 'date':
                return f"""select *
                           from employees
                           where birthday = {entity['value']}"""
        raise Exception("Данные не обнаружены")

    @staticmethod
    def check_task(data: dict) -> str:
        entities = data['entities']
        for entity in entities:
            if entity['name'] == 'name':
                return f'''
                select * 
                from tasks
                where employee_name = {entity['value']}
                '''
        raise Exception("Данные не обнаружены")


    @staticmethod
    def _entities_to_dict(entities: list) -> dict:
        result = {}
        for entity in entities:
            if entity["entity"] not in result:
                result[entity["entity"]] = [entity["value"]]
            else:
                result[entity["entity"]].append(entity["value"])
        return result
