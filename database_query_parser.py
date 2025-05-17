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
    def _entities_to_dict(entities: list) -> dict:
        result = {}
        for entity in entities:
            if entity["entity"] not in result:
                result[entity["entity"]] = [entity["value"]]
            else:
                result[entity["entity"]].append(entity["value"])
        return result

    @staticmethod
    def get_date(text):
        text = text.lower()
        today = datetime.date.today()

        if text.count("today") > 0:
            return today

        day = -1
        day_of_week = -1
        month = -1
        year = today.year

        for word in text.split():
            if word in MONTHS:
                month = MONTHS.index(word) + 1
            elif word in DAYS:
                day_of_week = DAYS.index(word)
            elif word.isdigit():
                day = int(word)
            else:
                for ext in DAY_EXTENTIONS:
                    found = word.find(ext)
                    if found > 0:
                        try:
                            day = int(word[:found])
                        except:
                            pass

        # THE NEW PART STARTS HERE
        if month < today.month and month != -1:  # if the month mentioned is before the current month set the year to the next
            year = year + 1

        # This is slighlty different from the video but the correct version
        if month == -1 and day != -1:  # if we didn't find a month, but we have a day
            if day < today.day:
                month = today.month + 1
            else:
                month = today.month

        # if we only found a dta of the week
        if month == -1 and day == -1 and day_of_week != -1:
            current_day_of_week = today.weekday()
            dif = day_of_week - current_day_of_week

            if dif < 0:
                dif += 7
                if text.count("next") >= 1:
                    dif += 7

            return today + datetime.timedelta(dif)

        if day != -1:  # FIXED FROM VIDEO
            return datetime.date(month=month, day=day, year=year)
