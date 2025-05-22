import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError


class Database:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        self.db_config = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Устанавливает соединение с базой данных."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            print("Подключение к базе данных успешно установлено.")
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def ensure_connection(self):
        """Проверяет и при необходимости восстанавливает соединение."""
        try:
            if self.conn is None or self.conn.closed != 0:
                print("Соединение отсутствует. Повторное подключение...")
                self.connect()
            else:
                self.conn.poll()  # проверка активности соединения
        except OperationalError as e:
            print(f"Ошибка соединения: {e}. Попытка переподключения...")
            self.connect()

    def execute_query(self, query, params=None, fetch=False):
        """Выполняет SQL-запрос с обеспечением стабильности соединения."""
        try:
            self.ensure_connection()
            self.cursor.execute(query, params)
            if fetch:
                return self.cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Ошибка выполнения запроса: {e}")
            return None

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Соединение с базой данных закрыто.")

    def __del__(self):
        self.close()
