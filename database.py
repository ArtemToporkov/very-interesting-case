import psycopg2
from psycopg2.extras import DictCursor


class Database:
    def __init__(self, dbname, user, password, host="localhost", port="5432"):
        """Инициализация подключения к базе данных."""
        self.conn = None
        self.cursor = None
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            # Используем DictCursor для получения результатов в виде словаря
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
            print("Подключение к базе данных успешно установлено.")
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def execute_query(self, query, params=None, fetch=False):
        """Выполняет SQL-запрос и возвращает результат (если fetch=True)."""
        try:
            self.cursor.execute(query, params)
            if fetch:
                return self.cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Ошибка выполнения запроса: {e}")
            return None

    def close(self):
        """Закрывает соединение с базой данных."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Соединение с базой данных закрыто.")

    def __del__(self):
        """Деструктор - автоматически закрывает соединение при удалении объекта."""
        self.close()