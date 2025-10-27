import os
import pandas as pd
import psycopg2


# Обёртка над соединением с PostgreSQL
# Получаем параметры из .env файла
class PostgresConnector:
    def __init__(self):
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT", 5432)
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.dbname = os.getenv("PG_DBNAME", "homeworks")

        required_vars = [self.host, self.user, self.password]
        if not all(required_vars):
            raise EnvironmentError("Не заданы необходимые env переменные: PG_HOST, PG_USER, PG_PASSWORD")

        self.conn = None
        self.cur = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.dbname,
        )
        self.cur = self.conn.cursor()
        return self

    def execute(self, query, params=None):
        self.cur.execute(query, params)

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self):
        self.conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.conn.close()


class DataFrameWriter:
    # Мапим типы pd на типы PostgreSQL
    DEFAULT_DTYPE_MAP = {
        "string[python]": "TEXT",
        "Int16": "SMALLINT",
        "Int32": "INTEGER",
        "Int64": "BIGINT",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "boolean": "BOOLEAN",
    }

    FIXED_SCHEMA = {
        "genre": "TEXT",
        "year": "INTEGER",
        "certificate": "TEXT",
        "runtime": "SMALLINT",
        "user-votes": "INTEGER",
        "imdb-scores": "REAL",
        "metacritic-scores": "SMALLINT",
        "descriptions": "TEXT",
        "stars": "TEXT",
        "start_year": "INTEGER",
        "end_year": "INTEGER",
    }

    def __init__(self, connector: PostgresConnector, table_name):
        self.connector = connector
        self.table_name = table_name

    def map_dtype(self, col, dtype):
        if col in self.FIXED_SCHEMA:
            return self.FIXED_SCHEMA[col]
        dtype_str = str(dtype)
        for k, v in self.DEFAULT_DTYPE_MAP.items():
            if k.lower() in dtype_str.lower():
                return v
        return "TEXT"

    def write(self, df, limit=100):
        df = df.head(limit).replace({pd.NA: None})

        # Затираем старые записи, чтобы не писать по 100 повторно
        self.connector.execute(f'DROP TABLE IF EXISTS public."{self.table_name}" CASCADE;')

        # Создаём таблицу с корректными типами
        columns_def = ", ".join(
            [f'"{col}" {self.map_dtype(col, dtype)}' for col, dtype in df.dtypes.items()]
        )
        self.connector.execute(f'CREATE TABLE public."{self.table_name}" ({columns_def});')

        # Записываем данные под экранированием, иначе есть таблицы со служебными именами PostgreSQL
        col_names = ", ".join([f'"{c}"' for c in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f'INSERT INTO public."{self.table_name}" ({col_names}) VALUES ({placeholders})'

        for _, row in df.iterrows():
            self.connector.execute(insert_query, tuple(row.values))
        # Проверяем результат
        self.connector.commit()
        self.validate(df)

    # Проверка количества строк
    def validate(self, df):
        self.connector.execute(f'SELECT COUNT(*) FROM public."{self.table_name}";')
        count = self.connector.fetchall()[0][0]
        print(f"В таблице {self.table_name}: {count} строк (ожидалось {len(df)})")
        # Проверка типов
        self.connector.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{self.table_name}'
            ORDER BY ordinal_position;
        """)
        db_types = dict(self.connector.fetchall())
        # Вывод результатов проверки
        for col, dtype in df.dtypes.items():
            expected = self.map_dtype(col, dtype)
            actual = db_types.get(col)
            if actual and expected.lower() in actual.lower():
                print(f"Всё хорошо {col:<20} - {expected}")
            else:
                print(f"Проблема {col:<20} - ожидался {expected}, в БД {actual}")


def write_dataframe_to_db(df, table_name):  # сохраняем сигнатуру
    with PostgresConnector() as connector:
        writer = DataFrameWriter(connector, table_name)
        writer.write(df)
