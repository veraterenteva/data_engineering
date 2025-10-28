import os
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class ParquetToPostgresLoader:
    def __init__(self):
        # Настройки подключения из окружения
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT", 5432)
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.dbname = os.getenv("PG_DBNAME", "homeworks")
        self.table_name = os.getenv("PG_TABLE", "dataset")  # фамилию не палим

        if not all([self.host, self.user, self.password]):
            raise EnvironmentError(
                "Не заданы все необходимые переменные окружения: PG_HOST, PG_USER, PG_PASSWORD"
            )

        # Подключение через SQLAlchemy
        self.engine: Engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}",
            isolation_level="AUTOCOMMIT",  # чтобы не зависали транзакции
        )

        # Проверяем соединение сразу при инициализации
        self._test_connection()

    def _test_connection(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1;"))
            print(f"Успешное подключение к PostgreSQL")
        except SQLAlchemyError as e:
            raise ConnectionError(f"Ошибка подключения к PostgreSQL: {e}")

    def load_parquet(self, parquet_path, limit=100):
        df = pd.read_parquet(parquet_path).head(limit)

        # Запись в таблицу, безопасно (SQLAlchemy сам экранирует имена)
        # Пересоздаём таблицу для того, чтобы при перезапуске кода не записывалось ещё 100 записей
        df.to_sql(self.table_name, self.engine, if_exists="replace", index=False)

        print(f"Загружено {len(df)} строк из {parquet_path} в таблицу {self.table_name}")

        # После записи проводим валидацию
        self.validate_load(df)

    def validate_load(self, df):
        with self.engine.connect() as conn:
            # Соединение и так будет закрыто, но мы сделаем это явно так же
            # Проверка количества строк
            count_query = text(f'SELECT COUNT(*) FROM "{self.table_name}";')
            count_in_db = conn.execute(count_query).scalar()

            print("Проверка количества записей:")
            print(f"Ожидалось: {len(df)}")
            print(f"В таблице: {count_in_db}")

            if count_in_db != len(df):
                print("Количество записей не совпадает!")
            else:
                print("Количество строк совпадает, всё хорошо")

            print("Проверка типов данных:")
            insp = inspect(self.engine)
            columns = insp.get_columns(self.table_name)
            db_types = {col["name"]: col["type"] for col in columns}

            for col, dtype in df.dtypes.items():
                expected = str(dtype)
                actual = str(db_types.get(col))
                print(f"В столбце {col:<20} тип данных pandas={expected:<15}, в postgres={actual}")

    def close(self):
        # На всякий случай закрываем откровенно
        self.engine.dispose()
        print("Соединение с PostgreSQL закрыто.")

    def run(self, parquet_path, limit=100):
        print(f"Начало загрузки parquet to PostgreSQL from {self.table_name}")
        self.load_parquet(parquet_path, limit)
        print("Загрузка и валидация завершены успешно")
        self.close()
