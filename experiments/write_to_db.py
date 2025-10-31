import os
import re
import pandas as pd
from sqlalchemy import create_engine, inspect, text, MetaData, Table, select, func
from sqlalchemy.exc import SQLAlchemyError, OperationalError


class ParquetToPostgresLoader:
    def __init__(self):
        self.table_name = os.environ.get("PG_TABLE")
        self._validate_table_name()
        self.engine = self._create_engine()
        self._check_connection()

    def _validate_table_name(self):  # Вспомогательная функция проверки имени таблицы для доп защиты
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.table_name):
            raise ValueError(f"Недопустимое имя таблицы: {self.table_name}")

    def _create_engine(self):
        PG_HOST = os.environ.get("PG_HOST")
        PG_PORT = os.environ.get("PG_PORT", "5432")
        PG_USER = os.environ.get("PG_USER")
        PG_PASSWORD = os.environ.get("PG_PASSWORD")
        PG_DBNAME = os.environ.get("PG_DBNAME", "homeworks")

        if not all([PG_HOST, PG_USER, PG_PASSWORD]):
            raise EnvironmentError(
                "Не заданы все переменные окружения: PG_HOST, PG_USER, PG_PASSWORD"
            )

        return create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
        )

    def _check_connection(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Соединение с PostgreSQL установлено")
        except OperationalError as e:
            raise ConnectionError(f"Ошибка подключения к PostgreSQL: {e}")

    def load_parquet(self, parquet_path, limit=100):
        df = pd.read_parquet(parquet_path).head(limit)
        print(f"Загрузка {len(df)} строк из {parquet_path} в таблицу {self.table_name}")

        # Пересоздаём таблицу полностью, чтобы не дописывать 100 строк при каждом перезапуске
        df.to_sql(
            name=self.table_name,
            con=self.engine,
            if_exists="replace",
            index=False,
            method="multi",
        )

        print(f"Таблица {self.table_name} успешно создана и заполнена.")

        if "id" in df.columns:
            self._add_primary_key()

        self.validate_load(df)

    def _add_primary_key(self):
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE "{self.table_name}" ADD PRIMARY KEY ("id");'))
            print("Поле 'id' установлено как PRIMARY KEY.")
        except SQLAlchemyError as e:
            print(f"Не удалось установить PRIMARY KEY: {e}")

    def validate_load(self, df: pd.DataFrame):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.table_name):
            raise ValueError(f"Недопустимое имя таблицы: {self.table_name}")

        with self.engine.connect() as conn:
            # Проверяем, что соединение реально установлено
            try:
                conn.execute(text("SELECT 1"))
                print("Соединение с базой данных успешно установлено.")
            except Exception as e:
                raise ConnectionError(f"Ошибка соединения с БД: {e}")

            # Отражаем таблицу безопасно
            metadata = MetaData()
            metadata.reflect(bind=self.engine, only=[self.table_name])
            table = Table(self.table_name, metadata, autoload_with=self.engine)

            count_query = select(func.count()).select_from(table)
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
