from sqlalchemy import create_engine, inspect, select, func, MetaData, Table
from etl.config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DBNAME, PG_TABLE
from etl.logger import get_logger

logger = get_logger("validate")


class PostgresValidator:
    """
    Validate.py для проверки корректности данных после загрузки в PostgreSQL

    Проверяет
    - наличие соединения с БД
    - количество записей в таблице (по сравнению с ожидаемым)
    - соответствие имён и типов столбцов между DataFrame и тем, что будет записано в БД
    """

    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
        )
        self.table = PG_TABLE

    def _check_connection(self):
        """Проверяет, что соединение с PostgreSQL установлено путём исполнения простого запроса"""
        try:
            with self.engine.connect() as conn:
                conn.execute(select(1))
            logger.info("Соединение с PostgreSQL установлено успешно.")
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            raise

    def validate_count(self, expected_count: int) -> bool:
        """Проверяет совпадение количества строк между загружаемым и обнаруженным в БД"""
        self._check_connection()

        metadata = MetaData()
        metadata.reflect(bind=self.engine, only=[self.table])
        table = Table(self.table, metadata, autoload_with=self.engine)

        with self.engine.connect() as conn:
            count_query = select(func.count()).select_from(table)
            count_in_db = conn.execute(count_query).scalar()

        logger.info(f"Проверка количества записей:")
        logger.info(f"  Ожидалось: {expected_count}")
        logger.info(f"  В таблице: {count_in_db}")

        if count_in_db != expected_count:
            logger.warning("Количество строк не совпадает!")
            return False

        logger.info("Количество строк совпадает.")
        return True

    def validate_schema(self, df):
        """
        Сверяет имена и типы столбцов между DataFrame и таблицей в БД.
        """
        insp = inspect(self.engine)
        columns = insp.get_columns(self.table)
        db_types = {col["name"]: str(col["type"]).lower() for col in columns}

        # соответствия типов pandas с PostgreSQL
        type_map = {
            "int16": "smallint",
            "int32": "integer",
            "int64": "bigint",
            "float32": "real",
            "float64": "double precision",
            "string": "text",
            "object": "text",
        }

        logger.info("Проверка структуры таблицы и типов данных:")

        for col, dtype in df.dtypes.items():
            pandas_type = str(dtype).lower()
            expected_pg_type = type_map.get(pandas_type, pandas_type)
            actual_pg_type = db_types.get(col)

            if actual_pg_type is None:
                logger.warning(f"В БД отсутствует столбец '{col}'")
            elif expected_pg_type not in actual_pg_type:
                logger.warning(
                    f"Несовпадение типа в '{col}': pandas={pandas_type}, postgres={actual_pg_type}"
                )
            else:
                logger.info(f"{col:<20} pandas={pandas_type:<10} postgres={actual_pg_type}")

    def run_validation(self, df):
        """Полный цикл проверки на столбцы, типы и число записей"""
        logger.info("Запуск полной валидации")
        self.validate_count(len(df))
        self.validate_schema(df)
        logger.info("Валидация завершена")