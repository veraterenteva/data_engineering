import pandas as pd
from sqlalchemy import create_engine, text
from etl.config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DBNAME, PG_TABLE
from etl.logger import get_logger

logger = get_logger("load")

class ParquetLoader:
    """
    Load.py для загрузки сформированного на этапах extract и transform .parquet файла
    в БД PostgreSQL по указанным кредам
    """
    def __init__(self, parquet_path: str, limit: int = 100):
        self.parquet_path = parquet_path
        self.limit = limit
        self.table = PG_TABLE
        self.engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
        )

    def load_to_postgres(self):
        df = pd.read_parquet(self.parquet_path).head(self.limit)
        logger.info(f"Загружаем {len(df)} строк из {self.parquet_path} в {self.table}")
        df.to_sql(self.table, con=self.engine, if_exists="replace", index=False)
        logger.info("Загрузка в БД завершена успешно.")
        return df