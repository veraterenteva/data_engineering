import argparse
import pandas as pd
from etl.extract import GoogleDriveLoader
from etl.transform import DataProcessor
from etl.load import ParquetLoader
from etl.validate import PostgresValidator
from etl.logger import get_logger

logger = get_logger("main")


def main():
    parser = argparse.ArgumentParser(description="ETL pipeline")

    parser.add_argument("--file-id", help="Google Drive file ID источника данных")
    parser.add_argument("--limit", type=int, default=100, help="Лимит строк для загрузки в БД")
    parser.add_argument("--columns", nargs="*", help="Столбцы для parquet и БД")
    parser.add_argument("--validate-only", action="store_true", help="Проверить таблицу в БД без ETL")
    parser.add_argument("--info", action="store_true", help="Показать информацию о пакете")

    args = parser.parse_args()

    if args.info:
        from etl import __version__
        print(f"ETL v{__version__} — Super-duper cookiecutter-style pipeline")
        print("Модули: extract, transform, load, validate")
        return

    if args.validate_only:
        validator = PostgresValidator()
        validator._check_connection()
        logger.info("Режим проверки структуры БД и соединения с ней завершён.")
        return

    if not args.file_id:
        parser.error("Параметр --file-id обязателен, если не указан --info или --validate-only.")

    # 1. Extract
    loader = GoogleDriveLoader(args.file_id)
    csv_path = loader.download_file()

    # 2. Transform
    df = pd.read_csv(csv_path)
    processor = DataProcessor(df)
    df = processor.clean_and_cast()
    df = processor.select_columns(args.columns)
    parquet_path = processor.to_parquet("data/processed/dataset.parquet")

    # 3. Load
    parquet_loader = ParquetLoader(parquet_path, args.limit)
    df_loaded = parquet_loader.load_to_postgres()

    # 4. Validate
    validator = PostgresValidator()
    validator.run_validation(df_loaded)


if __name__ == "__main__":
    main()
