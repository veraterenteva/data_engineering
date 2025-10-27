from tabulate import tabulate
from api_example.api_reader import get_object_ids, build_dataframe, COLUMNS_TO_KEEP, LIMIT
from link_data_retrieval.data_loader import GoogleDriveLoader
from link_data_retrieval.data_processor import DataProcessor
import pandas as pd
from write_to_db import write_dataframe_to_db
from dotenv import load_dotenv


def retrieve_by_api():
    object_ids = get_object_ids(LIMIT)
    df = build_dataframe(object_ids)

    print("Полный DataFrame:")
    print(tabulate(df.head(10), headers="keys", tablefmt="pretty"))

    df_simplified = df[COLUMNS_TO_KEEP]
    print("Упрощённый DataFrame:")
    print(tabulate(df_simplified.head(10), headers="keys", tablefmt="pretty"))
    return df_simplified


def retrieve_by_disk_link():
    file_id = "10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD"
    output = "dataset.csv"
    loader = GoogleDriveLoader(file_id, output)
    loader.download_file()

    df = pd.read_csv(output)
    print(tabulate(df.head(10), headers="keys", tablefmt="pretty"))
    print("Успешно загрузили dataset.csv")

    processor = DataProcessor(df)
    df = processor.clean_and_cast()
    processor.preview(10)

    print(df.dtypes.to_frame("dtype"))
    processor.to_parquet("dataset.parquet")
    return df


if __name__ == "__main__":
    load_dotenv()

    # 1. Получаем DataFrame
    df = retrieve_by_disk_link()  # тут есть шаг processor, который выполняет очистку и т.д.

    # 2. Отправляем данные в PostgreSQL
    write_dataframe_to_db(df, table_name="terenteva")
