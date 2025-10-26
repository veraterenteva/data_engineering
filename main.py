import os

from tabulate import tabulate
from api_example.api_reader import get_object_ids, build_dataframe, COLUMNS_TO_KEEP, LIMIT
from link_data_retrieval.data_loader import GoogleDriveLoader
from link_data_retrieval.data_processor import DataProcessor
import psycopg2
import pandas as pd


def retrieve_by_api():
    object_ids = get_object_ids(LIMIT)
    df = build_dataframe(object_ids)

    # Полный DataFrame
    print()
    print(tabulate(df.head(10), headers="keys", tablefmt="pretty"))

    # Упрощённый DataFrame
    df_simplified = df[COLUMNS_TO_KEEP]
    print("Упрощённый DataFrame")
    print(tabulate(df_simplified.head(10), headers="keys", tablefmt="pretty"))
    return df_simplified


def retrieve_by_disk_link():
    file_id = "10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD"
    output = "dataset.csv"
    loader = GoogleDriveLoader(file_id, output)
    loader.download_file()

    df = pd.read_csv(output)

    print(tabulate(df.head(10), headers="keys", tablefmt="pretty"))
    print("Успешно загружен dataset.csv")

    processor = DataProcessor(df)
    df = processor.clean_and_cast()
    processor.preview(10)

    print(df.dtypes.to_frame("dtype"))
    processor.to_parquet("dataset.parquet")
    return df


if __name__ == "__main__":
    df = retrieve_by_disk_link()

    PG_HOST = os.environ.get("PG_HOST")
    PG_PORT = os.environ.get("PG_PORT", 5432)
    PG_USER = os.environ.get("PG_USER")
    PG_PASSWORD = os.environ.get("PG_PASSWORD")
    PG_DBNAME = os.environ.get("PG_DBNAME", "homeworks")

    # Проверка наличия обязательных переменных, креды не пушим
    required_vars = [PG_HOST, PG_USER, PG_PASSWORD]
    if not all(required_vars):
        raise EnvironmentError(
            "Не заданы все переменные окружения: PG_HOST, PG_USER, PG_PASSWORD"
        )

    # Подключение к PostgreSQL
    conn_pg = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DBNAME,
    )
    cur = conn_pg.cursor()

    data = df.head(100)

    table_name = "terenteva"

    # Создаём таблицу по структуре датафрейма и записываем
    # Экранируем названия столбцов двойными кавычками, потому что у нас есть столбцы "user" и т.д.
    columns = ", ".join([f'"{col}" TEXT' for col in data.columns])
    cur.execute(f'CREATE TABLE IF NOT EXISTS public."{table_name}" ({columns});')

    col_names = ", ".join([f'"{c}"' for c in data.columns])
    placeholders = ", ".join(["%s"] * len(data.columns))
    insert_query = f'INSERT INTO public."{table_name}" ({col_names}) VALUES ({placeholders})'

    for _, row in data.iterrows():
        cur.execute(insert_query, tuple(map(str, row.values)))

    conn_pg.commit()
    print(f"Успешно добавлено 100 строк в таблицу {table_name}")

    cur.close()
    conn_pg.close()
