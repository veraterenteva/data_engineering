import os

import pandas as pd
import psycopg2

def map_dtype(dtype, dtype_map):
    dtype_str = str(dtype)
    for k, v in dtype_map.items():
        if k.lower() in dtype_str.lower():
            return v
    return "TEXT"

def write_dataframe_to_db(df, table_name):
    """
    Записывает DataFrame в таблицу PostgreSQL.
    Использует переменные окружения для подключения.
    """
    PG_HOST = os.environ.get("PG_HOST")
    PG_PORT = os.environ.get("PG_PORT", 5432)
    PG_USER = os.environ.get("PG_USER")
    PG_PASSWORD = os.environ.get("PG_PASSWORD")
    PG_DBNAME = os.environ.get("PG_DBNAME", "homeworks")

    # Проверка обязательных переменных окружения
    required_vars = [PG_HOST, PG_USER, PG_PASSWORD]
    if not all(required_vars):
        raise EnvironmentError(
            "Не заданы все переменные окружения: PG_HOST, PG_USER, PG_PASSWORD"
        )

    conn_pg = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DBNAME,
    )
    cur = conn_pg.cursor()

    # Маппинг типов pd на типы PostreSQL
    dtype_map = {
        "string[python]": "TEXT",
        "Int16": "SMALLINT",
        "Int32": "INTEGER",
        "Int64": "BIGINT",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "boolean": "BOOLEAN",
    }

    # Если таблица есть, мы её дропаем, чтобы при каждом запуске кода не делать запись 100 строк
    cur.execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;')

    columns_def = ", ".join(
        [f'"{col}" {map_dtype(dtype, dtype_map)}' for col, dtype in df.dtypes.items()]
    )
    cur.execute(f'CREATE TABLE public."{table_name}" ({columns_def});')

    # Берём только первые 100 строк
    data = df.head(100)
    data = data.replace({pd.NA: None})

    # Создание таблицы с экранированными названиями
    col_names = ", ".join([f'"{c}"' for c in data.columns])
    placeholders = ", ".join(["%s"] * len(data.columns))
    insert_query = f'INSERT INTO public."{table_name}" ({col_names}) VALUES ({placeholders})'

    for _, row in data.iterrows():
        cur.execute(insert_query, tuple(row.values))

    conn_pg.commit()
    print(f"Таблица {table_name} создана и заполнена {len(data)} строками.") # выводим, на случай, если записали меньше

    # Проверка количества строк
    cur.execute(f'SELECT COUNT(*) FROM public."{table_name}";')
    count = cur.fetchone()[0]
    if count != len(data):
        print(f"Не дописали, ожидалось {len(data)}")
    else:
        print(f"Количество строк совпадает с записанным нами {count}")

    # Проверка типов в БД
    cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)
    db_types = dict(cur.fetchall())

    print("Проверка типов данных:")
    for col, dtype in df.dtypes.items():
        expected = map_dtype(dtype, dtype_map)
        actual = db_types.get(col)
        if actual and expected.lower() in actual.lower():
            print(f"{col:<20} → {expected}")
        else:
            print(f"{col:<20} → ожидался {expected}, в БД {actual}")

    cur.close()
    conn_pg.close()