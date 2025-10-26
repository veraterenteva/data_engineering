import os
import psycopg2

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

    # Берём только первые 100 строк
    data = df.head(100)

    # Создаём таблицу, экранируем названия столбцов
    columns = ", ".join([f'"{col}" TEXT' for col in data.columns])
    cur.execute(f'CREATE TABLE IF NOT EXISTS public."{table_name}" ({columns});')

    col_names = ", ".join([f'"{c}"' for c in data.columns])
    placeholders = ", ".join(["%s"] * len(data.columns))
    insert_query = f'INSERT INTO public."{table_name}" ({col_names}) VALUES ({placeholders})'

    for _, row in data.iterrows():
        cur.execute(insert_query, tuple(map(str, row.values)))

    conn_pg.commit()
    print(f"Успешно добавлено {len(data)} строк в таблицу {table_name}")

    # Проверка количества записей
    cur.execute(f'SELECT COUNT(*) FROM public."{table_name}";')
    count = cur.fetchone()[0]
    print(f"Всего строк в таблице {table_name}: {count}")

    cur.close()
    conn_pg.close()