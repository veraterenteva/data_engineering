<div align="center">

# **Movies ETL ITMO 2025**
**ETL-пакет для загрузки, очистки данных IMDB и публикации в PostgreSQL**  
*Инжиниринг данных • Университет ИТМО • Год: 2025*   
</div>
В качестве данных были выбраны данные IMDB, полученные путём разбора html страниц сайта [IMDB](https://www.imdb.com/) . Примеры парсинга можно увидеть внутри проекта /experiments/data_retrieval/parse_example. Полученный датасет был размещён по ссылке на [GoogleDisk](https://drive.google.com/drive/folders/1BudgHC7Qf5wdLqyGeA76gvpOwXr7Ln8y) и использовался как начальная точка для ETL пайплайна. В ходе исполнения extract данные загружаются для последующей очистки в формат .csv, проходят этап transform с приведением типов и парсингом столбцов в соответствии с ожидаемым типом (например, Runtime) и на этапе load загружаются в базу данных PostgreSQL              
   
   
---
**EDA (но химический датасет)**
https://nbviewer.org/github/veraterenteva/data_engineering/blob/main/experiments/EDA/EDA.ipynb  
      
---    
    
**Пример первых 3 строк dataframe**
<div align="center">

| # | Title                          | Genre                       | Year | Cert. | Runtime | Votes  | IMDb | Meta | Description                                                                                           | Stars                                  |
|---|--------------------------------|-----------------------------|------|-------|---------|--------|------|------|-------------------------------------------------------------------------------------------------------|----------------------------------------|
| 0 | The Fall of the House of Usher | Drama, Horror, Mystery      | 2023 | TV-MA | 493     | 67677  | 8.0  | –    | To secure their fortune two ruthless siblings build a dynasty that crumbles as heirs mysteriously die. | Carla Gugino, Bruce Greenwood, Mary McDonnell, Henry Thomas |
| 1 | Killers of the Flower Moon     | Crime, Drama, History       | 2023 | R     | 206     | 83339  | 8.0  | 89   | When oil is discovered in 1920s Oklahoma, Osage people are murdered until the FBI steps in.           | Leonardo DiCaprio, Robert De Niro, Lily Gladstone, Jesse Plemons |
| 2 | Bodies                         | Crime, Drama, History       | 2023 | TV-MA | 455     | 24785  | 7.4  | –    | Four detectives in four time periods of London investigate the same murder.                           | Amaka Okafor, Kyle Soller, Stephen Graham, Shira Haas |
</div>

**Типы данных**      
  
<div align="center">       
    
| Столбец данных     | Тип после этапа transform    |
|--------------------|-----------------|
| title              | string[python]  |
| genre              | string[python]  |
| year               | Int32           |
| certificate        | string[python]  |
| runtime            | Int16           |
| user-votes         | Int32           |
| imdb-scores        | float32         |
| metacritic-scores  | Int16           |
| descriptions       | string[python]  |
| stars              | string[python]  |
| start_year         | Int32           |
| end_year           | Int32           |
   
</div>  

---


## **Содержание**

- [Цель проекта](#цель-проекта)
- [Установка и подготовка окружения](#установка-и-подготовка-окружения)
- [CLI-интерфейс и примеры использования](#cli-интерфейс-и-примеры-использования)
- [Примеры использования](#примеры-использования)
- [Архитектура пайплайна](#архитектура-пайплайна)
- [Артефакты](#артефакты)

---

# **Цель проекта**

Создать **ETL-сервис**, который автоматически:
1. Извлекает данные из Google Drive (CSV);
2. Преобразует и валидирует их (очистка, приведение типов, фильтрация);
3. Сохраняет в формат Parquet;
4. Загружает данные в PostgreSQL;
5. Проверяет корректность загруженной таблицы (валидация по количеству строк и типам).

---

# **Установка и подготовка окружения**

### **Предварительные требования**

- Python ≥ 3.10
- PostgreSQL 13+  
- Настроенное окружение и установленные зависимости
```bash
git clone https://github.com/veraterenteva/data_engineering
cd data_engineering
poetry install
poetry shell
```
- Файл .env в корне проекта с настройками подключения:
```bash
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=postgres
PG_DBNAME=etl_db
PG_TABLE=movies
 ```
---

# **CLI-интерфейс и примеры использования**

ETL-пайплайн управляется через единый CLI:
```bash
python -m etl.main [параметры]
 ```

<div align="center">

| Параметр     | Описание                                 | Пример                                          |
|--------------|------------------------------------------|-------------------------------------------------|
| `--file-id`  | ID CSV-файла в Google Drive              | `--file-id 10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD`  |
| `--limit`    | Количество строк для загрузки в БД       | `--limit 100`                                   |
| `--columns`  | Список столбцов для parquet и БД         | `--columns title year genre imdb-scores`       |
| `--validate-only` | Только проверить таблицу в БД         | `--validate-only`                               |
| `--info`     | Информация о пакете и версии             | `--info`                                        |
| `--help`     | Справка по параметрам                    | `--help`                                        |
</div>

---

# **Примеры использования**
1. Полный ETL-процесс
```bash
python -m etl.main \
  --file-id 10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD \
  --limit 200
```
2. Проверка состояния и валидности таблицы на текущий момент
```bash
python -m etl.main --validate-only
```
3. Информация о версии и содержании пакета
```bash
python -m etl.main --info
```
3. Информация о поддерживаемых параметрах
```bash
python -m etl.main --help
```
---

# **Архитектура пайплайна**
<div align="center">
Схема потока данных

Google Drive → Extract (CSV) → Transform (DataFrame) → Parquet → Load (PostgreSQL) → Validate

| Этап      | Модуль                       | Назначение                                      |
|-----------|------------------------------|-------------------------------------------------|
| Extract   | etl.extract.GoogleDriveLoader| Скачивание CSV-файла из Google Drive           |
| Transform | etl.transform.DataProcessor  | Очистка, приведение типов, создание parquet    |
| Load      | etl.load.ParquetLoader       | Загрузка parquet в PostgreSQL                  |
| Validate  | etl.validate.PostgresValidator| Проверка количества строк и типов данных       |
</div>

Структура проекта следующая:
```bash
Movies/
├── experiments/
│   └── data_retrieval/
│       └── api_example/
│           ├── api_reader.py
│       └── link_data_retrieval/
│           ├── data_loader.py
│           ├── data_processor.py
│       └── parse_example/
│           ├── crawler.py
│           ├── parser.py
│   └── EDA/
│       └── plots/
│           ├── ...
│       └── EDA.ipynb
│   └── main.py
│   └── write_to_db.py
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       ├── validate.py
│       ├── logger.py
│       └── main.py
├── data/
│   ├── raw/
│   └── processed/
├── .env
├── requirements.txt
├── .gitignore
└── README.md
├── pyproject.toml
└── setup.cfg
```

---

# **Артефакты**
Выходные данные и артефакты
| Файл/Хранилище            | Формат    | Назначение                                      |
|---------------------------|-----------|-------------------------------------------------|
| data/raw/dataset.csv      | CSV       | Сырые данные, загруженные из источника         |
| data/processed/dataset.parquet | Parquet | Обработанные данные, готовые для анализа       |
| PostgreSQL (таблица с указанным ранее именем) | SQL     | Финальное хранилище метрик                     |

