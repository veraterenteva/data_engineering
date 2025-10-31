import os
import pandas as pd
from tabulate import tabulate
from etl.logger import get_logger

logger = get_logger("transform")


class DataProcessor:
    """
    transform.py для приведения типов данных в корректный вид

    Осуществляет
    - удаление неинформативных колонок
    - приведение типов по смыслу
    - парсинг сложных колонок с дальнейшим приведением типов
    - выбор столбцов для записи в .parquet файл
    - запись в .parquet файл
    """
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe.copy()

    def _to_nullable_int(
        self, series: pd.Series, dtype: str = "Int16", round_vals: bool = False
    ) -> pd.Series:
        s_num = pd.to_numeric(series, errors="coerce")

        if round_vals:
            s_num = s_num.round()

        if s_num.isna().all():
            extracted = series.astype(str).str.extract(r"(-?\d+)")[0]
            s_num = pd.to_numeric(extracted, errors="coerce")
            if round_vals:
                s_num = s_num.round()

        converted = pd.Series(
            [int(x) if pd.notna(x) else pd.NA for x in s_num],
            index=series.index,
            dtype=dtype,
        )
        return converted

    def clean_and_cast(self) -> pd.DataFrame:
        logger.info("Начинаем очистку и преобразование данных")

        self.df = self.df.drop_duplicates().reset_index(drop=True)

        # удаляем лишние колонки
        if "Unnamed: 0" in self.df.columns:
            self.df = self.df.drop(columns=["Unnamed: 0"])

        # string-поля
        for txt_col in ("title", "genre", "certificate", "descriptions", "stars"):
            if txt_col in self.df.columns:
                self.df[txt_col] = self.df[txt_col].replace("", pd.NA).astype("string")

        # числовые преобразования
        if "imdb-scores" in self.df.columns:
            self.df["imdb-scores"] = pd.to_numeric(
                self.df["imdb-scores"], errors="coerce"
            ).astype("float32")

        if "metacritic-scores" in self.df.columns:
            self.df["metacritic-scores"] = self._to_nullable_int(
                self.df["metacritic-scores"], dtype="Int16", round_vals=True
            )

        if "year" in self.df.columns:
            years = self.df["year"].astype("string")
            self.df["start_year"] = (
                years.str.split("-").str[0].str.extract(r"(\d{4})").astype("Int32")
            )
            self.df["end_year"] = (
                years.str.split("-").str[1].str.extract(r"(\d{4})").astype("Int32")
            )
            self.df["year"] = self.df["start_year"]

        if "runtime" in self.df.columns:
            self.df["runtime"] = self._to_nullable_int(
                self.df["runtime"], dtype="Int16"
            )

        if "user-votes" in self.df.columns:
            self.df["user-votes"] = self._to_nullable_int(
                self.df["user-votes"], dtype="Int32"
            )

        # все object в string
        for col in self.df.select_dtypes(include=["object"]).columns:
            self.df[col] = self.df[col].astype("string")

        # добавляем ID
        self.df.insert(0, "id", range(1, len(self.df) + 1))
        self.df["id"] = self.df["id"].astype("Int32")

        logger.info(f"После очистки осталось {len(self.df)} строк.")
        return self.df

    def select_columns(self, columns: list[str] | None = None) -> pd.DataFrame:
        if columns:
            existing = [c for c in columns if c in self.df.columns]
            if not existing:
                logger.warning("Ни один из указанных столбцов не найден в данных.")
            else:
                self.df = self.df[existing]
                logger.info(f"Выбраны столбцы: {existing}")
        return self.df

    def preview(self, n: int = 10):
        print(tabulate(self.df.head(n), headers="keys", tablefmt="pretty"))

    def to_parquet(self, filename: str = "data/processed/dataset.parquet") -> str:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.df.to_parquet(filename, index=False)
        logger.info(f"Файл Parquet сохранён: {filename}")
        return filename
