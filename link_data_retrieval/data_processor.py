import requests
import pandas as pd
from tabulate import tabulate
from typing import Optional


class DataProcessor:
    def __init__(self, dataframe: pd.DataFrame):
        self.df: pd.DataFrame = dataframe.copy()

    def _to_nullable_int(
        self, series: pd.Series, dtype: str = "Int16", round_vals: bool = False
    ) -> pd.Series:
        # попытка прямого преобразования
        s_num = pd.to_numeric(series, errors="coerce")

        # при необходимости округлим
        if round_vals:
            s_num = s_num.round()

        # если почти все значения NaN, пробуем извлечь цифры из строк
        if s_num.isna().all():
            extracted = series.astype(str).str.extract(r"(-?\d+)")[0]
            s_num = pd.to_numeric(extracted, errors="coerce")
            if round_vals:
                s_num = s_num.round()

        # формируем Python int / pd.NA для каждого элемента
        converted = pd.Series(
            [int(x) if pd.notna(x) else pd.NA for x in s_num],
            index=series.index,
            dtype=dtype,
        )
        return converted

    def clean_and_cast(self) -> pd.DataFrame:
        self.df = self.df.drop_duplicates().reset_index(drop=True)

        if "Unnamed: 0" in self.df.columns:
            self.df = self.df.drop(columns=["Unnamed: 0"])

        for txt_col in ("title", "genre", "certificate", "descriptions", "stars"):
            if txt_col in self.df.columns:
                # заменить пустые строки на <NA> и установить string dtype
                self.df[txt_col] = self.df[txt_col].replace("", pd.NA).astype("string")

        # imdb-scores to float32
        if "imdb-scores" in self.df.columns:
            self.df["imdb-scores"] = pd.to_numeric(
                self.df["imdb-scores"], errors="coerce"
            ).astype("float32")

        # metacritic-scores to nullable Int16
        if "metacritic-scores" in self.df.columns:
            self.df["metacritic-scores"] = self._to_nullable_int(
                self.df["metacritic-scores"], dtype="Int16", round_vals=True
            )

        # year to 2 columns (for TV series)
        if "year" in self.df.columns:
            # Приводим всё к строковому типу (Pandas StringDtype, а не object)
            years = self.df["year"].astype("string")

            # start_year: до дефиса
            self.df["start_year"] = (
                years.str.split("-").str[0].str.extract(r"(\d{4})").astype("Int32")
            )

            # end_year: после дефиса
            self.df["end_year"] = (
                years.str.split("-").str[1].str.extract(r"(\d{4})").astype("Int32")
            )

            # заменяем year на start_year
            self.df["year"] = self.df["start_year"]

        if "runtime" in self.df.columns:
            # runtime может быть "122", "122 min" извлекаем цифры
            # используем round_vals=False, т.к. минуты это целые числа
            self.df["runtime"] = self._to_nullable_int(
                self.df["runtime"], dtype="Int16", round_vals=False
            )
        if "user-votes" in self.df.columns:
            self.df["user-votes"] = self._to_nullable_int(
                self.df["user-votes"], dtype="Int32", round_vals=False
            )

        # все string в тип string
        for col in self.df.select_dtypes(include=["object"]).columns:
            self.df[col] = self.df[col].astype("string")

        return self.df

    def preview(self, n: int = 10) -> None:
        print(tabulate(self.df.head(n), headers="keys", tablefmt="pretty"))

    def to_parquet(self, filename: str = "dataset.parquet") -> None:
        try:
            self.df.to_parquet(filename, index=False)
            print(f"Данные сохранены в {filename}")
        except Exception as e:
            print("Ошибка:", e)
            raise
