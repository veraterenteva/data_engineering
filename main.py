import pandas as pd
from tabulate import tabulate
from api_example.api_reader import get_object_ids, build_dataframe, COLUMNS_TO_KEEP, LIMIT
from data_loader import GoogleDriveLoader
from data_processor import DataProcessor

if __name__ == "__main__":
    object_ids = get_object_ids(LIMIT)
    df = build_dataframe(object_ids)

    # Полный DataFrame
    print()
    print(tabulate(df.head(10), headers="keys", tablefmt="pretty"))

    # Упрощённый DataFrame
    df_simplified = df[COLUMNS_TO_KEEP]
    print("Упрощённый DataFrame")
    print(tabulate(df_simplified.head(10), headers="keys", tablefmt="pretty"))

    # file_id = "10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD"
    # output = "dataset.csv"
    #
    # loader = GoogleDriveLoader(file_id, output)
    # loader.download_file()
    #
    # df = pd.read_csv(output)
    #
    # print(tabulate(df.head(10), headers="keys", tablefmt="pretty"))
    # print("Успешно загружен dataset.csv")
    #
    # processor = DataProcessor(df)
    # df = processor.clean_and_cast()
    # processor.preview(10)
    #
    # print(df.dtypes.to_frame("dtype"))
    # processor.to_parquet("dataset.parquet")