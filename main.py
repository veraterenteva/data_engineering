import pandas as pd
from tabulate import tabulate

from data_loader import GoogleDriveLoader
from data_processor import DataProcessor

if __name__ == "__main__":
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