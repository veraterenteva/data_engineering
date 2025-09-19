import requests
import pandas as pd
from tabulate import tabulate

file_id = "10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD"  
output = "dataset.csv"

def download_file_from_google_drive(file_id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params={'id': file_id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {'id': file_id, 'confirm': token}
        response = session.get(URL, params=params, stream=True)

    save_response_content(response, destination)

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None

def save_response_content(response, destination, chunk_size=32768):
    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size):
            if chunk:
                f.write(chunk)

download_file_from_google_drive(file_id, "dataset.csv")
dataframe = pd.read_csv("dataset.csv")

print(tabulate(dataframe.head(10), headers="keys", tablefmt="pretty"))
print("Успешно загружен dataset.csv")
