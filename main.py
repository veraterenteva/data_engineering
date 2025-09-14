from requests import get
import requests
from bs4 import BeautifulSoup
import warnings
from time import sleep
from random import randint
import numpy as np
import seaborn as sns
import pandas as pd
from tqdm import tqdm
import re
import json
import asyncio
import aiohttp
import urllib.request
from crawler import Crawler

# params = {'url': "https://www.imdb.com/search/title/?genres=drama",
#           "movies_to_parse": "50000"}
#
# crawl = Crawler(int(params["movies_to_parse"]), params["url"])
# df_movies = Crawler.get_dataset(crawl)

#print(df_movies)

# OR для получения полного датасета просто загрузкой по ссылке

file_id = "10Gm2LGbOyzTVef3XHA69NDmd-be3H-fD"  # замените на свой
url = f"https://drive.google.com/uc?export=download&id={file_id}"
output = "dataset.csv"

def download_file_from_google_drive(file_id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params={'id': file_id}, stream=True)
    token = get_confirm_token(response)

    if token:
        # если нужен токен подтверждения
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

print("Скачали dataset.csv")