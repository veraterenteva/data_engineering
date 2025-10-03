import pandas as pd
import requests

BASE_URL = "https://collectionapi.metmuseum.org/public/collection/v1/objects"

COLUMNS_TO_KEEP = [
    "objectID",
    "title",
    "objectName",
    "objectDate",
    "objectBeginDate",
    "objectEndDate",
    "accessionYear",
    "department",
    "medium",
    "classification",
    "artistDisplayName",
    "artistNationality",
    "artistBeginDate",
    "artistEndDate",
    "country",
    "city",
    "repository",
    "objectURL"
]

LIMIT = 10

def get_object_ids(limit=10, query="coin"):
    # Cписок ID объектов (ставим 10, чтобы оно не экспоненциально тормозилось на стороне сервиса)
    params = {"q": query, "hasImages": True}
    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data["objectIDs"][:limit]

def get_object_details(object_id):
    # Данные по одному объекту

    resp = requests.get(f"{BASE_URL}/{object_id}")
    if resp.status_code == 200:
        return resp.json()
    return None


def build_dataframe(object_ids):
    # По забранным id объектов забираем все данные
    objects = []
    for oid in object_ids:
        details = get_object_details(oid)
        if details:
            objects.append(details)
    return pd.DataFrame(objects)