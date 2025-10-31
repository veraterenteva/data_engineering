import requests
import os
from etl.logger import get_logger

logger = get_logger("extract")

class GoogleDriveLoader:
    """
    Extract.py для извлечения датасета из Google disk по базовому URL и передаваемому FILE ID
    Файлы из-за своего размера извлекаются чанками
    """
    BASE_URL = "https://docs.google.com/uc?export=download"

    def __init__(self, file_id: str, destination: str = "data/raw/dataset.csv"):
        self.file_id = file_id
        self.destination = destination
        os.makedirs(os.path.dirname(destination), exist_ok=True)

    def download_file(self) -> str:
        logger.info(f"Начинаем загрузку файла с Google Drive (id={self.file_id})")

        with requests.Session() as session:
            response = session.get(self.BASE_URL, params={"id": self.file_id}, stream=True)
            token = self._get_confirm_token(response)

            if token:
                response = session.get(self.BASE_URL, params={"id": self.file_id, "confirm": token}, stream=True)

            self._save_response_content(response)

        logger.info(f"[extract] CSV сохранён в {self.destination}")
        return self.destination

    def _get_confirm_token(self, response):
        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                return value
        return None

    def _save_response_content(self, response, chunk_size: int = 32768):
        with open(self.destination, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
        logger.debug(f"Файл успешно записан в {self.destination}")
