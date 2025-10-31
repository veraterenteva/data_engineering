import requests


class GoogleDriveLoader:
    def __init__(self, file_id: str, destination: str = "dataset.csv"):
        self.file_id = file_id
        self.destination = destination
        self.url = "https://docs.google.com/uc?export=download"

    def download_file(self):
        session = requests.Session()
        response = session.get(self.url, params={"id": self.file_id}, stream=True)
        token = self._get_confirm_token(response)

        if token:
            params = {"id": self.file_id, "confirm": token}
            response = session.get(self.url, params=params, stream=True)

        self.save_response_content(response)

    def _get_confirm_token(self, response):
        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                return value
        return None

    def save_response_content(self, response, chunk_size=32768):
        with open(self.destination, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
