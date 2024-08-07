# src/IndexManagement.py
import requests
import json
from ..config import SEARCH_SERVICE_URL, API_VERSION, API_KEY

class IndexManagement:
    def __init__(self):
        self.search_service_url = SEARCH_SERVICE_URL
        self.api_version = API_VERSION
        self.api_key = API_KEY
        self.headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key
        }

    def create_index(self, index_name, fields):
        url = f"{self.search_service_url}/indexes?api-version={self.api_version}"
        payload = {
            "name": index_name,
            "fields": fields
        }
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        if response.status_code == 201:
            print(f"Index {index_name} created successfully.")
        else:
            print(f"Failed to create index {index_name}: {response.text}")

    def delete_index(self, index_name):
        url = f"{self.search_service_url}/indexes/{index_name}?api-version={self.api_version}"
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 204:
            print(f"Index {index_name} deleted successfully.")
        else:
            print(f"Failed to delete index {index_name}: {response.text}")