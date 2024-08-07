# src/DataSourceManagement.py
import requests
import json
from ..config import SEARCH_SERVICE_URL, API_VERSION, API_KEY, CONTAINER_CONN_STR, CONTAINER_NAME

class DataSourceManagement:
    def __init__(self):
        self.search_service_url = SEARCH_SERVICE_URL
        self.api_version = API_VERSION
        self.api_key = API_KEY
        self.container_conn_str = CONTAINER_CONN_STR
        self.container_name = CONTAINER_NAME
        self.headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key
        }

    def create_data_source(self, data_source_name, partition):
        url = f"{self.search_service_url}/datasources?api-version={self.api_version}"
        payload = {
            "name": data_source_name,
            "type": "adlsgen2",
            "credentials": {
                "connectionString": self.container_conn_str
            },
            "container": {
                "name": self.container_name,
                "query": partition
            }
        }
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        if response.status_code == 201:
            print(f"Data source {data_source_name} created successfully.")
        else:
            print(f"Failed to create data source {data_source_name}: {response.text}")

    def delete_data_source(self, data_source_name):
        url = f"{self.search_service_url}/datasources/{data_source_name}?api-version={self.api_version}"
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 204:
            print(f"Data source {data_source_name} deleted successfully.")
        else:
            print(f"Failed to delete data source {data_source_name}: {response.text}")