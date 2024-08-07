# src/IndexerManagement.py
import requests
import json
from datetime import datetime
from ..config import SEARCH_SERVICE_URL, API_VERSION, API_KEY, PARTITIONED_INDEX_NAME, PARTITIONS

class IndexerManagement:
    def __init__(self):
        self.search_service_url = SEARCH_SERVICE_URL
        self.api_version = API_VERSION
        self.api_key = API_KEY
        self.target_index_name = PARTITIONED_INDEX_NAME
        self.partitions = PARTITIONS
        self.headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key
        }

    def create_indexer(self, indexer_name, data_source_name, field_mappings):
        url = f"{self.search_service_url}/indexers?api-version={self.api_version}"
        payload = {
            "name": indexer_name,
            "dataSourceName": data_source_name,
            "targetIndexName": self.target_index_name,
            "parameters": {
                "configuration": {
                    "parsingMode": "delimitedText",
                    "firstLineContainsHeaders": True
                }
            },
            "fieldMappings": field_mappings
        }
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        if response.status_code == 201:
            print(f"Indexer {indexer_name} created successfully.")
        else:
            print(f"Failed to create indexer {indexer_name}: {response.text}")

    def delete_indexer(self, indexer_name):
        url = f"{self.search_service_url}/indexers/{indexer_name}?api-version={self.api_version}"
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 204:
            print(f"Indexer {indexer_name} deleted successfully.")
        else:
            print(f"Failed to delete indexer {indexer_name}: {response.text}")

    def reset_indexer(self, indexer_name):
        url = f"{self.search_service_url}/indexers/{indexer_name}/reset?api-version={self.api_version}"
        response = requests.post(url, headers=self.headers)
        if response.status_code == 204:
            print(f"Indexer {indexer_name} reset successfully.")
        else:
            print(f"Failed to reset indexer {indexer_name}: {response.text}")

    def run_indexer(self, indexer_name):
        url = f"{self.search_service_url}/indexers/{indexer_name}/run?api-version={self.api_version}"
        response = requests.post(url, headers=self.headers)
        if response.status_code == 202:
            print(f"Indexer {indexer_name} run successfully.")
        else:
            print(f"Failed to run indexer {indexer_name}: {response.text}")

    def calculate_total_run_time(self):
        start_times = []
        end_times = []
        time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        for i in range(1, len(self.partitions) + 1):
            indexer_name = f"{self.target_index_name}-indexer-{i}"
            status = self.get_indexer_status(indexer_name)
            if status:
                runs = status.get("lastResult", {})
                if runs:
                    start_times.append(datetime.strptime(runs["startTime"], time_format))
                    end_times.append(datetime.strptime(runs["endTime"], time_format))

        if start_times and end_times:
            total_time = max(end_times) - min(start_times)
            print(f"Total time for all indexers to run: {total_time}")
            return total_time
        else:
            print("Could not calculate total run time.")

    def get_indexer_status(self, indexer_name):
        url = f"{self.search_service_url}/indexers/{indexer_name}/status?api-version={self.api_version}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get status for indexer {indexer_name}: {response.text}")
        return None
    
    def get_indexer_run_time(self, indexer_name):
        status = self.get_indexer_status(indexer_name)
        if status:
            runs = status.get("lastResult", {})
            if runs:
                time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
                start_time = datetime.strptime(runs["startTime"], time_format)
                end_time = datetime.strptime(runs["endTime"], time_format)
                run_time = end_time - start_time
                print(f"Run time for indexer {indexer_name}: {run_time}")
                return run_time
        print(f"Could not calculate run time for indexer {indexer_name}.")
        return None