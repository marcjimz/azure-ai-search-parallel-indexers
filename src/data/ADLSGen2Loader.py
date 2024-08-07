# src/ADLSGen2Loader.py
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from ..config import SEARCH_SERVICE_URL, API_VERSION, API_KEY, PARTITIONS, CONTAINER_NAME, PARTITIONED_INDEX_NAME, CONTAINER_CONN_STR

class ADLSGen2Loader:
    def __init__(self, data_dir):
        self.blob_service_client = BlobServiceClient.from_connection_string(CONTAINER_CONN_STR)
        self.container_name = CONTAINER_NAME
        self.partitions = PARTITIONS
        self.data_dir = data_dir

        # Create the container if it doesn't exist
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        if not self.container_client.exists():
            self.container_client.create_container()

    def get_files(self):
        files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
        files.sort()  # Ensure consistent ordering
        return files

    def upload_to_partition(self, file_path, partition_num):
        file_name = os.path.basename(file_path)
        blob_name = f"partition{partition_num}/{file_name}"
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    def upload_files(self):
        files = self.get_files()
        for i, file_name in enumerate(files):
            partition_num = i % len(self.partitions)
            self.upload_to_partition(os.path.join(self.data_dir, file_name), partition_num)
        print("Files uploaded successfully.")

    def delete_partition_folders(self):
        for partition_num in range(len(self.partitions)):
            prefix = f"partition{partition_num}/"
            blob_list = self.blob_service_client.get_container_client(self.container_name).list_blobs(name_starts_with=prefix)
            for blob in blob_list:
                blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob.name)
                try:
                    blob_client.delete_blob()
                    print(f"Deleted blob: {blob.name}")
                except Exception as e:
                    print(f"Failed to delete blob {blob.name}: {e}")
            print(f"Deleted partition folder: {prefix}")