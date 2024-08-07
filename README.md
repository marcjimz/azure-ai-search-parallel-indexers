# Azure Blob Storage Management and Indexing

This is a repository that quickly demonstrates how you can speed up performance utilizing many indexes on partitioned data to expedite ingestion into AI Search. This is not a repository advocating on one strategy (pull) over another (push), but merely an example of how you can achieve indexing performance. For more information on best practices, please read the [Azure AI Search guide on Indexing Large Datasets](https://learn.microsoft.com/en-us/azure/search/search-howto-large-index)

## Class Structure

### `ADLSGen2Loader`

Located in `src/ADLSGen2Loader.py`, this class is responsible for handling operations related to Azure Data Lake Storage Gen2. The key methods are:
- `__init__(self, data_dir)`: Initializes the loader with the specified data directory.
- `get_files(self)`: Retrieves all CSV files in the specified data directory.
- `upload_to_partition(self, file_path, partition_num)`: Uploads a file to a specified partition in the Azure Blob Storage.
- `upload_files(self)`: Uploads all CSV files in the data directory to Azure Blob Storage, distributing them across partitions.
- `delete_partition_folders(self)`: Deletes all blobs within each partition folder in the Azure Blob Storage.

### `DataSourceManagement`

Located in `src/data/DataSourceManagement.py`, this class handles the creation and deletion of data sources for Azure Cognitive Search.

### `IndexerManagement`

Located in `src/indexers/IndexerManagement.py`, this class manages the creation, deletion, resetting, and running of indexers for Azure Cognitive Search. It includes methods to calculate the total run time of indexers.

### `IndexManagement`

Located in `src/indexes/IndexManagement.py`, this class is responsible for creating and deleting indexes in Azure Cognitive Search.

## Usage

### Prerequisites

- Python 3.x
- Azure Storage Blob SDK
- Requests library
- Jupyter Notebook
- Azure Subscription
- Azure AI Search (Standard Tier)
- Azure Data Lake Gen2 Storage (Hierarchy Enabled)

### Installation

Install the required libraries using pip:
```bash
pip install azure-storage-blob requests
```

## Example Usage

## Configuration Setup

Copy and update the `src/config.py.tpl` file, ultimately saving it to `src/config.py` with your values.

### Uploading Files and Managing Indexers

The main.py script demonstrates how to use these classes to upload files, create data sources and indexers, and calculate indexer run times.

```python
from src.data.DataSourceManagement import DataSourceManagement
from src.indexers.IndexerManagement import IndexerManagement
from src.indexes.IndexManagement import IndexManagement
from src.ADLSGen2Loader import ADLSGen2Loader
from ..config import PARTITIONS, TARGET_INDEX_NAME, OPENPAYMENTSDATA_FIELDS, OPENPAYMENTSDATA_FIELD_MAPPINGS
from concurrent.futures import ThreadPoolExecutor, as_completed

def create_resources():
    index_manager = IndexManagement()
    data_source_manager = DataSourceManagement()
    indexer_manager = IndexerManagement()

    index_manager.create_index(TARGET_INDEX_NAME, OPENPAYMENTSDATA_FIELDS)

    loader = ADLSGen2Loader("/path/to/your/data")
    loader.upload_files()

    with ThreadPoolExecutor() as executor:
        futures = []
        for i, partition in enumerate(PARTITIONS, start=1):
            data_source_name = f"{TARGET_INDEX_NAME}-ds-{i}"
            indexer_name = f"{TARGET_INDEX_NAME}-indexer-{i}"
            futures.append(executor.submit(data_source_manager.create_data_source, data_source_name, partition))
            futures.append(executor.submit(indexer_manager.create_indexer, indexer_name, data_source_name, OPENPAYMENTSDATA_FIELD_MAPPINGS))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'Generated an exception: {exc}')

        for i in range(1, len(PARTITIONS) + 1):
            indexer_name = f"{TARGET_INDEX_NAME}-indexer-{i}"
            futures.append(executor.submit(indexer_manager.reset_indexer, indexer_name))
            futures.append(executor.submit(indexer_manager.run_indexer, indexer_name))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'Generated an exception: {exc}')

    indexer_manager.calculate_total_run_time()

if __name__ == "__main__":
    create_resources()
```

## Cleaning Up Resources

To delete the uploaded files and resources:

```python
if __name__ == "__main__":
    data_dir = "/path/to/your/data"  # Set your data directory path
    loader = ADLSGen2Loader(data_dir)
    loader.delete_partition_folders()
```

## Jupyter Notebook

The repository includes a Jupyter notebook Parallel-Indexer-Performance.ipynb that demonstrates parallel processing for indexer operations and measures their performance. To get started, open the notebook in Jupyter and follow the instructions provided.

## Disclaimer

Warning: This repository contains code for demonstration purposes only. It is not production-ready. Use at your own risk.

## License

This project is licensed under the MIT License.