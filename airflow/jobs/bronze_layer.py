from airflow.providers.http.operators.http import HttpOperator
from storage import AzureStorageClient
import logging


class APIClient:
    """A class responsible for handling API interactions."""

    def __init__(self, http_conn_id: str, endpoint: str) -> None:
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint

    def get_data(self) -> str:
        """Fetch data from the Open Brewery DB"""
        try:
            response = HttpOperator(
                task_id='get_api_data',
                http_conn_id=self.http_conn_id,
                endpoint=self.endpoint,
                method='GET',
                headers={"Content-Type": "application/json"},
            ).execute(context={})
            logging.info("API response received successfully")
            return response

        except Exception as e:
            logging.error(f"Failed to fetch data from API: {e}")
            raise


class BronzeLayerProcessor:
    """Processor class for handling bronze layer data"""

    def __init__(self, api_client: APIClient, storage_client: AzureStorageClient):
        self.api_client = api_client
        self.storage_client = storage_client
        self.local_directory = '/opt/airflow/bronze_layer'
        self.output_container = 'bronze-layer'

    def process(self):
        """Main method to fetch data and upload it to Azure Blob Storage."""
        api_response = self.api_client.get_data()

        self.storage_client.upload_blob(
            blob_name='breweries_data.json',
            container=self.output_container,
            data=api_response
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create API and Azure Storage clients
    api_client = APIClient(
        http_conn_id='breweries_api_connection', endpoint='breweries')

    storage_client = AzureStorageClient()

    # Create and run the bronze layer processor
    bronze_layer_processor = BronzeLayerProcessor(
        api_client=api_client, storage_client=storage_client)
    
    bronze_layer_processor.process()
