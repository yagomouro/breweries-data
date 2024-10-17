from airflow.providers.http.operators.http import HttpOperator
from storage_client import AzureStorageClient
from datetime import datetime as dt
import json
import logging


class APIClient:
    """A class responsible for handling API interactions."""

    def __init__(self, http_conn_id: str, endpoint: str) -> None:
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint

    def get_data(self) -> str:
        """Fetch data from the Open Brewery DB"""
        try:
            all_data = []
            page = 1
            per_page = 200  
            
            while True:
                paginated_endpoint = (
                    f"{self.endpoint}?page={page}&per_page={per_page}"
                )
                
                response = HttpOperator(
                    task_id='get_api_data',
                    http_conn_id=self.http_conn_id,
                    endpoint=paginated_endpoint,
                    method='GET',
                    headers={"Content-Type": "application/json"},
                ).execute(context={})

                data = json.loads(response)
                
                if not data:
                    break
                
                all_data.extend(data)
                logging.info(
                    f"Fetched page {page} with {len(data)} records"
                )
                page += 1

            logging.info("All API data retrieved successfully")
            return json.dumps(all_data)  
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
        """Main method to process the Bronze Layer."""
        api_response = self.api_client.get_data()
        
        today = dt.now()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        self.storage_client.upload_blob(
            blob_name=f'{year}/{month}/{day}/breweries_data.json',
            container=self.output_container,
            data=api_response
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    api_client = APIClient(
        http_conn_id='breweries_api_connection', endpoint='v1/breweries'
    )

    storage_client = AzureStorageClient()

    bronze_layer_processor = BronzeLayerProcessor(
        api_client=api_client, storage_client=storage_client
    )

    bronze_layer_processor.process()
