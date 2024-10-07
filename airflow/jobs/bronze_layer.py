from airflow.providers.http.operators.http import HttpOperator
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable
import os


def extract_and_upload():
    response = HttpOperator(
        task_id='get_breweries',
        http_conn_id='breweries_api_connection',
        endpoint='breweries',
        method='GET',
        headers={"Content-Type": "application/json"},
    ).execute(context={})

    print('API response received.', response)

    local_directory = '/opt/airflow/bronze_layer'
    os.makedirs(local_directory, exist_ok=True)
    local_file_path = os.path.join(local_directory, 'breweries_data.json')

    with open(local_file_path, 'w') as file:
        file.write(response)

    print(f'Local file saved at: {local_file_path}')

    azure_container_name = 'bronze-layer'
    connection_string = Variable.get('AZURE_STORAGE_CONNECTION_STRING')

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=azure_container_name, blob='breweries_data.json')

    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print('Data uploaded to Azure Blob Storage.')

extract_and_upload()