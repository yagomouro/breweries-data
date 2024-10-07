from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient
from io import BytesIO
from typing import IO, AnyStr, Iterable, Union
import pandas as pd


class AzureStorageClient:
    client: BlobServiceClient

    def __init__(self, connection_verify: bool = True) -> None:
        self.client: BlobServiceClient = BlobServiceClient.from_connection_string(
            conn_str=Variable.get('AZURE_STORAGE_CONNECTION_STRING'), connection_verify=connection_verify)

    def upload_blob(self, blob_name: str, data: Union[AnyStr, Iterable[AnyStr], IO[AnyStr]], container: str) -> str:
        blob_client = self.client.get_blob_client(
            container=container, blob=blob_name)

        try:
            blob_client.upload_blob(data=data, overwrite=True)
            return blob_client.url
        except:
            return None

    def download_dataframe(self, container: str, blob_path: str, file_type: str) -> pd.DataFrame:
        container_client = self.client.get_container_client(container)
        blobs = container_client.list_blobs(name_starts_with=blob_path)

        df_full = pd.DataFrame()

        if file_type == "json":
            blob_client = self.client.get_blob_client(
                container=container, blob=blob_path)

            stream_downloader = blob_client.download_blob()
            file_content = stream_downloader.readall()

            return file_content

        elif file_type == "parquet":
            for blob in blobs:
                if str(blob.name).endswith(f".parquet"):
                    blob_name = blob.name
                    blob_client: BlobClient = self.client.get_blob_client(
                        blob=blob_name, container=container)
                    with BytesIO() as b:
                        download_stream = blob_client.download_blob(0)
                        download_stream.readinto(b)

                        df = pd.read_parquet(b, engine="pyarrow")

                        df_full = pd.concat([df_full, df], ignore_index=True)

            return df_full
