from azure.storage.blob import BlobServiceClient
from io import BytesIO
from typing import IO, AnyStr, Iterable, Union
import logging
from airflow.models import Variable
import pandas as pd


class AzureStorageClient:
    def __init__(self) -> None:
        try:
            self.client = BlobServiceClient.from_connection_string(
                conn_str=Variable.get('AZURE_STORAGE_CONNECTION_STRING'))

        except Exception as e:
            logging.error(f"Failed to create BlobServiceClient: {e}")
            raise

    def upload_blob(self, blob_name: str, data: Union[AnyStr, Iterable[AnyStr], IO[AnyStr]], container: str) -> str:
        try:
            blob_client = self.client.get_blob_client(
                container=container, blob=blob_name)

            blob_client.upload_blob(data=data, overwrite=True)

            logging.info(
                f"Blob {blob_name} uploaded successfully to container {container}.")
            return blob_client.url

        except Exception as e:
            logging.error(
                f"Failed to upload blob {blob_name} to container {container}: {e}")
            raise

    def download_blob(self, container: str, blob_path: str, file_type: str) -> pd.DataFrame:
        try:

            container_client = self.client.get_container_client(container)
            blobs = container_client.list_blobs(name_starts_with=blob_path)

            df_full = pd.DataFrame()

            if file_type == "json":
                blob_client = self.client.get_blob_client(
                    container=container, blob=blob_path)
                stream_downloader = blob_client.download_blob()
                file_content = stream_downloader.readall()

                logging.info(f"Blob {blob_path} downloaded as JSON.")
                return file_content

            elif file_type == "parquet":
                for blob in blobs:
                    if str(blob.name).endswith(f".parquet"):
                        blob_name = blob.name
                        blob_client = self.client.get_blob_client(
                            blob=blob_name, container=container)

                        with BytesIO() as b:
                            download_stream = blob_client.download_blob(0)
                            download_stream.readinto(b)

                            df = pd.read_parquet(b, engine="pyarrow")
                            df_full = pd.concat(
                                [df_full, df], ignore_index=True)

                if df_full.empty:
                    logging.warning(
                        f"No parquet files found in the path: {blob_path}")
                else:
                    logging.info(
                        f"Parquet files from path {blob_path} downloaded and concatenated successfully.")
                return df_full

        except Exception as e:
            logging.error(
                f"Failed to download blob {blob_path} from container {container}: {e}")
            raise
