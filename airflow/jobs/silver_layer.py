# from spark_manager import SparkManager
# from storage_client import AzureStorageClient
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient, BlobClient
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as f
from typing import IO, AnyStr, Iterable, Union
from datetime import datetime as dt
import json
import logging
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


class SparkManager:
    def __init__(self, app_name: str = "spark_app", delta_version: str = "io.delta:delta-core_2.12:1.1.0"):
        self.app_name = app_name
        self.delta_version = delta_version
        self.spark_session = None

    def create_spark_session(self):
        """Initialize and return a Spark session"""
        try:
            self.spark_session = SparkSession.builder.appName(self.app_name) \
                .config("spark.jars.packages", self.delta_version) \
                .getOrCreate()
            logging.info(
                f"Spark session '{self.app_name}' created successfully.")
            return self.spark_session
        except Exception as e:
            logging.error(f"Error creating Spark session: {e}")
            return None

    def get_breweries_schema(self):
        """Define the schema for the brewery data"""
        try:
            schema = StructType([
                StructField('id', StringType(), True),
                StructField('name', StringType(), True),
                StructField('brewery_type', StringType(), True),
                StructField('address_1', StringType(), True),
                StructField('address_2', StringType(), True),
                StructField('address_3', StringType(), True),
                StructField('city', StringType(), True),
                StructField('state_province', StringType(), True),
                StructField('postal_code', StringType(), True),
                StructField('country', StringType(), True),
                StructField('longitude', StringType(), True),
                StructField('latitude', StringType(), True),
                StructField('phone', StringType(), True),
                StructField('website_url', StringType(), True),
                StructField('state', StringType(), True),
                StructField('street', StringType(), True)
            ])
            logging.info("Brewery schema defined successfully.")
            return schema
        except Exception as e:
            logging.error(f"Error defining brewery schema: {e}")
            return None


class SilverLayerProcessor:
    """Processor class for handling silver layer data"""

    def __init__(self, storage_client: AzureStorageClient, spark_manager: SparkManager):
        self.source_container = 'bronze-layer'
        self.target_container = 'silver-layer'
        self.schema = spark_manager.get_breweries_schema()
        self.spark = spark_manager.create_spark_session()
        self.storage_client = storage_client

    def process(self):
        """Main method to process the Silver Layer."""

        today = dt.now()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        data = self.storage_client.download_blob(
            container=self.source_container, blob_path=f'{year}/{month}/{day}/breweries_data.json', file_type='json')

        breweries_data = json.loads(data)

        df = self.spark.createDataFrame(breweries_data, schema=self.schema)

        df_cleaned = (
            df.dropDuplicates()
            .filter(df['name'].isNotNull())
            .filter(df['brewery_type'].isNotNull())
            .withColumn('phone', f.regexp_replace('phone', '[^0-9]', ''))
            .withColumn('postal_code', f.trim('postal_code'))
            .withColumn('website_url', f.when(f.col('website_url').rlike('^(http|https)://'), f.col('website_url')).otherwise(f.lit(None)))
            .repartition("country")
        )

        unique_countries = df_cleaned.select("country").distinct().collect()

        for row in unique_countries:
            country = row['country']
            df_partitioned = df_cleaned.filter(df_cleaned.country == country)

            pdf = df_partitioned.toPandas()

            buffer = BytesIO()
            pdf.to_parquet(buffer, index=False)
            buffer.seek(0)

            blob_name = f'breweries_data_processed/{country}.parquet'
            self.storage_client.upload_blob(
                container=self.target_container, blob_name=blob_name, data=buffer)

            logging.info(f"Country {country} data uploaded.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    storage_client = AzureStorageClient()
    spark_manager = SparkManager(
        app_name="silver_layer", delta_version="io.delta:delta-core_2.12:1.1.0")

    silver_layer_processor = SilverLayerProcessor(
        storage_client=storage_client, spark_manager=spark_manager)

    silver_layer_processor.process()
