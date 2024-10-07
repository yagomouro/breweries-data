from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd


def process_gold_layer():
    spark = SparkSession.builder.appName("gold_layer").getOrCreate()

    connection_string = Variable.get('AZURE_STORAGE_CONNECTION_STRING')

    container = 'silver-layer'
    blob_path = 'breweries_data_processed'

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)

    container_client = blob_service_client.get_container_client(container)

    blobs = container_client.list_blobs(name_starts_with=blob_path)

    df_silver = pd.DataFrame()

    for blob in blobs:
        if str(blob.name).endswith(".parquet"):
            blob_name = blob.name
            blob_client = blob_service_client.get_blob_client(
                blob=blob_name, container=container)
            with BytesIO() as b:
                download_stream = blob_client.download_blob(0)
                download_stream.readinto(b)

                df = pd.read_parquet(b, engine="pyarrow")

                df_silver = pd.concat([df_silver, df], ignore_index=True)

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

    df_gold = spark.createDataFrame(df_silver, schema=schema)

    df_gold = df_gold.groupBy("country", "state", "city", "brewery_type").agg(
        count("*").alias("brewery_count")).orderBy(col('brewery_count').desc())

    df_gold.show(5)

    pdf = df_gold.toPandas()

    output_buffer = BytesIO()
    pdf.to_parquet(output_buffer, index=False)

    output_buffer.seek(0)

    gold_blob_client = blob_service_client.get_blob_client(
        container='gold-layer', blob='breweries_data_aggregated.parquet'
    )

    gold_blob_client.upload_blob(output_buffer, overwrite=True)

    print('Data uploaded to Azure Blob Storage.')


process_gold_layer()
