from azure.storage.blob import BlobServiceClient
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from airflow.models import Variable
import json
from io import BytesIO


def process_silver_layer():
    spark = SparkSession.builder \
        .appName("silver_layer") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
        .getOrCreate()

    azure_container_name = 'bronze-layer'
    connection_string = Variable.get('AZURE_STORAGE_CONNECTION_STRING')

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=azure_container_name, blob='breweries_data.json')

    stream_downloader = blob_client.download_blob()
    file_content = stream_downloader.readall()
    breweries_data = json.loads(file_content)

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

    df = spark.createDataFrame(breweries_data, schema)

    df_cleaned = df.dropDuplicates().repartition("state")

    container_name = 'silver-layer'

    unique_states = df_cleaned.select("state").distinct().collect()

    for row in unique_states:
        state = row['state']
        df_partitioned = df_cleaned.filter(df_cleaned.state == state)

        pdf = df_partitioned.toPandas()

        buffer = BytesIO()

        pdf.to_parquet(buffer, index=False)

        buffer.seek(0)

        blob_name = f'breweries_data_processed/{state}.parquet'

        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name)

        blob_client.upload_blob(buffer, overwrite=True)

        print(
            f'State {state} uploaded')


process_silver_layer()
