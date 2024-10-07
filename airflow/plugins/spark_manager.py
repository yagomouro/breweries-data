from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


class SparkManager:
    def __init__(self, app_name: str = "spark_app", delta_version: str = "io.delta:delta-core_2.12:1.1.0"):
        self.app_name = app_name
        self.delta_version = delta_version

    def create_spark_session(self):
        """Initialize and return a Spark session"""
        return SparkSession.builder.appName(self.app_name) \
            .config("spark.jars.packages", self.delta_version) \
            .getOrCreate()

    def get_breweries_schema(self):
        """Define the schema for the brewery data"""
        return StructType([
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
