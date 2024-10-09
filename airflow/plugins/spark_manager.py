from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import logging

logging.basicConfig(level=logging.INFO)


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
