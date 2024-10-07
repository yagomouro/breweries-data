from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from datetime import timedelta

default_args = {
    'owner': 'Yago Mouro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'spark_jobs_orchestration',
    default_args=default_args,
    description='DAG to orchestrate breweries data processing',
    tags=['spark', 'ETL']
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    with TaskGroup('datalake_spark_jobs', dag=dag) as spark_jobs:

        bronze_layer = SparkSubmitOperator(
            application='/opt/airflow/jobs/bronze_layer.py',
            conn_id='spark_connection',
            task_id='bronze_layer',
            dag=dag
        )
        silver_layer = SparkSubmitOperator(
            task_id='silver_layer',
            application='/opt/airflow/jobs/silver_layer.py',
            conn_id='spark_connection',
            verbose=True,
            conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            },
            packages="io.delta:delta-spark_2.12:3.2.0",
            dag=dag
        )

        bronze_layer >> silver_layer

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start >> spark_jobs >> end
