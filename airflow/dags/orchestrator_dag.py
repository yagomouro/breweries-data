from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from airflow.decorators import dag

default_args = {
    'owner': 'Yago Mouro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


class SparkSubmitTask:
    """Returns a SparkSubmitOperator with the provided task_id, application, and other configurations"""

    def __init__(self, task_id, application, conn_id='spark_connection', retries=3, retry_delay=timedelta(minutes=2)):
        self.task_id = task_id
        self.application = application
        self.conn_id = conn_id
        self.retries = retries
        self.retry_delay = retry_delay

    def get_task(self):
        return SparkSubmitOperator(
            task_id=self.task_id,
            application=self.application,
            conn_id=self.conn_id,
            verbose=True,
            retries=self.retries,
            retry_delay=self.retry_delay
        )


@dag(
    dag_id='breweries_dags_orchestration',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
    tags=['spark', 'etl', 'azure', 'datalake_storage']
)
def spark_jobs_orchestration():
    start_task = DummyOperator(task_id='start')

    with TaskGroup('datalake_spark_jobs') as spark_jobs:

        # bronze_layer = SparkSubmitTask(
        #     task_id='bronze_layer',
        #     application='/opt/airflow/jobs/bronze_layer.py'
        # ).get_task()

        # silver_layer = SparkSubmitTask(
        #     task_id='silver_layer',
        #     application='/opt/airflow/jobs/silver_layer.py'
        # ).get_task()

        gold_layer = SparkSubmitTask(
            task_id='gold_layer',
            application='/opt/airflow/jobs/gold_layer.py'
        ).get_task()

        # bronze_layer >> silver_layer >> 
        gold_layer

    end_task = DummyOperator(task_id='end')

    start_task >> spark_jobs >> end_task


dag_instance = spark_jobs_orchestration()
