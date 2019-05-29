import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow_training.operators.postgres_to_gcs import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook

class MyOwnOperator(BaseOperator):

    ui_color = '#0090e3'
    ui_fgcolor = '#000000'

    @apply_defaults
    def __init__(self, endpoint, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint

    def execute(self, context):
        print("exec")
        self._download_from_http()

    # def _upload_to_gcs(self):
    #     """
    #     Upload all of the file splits (and optionally the schema .json file) to
    #     Google Cloud Storage.
    #     """
    #     hook = GoogleCloudStorageHook(
    #         google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
    #         delegate_to=self.delegate_to,
    #     )
    #     for object, tmp_file_handle in files_to_upload.items():
    #         hook.upload(self.bucket, object, tmp_file_handle.name, "application/json")

    def _download_from_http(self):
        http = HttpHook('GET')
        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint)
        self.log.info(response)

dag = DAG(
    dag_id="superjob",
    default_args={
        "owner": "naamhierinvullen",
        "start_date": airflow.utils.dates.days_ago(7),
    },
    schedule_interval="@daily",
    catchup=False,
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="prices_uk_from_postgres_to_cloudstorage",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="een_emmer",
    filename="daily_load_{{ ds }}",
    postgres_conn_id="stuff_postgres",
    dag=dag,
)

myown = MyOwnOperator(task_id='myown', dag=dag, endpoint="https://www.google.com")


myown >> pgsl_to_gcs