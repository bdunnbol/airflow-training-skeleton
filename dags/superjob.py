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
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class MyOwnOperator(BaseOperator):

    ui_color = "#0090e3"
    ui_fgcolor = "#000000"

    @apply_defaults
    def __init__(
        self,
        http_endpoint,
        http_connection_id,
        gcs_connection_id,
        gcs_bucket,
        gcs_filename,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.http_endpoint = http_endpoint
        self.http_connection_id = http_connection_id
        self.gcs_connection_id = gcs_connection_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filename = gcs_filename

    def execute(self, context):
        print("exec")
        self._download_from_http()
        self._upload_to_gcs()

    def _upload_to_gcs(self):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google Cloud Storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcs_connection_id
        )

        hook.upload(
            self.gcs_bucket, "imthecontent", self.gcs_filename, "application/json"
        )

    def _download_from_http(self):
        http = HttpHook("GET", http_conn_id=self.http_connection_id)
        self.log.info("Calling HTTP method")
        response = http.run(self.http_endpoint)
        self.log.info(response.text)

        return response.text


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

myown = MyOwnOperator(
    task_id="myown",
    dag=dag,
    http_connection_id="http_exchangerate",
    http_endpoint="/airflow-training-transform-valutas?date={{  ds  }}&to=EUR",
    gcs_connection_id="google_cloud_default",
    gcs_bucket="een_emmer",
    gcs_filename="exchangerate_{{ds}}",
)


myown >> pgsl_to_gcs
