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
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

from airflow_training.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import json


class MyOwnOperator(BaseOperator):

    template_fields = ("http_endpoint", "gcs_filename")

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
        r = self._download_from_http()
        r = json.loads(r)

        with open("data.json", "w") as outfile:
            json.dump(r, outfile)
        self._upload_to_gcs()

    def _upload_to_gcs(self):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google Cloud Storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcs_connection_id
        )

        hook.upload(self.gcs_bucket, self.gcs_filename, "data.json", "application/json")

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

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="dataproc_create_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-may2829-b2a87b4d",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

dataproc_remove_cluster = DataprocClusterDeleteOperator(
    task_id="dataproc_remove_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-may2829-b2a87b4d",
    dag=dag,
)

dataproc_run_pyspark = DataProcPySparkOperator(
    task_id="dataproc_run_pyspark",
    main="gs://een_emmer/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=[
        "gs://een_emmer/daily_load_{{ ds }}",
        "gs://een_emmer/exchangerate_{{ ds }}.txt",
        "gs://een_emmer/dataproc_output_{{ ds }}",
    ],
    dag=dag,
)

prices_uk_from_postgres_to_cloudstorage = PostgresToGoogleCloudStorageOperator(
    task_id="prices_uk_from_postgres_to_cloudstorage",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="een_emmer",
    filename="daily_load_{{ ds }}",
    postgres_conn_id="stuff_postgres",
    dag=dag,
)

exchange_rate_to_gcs = MyOwnOperator(
    task_id="exchange_rate_to_gcs",
    dag=dag,
    http_connection_id="http_exchangerate",
    http_endpoint="/airflow-training-transform-valutas?date={{ ds }}&to=EUR",
    gcs_connection_id="google_cloud_default",
    gcs_bucket="een_emmer",
    gcs_filename="exchangerate_{{ ds }}.txt",
)

write_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket="een_emmer",
    source_objects=["dataproc_output_{{ ds }}/*"],
    destination_project_dataset_table="airflowbolcom-may2829-b2a87b4d:ditiseendataset.land_registry_prices{{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,)


[
    prices_uk_from_postgres_to_cloudstorage,
    exchange_rate_to_gcs,
] >> dataproc_create_cluster >> dataproc_run_pyspark >> dataproc_remove_cluster
