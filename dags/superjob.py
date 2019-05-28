import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow_training.operators.postgres_to_gcs import (
    PostgresToGoogleCloudStorageOperator,
)

dag = DAG(
    dag_id="superjob",
    default_args={
        "owner": "naamhierinvullen",
        "start_date": airflow.utils.dates.days_ago(7),
        "catchup": False,
    },
    schedule_interval="@daily",
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="prices_uk_from_postgres_to_cloudstorage",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="een_emmer",
    filename="daily_load_{{ ds }}",
    postgres_conn_id="stuff_postgres",
    dag=dag,
)
