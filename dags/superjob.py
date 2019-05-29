import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow_training.operators.postgres_to_gcs import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyOwnOperator(BaseOperator):

    ui_color = '#0090e3'
    ui_fgcolor = '#000000'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        def execute(self, context):
            pass

    def execute(self, context):
        print("exec")
        self.log("Im a log message")

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

myown = MyOwnOperator(task_id='myown', dag=dag)


myown >> pgsl_to_gcs