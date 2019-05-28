import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="dagsinterklaasje",
    default_args={
        "owner": "naamhierinvullen",
        "start_date": airflow.utils.dates.days_ago(3),
    },
    schedule_interval=None,
)

t1 = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)

t2 = BashOperator(task_id="wait_5", bash_command="sleep 5", dag=dag)

t3 = BashOperator(task_id="wait_1", bash_command="sleep 1", dag=dag)

t4 = BashOperator(task_id="wait_10", bash_command="sleep 10", dag=dag)

t5 = DummyOperator(task_id="the_end", dag=dag)

t1 >> [t2, t3, t4] >> t5
