import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="dagbob",
    default_args={
        "owner": "naamhierinvullen",
        "start_date": airflow.utils.dates.days_ago(3),
    },
    schedule_interval=None,
)

def print_weekday(execution_date, **context):
    print(execution_date.strftime("%a"))

weekday_person_to_email = {
    0: "Bob",
    1: "Joe",
    2: "Alice",
    3: "Joe",
    4: "Alice",
    5: "Alice",
    6: "Alice"
}
def _who_to_mail(execution_date, **context):
    return 'mail_' + str(weekday_person_to_email[execution_date.weekday()])


t1 = PythonOperator(
    task_id="print_exec_date", python_callable=print_weekday, dag=dag
)

# t2 = BashOperator(
#     task_id="wait_5", bash_command="sleep 5", dag=dag
# )
#
# t3 = BashOperator(
#     task_id="wait_1", bash_command="sleep 1", dag=dag
# )
#
# t4 = BashOperator(
#     task_id="wait_10", bash_command="sleep 10", dag=dag
# )
#
# t5 = DummyOperator(
#     task_id="the_end", dag=dag
# )
#
# t1 >> [t2, t3, t4] >> t5
