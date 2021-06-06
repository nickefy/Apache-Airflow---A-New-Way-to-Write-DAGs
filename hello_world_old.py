from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.print_text_operator import PrintText

text_to_print_dag = 'Hello World!'

args = {
    'owner': 'Airflow',
    'start_date': '2021-01-01',
    'retries': 1,
}

dag = DAG(
    dag_id='HELLO WORLD',
    default_args=args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
)

run_this = PrintText(
    task_id='Print Hello World',
    text_to_print=text_to_print_dag,
)

run_this
