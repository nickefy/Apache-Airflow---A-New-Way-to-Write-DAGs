from airflow import DAG
from operators.create_dag_operator import CreateDagOperator


default_dag_args = {
    'owner': 'airflow',
    }  

dag = CreateDagOperator.create_dag('Hello_world',default_dag_args)

