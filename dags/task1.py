from airflow import DAG

from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

with DAG('Dummy_operator_task',
         start_date=datetime(2024,7,11),
         schedule_interval='0 11 * * *',
         catchup=False,
         default_args={
        "retries": 2,
    }) as dag:
    
    dummy_task=DummyOperator(task_id='dummy_task')