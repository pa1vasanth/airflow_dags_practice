from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime


def friday_check():
    return datetime.now().weekday==4

with DAG('Friday_scheduling',start_date=datetime(2024,7,22),
         schedule_interval='@daily',
         catchup=False,
         description='Friday scheduler',tags=['friday','task scheduler']) as dag:
    start_task=DummyOperator(task_id='start_task')

    friday_condition_task=ShortCircuitOperator(task_id='friday_condition_task',
                                               python_callable=friday_check)
    
    print_task=DummyOperator(task_id='print_task')


    end_task=DummyOperator(task_id='end_task')

    start_task >> friday_condition_task >> print_task >> end_task