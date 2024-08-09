from airflow import DAG

from airflow.operators.python_operator import PythonOperator,BranchPythonOperator

from datetime import datetime

from random import randint
from airflow.operators.dummy import DummyOperator


def choose_branch():
    return 'even_minute' if datetime.now().minute % 2 == 0 else 'odd_minute'



with DAG('Branchtask',
         start_date=datetime(2024,7,11),
         schedule_interval='@daily',
         catchup=False,
         default_args={
        "retries": 2,},
         tags=['Assignment','Pavan']) as dag:
    start_task = DummyOperator(
        task_id='start'
    )
    
    branching_task = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch
    )
    
    task_even_minute = DummyOperator(
        task_id='even_minute'
    )
    
    task_odd_minute = DummyOperator(
        task_id='odd_minute'
    )
    
    end_task = DummyOperator(
        task_id='end'
    )
    
    start_task >> branching_task
    branching_task >> [task_even_minute, task_odd_minute]
    task_even_minute >> end_task
    task_odd_minute >> end_task

        







