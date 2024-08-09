from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from random import randint

def choose_branch():
    minute = datetime.now().minute
    print(f"Current minute: {minute}")
    return 'even_Condition_task' if minute % 2 == 0 else 'odd_task'

def even_min():
    a = randint(1, 100)
    print(f"Generated number: {a}")
    return a % 2 == 0

def print_even():
    print("It is an even number and an even minute")

with DAG('Dag_Circuit_Branch',
         schedule_interval='@daily',
         catchup=False,
         start_date=datetime(2024, 7, 12),
         default_args={"retries": 2},
         description='DAG using both operators',
         tags=['Operator', 'Pavan']) as dag:

    start_task = DummyOperator(task_id='Start_task')

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch
    )
    
    odd_task = DummyOperator(task_id='odd_task')

    even_Condition_task = ShortCircuitOperator(
        task_id='even_Condition_task',
        python_callable=even_min
    )
    
    print_task = PythonOperator(
        task_id='print_task',
        python_callable=print_even
    )
    
    join_task = DummyOperator(
        task_id='join_task', 
        trigger_rule='none_failed_or_skipped'
    )
    
    end_task = DummyOperator(task_id='end_task')

    start_task >> branch_task
    branch_task >> [odd_task, even_Condition_task]
    odd_task >> join_task
    even_Condition_task >> print_task >> join_task
    join_task >> end_task