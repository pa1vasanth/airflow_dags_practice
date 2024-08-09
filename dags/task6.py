from airflow import DAG

from airflow.operators.python import PythonOperator,ShortCircuitOperator

from datetime import datetime
from airflow.operators.dummy import DummyOperator

def condition_branch():
    return datetime.now().minute % 2 == 0

def gg():
    print('Its is even time')


with DAG('short_circuit_dag',schedule_interval='@daily',
         start_date=datetime(2024,7,12),catchup=False,
         default_args={"retries":2,},description="Dag for short circuit",
         tags=['data','pavan']):
    
    start_task=DummyOperator(task_id='start')

    end_task=DummyOperator(task_id='end')

    Checking_condition=ShortCircuitOperator(task_id='Checking_condition',
                                            python_callable=condition_branch)
    
    print_task=PythonOperator(task_id='print_task',
                              python_callable=gg)
    
    start_task >> Checking_condition >> print_task >> end_task