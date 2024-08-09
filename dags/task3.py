from airflow import DAG

from datetime import datetime

from airflow.operators.python_operator import PythonOperator

def print_Afun():
    print ("It's been 15 minutes isnce last run")


with DAG('dag_15min_task',
         start_date=datetime(2024,7,11),
         schedule_interval='*/15 * * * *',
        catchup=False,
        default_args={
        "retries": 2,  
    }):
    
    Python_task=PythonOperator(task_id='task1',
                               python_callable=print_Afun)
    

    Python_task
