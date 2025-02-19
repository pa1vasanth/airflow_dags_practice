
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
from datetime import datetime
import requests

def print_welcome():
    print("welcome to airflow")

def print_date():
    print("today date is {}".format(datetime.today().date()))

def print_random_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the day: "{}"'.format(quote))

# Define default arguments for the DAG
default_args = {
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'welcome_dag',
    default_args=default_args,
    schedule_interval='0 14 * * *',
    catchup=False,
)
    
print_welcome_task = PythonOperator (
    task_id= 'print_welcome',
        python_callable=print_welcome
        , dag=dag
)


print_date_task = PythonOperator(
    task_id= 'print_date',
      python_callable=print_date,
        dag=dag)

print_random_quote = PythonOperator (
    task_id='print_random_quote',
    python_callable=print_random_quote,
      dag=dag)

print_welcome_task >> print_date_task >> print_random_quote