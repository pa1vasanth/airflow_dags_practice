from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.dummy import DummyOperator

from datetime import datetime



def sending_list(ti):
    x=[1,4,7,9]
    print("Pushing List")
    ti.xcom_push(key='my_key',value=x)

def max_no_lst(ti):
    list_pull=ti.xcom_pull(key='my_key',task_ids='sendinglist_task')
    print(list_pull)
    ti.xcom_push(key='max_no',value=list_pull[2])

def print_value(ti):
    max_no=ti.xcom_pull(key='max_no',task_ids='maxno_task')
    print(max_no)
    return max_no



with DAG('Xcom_xpull_dag',start_date=datetime(2024,7,12), schedule_interval='@daily',
         catchup=False,
         description="work with xcom",tags=['data','pavan']):
    
    start_task=DummyOperator(task_id='start')

    sendinglist_task=PythonOperator(task_id='sendinglist_task',
                                    python_callable=sending_list)
    
    maxno_task=PythonOperator(task_id='maxno_task',
                              python_callable=max_no_lst)
    
    print_task=PythonOperator(task_id='print_task',
                              python_callable=print_value)
    
    end_task=DummyOperator(task_id='end')

    start_task >> sendinglist_task >> maxno_task >> print_task >> end_task

