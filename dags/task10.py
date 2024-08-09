from airflow import DAG
from airflow.operators.python import PythonOperator

import snowflake.connector

from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime 


with DAG('snowflake_procedure_dag',
         start_date=datetime(2024,7,14),
         schedule_interval='@daily',
         default_args = {
    'retries': 2,},
    catchup=False,
    description='snowflake procedure calling',
    tags=['snowflake_procedure','pavan']) as dag:

    start_task=DummyOperator(task_id='start_task')

    execute_procedure_task = SnowflakeOperator( task_id='execute_procedure', 
                                               snowflake_conn_id='snowflake_conn'
                                               , sql="""USE WAREHOUSE compute_wh;
                                               select * from emp;""")
    
    end_task=DummyOperator(task_id='end_task')

    start_task >> execute_procedure_task >> end_task

