
from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime,timedelta

with DAG('move_files_gcs',
         start_date=datetime(2024,7,16),
         schedule_interval='@daily',
         catchup=False,
         default_args={"retries":2,},
         tags=['gcs_transfer','pavan']):
    
    start_task=DummyOperator(task_id='start_task')

    move_files_gcs_task=GCSToGCSOperator(task_id='move_files_gcs_task',
                                         source_bucket='rt-data-practice',
                                         source_object='data-transfer/export_amazon_cust.csv',
                                         destination_bucket='rt-data-practice',
                                         destination_object='rt-data-practice/moving_folder',
                                         gcp_conn_id='gcp_conn')
    
    end_task=DummyOperator(task_id='end_task')

    start_task >> move_files_gcs_task >> end_task