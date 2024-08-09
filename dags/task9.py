
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime,timedelta
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def download_data(url, local_path):
    response = requests.get(url)
    response.raise_for_status()  
    with open(local_path, 'wb') as file:
        file.write(response.content)

def upload_to_gcs(bucket_name, object_name, local_file):
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn')
    gcs_hook.upload(bucket_name=bucket_name, object_name=object_name, filename=local_file)

with DAG('gcs_to_bigquery_dag',start_date=datetime(2024,7,14),schedule_interval='@daily',
        catchup=False,description='Load gcs data to bigquery',
         default_args={"retries":2,},tags=['transfer','pavan']):
    
    start_task=DummyOperator(task_id='start_task')
    
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
        op_kwargs={
            'url': 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv',
            'local_path': '/tmp/data.csv',
        },
    )
    
    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket_name': 'rt-data-gg',
            'object_name': 'us_counties.csv',
            'local_file': '/tmp/data.csv',
        },
    )

    load_gcs_to_bq=GCSToBigQueryOperator(task_id='load_gcs_to_bq',
                                         bucket='rt-data-gg',
                                         source_objects=['us_counties.csv'],
                                         destination_project_dataset_table='brave-joy-428916-f6.data_Set1.covid_data',
                                         source_format='CSV',
                                         skip_leading_rows=1,
                                         write_disposition='WRITE_TRUNCATE',
                                         gcp_conn_id='gcp_conn')
    
  
    end_task=DummyOperator(task_id='end_task')

    start_task >> download_task >> upload_task >>load_gcs_to_bq >> end_task
    