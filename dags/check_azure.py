from airflow import DAG

from airflow.operators.dummy import DummyOperator

from airflow.operators.python import PythonOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from airflow.hooks.base import BaseHook

from datetime import datetime

#def get_azure_connection(conn_id):
   # conn = BaseHook.get_connection(conn_id)
    #return conn

def  load_data_from_blob_to_snowflake():
    #azure_conn1 = get_azure_connection('azure_conn')
    #print("Azure connection details")
    #print(azure_conn1)

    client_id = 'f0f17218-ba5c-4aac-85ea-ba8029831ee8'
    secret = 'XXXX'
    tenant_id = '118b2509-1765-4cc5-b637-2802f506b748'

    credential = ClientSecretCredential(
        client_id=client_id,
        client_secret=secret,
        tenant_id=tenant_id
    )

    blob_service_client = BlobServiceClient(
        account_url="https://trialblobstorage33.blob.core.windows.net" ,  
        credential=credential
    )

    container_name = 'airflow'
    blob_name = 'sample_data1.csv'
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    blob_data = blob_client.download_blob().readall()
    csv_df = pd.read_csv(StringIO(blob_data.decode('utf-8')))

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake-conn')
    
    table = 'ecommerce_liv.lineitem'
    snowflake_hook.insert_rows(
    table=table, 
    rows=csv_df.values.tolist(), 
    target_fields=csv_df.columns.tolist(), 
    commit_every=10 
    )

with DAG( 'azure_blob',
         start_date=datetime(2024,8,8),
         schedule_interval=None,
         catchup=False,
         default_args={
             'owner': 'airflow',
             'retries':2,
             },
         description="Azure blob to snowflake"):
    
    start_task=DummyOperator(task_id='start_task')

    load_data_task=PythonOperator(task_id='load_data_task',
                                  python_callable=load_data_from_blob_to_snowflake)

    end_task=DummyOperator(task_id='end_task')


    start_task >>load_data_task >> end_task
    
