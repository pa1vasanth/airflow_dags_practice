from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime


with DAG('export_bigquery_to_gcs',
         default_args={'retries':2},
         start_date=datetime(2024,7,15),
         schedule_interval='@daily',
         catchup=False,
         tags=['bigquery','pavan']):
    
    start_task=DummyOperator(task_id='start_task')

    bq_to_gcs_task=BigQueryToGCSOperator(task_id='bq_to_gcs_task',
                                         source_project_dataset_table='brave-joy-428916-f6.CSV_files.Amazon_cust',
                                         destination_cloud_storage_uris=['gs://rt-data-practice/data-transfer/export_amazon_cust.csv'],
                                         export_format='CSV',
                                         field_delimiter=',',
                                        print_header=True,
                                        gcp_conn_id='gcp_conn')
    
    end_task=DummyOperator(task_id='end_task')

    start_task >> bq_to_gcs_task >> end_task
    