from airflow import DAG
from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteObjectsOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

def fetch_data_from_snowflake():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sql_query = ' use compute_wh; SELECT * FROM Random_tress.SCHEMA1.emp;'
    df = snowflake_hook.get_pandas_df(sql_query)
    df.to_csv('/tmp/snowflake_data.csv', index=False)


with DAG(
    'snowflake_to_bq',
    default_args={'retries':2,},
    start_date=datetime(2024,7,24),
    schedule_interval='@daily',
    description='transfer data from Snowflake to BigQuery',
    tags=['bq','snowflake']) as dag:

    fetch_data_task=PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_snowflake
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src='/tmp/snowflake_data.csv',
    dst='snowflake_data.csv',
    bucket='rt-data-gg',
    gcp_conn_id='gcp_conn'
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='rt-data-gg',
        source_objects=['snowflake_data.csv'],
        destination_project_dataset_table='brave-joy-428916-f6.data_Set1.emp_data',
        schema_fields=None,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        gcp_conn_id='gcp_conn'
    )

    fetch_data_task >> upload_to_gcs_task >>load_to_bigquery_task



