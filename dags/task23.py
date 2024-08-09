from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.operators.dummy import DummyOperator

from datetime import datetime

def get_value_from_source_table(**kwargs):
    hook = BigQueryHook(gcp_conn_id='gcp_conn', use_legacy_sql=False)
    sql = "SELECT High FROM `brave-joy-428916-f6.CSV_files.ama_customer_filtered` LIMIT 1"
    result = hook.get_pandas_df(sql)
    if not result.empty:
        return result['High'].iloc[0]
    else:
        raise ValueError("No value found in the source table")

with DAG('bigquery_value_pass_and_load',
         start_date=datetime(2024, 7, 22),
         schedule_interval='@daily',
         default_args={'retries': 1,},
         description='bigquery', tags=['bigquery']) as dag:
    
    start_task = DummyOperator(task_id='start_task')

    get_value_task = PythonOperator(
        task_id='get_value_task',
        python_callable=get_value_from_source_table
    )
    
    bigquery_execute_task = BigQueryExecuteQueryOperator(
        task_id='bigquery_execute_task',
        sql='''
            SELECT * FROM `brave-joy-428916-f6.CSV_files.ama_customer_filtered`
            WHERE High = '{{ task_instance.xcom_pull(task_ids='get_value_task') }}'
        ''',
        gcp_conn_id='gcp_conn',
        use_legacy_sql=False,
        destination_dataset_table='brave-joy-428916-f6.CSV_files.filtered_ama',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
    )
    
    end_task = DummyOperator(task_id='end_task')

    start_task >> get_value_task >> bigquery_execute_task >> end_task