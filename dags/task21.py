from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime

def check_if_table_is_empty(**kwargs):
    hook = BigQueryHook(gcp_conn_id='gcp_conn', use_legacy_sql=False)
    sql = """
        SELECT COUNT(*) as total_count
        FROM `your_project_id.your_dataset_id.your_table_id`
    """
    df = hook.get_pandas_df(sql)
    count = df['total_count'].iloc[0]
    if count == 0:
        return 'load_data'
    else:
        return 'skip_loading'

with DAG(
    'check_table_and_load_data',
    start_date=datetime(2024, 7, 22),
    schedule_interval='@daily',
    default_args={'retries': 1},
    description='Check if table is empty and conditionally load data',
    tags=['bigquery']
) as dag:

    start_task = DummyOperator(task_id='start')

    check_table_task = PythonOperator(
        task_id='check_if_table_is_empty',
        python_callable=check_if_table_is_empty
    )

    load_data_task = BigQueryInsertJobOperator(
    task_id='load_data',
    configuration={
        "load": {
            "sourceUris": ["gs://rt-data-gg/*.csv"],
            "destinationTable": {
                "projectId": "brave-joy-428916-f6",
                "datasetId": ".CSV_files",
                "tableId": "ama_customer_filtered"
            },
            "writeDisposition": "WRITE_APPEND",
            "sourceFormat": "CSV",
            "skipLeadingRows": 1
        }
    },
    gcp_conn_id='gcp_conn'
)


    skip_loading_task = DummyOperator(task_id='skip_loading')

    end_task = DummyOperator(task_id='end')

    start_task >> check_table_task >> [load_data_task, skip_loading_task] >> end_task