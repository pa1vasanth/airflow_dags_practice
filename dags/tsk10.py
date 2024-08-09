from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.operators.dummy import DummyOperator
from datetime import datetime


with DAG('execute_bq_query_load_newtable',
         start_date=datetime(2024,7,15),
         schedule_interval='@daily',
         catchup=False,
          default_args={"retries":2,},
         tags=['bq','pavan']) as dag:
    
    start_task=DummyOperator(task_id='start_task')

    sql_query="""
    select * from `brave-joy-428916-f6.CSV_files.ama_customer`
    where Volume >98390000
    """
    job_config={
        "query" :{
                "query": sql_query,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "brave-joy-428916-f6",
                "datasetId": "CSV_files",
                "tableId": "ama_customer_filtered"
            },
            "writeDisposition": "WRITE_TRUNCATE"

        }
    }

    execute_bq_query=BigQueryInsertJobOperator(
        task_id='execute_bq_query',
        configuration=job_config,
        gcp_conn_id='gcp_conn',
        location='US'
    )

    end_task=DummyOperator(task_id='end_task')

    start_task >> execute_bq_query >> end_task