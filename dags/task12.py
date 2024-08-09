from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, date
import json
import tempfile

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

def extract_from_snowflake(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    # Set the warehouse
    cursor.execute("USE WAREHOUSE compute_wh")
    
    # Execute the query
    query = "SELECT * FROM emp"
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    
    # Convert results to list of dictionaries
    data = [dict(zip(columns, row)) for row in results]
    
    # Save results to a JSON file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
        json.dump(data, tmp_file, cls=CustomJSONEncoder)
        tmp_file.flush()
        return tmp_file.name

def upload_to_gcs(**kwargs):
    from google.cloud import storage
    task_instance = kwargs['ti']
    temp_file = task_instance.xcom_pull(task_ids='extract_from_snowflake')
    
    client = storage.Client()
    bucket = client.bucket('rt-data-practice')
    blob = bucket.blob('data.json')
    blob.upload_from_filename(temp_file)

with DAG('snowflake_to_bigquery_dag',
         start_date=datetime(2024, 7, 17),
         schedule_interval='@daily',
         catchup=False,
         default_args={"retries": 2},
         tags=['snowflake', 'bigquery', 'data_pipeline']) as dag:

    start_task = DummyOperator(task_id='start_task')

    extract_task = PythonOperator(
        task_id='extract_from_snowflake',
        python_callable=extract_from_snowflake,
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True
    )

    gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='rt-data-practice',
        source_objects=['data.json'],
        destination_project_dataset_table='brave-joy-428916-f6.CSV_files.emp',
        schema_fields=None,
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,
        gcp_conn_id='gcp_conn',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    end_task = DummyOperator(task_id='end_task')

    start_task >> extract_task >> upload_task >> gcs_to_bigquery_task >> end_task