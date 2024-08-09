from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

def test_snowflake_connection():
    try:
        # Get Snowflake hook
        snowflake_hook = BaseHook.get_hook('snowflake_self')
        
        # Test connection by executing a simple query
        result = snowflake_hook.get_first("SELECT current_version()")
        if result:
            print("Snowflake connection successful!")
        else:
            print("Snowflake connection failed!")
    except Exception as e:
        print(f"Error testing Snowflake connection: {str(e)}")

# Define DAG
with DAG('test_snowflake_connection',
         start_date=datetime(2024, 7, 15),
         schedule_interval='@once',
         catchup=False) as dag:
    
    # Task to test Snowflake connection
    test_connection_task = PythonOperator(
        task_id='test_snowflake_connection',
        python_callable=test_snowflake_connection
    )

# Set task dependencies
test_connection_task