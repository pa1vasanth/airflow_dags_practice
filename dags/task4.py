from airflow import DAG

from datetime import datetime


from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG('Trigering_dag',
         start_date=datetime(2024,7,11),
         schedule_interval='0 11 * * *',
         catchup=False,default_args={
        "retries": 2,},description="Triggering_15 min tag",tags=["Data","pavan"]
         ):
    trigger_dag_task=TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dag_15min_task',
    )
    
    trigger_dag_task
    
    