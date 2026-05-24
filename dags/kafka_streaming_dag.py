import os, sys
from pathlib import Path
from airflow import DAG
from airflow.utils import timezone
from datetime import timedelta
from airflow.operators.python import PythonOperator

def get_project_root() -> str:
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / 'utils').is_dir():
            return str(parent)
    return str(current_path.parents[1])

sys.path.insert(0, get_project_root())

from utils.airflow_tasks import validate_trained_model, run_inference_pipeline

default_arguments = {
    'owner': 'sanila wijesekara',
    'depends_on_past': False,
    'start_date': timezone.datetime(2026, 5, 10, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='kafka_consumer_streaming_dag',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args=default_arguments,
    description='Kafka Streaming Inference DAG - frequent inference runs',
    tags=['kafka','inference','ml']
):

    validate_trained_model_task = PythonOperator(
        task_id='validate_trained_model',
        python_callable=validate_trained_model,
        execution_timeout=timedelta(minutes=2)
    )

    run_inference_pipeline_task = PythonOperator(
        task_id='run_inference_pipeline',
        python_callable=run_inference_pipeline,
        execution_timeout=timedelta(minutes=2)
    )

    validate_trained_model_task >> run_inference_pipeline_task
