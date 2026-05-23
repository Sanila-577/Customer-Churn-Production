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

from pipelines.consumer import MLKafkaConsumer

default_arguments = {
    'owner': 'sanila wijesekara',
    'depends_on_past': False,
    'start_date': timezone.datetime(2026, 5, 10, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def run_consumer_batch(**context):
    # Parameters can be passed via dag run conf
    conf = context.get('dag_run').conf if context.get('dag_run') else {}
    max_messages = int(conf.get('max_messages', 1000))
    timeout = int(conf.get('timeout', 10))

    MLKafkaConsumer.run_kafka_consumer_batch(max_messages=max_messages, timeout=timeout)

with DAG(
    dag_id='kafka_consumer_dag',
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args=default_arguments,
    description='Kafka Consumer DAG - batch scoring',
    tags=['kafka','streaming','ml']
):

    consume_and_score = PythonOperator(
        task_id='consume_and_score',
        python_callable=run_consumer_batch,
        provide_context=True,
        execution_timeout=timedelta(minutes=30)
    )

    consume_and_score
