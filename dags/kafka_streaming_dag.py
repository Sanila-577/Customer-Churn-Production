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

def trigger_continuous_consumer(**context):
    # Trigger a small batch to simulate streaming
    conf = context.get('dag_run').conf if context.get('dag_run') else {}
    max_messages = int(conf.get('max_messages', 50))
    timeout = int(conf.get('timeout', 5))

    MLKafkaConsumer.run_kafka_consumer_batch(max_messages=max_messages, timeout=timeout)

with DAG(
    dag_id='kafka_streaming_dag',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args=default_arguments,
    description='Kafka Streaming DAG - trigger small batches frequently',
    tags=['kafka','streaming','ml']
):

    trigger_stream = PythonOperator(
        task_id='trigger_stream_batch',
        python_callable=trigger_continuous_consumer,
        provide_context=True,
        execution_timeout=timedelta(minutes=5)
    )

    trigger_stream
