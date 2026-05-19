import os, sys
from pathlib import Path
from airflow import DAG
from airflow.utils import timezone
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def get_project_root() -> str:
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / 'utils').is_dir():
            return str(parent)
    return str(current_path.parents[1])

sys.path.insert(0, get_project_root())

from utils.airflow_tasks import validate_processed_data, run_training_pipeline

"""

============== DAG ============================

Validate Processed Data -> Run Train Pipeline Task

"""

default_arguments = {
                    'owner' : 'sanila wijesekara',
                    'depends_on_past' : False,
                    'start_date': timezone.datetime(2026, 5, 10, 0, 0),
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 0,
                    }

with DAG(
        dag_id = 'train_pipeline_dag',
        schedule_interval='30 19 * * *', # 1AM daily (IST) -> 19.30 UTC
        catchup=False,
        max_active_runs=1,
        default_args = default_arguments,
        description='Train Pipeline - 1AM daily Sheduled',
        tags=['pyspark', 'mllib', 'mlflow', 'batch-processing']
        ) as dag:
    
    # Step 1
    validate_processed_data_task = PythonOperator(
                                            task_id='validate_processed_data',
                                            python_callable=validate_processed_data,
                                            execution_timeout=timedelta(minutes=2)
                                            )

    # Step 2
    run_training_pipeline_task = PythonOperator(
                                            task_id='run_training_pipeline',
                                            python_callable=run_training_pipeline,
                                            execution_timeout=timedelta(minutes=30)
                                            )

    validate_processed_data_task >> run_training_pipeline_task