"""
Professional Airflow Task Wrappers

This module provides clean, testable, and maintainable task functions
for Airflow DAGs, following best practices for production environments.
"""

import os
import sys
import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _log_to_terminal(message: str) -> None:
    """Write a message to both stdout and the Airflow/task logger."""
    print(message, flush=True)
    logger.info(message)

def validate_input_data(data_path: str = 'data/raw/TelcoCustomerChurn.csv') -> Dict[str, Any]:
    """
    Lightweight validation that input data exists.
    
    Args:
        data_path: Path to input data file
        
    Returns:
        Dict with validation results
    """
    project_root = setup_project_environment()
    full_path = Path(project_root) / data_path
    
    _log_to_terminal(f"[airflow_tasks] Validating input data at: {full_path}")
    
    if not full_path.exists():
        logger.warning(f"Input data file not found: {full_path}")
        return {
            'status': 'warning',
            'message': 'Input data file not found',
            'file_path': str(full_path)
        }
    
    # Check file size
    file_size = full_path.stat().st_size
    if file_size == 0:
        logger.warning(f"Input data file is empty: {full_path}")
        return {
            'status': 'warning',
            'message': 'Input data file is empty',
            'file_path': str(full_path)
        }
    
    _log_to_terminal(f"✅ Input data validation passed: {file_size} bytes")
    
    return {
        'status': 'success',
        'file_path': str(full_path),
        'file_size_bytes': file_size,
        'message': 'Input data file exists and has content'
    }

def validate_processed_data(data_path: str = 'data/processed/imputed.csv') -> Dict[str, Any]:
    """
    Lightweight validation that processed data exists.
    
    Args:
        data_path: Path to processed data file
        
    Returns:
        Dict with validation results
    """
    project_root = setup_project_environment()
    full_path = Path(project_root) / data_path
    
    _log_to_terminal(f"[airflow_tasks] Validating processed data at: {full_path}")
    
    if not full_path.exists():
        logger.warning(f"Processed data file not found: {full_path}")
        return {
            'status': 'warning',
            'message': 'Processed data file not found. Run data pipeline first.',
            'file_path': str(full_path)
        }
    
    file_size = full_path.stat().st_size
    if file_size == 0:
        logger.warning(f"Processed data file is empty: {full_path}")
        return {
            'status': 'warning',
            'message': 'Processed data file is empty',
            'file_path': str(full_path)
        }
    
    _log_to_terminal(f"✅ Processed data validation passed: {file_size} bytes")
    
    return {
        'status': 'success',
        'file_path': str(full_path),
        'file_size_bytes': file_size,
        'message': 'Processed data file exists and has content'
    }

def validate_trained_model(model_path: str = 'artifacts/models') -> Dict[str, Any]:
    """
    Lightweight validation that trained model exists.
    
    Args:
        model_path: Path to model artifacts directory
        
    Returns:
        Dict with validation results
    """
    project_root = setup_project_environment()
    model_dir = Path(project_root) / model_path
    
    _log_to_terminal(f"[airflow_tasks] Validating trained model at: {model_dir}")
    
    if not model_dir.exists():
        logger.warning(f"Model directory not found: {model_dir}")
        return {
            'status': 'warning',
            'message': 'Model directory not found. Run training pipeline first.',
            'model_directory': str(model_dir)
        }
    
    # Check for any model files
    model_files = list(model_dir.glob('**/*'))
    
    if not model_files:
        logger.warning(f"No model files found in: {model_dir}")
        return {
            'status': 'warning',
            'message': 'No model files found. Run training pipeline first.',
            'model_directory': str(model_dir)
        }
    
    _log_to_terminal(f"✅ Model validation passed: {len(model_files)} file(s) found")
    
    return {
        'status': 'success',
        'model_directory': str(model_dir),
        'model_files_count': len(model_files),
        'message': 'Model files found'
    }

def trigger_training_if_needed(**context) -> Dict[str, Any]:
    """
    Check if model exists, and trigger training DAG if not.
    
    Returns:
        Dict with action taken
    """
    try:
        # Try to validate model
        result = validate_trained_model()
        logger.info("✅ Model exists and is valid")
        return {
            'status': 'model_exists',
            'action': 'none',
            'message': 'Model is ready for inference'
        }
    except FileNotFoundError as e:
        logger.warning(f"⚠️ Model not found: {e}")
        
        # Trigger training DAG
        from airflow.models import DagBag
        from airflow.api.client.local_client import Client
        
        try:
            client = Client(None, None)
            client.trigger_dag('training_pipeline_dag')
            logger.info("🚀 Triggered training_pipeline_dag")
            
            return {
                'status': 'model_missing',
                'action': 'triggered_training',
                'message': 'Training DAG triggered due to missing model'
            }
        except Exception as trigger_error:
            logger.error(f"❌ Failed to trigger training DAG: {trigger_error}")
            raise RuntimeError(f"Model missing and failed to trigger training: {trigger_error}")

def setup_project_environment() -> str:
    """
    Setup project environment and return PROJECT_ROOT.
    
    Returns:
        str: Absolute path to project root
    """
    # Get project root (works from any location)
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    
    # Add project paths to Python path
    paths_to_add = [
        str(project_root),
        str(project_root / 'src'),
        str(project_root / 'utils'),
        str(project_root / 'pipelines')
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    # Set environment variables
    os.environ['PYTHONPATH'] = ':'.join(paths_to_add)
    
    return str(project_root)

def run_data_pipeline(
                    data_path: str = 'data/raw/TelcoCustomerChurn.csv',
                    force_rebuild: bool = False,
                    output_format: str = 'both'
                    ) -> Dict[str, Any]:
    """
    Professional wrapper for data pipeline execution.
    
    Args:
        data_path: Path to input data file
        force_rebuild: Whether to force rebuild of existing artifacts
        output_format: Output format ('csv', 'parquet', or 'both')
    
    Returns:
        Dict containing pipeline execution results
    """
    project_root = setup_project_environment()
    
    try:
        # Change to project directory
        os.chdir(project_root)

        _log_to_terminal(
            f"[airflow_tasks] Starting data pipeline | data_path={data_path} | force_rebuild={force_rebuild} | output_format={output_format}"
        )
        
        # Import and execute pipeline
        from pipelines.data_pipeline import data_pipeline
        
        result = data_pipeline(
            data_path=data_path,
            target_column='Churn',
            test_size=0.2,
            force_rebuild=force_rebuild,
            output_format=output_format
        )
        
        _log_to_terminal("✓ Data pipeline completed successfully")
        
        # Return serializable summary instead of raw numpy arrays
        return {
            'status': 'success',
            'X_train_shape': result['X_train'].shape if 'X_train' in result else None,
            'X_test_shape': result['X_test'].shape if 'X_test' in result else None,
            'Y_train_shape': result['Y_train'].shape if 'Y_train' in result else None,
            'Y_test_shape': result['Y_test'].shape if 'Y_test' in result else None,
            'message': 'Data pipeline completed successfully'
        }
        
    except Exception as e:
        logger.error(f"✗ Data pipeline failed: {str(e)}")
        raise

def run_training_pipeline(
        data_path: str = 'data/raw/TelcoCustomerChurn.csv',
        model_params: Optional[Dict[str, Any]] = None,
        test_size: float = 0.2, random_state: int = 42,
        model_path: str = 'artifacts/models/churn_analysis.joblib',
        training_engine: str = 'pyspark',
        data_format: str = 'parquet'
) -> Dict[str, Any]:
    """
    Professional wrapper for training pipeline execution.
    
    Args:
        data_path: Path to input data file
        model_params: Model hyperparameters
        test_size: Test set size ratio
        random_state: Random seed for reproducibility
        model_path: Path to save trained model
        data_format: Input data format
        training_engine: Training engine ('pyspark' or 'sklearn')
    
    Returns:
        Dict containing training results and metrics
    """
    project_root = setup_project_environment()
    
    try:
        # Change to project directory
        os.chdir(project_root)

        _log_to_terminal(
            f"[airflow_tasks] Starting training pipeline | data_path={data_path} | training_engine={training_engine} | data_format={data_format}"
        )
        
        # Set default model parameters
        if model_params is None:
            model_params = {
                'numTrees': 100,
                'maxDepth': 10,
                'seed': 42
            }
        
        # Import and execute pipeline
        from pipelines.training_pipeline import training_pipeline
        
        result = training_pipeline(
            data_path=data_path,
            model_params=model_params,
            test_size=test_size,
            model_path=model_path,
            training_engine=training_engine,
            data_format=data_format
        )
        
        _log_to_terminal("✓ Training pipeline completed successfully")
        
        # Return serializable summary
        return {
            'status': 'success',
            'model_path': model_path,
            'training_engine': training_engine,
            'metrics': result if isinstance(result, dict) else {'message': str(result)},
            'message': 'Training pipeline completed successfully'
        }
        
    except Exception as e:
        logger.error(f"✗ Training pipeline failed: {str(e)}")
        raise

def run_kafka_batch_consumer(
        max_messages: int = 1000,
        timeout: int = 10,
        group_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run the Kafka batch consumer through the pipeline module.

    This wrapper is used by the Airflow batch consumer DAG so the DAG itself
    stays declarative and keeps all execution logic in one place.
    """
    project_root = setup_project_environment()

    try:
        os.chdir(project_root)

        _log_to_terminal(
            f"[airflow_tasks] Starting Kafka batch consumer | max_messages={max_messages} | timeout={timeout} | group_id={group_id}"
        )

        from pipelines.consumer import MLKafkaConsumer

        processed = MLKafkaConsumer.run_kafka_consumer_batch(
            max_messages=max_messages,
            timeout=timeout,
            group_id=group_id
        )

        _log_to_terminal(f"✓ Kafka batch consumer completed successfully | processed={processed}")

        return {
            'status': 'success',
            'processed_messages': processed,
            'message': 'Kafka batch consumer completed successfully'
        }

    except Exception as e:
        logger.error(f"✗ Kafka batch consumer failed: {str(e)}")
        raise

def run_kafka_consumer_streaming(
        max_messages: int = 50,
        timeout: int = 5,
        group_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run a small Kafka consumer batch on a streaming schedule.

    The DAG uses this to simulate streaming via frequent small batches while
    keeping the actual processing logic inside the pipeline module.
    """
    project_root = setup_project_environment()

    try:
        os.chdir(project_root)

        _log_to_terminal(
            f"[airflow_tasks] Starting Kafka consumer streaming batch | max_messages={max_messages} | timeout={timeout} | group_id={group_id}"
        )

        from pipelines.consumer import MLKafkaConsumer

        processed = MLKafkaConsumer.run_kafka_consumer_batch(
            max_messages=max_messages,
            timeout=timeout,
            group_id=group_id
        )

        _log_to_terminal(f"✓ Kafka consumer streaming batch completed successfully | processed={processed}")

        return {
            'status': 'success',
            'processed_messages': processed,
            'message': 'Kafka consumer streaming batch completed successfully'
        }

    except Exception as e:
        logger.error(f"✗ Kafka consumer streaming batch failed: {str(e)}")
        raise

def trigger_kafka_consumer_streaming_dag(
        cycle: int,
        processed_messages: int,
        total_processed: int,
        group_id: Optional[str] = None
) -> Dict[str, Any]:
    """Queue the streaming consumer DAG in Airflow UI for a completed cycle."""
    project_root = setup_project_environment()

    try:
        os.chdir(project_root)

        payload = {
            'cycle': cycle,
            'processed_messages': processed_messages,
            'total_processed': total_processed,
            'group_id': group_id,
        }
        _log_to_terminal(
            f"[airflow_tasks] Triggering kafka_consumer_streaming_dag | cycle={cycle} | processed={processed_messages} | total_processed={total_processed}"
        )

        try:
            from airflow.api.client.local_client import Client
            client = Client(None, None)
            client.trigger_dag(
                dag_id='kafka_consumer_streaming_dag',
                run_id=None,
                conf=payload,
            )
            _log_to_terminal("✓ kafka_consumer_streaming_dag queued via Airflow local client")
            return {
                'status': 'success',
                'dag_id': 'kafka_consumer_streaming_dag',
                'trigger_mode': 'local_client',
                'conf': payload,
            }
        except Exception as local_error:
            logger.warning(f"Local client trigger failed for kafka_consumer_streaming_dag: {local_error}")

        conf = json.dumps(payload)
        subprocess.run(
            ["airflow", "dags", "trigger", "kafka_consumer_streaming_dag", "--conf", conf],
            check=True,
        )
        _log_to_terminal("✓ kafka_consumer_streaming_dag queued via Airflow CLI")
        return {
            'status': 'success',
            'dag_id': 'kafka_consumer_streaming_dag',
            'trigger_mode': 'cli',
            'conf': payload,
        }

    except Exception as e:
        logger.error(f"✗ Failed to trigger kafka_consumer_streaming_dag: {str(e)}")
        raise

def validate_data_pipeline_outputs(project_root: str) -> bool:
    """
    Validate data pipeline outputs.
    
    Args:
        project_root: Project root directory path
    
    Returns:
        bool: True if validation passes
    """
    expected_files = [
        'artifacts/data/X_train.csv',
        'artifacts/data/X_test.csv', 
        'artifacts/data/Y_train.csv',
        'artifacts/data/Y_test.csv',
        'artifacts/data/X_train.parquet',
        'artifacts/data/X_test.parquet',
        'artifacts/data/Y_train.parquet', 
        'artifacts/data/Y_test.parquet'
    ]
    
    missing_files = []
    for file_path in expected_files:
        full_path = os.path.join(project_root, file_path)
        if not os.path.exists(full_path):
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"✗ Missing output files: {missing_files}")
        raise FileNotFoundError(f"Missing output files: {missing_files}")
    
    logger.info("✓ All expected output files found")
    return True

def validate_training_pipeline_outputs(
    project_root: str, 
    training_engine: str = 'pyspark'
) -> bool:
    """
    Validate training pipeline outputs.
    
    Args:
        project_root: Project root directory path
        training_engine: Training engine used
    
    Returns:
        bool: True if validation passes
    """
    if training_engine == 'pyspark':
        model_path = 'artifacts/models/airflow_spark_random_forest_model'
    else:
        model_path = 'artifacts/models/airflow_sklearn_model.joblib'
    
    full_model_path = os.path.join(project_root, model_path)
    if not os.path.exists(full_model_path):
        raise FileNotFoundError(f"Trained model not found: {model_path}")
    
    logger.info(f"✓ Training output validation completed - Model found: {model_path}")
    return True

def run_inference_pipeline(
    model_path: Optional[str] = None,
    encoders_path: str = 'artifacts/encode',
    sample_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Professional wrapper for inference pipeline execution.
    
    Args:
        model_path: Path to trained model (auto-detected if None)
        encoders_path: Path to feature encoders
        sample_data: Sample data for inference (uses default if None)
    
    Returns:
        Dict containing inference results
    """
    project_root = setup_project_environment()
    
    try:
        # Change to project directory
        os.chdir(project_root)

        _log_to_terminal(
            f"[airflow_tasks] Starting inference pipeline | model_path={model_path} | encoders_path={encoders_path}"
        )
        
        # Auto-detect model path if not provided
        if model_path is None:
            candidate_paths = [
                'artifacts/models/airflow_spark_random_forest_model',
                'artifacts/models/spark_random_forest_model'
            ]
            
            for path in candidate_paths:
                if os.path.exists(path):
                    model_path = path
                    break
            
            if model_path is None:
                raise FileNotFoundError(f"No model found in: {candidate_paths}")
        
        # Use default sample data if not provided
        if sample_data is None:
            sample_data = {
    "customerID": "9999-TEST",
    "gender": "Female",
    "SeniorCitizen": 0,
    "Partner": "No",
    "Dependents": "No",
    "tenure": 12,
    "PhoneService": "Yes",
    "MultipleLines": "No",
    "InternetService": "DSL",
    "OnlineSecurity": "No",
    "OnlineBackup": "No",
    "DeviceProtection": "No",
    "TechSupport": "No",
    "StreamingTV": "No",
    "StreamingMovies": "No",
    "Contract": "Month-to-month",
    "PaperlessBilling": "Yes",
    "PaymentMethod": "Electronic check",
    "MonthlyCharges": 29.85,
    "TotalCharges": 358.5
}

        _log_to_terminal(
            f"[airflow_tasks] Inference sample payload prepared | customerID={sample_data.get('customerID', 'N/A')}"
        )
        
        # Import and execute pipeline
        from pipelines.streaming_inference_pipeline import initialize_inference_system, streaming_inference
        
        _log_to_terminal(f"[airflow_tasks] Loading inference system for model_path={model_path}")
        
        # Initialize inference system
        inference = initialize_inference_system(
            model_path=model_path,
            encoders_path=encoders_path
        )
        
        # Run inference
        result = streaming_inference(inference, sample_data)
        
        _log_to_terminal("✓ Inference pipeline completed successfully")
        
        # Return serializable summary
        return {
            'status': 'success',
            'model_path': model_path,
            'prediction': result if isinstance(result, dict) else {'message': str(result)},
            'sample_data': sample_data,
            'message': 'Inference pipeline completed successfully'
        }
        
    except Exception as e:
        logger.error(f"✗ Inference pipeline failed: {str(e)}")
        raise
