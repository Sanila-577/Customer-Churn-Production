import os
import sys
import joblib
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pipelines.data_pipeline import data_pipeline
from typing import Dict, Any, Tuple, Optional
import json
from pathlib import Path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.model_training import ModelTrainer, SparkModelTrainer
from src.model_evaluation import ModelEvaluator, SparkModelEvaluator
from src.model_building import XGboostModelBuilder, SparkRandomForestModelBuilder
from src.spark_session import create_spark_session, stop_spark_session
from src.spark_utils import spark_to_pandas
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from utils.mlflow_utils import MLflowTracker, setup_mlflow_autolog, create_mlflow_run_tags
from utils.config import get_model_config, get_data_paths
import mlflow
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_model_performance_visualizations(model, X_test: pd.DataFrame, y_test: pd.Series, 
                                          evaluation_results: dict, artifacts_dir: str, model_name: str):
    """Create comprehensive model performance visualizations."""
    try:
        # Create model-specific directory
        model_dir = os.path.join(artifacts_dir, f"model_performance_{model_name}")
        os.makedirs(model_dir, exist_ok=True)
        
        # 1. Confusion Matrix Heatmap
        if 'cm' in evaluation_results:
            plt.figure(figsize=(8, 6))
            sns.heatmap(evaluation_results['cm'], annot=True, fmt='d', cmap='Blues',
                       xticklabels=['Retain', 'Churn'], yticklabels=['Retain', 'Churn'])
            plt.title(f'{model_name} - Confusion Matrix')
            plt.ylabel('True Label')
            plt.xlabel('Predicted Label')
            plt.tight_layout()
            cm_path = os.path.join(model_dir, f'confusion_matrix_{model_name}.png')
            plt.savefig(cm_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Log confusion matrix as artifact
            mlflow.log_artifact(cm_path, f"model_performance/{model_name}")
        
        # 2. Feature Importance (if available)
        if hasattr(model, 'feature_importances_'):
            plt.figure(figsize=(12, 8))
            feature_importance = pd.DataFrame({
                'feature': X_test.columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=True)
            
            # Plot top 15 features
            top_features = feature_importance.tail(15)
            plt.barh(range(len(top_features)), top_features['importance'])
            plt.yticks(range(len(top_features)), top_features['feature'])
            plt.xlabel('Feature Importance')
            plt.title(f'{model_name} - Top 15 Feature Importances')
            plt.tight_layout()
            
            importance_path = os.path.join(model_dir, f'feature_importance_{model_name}.png')
            plt.savefig(importance_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Save feature importance as JSON
            importance_json_path = os.path.join(model_dir, f'feature_importance_{model_name}.json')
            feature_importance.to_json(importance_json_path, indent=2)
            
            # Log artifacts
            mlflow.log_artifact(importance_path, f"model_performance/{model_name}")
            mlflow.log_artifact(importance_json_path, f"model_performance/{model_name}")
        
        # 3. ROC Curve (if probabilities available)
        try:
            from sklearn.metrics import roc_curve, auc
            y_proba = model.predict_proba(X_test)[:, 1]
            fpr, tpr, _ = roc_curve(y_test, y_proba)
            roc_auc = auc(fpr, tpr)
            
            plt.figure(figsize=(8, 6))
            plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (AUC = {roc_auc:.3f})')
            plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
            plt.xlim([0.0, 1.0])
            plt.ylim([0.0, 1.05])
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.title(f'{model_name} - ROC Curve')
            plt.legend(loc="lower right")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            roc_path = os.path.join(model_dir, f'roc_curve_{model_name}.png')
            plt.savefig(roc_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Log ROC curve
            mlflow.log_artifact(roc_path, f"model_performance/{model_name}")
            mlflow.log_metric(f'{model_name}_roc_auc', roc_auc)
            
        except Exception as e:
            logger.warning(f"Could not create ROC curve: {str(e)}")
        
        # 4. Prediction Distribution
        try:
            y_pred = model.predict(X_test)
            y_proba = model.predict_proba(X_test)[:, 1]
            
            fig, axes = plt.subplots(1, 2, figsize=(15, 6))
            
            # Prediction distribution
            pred_counts = pd.Series(y_pred).value_counts()
            axes[0].bar(['Retain', 'Churn'], [pred_counts.get(0, 0), pred_counts.get(1, 0)])
            axes[0].set_title('Prediction Distribution')
            axes[0].set_ylabel('Count')
            
            # Probability distribution
            axes[1].hist(y_proba, bins=30, alpha=0.7, edgecolor='black')
            axes[1].set_xlabel('Churn Probability')
            axes[1].set_ylabel('Frequency')
            axes[1].set_title('Churn Probability Distribution')
            
            plt.suptitle(f'{model_name} - Prediction Analysis')
            plt.tight_layout()
            
            pred_dist_path = os.path.join(model_dir, f'prediction_distribution_{model_name}.png')
            plt.savefig(pred_dist_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            mlflow.log_artifact(pred_dist_path, f"model_performance/{model_name}")
            
        except Exception as e:
            logger.warning(f"Could not create prediction distribution: {str(e)}")
        
        logger.info(f"✓ Model performance visualizations created for {model_name}")
        
    except Exception as e:
        logger.error(f"✗ Failed to create model performance visualizations: {str(e)}")


def log_model_metadata(model, model_name: str, model_params: dict, training_time: float, artifacts_dir: str):
    """Log comprehensive model metadata."""
    try:
        metadata = {
            'model_name': model_name,
            'model_type': type(model).__name__,
            'model_parameters': model_params,
            'training_time_seconds': training_time,
            'sklearn_version': None,
            'model_size_mb': None,
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Try to get sklearn version
        try:
            import sklearn
            metadata['sklearn_version'] = sklearn.__version__
        except:
            pass
        
        # Try to get model size
        try:
            model_path = os.path.join(artifacts_dir, f'temp_{model_name}_model.pkl')
            joblib.dump(model, model_path)
            metadata['model_size_mb'] = os.path.getsize(model_path) / (1024**2)
            os.remove(model_path)  # Clean up temp file
        except:
            pass
        
        # Save metadata
        metadata_path = os.path.join(artifacts_dir, f'model_metadata_{model_name}.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        # Log as MLflow artifact
        mlflow.log_artifact(metadata_path, f"model_metadata/{model_name}")
        
        # Log key metadata as parameters and metrics
        mlflow.log_params({
            f'{model_name}_model_type': type(model).__name__,
            f'{model_name}_sklearn_version': metadata.get('sklearn_version', 'unknown')
        })
        
        mlflow.log_metrics({
            f'{model_name}_training_time_seconds': training_time,
            f'{model_name}_model_size_mb': metadata.get('model_size_mb', 0)
        })
        
        logger.info(f"✓ Model metadata logged for {model_name}")
        
    except Exception as e:
        logger.error(f"✗ Failed to log model metadata: {str(e)}")


def training_pipeline(
                    data_path: str = 'data/raw/TelcoCustomerChurn.csv',
                    model_params: Optional[Dict[str, Any]] = None,
                    test_size: float = 0.2, random_state: int = 42,
                    model_path: str = 'artifacts/models/churn_analysis.joblib',
                    training_engine: str = 'pyspark',
                    data_format: str = 'parquet'):
    # if (not os.path.exists(get_data_paths()['X_train'])) or \
    #     (not os.path.exists(get_data_paths()['X_test'])) or \
    #     (not os.path.exists(get_data_paths()['y_train'])) or \
    #     (not os.path.exists(get_data_paths()['y_test'])):
        
    #     data_pipeline()
    # else:
    #     print("Loading Data Artifacts from Data Pipeline.")

    """
    Execute model training pipeline with either scikit-learn or PySpark MLlib.
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"STARTING TRAINING PIPELINE - ENGINE: {training_engine.upper()}")
    logger.info(f"{'='*80}")

    # Run data pipeline first
    data_pipeline()

    # Initialize Spark session (needed for both data loading and PySpark training)
    spark = create_spark_session("ChurnPredictionTrainingPipeline")
    

    try:
        mlflow_tracker = MLflowTracker()
        run_tags = create_mlflow_run_tags(
                                    'training_pipeline', {
                                                        'model_type' : 'XGboost',
                                                        'training_strategy' : 'simple',
                                                        'other_models' : 'randomforest',
                                                        'data_path': data_path,
                                                        'model_path': model_path
                                                        }
                                                        )
        run = mlflow_tracker.start_run(run_name='training_pipeline', tags=run_tags)
        
        # Create artifacts directory for this run
        run_artifacts_dir = os.path.join('artifacts', 'mlflow_training_artifacts', run.info.run_id)
        os.makedirs(run_artifacts_dir, exist_ok=True)

        # Load training data with logging
        logger.info("Loading training and test datasets...")
        data_paths = get_data_paths()

        if data_format == 'parquet':
            # Determine parquet paths (config may point to CSV paths)
            def _parquet_path(p):
                if p.lower().endswith('.csv'):
                    candidate = p[:-4] + '.parquet'
                else:
                    candidate = p
                return candidate if os.path.exists(candidate) else p

            X_train_path = _parquet_path(data_paths['X_train'])
            X_test_path = _parquet_path(data_paths['X_test'])
            Y_train_path = _parquet_path(data_paths['Y_train'])
            Y_test_path = _parquet_path(data_paths['Y_test'])

            X_train = spark.read.parquet(X_train_path)
            X_test = spark.read.parquet(X_test_path)
            y_train = spark.read.parquet(Y_train_path)
            y_test = spark.read.parquet(Y_test_path)

            # Convert to pandas for sklearn or keep as Spark for PySpark
            if training_engine == 'sklearn':
                X_train = spark_to_pandas(X_train)
                X_test = spark_to_pandas(X_test)
                y_train = spark_to_pandas(y_train)
                y_test = spark_to_pandas(y_test)
        else:
            # Load CSV data
            X_train = pd.read_csv(data_paths['X_train'])
            X_test = pd.read_csv(data_paths['X_test'])
            y_train = pd.read_csv(data_paths['Y_train'])
            y_test = pd.read_csv(data_paths['Y_test'])

        logger.info(f"✓ Data loaded successfully")

        # Normalize counts and feature metrics for Spark or pandas inputs
        def _nrows(df):
            try:
                if hasattr(df, 'count') and not isinstance(df, pd.DataFrame):
                    return int(df.count())
                return int(len(df))
            except Exception:
                return 0

        def _ncols(df):
            try:
                if hasattr(df, 'columns'):
                    return len(df.columns)
                return int(getattr(df, 'shape')[1])
            except Exception:
                return 0

        def _class_counts(y):
            try:
                # Spark DataFrame
                if hasattr(y, 'filter') and hasattr(y, 'columns'):
                    col = y.columns[0]
                    from pyspark.sql import functions as _F
                    return int(y.filter(_F.col(col) == 0).count()), int(y.filter(_F.col(col) == 1).count())
                # pandas DataFrame/Series
                if isinstance(y, pd.DataFrame):
                    ser = y.iloc[:, 0]
                elif isinstance(y, pd.Series) or isinstance(y, np.ndarray):
                    ser = pd.Series(y.squeeze())
                else:
                    ser = pd.Series(y)
                return int((ser == 0).sum()), int((ser == 1).sum())
            except Exception:
                return 0, 0

        train_cnt = _nrows(X_train)
        test_cnt = _nrows(X_test)
        num_features = _ncols(X_train)
        train_c0, train_c1 = _class_counts(y_train)
        test_c0, test_c1 = _class_counts(y_test)

        mlflow.log_metrics({
            'train_samples': train_cnt,
            'test_samples': test_cnt,
            'num_features': num_features,
            'train_class_0': train_c0,
            'train_class_1': train_c1,
            'test_class_0': test_c0,
            'test_class_1': test_c1
        })
        

        # Log feature names
        mlflow.log_param('feature_names', list(X_train.columns))
        # Train model based on engine
        if training_engine == 'pyspark':
            evaluation_results = _train_pyspark_model(
                spark, X_train, X_test, y_train, y_test, model_params, model_path
            )
        else:
            evaluation_results = _train_sklearn_model(
                X_train, X_test, y_train, y_test, model_params, model_path
            )

        # Log results to MLflow
        mlflow.log_metrics({
            'accuracy': evaluation_results.get('accuracy', 0),
            'precision': evaluation_results.get('precision', 0),
            'recall': evaluation_results.get('recall', 0),
            'f1_score': evaluation_results.get('f1', 0)
        })

        logger.info("✓ Training pipeline completed successfully!")
        return evaluation_results
        
    except Exception as e:
        logger.error(f"✗ Training pipeline failed: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)


def _train_sklearn_model(X_train, X_test, y_train, y_test, model_params, model_path):
    """Train model using scikit-learn."""
    logger.info("Training with scikit-learn...")

    # Build model
    model_builder = XGboostModelBuilder(**model_params)
    model = model_builder.build_model()

    # Train model
    trainer = ModelTrainer()
    model, training_score = trainer.train(model, X_train, y_train.squeeze())

    # Save model
    trainer.save_model(model, model_path)

    # Evaluate model
    evaluator = ModelEvaluator(model, 'XGboost')
    evaluation_results = evaluator.evaluate(X_test, y_test)

    return evaluation_results


def _train_pyspark_model(spark, X_train, X_test, y_train, y_test, model_params, model_path):
    """Train model using PySpark MLlib."""
    logger.info("Training with PySpark MLlib...")

    # Ensure train/test Spark DataFrames include a 'label' column
    if isinstance(X_train, pd.DataFrame):
        # Combine pandas features and labels then convert to Spark
        train_pandas = X_train.copy()
        train_pandas['label'] = y_train.squeeze()
        train_spark_df = spark.createDataFrame(train_pandas)

        test_pandas = X_test.copy()
        test_pandas['label'] = y_test.squeeze()
        test_spark_df = spark.createDataFrame(test_pandas)

        feature_columns = X_train.columns.tolist()
    else:
        # X_train is Spark DataFrame; convert both to pandas, combine, then back to Spark
        X_train_pd = spark_to_pandas(X_train)
        y_train_pd = spark_to_pandas(y_train)
        train_pandas = X_train_pd.copy()
        train_pandas['label'] = y_train_pd.iloc[:, 0]
        train_spark_df = spark.createDataFrame(train_pandas)

        X_test_pd = spark_to_pandas(X_test)
        y_test_pd = spark_to_pandas(y_test)
        test_pandas = X_test_pd.copy()
        test_pandas['label'] = y_test_pd.iloc[:, 0]
        test_spark_df = spark.createDataFrame(test_pandas)

        feature_columns = list(X_train_pd.columns)

    # Build PySpark model
    model_builder = SparkRandomForestModelBuilder(**model_params)
    model = model_builder.build_model()

    # Train model
    trainer = SparkModelTrainer(spark)
    trained_pipeline, training_metrics = trainer.train(
        model, train_spark_df, feature_columns
    )

    # Save model
    trainer.save_model(trained_pipeline, model_path)

    # Evaluate model
    evaluator = SparkModelEvaluator(trained_pipeline, 'SparkRandomForest')
    evaluation_results = evaluator.evaluate(test_spark_df)

    return evaluation_results


if __name__ == '__main__':
    model_config = get_model_config()
    training_engine = model_config.get('training_engine', 'pyspark')

    if training_engine == 'pyspark':
        model_params = model_config.get('pyspark_model_types', {}).get('spark_random_forest', {})
        model_path = 'artifacts/models/spark_random_forest_model'
    else:
        model_params = model_config.get('model_params', {})
        model_path = 'artifacts/models/sklearn_model.joblib'

    training_pipeline(
        model_params=model_params,
        model_path=model_path,
        training_engine=training_engine
    )