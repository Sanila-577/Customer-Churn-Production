import os
import sys
import logging
import json
from typing import Dict, Optional, List, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from src.data_ingestion import DataIngestorCSV
from src.handle_missing_values import DropMissingValuesStrategy, FillMissingValuesStrategy
from src.outlier_detection import OutlierDetector, IQROutlierDetection
from src.feature_binning import CustomBinningStrategy
from src.feature_encoding import OrdinalEncodingStrategy, NominalEncodingStrategy
from src.feature_scaling import StandardScalingStrategy
from src.data_spiltter import SimpleTrainTestSplitStratergy

from utils.config import (
    get_data_paths,
    get_columns,
    get_missing_values_config,
    get_outlier_config,
    get_binning_config,
    get_encoding_config,
    get_scaling_config,
    get_splitting_config,
    
)

from src.spark_session import create_spark_session, stop_spark_session
from src.spark_utils import save_dataframe, spark_to_pandas, get_dataframe_info, check_missing_values
from utils.mlflow_utils import MLflowTracker, setup_mlflow_autolog, create_mlflow_run_tags
import mlflow

def _ensure_pandas_df(df):
    if isinstance(df, pd.DataFrame):
        return df
    if isinstance(df, DataFrame):
        return spark_to_pandas(df)
    return pd.DataFrame(df)


def _get_value_counts(df, column: str) -> Dict:
    if isinstance(df, pd.DataFrame):
        return df[column].value_counts().to_dict()

    rows = df.groupBy(column).count().collect()
    return {row[column]: row['count'] for row in rows}


def create_data_visualizations(df: pd.DataFrame, stage: str, artifacts_dir: str):
    """Create essential data visualizations for MLflow artifacts."""
    try:
        df = _ensure_pandas_df(df)
        stage_dir = os.path.join(artifacts_dir, f"visualizations_{stage}")
        os.makedirs(stage_dir, exist_ok=True)
        
        # 1. Data distribution for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            axes = axes.flatten()
            
            for i, col in enumerate(numeric_cols[:4]):  # Top 4 numeric columns
                df[col].hist(bins=30, ax=axes[i], alpha=0.7)
                axes[i].set_title(f'{col} Distribution')
                axes[i].set_xlabel(col)
                axes[i].set_ylabel('Frequency')
            
            # Hide unused subplots
            for i in range(len(numeric_cols), 4):
                axes[i].set_visible(False)
            
            plt.suptitle(f'Data Distributions - {stage.title()}')
            plt.tight_layout()
            plt.savefig(os.path.join(stage_dir, f'distributions_{stage}.png'), dpi=300, bbox_inches='tight')
            plt.close()
        
        # 2. Correlation heatmap for numeric features
        if len(numeric_cols) > 1:
            plt.figure(figsize=(10, 8))
            correlation_matrix = df[numeric_cols].corr()
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                       square=True, linewidths=0.5)
            plt.title(f'Feature Correlation - {stage.title()}')
            plt.tight_layout()
            plt.savefig(os.path.join(stage_dir, f'correlation_{stage}.png'), dpi=300, bbox_inches='tight')
            plt.close()
        
        # Log visualizations to MLflow
        for viz_file in os.listdir(stage_dir):
            if viz_file.endswith('.png'):
                mlflow.log_artifact(os.path.join(stage_dir, viz_file), f"visualizations/{stage}")
        
        logger.info(f"✓ Visualizations created for {stage}")
        
    except Exception as e:
        logger.error(f"✗ Failed to create visualizations for {stage}: {str(e)}")


def log_stage_metrics(df: pd.DataFrame, stage: str, additional_metrics: Dict = None, spark: SparkSession = None):
    """Log key metrics for each processing stage."""
    try:
        if isinstance(df, pd.DataFrame):
            total_missing = int(df.isna().sum().sum())
            metrics = {
                f'{stage}_rows': len(df),
                f'{stage}_columns': len(df.columns),
                f'{stage}_missing_values': total_missing,
                f'{stage}_partitions': 1
            }
        else:
            missing_counts = []
            for col in df.columns:
                missing_counts.append(df.filter(F.col(col).isNull()).count())
            total_missing = sum(missing_counts)

            metrics = {
                f'{stage}_rows': df.count(),
                f'{stage}_columns': len(df.columns),
                f'{stage}_missing_values': total_missing,
                f'{stage}_partitions': df.rdd.getNumPartitions()
            }
        
        if additional_metrics:
            metrics.update({f'{stage}_{k}': v for k, v in additional_metrics.items()})
        
        mlflow.log_metrics(metrics)
        logger.info(f"✓ Metrics logged for {stage}: ({metrics[f'{stage}_rows']}, {metrics[f'{stage}_columns']})")
        
    except Exception as e:
        logger.error(f"✗ Failed to log metrics for {stage}: {str(e)}")


def log_csv_artifacts(csv_files: Dict[str, str], artifacts_dir: str):
    """Log final CSV files as MLflow artifacts with metadata."""
    try:
        csv_metadata = {
            'csv_files': {},
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Create CSV artifacts directory
        csv_artifacts_dir = os.path.join(artifacts_dir, 'final_csv_files')
        os.makedirs(csv_artifacts_dir, exist_ok=True)
        
        total_files_logged = 0
        
        for file_name, file_path in csv_files.items():
            if os.path.exists(file_path):
                try:
                    # Get file metadata
                    file_size = os.path.getsize(file_path) / (1024**2)  # MB
                    df = pd.read_csv(file_path)
                    
                    csv_metadata['csv_files'][file_name] = {
                        'file_path': file_path,
                        'file_size_mb': round(file_size, 2),
                        'shape': df.shape,
                        'columns': list(df.columns) if len(df.columns) <= 20 else f"{len(df.columns)} columns",
                        'sample_values': df.head(2).to_dict() if df.shape[0] > 0 else "No data"
                    }
                    
                    # Log the CSV file as artifact
                    mlflow.log_artifact(file_path, "final_datasets")
                    
                    # Log key metrics
                    mlflow.log_metrics({
                        f'final_{file_name}_rows': df.shape[0],
                        f'final_{file_name}_columns': df.shape[1],
                        f'final_{file_name}_size_mb': file_size
                    })
                    
                    total_files_logged += 1
                    logger.info(f"✓ Logged {file_name}: {df.shape} ({file_size:.2f}MB)")
                    
                except Exception as e:
                    logger.warning(f"⚠ Could not process {file_name}: {str(e)}")
                    csv_metadata['csv_files'][file_name] = {
                        'file_path': file_path,
                        'error': str(e)
                    }
            else:
                logger.warning(f"⚠ File not found: {file_path}")
                csv_metadata['csv_files'][file_name] = {
                    'file_path': file_path,
                    'status': 'not_found'
                }
        
        # Save CSV metadata
        metadata_path = os.path.join(csv_artifacts_dir, 'final_csv_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(csv_metadata, f, indent=2, default=str)
        
        # Log metadata as artifact
        mlflow.log_artifact(metadata_path, "final_datasets")
        
        # Log summary metrics
        mlflow.log_metrics({
            'total_csv_files_logged': total_files_logged,
            'csv_artifacts_created': len(csv_files)
        })
        
        logger.info(f"✓ CSV artifacts logged: {total_files_logged}/{len(csv_files)} files")
        
    except Exception as e:
        logger.error(f"✗ Failed to log CSV artifacts: {str(e)}")

def save_processed_data(
    X_train: DataFrame, 
    X_test: DataFrame, 
    Y_train: DataFrame, 
    Y_test: DataFrame,
    output_format: str = "both"
) -> Dict[str, str]:
    """
    Save processed data in specified format(s).
    
    Args:
        X_train, X_test, Y_train, Y_test: PySpark DataFrames
        output_format: "csv", "parquet", or "both"
        
    Returns:
        Dictionary of output paths
    """
    os.makedirs('artifacts/data', exist_ok=True)
    paths = {}
    
    if output_format in ["csv", "both"]:
        # Save as CSV
        logger.info("Saving data in CSV format...")
        
        # Convert to pandas and save
        X_train_pd = spark_to_pandas(X_train)
        X_test_pd = spark_to_pandas(X_test)
        Y_train_pd = spark_to_pandas(Y_train)
        Y_test_pd = spark_to_pandas(Y_test)
        
        # Define correct column order
        column_order = [
            'customerID', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 
            'tenure', 'PhoneService', 'MultipleLines', 'InternetService',
            'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
            'TechSupport', 'StreamingTV','StreamingMovies', 'Contract',
            'PaperlessBilling','PaymentMethod', 'MonthlyCharges', 'TotalCharges',
            'Churn'
        ]
        
        # Reorder columns if all expected columns exist
        expected_cols = [col for col in column_order if col in X_train_pd.columns]
        if len(expected_cols) == len(column_order):
            X_train_pd = X_train_pd[column_order]
            X_test_pd = X_test_pd[column_order]
            logger.info(f"✓ Columns reordered to match expected structure")
        else:
            logger.warning(f"⚠ Column mismatch. Expected: {column_order}, Found: {list(X_train_pd.columns)}")
        
        paths['X_train_csv'] = 'artifacts/data/X_train.csv'
        paths['X_test_csv'] = 'artifacts/data/X_test.csv'
        paths['Y_train_csv'] = 'artifacts/data/Y_train.csv'
        paths['Y_test_csv'] = 'artifacts/data/Y_test.csv'
        
        X_train_pd.to_csv(paths['X_train_csv'], index=False)
        X_test_pd.to_csv(paths['X_test_csv'], index=False)
        Y_train_pd.to_csv(paths['Y_train_csv'], index=False)
        Y_test_pd.to_csv(paths['Y_test_csv'], index=False)
        
        logger.info("✓ CSV files saved with correct column order")
    
    if output_format in ["parquet", "both"]:
        # Save as Parquet
        logger.info("Saving data in Parquet format...")
        
        paths['X_train_parquet'] = 'artifacts/data/X_train.parquet'
        paths['X_test_parquet'] = 'artifacts/data/X_test.parquet'
        paths['Y_train_parquet'] = 'artifacts/data/Y_train.parquet'
        paths['Y_test_parquet'] = 'artifacts/data/Y_test.parquet'
        
        save_dataframe(X_train, paths['X_train_parquet'], format='parquet')
        save_dataframe(X_test, paths['X_test_parquet'], format='parquet')
        save_dataframe(Y_train, paths['Y_train_parquet'], format='parquet')
        save_dataframe(Y_test, paths['Y_test_parquet'], format='parquet')
        
        logger.info("✓ Parquet files saved")
    
    return paths

def data_pipeline(
    data_path: str = 'data/raw/TelcoCustomerChurn.csv',
    target_column: str = 'Churn',
    test_size: float = 0.2,
    force_rebuild: bool = False,
    output_format: str = "both"
) -> Dict[str, np.ndarray]:
    """
    Execute comprehensive data processing pipeline with MLflow tracking.
    
    Args:
        data_path: Path to the raw data file
        target_column: Name of the target column
        test_size: Proportion of data to use for testing
        force_rebuild: Whether to force rebuild of existing artifacts
        
    Returns:
        Dictionary containing processed train/test splits
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"STARTING DATA PIPELINE")
    logger.info(f"{'='*80}")
    
    # Input validation
    if not os.path.exists(data_path):
        logger.error(f"✗ Data file not found: {data_path}")
        raise FileNotFoundError(f"Data file not found: {data_path}")
    
    if not 0 < test_size < 1:
        logger.error(f"✗ Invalid test_size: {test_size}")
        raise ValueError(f"Invalid test_size: {test_size}")
    
    spark = create_spark_session("ChurnPredictionDataPipeline")
    try:
        # Load configurations
        data_paths = get_data_paths()
        columns = get_columns()
        outlier_config = get_outlier_config()
        binning_config = get_binning_config()
        encoding_config = get_encoding_config()
        scaling_config = get_scaling_config()
        splitting_config = get_splitting_config()
        
        # Initialize MLflow tracking
        mlflow_tracker = MLflowTracker()
        run_tags = create_mlflow_run_tags('data_pipeline_pyspark', {
            'data_source': data_path,
            'force_rebuild': str(force_rebuild),
            'target_column': target_column,
            'output_format': output_format,
            'processing_engine': 'pyspark'
        })
        run = mlflow_tracker.start_run(run_name='data_pipeline_pyspark', tags=run_tags)
        
        # Create artifacts directory
        run_artifacts_dir = os.path.join('artifacts', 'mlflow_run_artifacts', run.info.run_id)
        os.makedirs(run_artifacts_dir, exist_ok=True)
        
        # Check for existing artifacts
        x_train_path = os.path.join('artifacts', 'data', 'X_train.csv')
        x_test_path = os.path.join('artifacts', 'data', 'X_test.csv')
        y_train_path = os.path.join('artifacts', 'data', 'Y_train.csv')
        y_test_path = os.path.join('artifacts', 'data', 'Y_test.csv')
        scaling_metadata_path = os.path.join('artifacts', 'scale', 'scaling_metadata.json')
        
        artifacts_exist = all(os.path.exists(p) for p in [x_train_path, x_test_path, y_train_path, y_test_path])
        scaler_artifacts_exist = os.path.exists(scaling_metadata_path)
        
        if artifacts_exist and scaler_artifacts_exist and not force_rebuild:
            logger.info("✓ Loading existing processed data artifacts")
            X_train = pd.read_csv(x_train_path)
            X_test = pd.read_csv(x_test_path)
            Y_train = pd.read_csv(y_train_path)
            Y_test = pd.read_csv(y_test_path)
            
            mlflow_tracker.log_data_pipeline_metrics({
                'total_samples': len(X_train) + len(X_test),
                'train_samples': len(X_train),
                'test_samples': len(X_test),
                'processing_engine': 'existing_artifacts'
            })

            # Log existing data metrics
            log_stage_metrics(X_train, 'existing_train')
            log_stage_metrics(X_test, 'existing_test')
            
            # Log existing datasets as MLflow dataset artifacts
            try:
                import mlflow.data
                
                # Create training dataset from existing data
                train_dataset = mlflow.data.from_pandas(
                    pd.concat([X_train, Y_train], axis=1),
                    source=f"existing_processed_from_{data_path}",
                    name="existing_churn_train_data",
                    targets=target_column
                )
                
                # Create test dataset from existing data
                test_dataset = mlflow.data.from_pandas(
                    pd.concat([X_test, Y_test], axis=1),
                    source=f"existing_processed_from_{data_path}",
                    name="existing_churn_test_data",
                    targets=target_column
                )
                
                # Log the datasets
                mlflow.log_input(train_dataset, context="training")
                mlflow.log_input(test_dataset, context="testing")
                
                logger.info("✓ Existing datasets logged as MLflow dataset artifacts")
                
            except Exception as e:
                logger.warning(f"⚠ Could not log existing dataset artifacts: {str(e)}")
            
            # Log existing CSV files as artifacts with metadata
            logger.info("Logging existing train/test CSV files as MLflow artifacts...")
            existing_csv_files = {
                'X_train': x_train_path,
                'X_test': x_test_path,
                'Y_train': y_train_path,
                'Y_test': y_test_path
            }
            log_csv_artifacts(existing_csv_files, run_artifacts_dir)
            
            mlflow_tracker.log_data_pipeline_metrics({
                'total_samples': len(X_train) + len(X_test),
                'train_samples': len(X_train),
                'test_samples': len(X_test)
            })
            mlflow_tracker.end_run()
            
            logger.info("✓ Data pipeline completed using existing artifacts")
            return {
                'X_train': X_train.values,
                'X_test': X_test.values,
                'Y_train': Y_train.values.ravel(),
                'Y_test': Y_test.values.ravel()
            }
        
        # Process data from scratch
        logger.info("Processing data from scratch...")
        
        # Data ingestion
        ingestor = DataIngestorCSV()
        df = ingestor.ingest(data_path)
        logger.info(f"✓ Raw data loaded: {get_dataframe_info(df)}")
        
        # Log raw data metrics and create visualizations
        log_stage_metrics(df, 'raw', spark=spark)
        create_data_visualizations(df, 'raw', run_artifacts_dir)
        
        # Log raw dataset as MLflow dataset artifact
        try:
            import mlflow.data
            from mlflow.data.pandas_dataset import PandasDataset
            
            # Create MLflow dataset from raw data
            raw_dataset = mlflow.data.from_pandas(
                spark_to_pandas(df), 
                source=data_path,
                name="raw_churn_data",
                targets=target_column
            )
            
            # Log the dataset
            mlflow.log_input(raw_dataset, context="raw_data")
            logger.info("✓ Raw dataset logged as MLflow dataset artifact")
            
        except Exception as e:
            logger.warning(f"⚠ Could not log raw dataset artifact: {str(e)}")
            # Fallback: log raw data file as regular artifact
            mlflow.log_artifact(data_path, "raw_data")
        
        # Validate target column
        if target_column not in df.columns:
            raise ValueError(f"Target column '{target_column}' not found")
        
        # Handle missing values
        logger.info("Handling missing values...")
        initial_count = df.count()

        drop_handler = DropMissingValuesStrategy(critical_columns=columns['drop_columns'])
        fill_handler = FillMissingValuesStrategy(critical_columns=columns['critical_features'], fill_value=0)
        # gender_handler = FillMissingValuesStrategy(
        #     relevant_column='Gender',
        #     is_custom_imputer=True,
        #     custom_imputer=GenderImputer()
        # )
        df = fill_handler.convert(df,critical_features={
        'TotalCharges':'numeric'
        })
        #df = drop_handler.handle(df)
        df = fill_handler.handle(df)
        #df = gender_handler.handle(df)
        
        rows_removed = initial_count - df.count()
        log_stage_metrics(df, 'missing_handled', {'rows_removed': rows_removed},spark)
        logger.info(f"✓ Missing values handled: {initial_count} rows → {df.count()} rows")
        
        # Outlier detection
        logger.info("Detecting and removing outliers...")
        initial_count = df.count()
        outlier_detector = OutlierDetector(strategy=IQROutlierDetection(spark=spark))
        df = outlier_detector.handle_outliers(df, columns['outlier_columns'])
        
        outliers_removed = initial_count - df.count()
        log_stage_metrics(df, 'outliers_removed', {'outliers_removed': outliers_removed})
        logger.info(f"✓ Outliers removed: {initial_count} rows → {df.count()} rows")
        
        # Feature binning
        logger.info("Applying feature binning...")
        binning = CustomBinningStrategy(binning_config['tenure_bins'], spark=spark)
        df = binning.bin_feature(df, 'tenure')
        df = binning.bin_feature(df, 'MonthlyCharges')
        df = binning.service_count(df, binning_config['service_columns'])
        df = binning.bundle_user(df, binning_config['bundle_columns'])

        
        # Log binning distribution
        if 'Charge_category' in df.columns:
            bin_dist = _get_value_counts(df, 'Charge_category')
            mlflow.log_metrics({f'Charge_category_{k}': v for k, v in bin_dist.items()})

        if 'Tenure_category' in df.columns:
            bin_dist = _get_value_counts(df, 'Tenure_category')
            mlflow.log_metrics({f'Tenure_category_{k}': v for k, v in bin_dist.items()})

        if 'Service_count' in df.columns:
            bin_dist = _get_value_counts(df, 'Service_count')
            mlflow.log_metrics({f'Service_count_{k}': v for k, v in bin_dist.items()})
        
        if 'Bundle_user' in df.columns:
            bin_dist = _get_value_counts(df, 'Bundle_user')
            mlflow.log_metrics({f'Bundle_user_{k}': v for k, v in bin_dist.items()})

        
        logger.info("✓ Feature binning completed")
        
        # Feature encoding
        logger.info("Applying feature encoding...")
        nominal_strategy = NominalEncodingStrategy(encoding_config['nominal_columns'], spark=spark)
        ordinal_strategy = OrdinalEncodingStrategy({'Churn': {'No': 0, 'Yes': 1}}, spark=spark)
        
        df = nominal_strategy.encode(df)
        df = ordinal_strategy.encode(df)
        
        logger.info(f"statistics after ordinal encoding {get_dataframe_info(df)}")
        log_stage_metrics(df, 'encoded')
        create_data_visualizations(df, 'encoded', run_artifacts_dir)
        logger.info("✓ Feature encoding completed")
        
        from src.data_spiltter import StratifiedTrainTestSplitStrategy
        # Data splitting
        logger.info("Splitting data...")
        splitting_strategy = StratifiedTrainTestSplitStrategy(test_size=splitting_config['test_size'], spark=spark)
        X_train, X_test, Y_train, Y_test = splitting_strategy.split_data(df, target_column)

        # Drop non-predictive identifier columns before scaling
        drop_columns = columns['drop_columns']
        existing_drop_columns = [col for col in drop_columns if col in X_train.columns]
        if existing_drop_columns:
            X_train = X_train.drop(*existing_drop_columns)
            X_test = X_test.drop(*existing_drop_columns)
            logger.info(f"✓ Dropped columns before scaling: {existing_drop_columns}")

        # Feature scaling after train/test split using train-fitted scalers
        logger.info("Applying feature transformation...")
        standard_strategy = StandardScalingStrategy(spark=spark)
        logger.info("Applying feature scaling...")
        X_train_scaled, X_test_scaled = standard_strategy.fit_scale_pair(
            X_train,
            X_test,
            scaling_config['columns_to_scale']
        )
        standard_strategy.save_scalers(scaling_config['columns_to_scale'], save_dir='artifacts/scale')
        logger.info("✓ Feature scaling completed")
        
        output_paths = save_processed_data(X_train_scaled, X_test_scaled, Y_train, Y_test, output_format)
        
        # # Create directories and save splits
        # os.makedirs('artifacts/data', exist_ok=True)
        # X_train.to_csv(x_train_path, index=False)
        # X_test.to_csv(x_test_path, index=False)
        # Y_train.to_csv(y_train_path, index=False)
        # Y_test.to_csv(y_test_path, index=False)
        
        logger.info("✓ Data splitting completed")
        logger.info(f"\nDataset shapes after splitting:")
        logger.info(f"  • X_train: {X_train_scaled.count()} rows, {len(X_train_scaled.columns)} columns")
        logger.info(f"  • X_test:  {X_test_scaled.count()} rows, {len(X_test_scaled.columns)} columns")
        logger.info(f"  • Y_train: {Y_train.count()} rows, 1 column")
        logger.info(f"  • Y_test:  {Y_test.count()} rows, 1 column")
        logger.info(f"  • Feature columns: {X_train_scaled.columns}")
        
        if hasattr(standard_strategy, 'scaler_models'):
            model_path = os.path.join('artifacts', 'encode', 'fitted_preprocessing_model')
            os.makedirs(model_path, exist_ok=True)
            
            # Save metadata about the preprocessing
            preprocessing_metadata = {
                'scaling_columns': scaling_config['columns_to_scale'],
                'encoding_columns': encoding_config['nominal_columns'],
                'ordinal_mappings': encoding_config['ordinal_mappings'],
                'binning_config': binning_config,
                'spark_version': spark.version
            }
            
            with open(os.path.join(model_path, 'metadata.json'), 'w') as f:
                json.dump(preprocessing_metadata, f, indent=2)
            
            logger.info(f"✓ Saved preprocessing metadata to {model_path}")
        
        # Final metrics and visualizations
        log_stage_metrics(X_train_scaled, 'final_train', spark=spark)
        log_stage_metrics(X_test_scaled, 'final_test',spark=spark)
        create_data_visualizations(pd.concat([spark_to_pandas(X_train_scaled), spark_to_pandas(X_test_scaled)], axis=0, ignore_index=True), 'final', run_artifacts_dir)
        
        # Log final processed datasets as MLflow dataset artifacts
        try:
            import mlflow.data
            
            # Create training dataset
            train_dataset = mlflow.data.from_pandas(
                pd.concat([spark_to_pandas(X_train_scaled), spark_to_pandas(Y_train)], axis=1),
                source=f"processed_from_{data_path}",
                name="processed_churn_train_data",
                targets=target_column
            )
            
            # Create test dataset  
            test_dataset = mlflow.data.from_pandas(
                pd.concat([spark_to_pandas(X_test_scaled), spark_to_pandas(Y_test)], axis=1),
                source=f"processed_from_{data_path}",
                name="processed_churn_test_data", 
                targets=target_column
            )
            
            # Log the datasets
            mlflow.log_input(train_dataset, context="training")
            mlflow.log_input(test_dataset, context="testing")
            
            logger.info("✓ Final processed datasets logged as MLflow dataset artifacts")
            
        except Exception as e:
            logger.warning(f"⚠ Could not log processed dataset artifacts: {str(e)}")
        
        # Log comprehensive pipeline metrics
        comprehensive_metrics = {
            'total_samples': X_train_scaled.count() + X_test_scaled.count(),
            'train_samples': X_train_scaled.count(),
            'test_samples': X_test_scaled.count(),
            'final_features': len(X_train_scaled.columns),
            'processing_engine': 'pyspark',
            'output_format': output_format
        }
        
        # Get class distribution
        train_dist = Y_train.groupBy(target_column).count().collect()
        test_dist = Y_test.groupBy(target_column).count().collect()
        
        for row in train_dist:
            comprehensive_metrics[f'train_class_{row[target_column]}'] = row['count']
        for row in test_dist:
            comprehensive_metrics[f'test_class_{row[target_column]}'] = row['count']
        
        mlflow_tracker.log_data_pipeline_metrics(comprehensive_metrics)
        
        # Log parameters
        mlflow.log_params({
            'final_feature_names': list(X_train_scaled.columns),
            'preprocessing_steps': ['missing_values', 'outlier_detection', 'feature_binning', 'feature_encoding', 'feature_scaling'],
            'data_pipeline_version': '3.0_pyspark'
        })
        
         # Log artifacts
        for path_key, path_value in output_paths.items():
            if os.path.exists(path_value):
                mlflow.log_artifact(path_value, "processed_datasets")
        
        mlflow_tracker.end_run()
        
        # Convert to numpy arrays for return
        X_train_np = spark_to_pandas(X_train_scaled).values
        X_test_np = spark_to_pandas(X_test_scaled).values
        Y_train_np = spark_to_pandas(Y_train).values.ravel()
        Y_test_np = spark_to_pandas(Y_test).values.ravel()
        
        logger.info(f"\n{'='*80}")
        logger.info(f"FINAL DATASET SHAPES")
        logger.info(f"{'='*80}")
        logger.info(f"✓ Final dataset shapes:")
        logger.info(f"  • X_train shape: {X_train_np.shape} (rows: {X_train_np.shape[0]}, features: {X_train_np.shape[1]})")
        logger.info(f"  • X_test shape:  {X_test_np.shape} (rows: {X_test_np.shape[0]}, features: {X_test_np.shape[1]})")
        logger.info(f"  • Y_train shape: {Y_train_np.shape} (rows: {Y_train_np.shape[0]})")
        logger.info(f"  • Y_test shape:  {Y_test_np.shape} (rows: {Y_test_np.shape[0]})")
        logger.info(f"  • Total samples: {X_train_np.shape[0] + X_test_np.shape[0]}")
        logger.info(f"  • Train/Test ratio: {X_train_np.shape[0]/(X_train_np.shape[0] + X_test_np.shape[0]):.1%} / {X_test_np.shape[0]/(X_train_np.shape[0] + X_test_np.shape[0]):.1%}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"{'='*80}")
        logger.info("✓ PySpark data pipeline completed successfully!")
        
        return {
            'X_train': X_train_np,
            'X_test': X_test_np,
            'Y_train': Y_train_np,
            'Y_test': Y_test_np
        }
    except Exception as e:
        logger.error(f"✗ Data pipeline failed: {str(e)}")
        if 'mlflow_tracker' in locals():
            mlflow_tracker.end_run()
        raise
    finally:
        stop_spark_session(spark)
if __name__ == "__main__":
    processed_data = data_pipeline(
        data_path =  'data/raw/TelcoCustomerChurn.csv',
        target_column =  'Churn',
        test_size =  0.2,
        force_rebuild = False,
        output_format="both"
        )
    logger.info(f"Pipeline completed. Train samples: {processed_data['X_train'].shape[0]}")