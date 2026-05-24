import json
import logging
import os
import joblib, sys
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.spark_session import get_or_create_spark_session
from src.spark_utils import spark_to_pandas

from utils.config import get_binning_config, get_encoding_config, get_scaling_config
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

"""
Example Telco churn input:
{
    "customerID": "7590-VHVEG",
    "gender": "Female",
    "SeniorCitizen": 0,
    "Partner": "Yes",
    "Dependents": "No",
    "tenure": 1,
    "PhoneService": "No",
    "MultipleLines": "No phone service",
    "InternetService": "DSL",
    "OnlineSecurity": "No",
    "OnlineBackup": "Yes",
    "DeviceProtection": "No",
    "TechSupport": "No",
    "StreamingTV": "No",
    "StreamingMovies": "No",
    "Contract": "Month-to-month",
    "PaperlessBilling": "Yes",
    "PaymentMethod": "Electronic check",
    "MonthlyCharges": 29.85,
    "TotalCharges": 29.85,
    "Churn": "No"
}
"""
class ModelInference:
    """
    Enhanced model inference class with comprehensive logging and error handling.
    """
    
    def __init__(self, model_path: str, use_spark: bool = False, spark: Optional[SparkSession] = None):
        """
        Initialize the model inference system.
        
        Args:
            model_path: Path to the trained model file
            use_spark: Whether to use PySpark for preprocessing (default: False for single records)
            spark: Optional SparkSession instance
            
        Raises:
            ValueError: If model_path is invalid
            FileNotFoundError: If model file doesn't exist
        """
        logger.info(f"\n{'='*60}")
        logger.info("INITIALIZING MODEL INFERENCE")
        logger.info(f"{'='*60}")
        
        if not model_path or not isinstance(model_path, str):
            logger.error("✗ Invalid model path provided")
            raise ValueError("Invalid model path provided")
            
        self.model_path = model_path
        self.encoders = {}
        self.model = None
        self.use_spark = use_spark
        self.spark = spark if spark else (get_or_create_spark_session() if use_spark else None)
        self.scaler = None  # Initialize scaler
        self.scaler_params = None  # Initialize scaler parameters
        
        logger.info(f"Model Path: {model_path}")
        logger.info(f"Processing Engine: {'PySpark' if use_spark else 'Pandas'}")
        
        try:
            # Load model and configurations
            self.load_model()
            self.binning_config = get_binning_config()
            self.encoding_config = get_encoding_config()
            self.scaling_config = get_scaling_config()  # Load scaling config
            
            logger.info("✓ Model inference system initialized successfully")
            logger.info(f"{'='*60}\n")
            
        except Exception as e:
            logger.error(f"✗ Failed to initialize model inference: {str(e)}")
            raise

    def load_model(self) -> None:
        """
        Load the trained model from disk with validation.
        
        Raises:
            FileNotFoundError: If model file doesn't exist
            Exception: For any loading errors
        """
        logger.info("Loading trained model...")
        
        if not os.path.exists(self.model_path):
            logger.error(f"✗ Model file not found: {self.model_path}")
            raise FileNotFoundError(f"Model file not found: {self.model_path}")
        
        try:
            import time
            start_time = time.time()
            
            # Check if it's a PySpark model (directory) or scikit-learn model (file)
            if os.path.isdir(self.model_path):
                # PySpark model
                logger.info("Detected PySpark model directory")
                if not self.use_spark:
                    # Initialize Spark session for PySpark model
                    self.use_spark = True
                    self.spark = get_or_create_spark_session()
                
                from pyspark.ml import PipelineModel
                self.model = PipelineModel.load(self.model_path)
                self.model_type = 'pyspark'
                logger.info("✓ PySpark model loaded successfully")
                
            else:
                # Scikit-learn model
                logger.info("Detected scikit-learn model file")
                self.model = joblib.load(self.model_path)
                self.model_type = 'sklearn'
                file_size = os.path.getsize(self.model_path) / (1024**2)  # MB
                logger.info(f"  • File Size: {file_size:.2f} MB")
                logger.info("✓ Scikit-learn model loaded successfully")
            
            load_time = time.time() - start_time
            logger.info(f"  • Model Type: {type(self.model).__name__}")
            logger.info(f"  • Load Time: {load_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"✗ Failed to load model: {str(e)}")
            raise

    def load_encoders(self, encoders_dir: str) -> None:
        """
        Load feature encoders from directory with validation and logging.
        
        Args:
            encoders_dir: Directory containing encoder JSON files
            
        Raises:
            FileNotFoundError: If encoders directory doesn't exist
            Exception: For any loading errors
        """
        logger.info(f"\n{'='*50}")
        logger.info("LOADING FEATURE ENCODERS")
        logger.info(f"{'='*50}")
        
        if not os.path.exists(encoders_dir):
            logger.error(f"✗ Encoders directory not found: {encoders_dir}")
            raise FileNotFoundError(f"Encoders directory not found: {encoders_dir}")
        
        try:
            encoder_files = [f for f in os.listdir(encoders_dir) if f.endswith('_encoder.json')]
            
            if not encoder_files:
                logger.warning("⚠ No encoder files found in directory")
                return
            
            logger.info(f"Found {len(encoder_files)} encoder files")
            
            for file in encoder_files:
                feature_name = file.split('_encoder.json')[0]
                file_path = os.path.join(encoders_dir, file)
                
                with open(file_path, 'r') as f:
                    encoder_data = json.load(f)
                    self.encoders[feature_name] = encoder_data
                    
                logger.info(f"  ✓ Loaded encoder for '{feature_name}': {len(encoder_data)} mappings")
            
            logger.info(f"✓ All encoders loaded successfully")
            logger.info(f"{'='*50}\n")
            
        except Exception as e:
            logger.error(f"✗ Failed to load encoders: {str(e)}")
            raise
    
    def load_scalers(self, scalers_dir: str = 'artifacts/scale') -> None:
        """
        Load feature scalers from directory with validation and logging.
        
        Args:
            scalers_dir: Directory containing scaler artifacts
            
        Raises:
            FileNotFoundError: If scalers directory doesn't exist
            Exception: For any loading errors
        """
        logger.info(f"\n{'='*50}")
        logger.info("LOADING FEATURE SCALERS")
        logger.info(f"{'='*50}")
        logger.info(f"Scalers directory: {scalers_dir}")
        
        if not os.path.exists(scalers_dir):
            logger.warning(f"⚠ Scalers directory not found: {scalers_dir}")
            logger.info("  • Inference will proceed without feature scaling")
            logger.info(f"{'='*50}\n")
            return
        
        try:
            # Load scaling metadata
            metadata_path = os.path.join(scalers_dir, 'scaling_metadata.json')
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                    self.scaling_type = metadata.get('scaling_type', 'standard')
                    self.scaler_params = metadata.get('scaler_params', {})
                    self.columns_to_scale = metadata.get('columns_to_scale', [])
                    
                logger.info(f"✓ Loaded scaler metadata for {len(self.scaler_params)} columns")
                logger.info(f"  • Columns to scale: {self.columns_to_scale}")
                logger.info(f"  • Scaling type: {self.scaling_type}")
                
                # Log scaler parameters
                for col, params in self.scaler_params.items():
                    if self.scaling_type == 'standard':
                        logger.info(
                            f"  • {col}: mean={params['mean']:.4f}, std={params['std']:.4f}"
                        )
                    else:
                        logger.info(f"  • {col}: min={params['original_min']:.2f}, max={params['original_max']:.2f}")
            else:
                logger.warning(f"⚠ Scaling metadata not found: {metadata_path}")
                
            logger.info(f"✓ Scaler loading completed")
            logger.info(f"{'='*50}\n")
            
        except Exception as e:
            logger.error(f"✗ Failed to load scalers: {str(e)}")
            raise

    def preprocess_input(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Preprocess input data for model prediction with comprehensive logging.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Preprocessed DataFrame ready for prediction
            
        Raises:
            ValueError: If input data is invalid
            Exception: For any preprocessing errors
        """
        logger.info(f"\n{'='*50}")
        logger.info("PREPROCESSING INPUT DATA")
        logger.info(f"{'='*50}")
        
        if not data or not isinstance(data, dict):
            logger.error("✗ Input data must be a non-empty dictionary")
            raise ValueError("Input data must be a non-empty dictionary")
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame([data])
            logger.info(f"✓ Input data converted to DataFrame: {df.shape}")
            logger.info(f"  • Input features: {list(df.columns)}")
            
            # Apply encoders
            if self.encoders:
                logger.info("Applying feature encoders...")
                for col, encoder_data in self.encoders.items():
                    if col in df.columns:
                        original_value = df[col].iloc[0]
                        
                        # Check if it's one-hot encoding or label encoding
                        if isinstance(encoder_data, dict):
                            if encoder_data.get('encoding_type') == 'one_hot':
                                # Apply one-hot encoding
                                categories = encoder_data.get('categories', [])
                                logger.info(f"  ✓ One-hot encoding '{col}': {original_value} → {len(categories)} binary columns")
                                
                                # Create binary columns
                                for category in categories:
                                    new_col_name = f"{col}_{category}"
                                    df[new_col_name] = (df[col] == category).astype(int)
                                
                                # Drop original column
                                df = df.drop(columns=[col])
                            else:
                                # Label encoding
                                mapping = encoder_data.get('mapping', encoder_data)
                                df[col] = df[col].map(mapping)
                                encoded_value = df[col].iloc[0]
                                logger.info(f"  ✓ Label encoded '{col}': {original_value} → {encoded_value}")
                        else:
                            # Old format - assume label encoding
                            df[col] = df[col].map(encoder_data)
                            encoded_value = df[col].iloc[0]
                            logger.info(f"  ✓ Encoded '{col}': {original_value} → {encoded_value}")
                    else:
                        logger.warning(f"  ⚠ Column '{col}' not found in input data")
            else:
                logger.info("No encoders available - skipping encoding step")

            # Apply feature scaling
            if self.scaler_params and self.columns_to_scale:
                logger.info("Applying feature scaling...")
                for col in self.columns_to_scale:
                    if col in df.columns:
                        if col in self.scaler_params:
                            params = self.scaler_params[col]
                            original_value = df[col].iloc[0]

                            if getattr(self, 'scaling_type', 'standard') == 'standard':
                                std_val = params.get('std', 0)
                                mean_val = params.get('mean', 0)
                                if std_val:
                                    scaled_value = (original_value - mean_val) / std_val
                                    df[col] = scaled_value
                                    logger.info(f"  ✓ Transformed '{col}': {original_value} → {scaled_value:.4f}")
                                else:
                                    logger.warning(f"  ⚠ Invalid scaling std for '{col}': std={std_val}")
                            else:
                                # Apply min-max scaling: (x - min) / (max - min)
                                min_val = params['original_min']
                                max_val = params['original_max']

                                if max_val > min_val:
                                    scaled_value = (original_value - min_val) / (max_val - min_val)
                                    df[col] = scaled_value
                                    logger.info(f"  ✓ Transformed '{col}': {original_value} → {scaled_value:.4f}")
                                else:
                                    logger.warning(f"  ⚠ Invalid scaling range for '{col}': min={min_val}, max={max_val}")
                        else:
                            logger.warning(f"  ⚠ No scaling parameters found for '{col}'")
                    else:
                        logger.warning(f"  ⚠ Column '{col}' not found for scaling")
            else:
                logger.info("No scalers available - skipping scaling step")

            # Drop identifiers and target columns that are not model features
            drop_columns = ['customerID', 'Churn', 'event_id', 'event_timestamp', 'true_churn_label']
            existing_drop_columns = [col for col in drop_columns if col in df.columns]
            
            if existing_drop_columns:
                df = df.drop(columns=existing_drop_columns)
                logger.info(f"  ✓ Dropped columns: {existing_drop_columns}")
            
            # Reorder columns to match training data
            expected_columns = None

            # If using a PySpark model, try to infer the exact assembler input columns
            try:
                if getattr(self, 'model_type', None) == 'pyspark' and hasattr(self, 'model'):
                    for stage in getattr(self.model, 'stages', []) or []:
                        if hasattr(stage, 'getInputCols'):
                            expected_columns = list(stage.getInputCols())
                            logger.info(f"  • Inferred assembler input columns from model: {len(expected_columns)} columns")
                            break
            except Exception as _:
                expected_columns = None

            # Fallback: use whatever columns are present after preprocessing
            if expected_columns is None:
                expected_columns = list(df.columns)

            # Check if all expected columns are present
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"  ⚠ Missing columns required by model: {missing_columns}")
                # Create missing columns with default zeros to allow model to run
                for col in missing_columns:
                    df[col] = 0

            # Reorder columns to match training order
            available_columns = [col for col in expected_columns if col in df.columns]
            df = df[available_columns]
            
            logger.info(f"✓ Preprocessing completed - Final shape: {df.shape}")
            logger.info(f"  • Final features (reordered): {list(df.columns)}")
            logger.info(f"{'='*50}\n")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Preprocessing failed: {str(e)}")
            raise
    
    def predict(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        Make prediction on input data with comprehensive logging.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Dictionary containing prediction status and confidence
            
        Raises:
            ValueError: If input data is invalid
            Exception: For any prediction errors
        """
        logger.info(f"\n{'='*60}")
        logger.info("MAKING PREDICTION")
        logger.info(f"{'='*60}")
        
        if not data:
            logger.error("✗ Input data cannot be empty")
            raise ValueError("Input data cannot be empty")
        
        if self.model is None:
            logger.error("✗ Model not loaded")
            raise ValueError("Model not loaded")
        
        try:
            # Preprocess input data
            processed_data = self.preprocess_input(data)
            
            # Make prediction based on model type
            logger.info("Generating predictions...")
            
            if hasattr(self, 'model_type') and self.model_type == 'pyspark':
                # PySpark model prediction
                spark_df = self.spark.createDataFrame(processed_data)
                predictions = self.model.transform(spark_df)
                
                # Get prediction and probability
                prediction_row = predictions.select("prediction", "probability").collect()[0]
                prediction = int(prediction_row.prediction)
                
                # Extract probability for positive class (index 1)
                probability_vector = prediction_row.probability
                probability = float(probability_vector[1])
                
            else:
                # Scikit-learn model prediction
                y_pred = self.model.predict(processed_data)
                y_proba = self.model.predict_proba(processed_data)[:, 1]
                
                prediction = int(y_pred[0])
                probability = float(y_proba[0])
            
            status = 'Churn' if prediction == 1 else 'Retain'
            confidence = round(probability * 100, 2)
            
            result = {
                "Status": status,
                "Confidence": f"{confidence}%"
            }
            
            logger.info("✓ Prediction completed:")
            logger.info(f"  • Raw Prediction: {prediction}")
            logger.info(f"  • Raw Probability: {probability:.4f}")
            logger.info(f"  • Final Status: {status}")
            logger.info(f"  • Confidence: {confidence}%")
            logger.info(f"{'='*60}\n")
            
            return result
            
        except Exception as e:
            logger.error(f"✗ Prediction failed: {str(e)}")
            raise