"""
Feature scaling strategies for PySpark DataFrames.
Supports MinMaxScaler and StandardScaler transformations with persistence.
"""

import logging
import os
import json
from enum import Enum
from typing import List, Optional, Dict
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import MinMaxScaler, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
from src.spark_session import get_or_create_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FeatureScalingStrategy(ABC):
    """Abstract base class for feature scaling strategies."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize with SparkSession."""
        self.spark = spark or get_or_create_spark_session()
        self.fitted_model = None
    
    @abstractmethod
    def scale(self, df: DataFrame, columns_to_scale: List[str]) -> DataFrame:
        """
        Scale specified columns in the DataFrame.
        
        Args:
            df: PySpark DataFrame
            columns_to_scale: List of column names to scale
            
        Returns:
            DataFrame with scaled features
        """
        pass


class ScalingType(str, Enum):
    """Enumeration of scaling types."""
    MINMAX = 'minmax'
    STANDARD = 'standard'


class MinMaxScalingStrategy(FeatureScalingStrategy):
    """Min-Max scaling strategy to scale features to [0, 1] range."""
    
    def __init__(self, output_col_suffix: str = "_scaled", spark: Optional[SparkSession] = None):
        """
        Initialize Min-Max scaling strategy.
        
        Args:
            output_col_suffix: Suffix to add to scaled column names
            spark: Optional SparkSession
        """
        super().__init__(spark)
        self.output_col_suffix = output_col_suffix
        self.scaler_models = {}
        logger.info("MinMaxScalingStrategy initialized (PySpark)")
    
    def scale(self, df: DataFrame, columns_to_scale: List[str]) -> DataFrame:
        """
        Apply Min-Max scaling to specified columns.
        
        Args:
            df: PySpark DataFrame
            columns_to_scale: List of column names to scale
            
        Returns:
            DataFrame with scaled columns
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"FEATURE SCALING - MIN-MAX (PySpark)")
        logger.info(f"{'='*60}")
        logger.info(f'Starting Min-Max scaling for {len(columns_to_scale)} columns: {columns_to_scale}')
        
        # Log statistics before scaling
        logger.info(f"\nStatistics BEFORE scaling:")
        for col in columns_to_scale:
            stats = df.select(
                F.min(col).alias('min'),
                F.max(col).alias('max'),
                F.mean(col).alias('mean'),
                F.stddev(col).alias('std')
            ).collect()[0]
            
            logger.info(f"  {col}: Min={stats['min']:.2f}, Max={stats['max']:.2f}, "
                       f"Mean={stats['mean']:.2f}, Std={stats['std']:.2f}")
        
        df_scaled = df
        
        # Scale each column individually to maintain column structure
        for col in columns_to_scale:
            # Create a vector column for this feature
            vector_col = f"{col}_vec"
            assembler = VectorAssembler(inputCols=[col], outputCol=vector_col)
            
            # Create MinMaxScaler
            scaled_vec_col = f"{col}_scaled_vec"
            scaler = MinMaxScaler(inputCol=vector_col, outputCol=scaled_vec_col)
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, scaler])
            
            # Fit and transform
            pipeline_model = pipeline.fit(df_scaled)
            df_scaled = pipeline_model.transform(df_scaled)
            
            # Extract scalar value from vector
            get_value_udf = F.udf(lambda x: float(x[0]) if x is not None else None, "double")
            df_scaled = df_scaled.withColumn(
                f"{col}{self.output_col_suffix}",
                get_value_udf(F.col(scaled_vec_col))
            )
            
            # Drop intermediate columns and original column
            df_scaled = df_scaled.drop(vector_col, scaled_vec_col, col)
            
            # Rename scaled column to original name
            df_scaled = df_scaled.withColumnRenamed(f"{col}{self.output_col_suffix}", col)
            
            # Store the scaler model
            self.scaler_models[col] = pipeline_model.stages[1]  # MinMaxScaler model
            
            # Log scaler parameters
            scaler_model = self.scaler_models[col]
            logger.info(f"\nScaler Parameters for {col}:")
            logger.info(f"  Original Min: {scaler_model.originalMin}")
            logger.info(f"  Original Max: {scaler_model.originalMax}")
        
        # Log statistics after scaling
        logger.info(f"\nStatistics AFTER scaling:")
        for col in columns_to_scale:
            stats = df_scaled.select(
                F.min(col).alias('min'),
                F.max(col).alias('max'),
                F.mean(col).alias('mean'),
                F.stddev(col).alias('std')
            ).collect()[0]
            
            logger.info(f"  {col}: Min={stats['min']:.4f}, Max={stats['max']:.4f}, "
                       f"Mean={stats['mean']:.4f}, Std={stats['std']:.4f}")
            
            # Check if scaling worked correctly
            if abs(stats['min']) > 0.001 or abs(stats['max'] - 1.0) > 0.001:
                logger.warning(f"  ⚠ Column '{col}' may not be properly scaled to [0,1] range")
        
        logger.info(f"\n{'='*60}")
        logger.info(f'✓ MIN-MAX SCALING COMPLETE - {len(columns_to_scale)} columns processed')
        logger.info(f"{'='*60}\n")
        
        return df_scaled
    
    def get_scaler_models(self) -> Dict[str, MinMaxScaler]:
        """Get the fitted scaler models for each column."""
        return self.scaler_models
    
    def save_scalers(self, columns_to_scale: List[str], save_dir: str = 'artifacts/scale') -> bool:
        """
        Save the fitted scaler models and metadata for inference.
        
        Args:
            columns_to_scale: List of columns that were scaled
            save_dir: Directory to save scaler artifacts
            
        Returns:
            bool: True if successfully saved
        """
        try:
            os.makedirs(save_dir, exist_ok=True)
            
            # Save metadata
            metadata = {
                'columns_to_scale': columns_to_scale,
                'n_features': len(columns_to_scale),
                'scaling_type': 'minmax',
                'framework': 'pyspark',
                'scaler_params': {}
            }
            
            # Extract and save parameters for each scaler
            for col, scaler_model in self.scaler_models.items():
                if hasattr(scaler_model, 'originalMin') and hasattr(scaler_model, 'originalMax'):
                    # Convert Spark vectors to lists
                    original_min = float(scaler_model.originalMin[0])
                    original_max = float(scaler_model.originalMax[0])
                    
                    metadata['scaler_params'][col] = {
                        'original_min': original_min,
                        'original_max': original_max,
                        'scaled_min': 0.0,
                        'scaled_max': 1.0
                    }
                    
                    logger.info(f"  • Saved scaler params for {col}: min={original_min:.2f}, max={original_max:.2f}")
            
            # Save metadata
            metadata_path = os.path.join(save_dir, 'scaling_metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Save the actual scaler models for PySpark
            for col in columns_to_scale:
                if col in self.scaler_models:
                    model_path = os.path.join(save_dir, f'{col}_scaler_model')
                    # Note: PySpark models need special handling - save parameters instead
                    # The actual model loading will recreate from parameters
                    
            logger.info(f"✓ Scaler artifacts saved to: {save_dir}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to save scaler artifacts: {str(e)}")
            return False
    
    def load_scalers(self, save_dir: str = 'artifacts/scale') -> bool:
        """
        Load the fitted scaler parameters for inference.
        
        Args:
            save_dir: Directory containing scaler artifacts
            
        Returns:
            bool: True if successfully loaded
        """
        try:
            metadata_path = os.path.join(save_dir, 'scaling_metadata.json')
            
            if not os.path.exists(metadata_path):
                logger.warning(f"⚠ Scaler metadata not found at: {metadata_path}")
                return False
            
            # Load metadata
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            self.loaded_params = metadata['scaler_params']
            self.columns_to_scale = metadata['columns_to_scale']
            
            logger.info(f"✓ Loaded scaler parameters for {len(self.loaded_params)} columns")
            logger.info(f"  • Columns: {list(self.loaded_params.keys())}")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to load scaler artifacts: {str(e)}")
            return False
    
    def transform(self, df: DataFrame, columns_to_scale: List[str]) -> DataFrame:
        """
        Apply loaded scaler parameters to transform data (no fitting).
        
        Args:
            df: PySpark DataFrame to transform
            columns_to_scale: List of columns to scale
            
        Returns:
            DataFrame with scaled columns
        """
        if not hasattr(self, 'loaded_params'):
            raise ValueError("Scaler not loaded. Call load_scalers() first.")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"APPLYING LOADED SCALERS (PySpark)")
        logger.info(f"{'='*60}")
        
        df_scaled = df
        
        for col in columns_to_scale:
            if col not in self.loaded_params:
                logger.warning(f"⚠ Column '{col}' not found in loaded scaler params, skipping")
                continue
            
            params = self.loaded_params[col]
            original_min = params['original_min']
            original_max = params['original_max']
            
            # Apply min-max scaling formula: (x - min) / (max - min)
            df_scaled = df_scaled.withColumn(
                col,
                (F.col(col) - original_min) / (original_max - original_min)
            )
            
            # Log transformation
            logger.info(f"  ✓ Transformed '{col}' using loaded params: min={original_min:.2f}, max={original_max:.2f}")
        
        logger.info(f"✓ Scaling transformation complete")
        logger.info(f"{'='*60}\n")
        
        return df_scaled


class StandardScalingStrategy(FeatureScalingStrategy):
    """Standard scaling strategy to scale features to zero mean and unit variance."""
    
    def __init__(self, with_mean: bool = True, with_std: bool = True, 
                 output_col_suffix: str = "_scaled", spark: Optional[SparkSession] = None):
        """
        Initialize Standard scaling strategy.
        
        Args:
            with_mean: Whether to center the data before scaling
            with_std: Whether to scale the data to unit variance
            output_col_suffix: Suffix to add to scaled column names
            spark: Optional SparkSession
        """
        super().__init__(spark)
        self.with_mean = with_mean
        self.with_std = with_std
        self.output_col_suffix = output_col_suffix
        self.scaler_models = {}
        logger.info(f"StandardScalingStrategy initialized (PySpark) - "
                   f"with_mean={with_mean}, with_std={with_std}")
    
    def scale(self, df: DataFrame, columns_to_scale: List[str]) -> DataFrame:
        """
        Apply Standard scaling to specified columns.
        
        Args:
            df: PySpark DataFrame
            columns_to_scale: List of column names to scale
            
        Returns:
            DataFrame with scaled columns
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"FEATURE SCALING - STANDARD (PySpark)")
        logger.info(f"{'='*60}")
        logger.info(f'Starting Standard scaling for {len(columns_to_scale)} columns')
        
        df_scaled = df
        
        # Scale each column individually
        for col in columns_to_scale:
            # Create a vector column for this feature
            vector_col = f"{col}_vec"
            assembler = VectorAssembler(inputCols=[col], outputCol=vector_col)
            
            # Create StandardScaler
            scaled_vec_col = f"{col}_scaled_vec"
            scaler = StandardScaler(
                inputCol=vector_col, 
                outputCol=scaled_vec_col,
                withMean=self.with_mean,
                withStd=self.with_std
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[assembler, scaler])
            
            # Fit and transform
            pipeline_model = pipeline.fit(df_scaled)
            df_scaled = pipeline_model.transform(df_scaled)
            
            # Extract scalar value from vector
            get_value_udf = F.udf(lambda x: float(x[0]) if x is not None else None, "double")
            df_scaled = df_scaled.withColumn(
                f"{col}{self.output_col_suffix}",
                get_value_udf(F.col(scaled_vec_col))
            )
            
            # Drop intermediate columns and original column
            df_scaled = df_scaled.drop(vector_col, scaled_vec_col, col)
            
            # Rename scaled column to original name
            df_scaled = df_scaled.withColumnRenamed(f"{col}{self.output_col_suffix}", col)
            
            # Store the scaler model
            self.scaler_models[col] = pipeline_model.stages[1]  # StandardScaler model
        
        logger.info(f"✓ STANDARD SCALING COMPLETE - {len(columns_to_scale)} columns processed")
        logger.info(f"{'='*60}\n")
        
        return df_scaled

    def fit_scale_pair(
        self,
        train_df: DataFrame,
        test_df: DataFrame,
        columns_to_scale: List[str],
    ) -> tuple[DataFrame, DataFrame]:
        """Fit scalers on the training frame and apply them to both train and test."""
        logger.info(f"\n{'='*60}")
        logger.info(f"FEATURE SCALING - STANDARD TRAIN/TEST (PySpark)")
        logger.info(f"{'='*60}")
        logger.info(f"Starting Standard scaling for {len(columns_to_scale)} columns")

        train_scaled = train_df
        test_scaled = test_df

        for col in columns_to_scale:
            vector_col = f"{col}_vec"
            scaled_vec_col = f"{col}_scaled_vec"

            assembler = VectorAssembler(inputCols=[col], outputCol=vector_col)
            scaler = StandardScaler(
                inputCol=vector_col,
                outputCol=scaled_vec_col,
                withMean=self.with_mean,
                withStd=self.with_std,
            )

            pipeline = Pipeline(stages=[assembler, scaler])
            pipeline_model = pipeline.fit(train_scaled)
            self.scaler_models[col] = pipeline_model.stages[1]

            train_scaled = self._apply_scale_model(train_scaled, col, pipeline_model, vector_col, scaled_vec_col)
            test_scaled = self._apply_scale_model(test_scaled, col, pipeline_model, vector_col, scaled_vec_col)

        logger.info(f"✓ STANDARD TRAIN/TEST SCALING COMPLETE - {len(columns_to_scale)} columns processed")
        logger.info(f"{'='*60}\n")
        return train_scaled, test_scaled

    def save_scalers(self, columns_to_scale: List[str], save_dir: str = 'artifacts/scale') -> bool:
        """Save fitted standard scaler metadata for inference."""
        try:
            os.makedirs(save_dir, exist_ok=True)

            metadata = {
                'columns_to_scale': columns_to_scale,
                'n_features': len(columns_to_scale),
                'scaling_type': 'standard',
                'framework': 'pyspark',
                'scaler_params': {}
            }

            for col, scaler_model in self.scaler_models.items():
                if hasattr(scaler_model, 'mean') and hasattr(scaler_model, 'std'):
                    metadata['scaler_params'][col] = {
                        'mean': float(scaler_model.mean[0]),
                        'std': float(scaler_model.std[0]),
                        'with_mean': self.with_mean,
                        'with_std': self.with_std,
                    }
                    logger.info(
                        f"  • Saved scaler params for {col}: mean={float(scaler_model.mean[0]):.4f}, std={float(scaler_model.std[0]):.4f}"
                    )

            metadata_path = os.path.join(save_dir, 'scaling_metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"✓ Scaler artifacts saved to: {save_dir}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to save scaler artifacts: {str(e)}")
            return False

    def _apply_scale_model(
        self,
        df: DataFrame,
        col: str,
        pipeline_model,
        vector_col: str,
        scaled_vec_col: str,
    ) -> DataFrame:
        """Apply a fitted scaling pipeline to a frame and replace the original column."""
        df_scaled = pipeline_model.transform(df)

        get_value_udf = F.udf(lambda x: float(x[0]) if x is not None else None, "double")
        df_scaled = df_scaled.withColumn(
            f"{col}{self.output_col_suffix}",
            get_value_udf(F.col(scaled_vec_col))
        )
        df_scaled = df_scaled.drop(vector_col, scaled_vec_col, col)
        df_scaled = df_scaled.withColumnRenamed(f"{col}{self.output_col_suffix}", col)
        return df_scaled


class VectorScalingStrategy(FeatureScalingStrategy):
    """
    Scaling strategy that works with vector columns.
    More efficient when scaling many features together.
    """
    
    def __init__(self, scaling_type: ScalingType = ScalingType.MINMAX, 
                 spark: Optional[SparkSession] = None):
        """
        Initialize vector scaling strategy.
        
        Args:
            scaling_type: Type of scaling to apply
            spark: Optional SparkSession
        """
        super().__init__(spark)
        self.scaling_type = scaling_type
        logger.info(f"VectorScalingStrategy initialized with {scaling_type} scaling")
    
    def scale(self, df: DataFrame, columns_to_scale: List[str]) -> DataFrame:
        """
        Apply scaling to multiple columns as a vector.
        
        Args:
            df: PySpark DataFrame
            columns_to_scale: List of column names to scale
            
        Returns:
            DataFrame with scaled features in a vector column
        """
        # Assemble features into a vector
        assembler = VectorAssembler(inputCols=columns_to_scale, outputCol="features")
        
        # Choose scaler based on type
        if self.scaling_type == ScalingType.MINMAX:
            scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
        else:
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler])
        
        # Fit and transform
        pipeline_model = pipeline.fit(df)
        df_scaled = pipeline_model.transform(df)
        
        self.fitted_model = pipeline_model
        
        logger.info(f"✓ Vector scaling complete for {len(columns_to_scale)} features")
        
        return df_scaled