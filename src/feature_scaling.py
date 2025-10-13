import logging
import pandas as pd
import numpy as np
from enum import Enum
from typing import List
from abc import ABC, abstractmethod
from sklearn.preprocessing import StandardScaler
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FeatureScalingStrategy(ABC):

    @abstractmethod
    def scale(self, df: pd.DataFrame, columns_to_scale: List[str]) -> pd.DataFrame:
        pass


class ScalingType(str, Enum):
    MINMAX = 'minmax'
    STANDARD = 'standard'

class StandardScalingStratergy(FeatureScalingStrategy):
    def __init__(self):
        self.scaler = StandardScaler()
        self.fitted = False
        logger.info("Standard Scaling Strategy initialized")

    def transform(self,df,columns_to_transform):
        df[columns_to_transform[0]] = np.log1p(df[columns_to_transform[0]])
        return df

    def scale(self, df, columns_to_scale):
        logger.info(f"\n{'='*60}")
        logger.info(f"FEATURE SCALING - MIN-MAX")
        logger.info(f"{'='*60}")
        logger.info(f'Starting Min-Max scaling for {len(columns_to_scale)} columns: {columns_to_scale}')
        
        # Log statistics before scaling
        logger.info(f"\nStatistics BEFORE scaling:")
        df.info()
        print(columns_to_scale)
        for col in columns_to_scale:
            col_stats = {
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': df[col].mean(),
                'std': df[col].std()
            }
            logger.info(f"  {col}: Min={col_stats['min']:.2f}, Max={col_stats['max']:.2f}, Mean={col_stats['mean']:.2f}, Std={col_stats['std']:.2f}")
        
        # Apply scaling
        df[columns_to_scale] = self.scaler.fit_transform(df[columns_to_scale])
        self.fitted = True
        
        # # Log min/max values learned by scaler
        # logger.info(f"\nScaler Parameters:")
        # for i, col in enumerate(columns_to_scale):
        #     logger.info(f"  {col}: Data min={self.scaler.data_min_[i]:.2f}, Data max={self.scaler.data_max_[i]:.2f}")
        
        # Log statistics after scaling
        logger.info(f"\nStatistics AFTER scaling:")
        for col in columns_to_scale:
            col_stats = {
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': df[col].mean(),
                'std': df[col].std()
            }
            logger.info(f"  {col}: Min={col_stats['min']:.4f}, Max={col_stats['max']:.4f}, Mean={col_stats['mean']:.4f}, Std={col_stats['std']:.4f}")
            
            # Check if scaling worked correctly
            if abs(col_stats['min']) > 0.001 or abs(col_stats['max'] - 1.0) > 0.001:
                logger.warning(f"  ⚠ Column '{col}' may not be properly scaled to [0,1] range")
        
        logger.info(f"\n{'='*60}")
        logger.info(f'✓ MIN-MAX SCALING COMPLETE - {len(columns_to_scale)} columns processed')
        logger.info(f"{'='*60}\n")
        return df 
    
    def get_scaler(self):
        return self.scaler

