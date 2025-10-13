import logging
import pandas as pd
import os
import json
from enum import Enum
from typing import Dict, List
from abc import ABC, abstractmethod
logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FeatureEncodingStrategy(ABC):
    @abstractmethod
    def encode(self, df: pd.DataFrame) ->pd.DataFrame:
        pass


class VariableType(str, Enum):
    NOMINAL = 'nominal'
    ORDINAL = 'ordinal'

class NominalEncodingStrategy(FeatureEncodingStrategy):
    def __init__(self, nominal_columns):
        self.nominal_columns = nominal_columns
        self.encoder_dicts = {}
        os.makedirs('artifacts/encode', exist_ok=True)
        logger.info(f"NominalEncodingStrategy initialized for columns: {nominal_columns}")

    def encode(self,df):
        logger.info(f"\n{'='*60}")
        logger.info(f"NOMINAL ENCODING")
        logger.info(f"{'='*60}")
        logger.info(f"Starting nominal encoding for {len(self.nominal_columns)} columns")
        

        for column in self.nominal_columns:
            logger.info(f"\n--- Processing column: {column} ---")
            unique_values = df[column].unique()
            logger.info(f"  Unique values ({len(unique_values)}): {list(unique_values)[:5]}{'...' if len(unique_values) > 5 else ''}")

            # ✅ Convert keys to str to avoid JSON serialization error
            encoder_dict = {str(value): int(i) for i, value in enumerate(unique_values)}
            self.encoder_dicts[column] = encoder_dict

            encoder_path = os.path.join('artifacts/encode', f"{column}_encoder.json")
            with open(encoder_path, "w") as f:
                json.dump(encoder_dict, f)

            logger.info(f"  ✓ Saved encoder to {encoder_path}")

            # Handle missing and unmapped values
            missing_count = df[column].isnull().sum()
            if missing_count > 0:
                logger.warning(f"  ⚠ Column has {missing_count} missing values before encoding")

            # ✅ Map using the same stringified keys
            df[column] = df[column].astype(str).map(encoder_dict)

            unmapped_count = df[column].isnull().sum()
            if unmapped_count > missing_count:
                logger.warning(f"  ⚠ Column has {unmapped_count - missing_count} unmapped values after encoding")

            logger.info(f"  ✓ Successfully encoded column '{column}'")

            
        logger.info(f"\n{'='*60}")
        logger.info(f"✓ NOMINAL ENCODING COMPLETE")
        logger.info(f"{'='*60}\n")
        return df
    
    def get_encoder_dicts(self):
        return self.encoder_dicts
    
class OrdinalEncodingStratergy(FeatureEncodingStrategy):
    def __init__(self, ordinal_mappings):
        self.ordinal_mappings = ordinal_mappings
        self.encoder_dicts = {}
        logger.info(f"OrdinalEncodingStrategy initialized for columns: {list(ordinal_mappings.keys())}")

    def encode(self, df):
        logger.info(f"\n{'='*60}")
        logger.info(f"ORDINAL ENCODING")
        logger.info(f"{'='*60}")
        logger.info(f"Starting ordinal encoding for {len(self.ordinal_mappings)} columns")

        for column, mapping in self.ordinal_mappings.items():
            # Strip string values
            if df[column].dtype == object:
                df[column] = df[column].str.strip()

            logger.info(f"\n--- Processing column: {column} ---")
            initial_values = df[column].value_counts()
            logger.info(f"  Mapping: {mapping}")
            logger.info(f"  Before encoding: {initial_values.to_dict()}")

            # Missing values
            missing_count = df[column].isnull().sum()
            if missing_count > 0:
                logger.warning(f"  ⚠ Column has {missing_count} missing values before encoding")

            # Apply mapping
            df[column] = df[column].map(mapping)

            # Unmapped values
            unmapped_count = df[column].isnull().sum()
            if unmapped_count > missing_count:
                logger.warning(f"  ⚠ Column has {unmapped_count - missing_count} unmapped values after encoding")

            # Log encoded values
            encoded_values = df[column].value_counts()
            logger.info(f"  ✓ Encoded with {len(mapping)} categories")
            logger.info(f"  After encoding: {encoded_values.to_dict()}")

            # Save encoder dict for JSON
            encoder_dict = {str(k): int(v) for k, v in mapping.items()}
            self.encoder_dicts[column] = encoder_dict

            os.makedirs('artifacts/encode', exist_ok=True)
            encoder_path = os.path.join('artifacts/encode', f"{column}_encoder.json")
            with open(encoder_path, "w") as f:
                json.dump(encoder_dict, f)
            logger.info(f"  ✓ Saved encoder to {encoder_path}")

        logger.info(f"\n{'='*60}")
        logger.info(f"✓ ORDINAL ENCODING COMPLETE")
        logger.info(f"{'='*60}\n")
        return df

            

