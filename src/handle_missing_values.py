import logging
import pandas as pd
from enum import Enum
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel
from abc import ABC, abstractmethod
import os
import yaml

import sys
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.config import get_columns

logging.basicConfig(level=logging.INFO, format=
    '%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

CONFIG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')

class MissingValueHandlingStrategy(ABC):
    @abstractmethod
    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class DropMissingValuesStrategy(MissingValueHandlingStrategy):
    def __init__(self, critical_columns = []):
        self.critical_columns = critical_columns
        logging.info(f"Dropping rows with missing values in critical columns: {self.critical_columns}")


    def handle(self, df):
        df_cleaned = df.dropna(subset=self.critical_columns)
        n_dropped = len(df) - len(df_cleaned)
        logging.info(f"{n_dropped} has been dropped")
        return df_cleaned
    
class FillMissingValuesStrategy(MissingValueHandlingStrategy):
    """
    NaN -> 0
    """
    def __init__(
            self,
            fill_value = None,
            critical_column = None,
            is_custom_imputer = False,
            custom_imputer = None
            ):
        
        self.fill_value = fill_value
        self.critical_column = critical_column
        self.is_custom_imputer = is_custom_imputer
        self.is_custom_imputer = custom_imputer


    def convert(self, df, critical_features={
        'TotalCharges': 'numeric'
    }):
        for col, dtype in critical_features.items():
            if dtype == 'numeric':
                df[col] = pd.to_numeric(df[col], errors="coerce")

        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)

        columns = config.get('columns', {})

        # ✅ Ensure 'TotalCharges' is added to numeric_columns if missing
        numeric_cols = columns.get('numeric_columns', [])
        if 'TotalCharges' not in numeric_cols:
            numeric_cols.append('TotalCharges')
            columns['numeric_columns'] = numeric_cols

        # ✅ Write back only updated 'columns' section, preserving everything else
        config['columns'] = columns

        with open(CONFIG_FILE, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        logging.info("Updated 'TotalCharges' added to numeric_columns if missing.")
        logging.info(df.info())

        return df

    def handle(self,df):
        if self.is_custom_imputer:
            return self.custom_imputer.impute(df)
        df[self.critical_column] = df[self.critical_column].fillna(self.fill_value)
        logging.info(f'Missing values filled in column {self.critical_column}.')
        logging.info(df.info())
        return df
    
