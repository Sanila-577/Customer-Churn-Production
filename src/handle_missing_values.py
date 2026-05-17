import logging
import pandas as pd
from enum import Enum
from typing import Optional
from dotenv import load_dotenv
from abc import ABC, abstractmethod
import os
import yaml
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

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
            critical_columns = None,
            is_custom_imputer = False,
            custom_imputer = None
            ):
        
        self.fill_value = fill_value
        if critical_columns is None:
            critical_columns = critical_column
        if critical_columns is None:
            self.critical_columns = []
        elif isinstance(critical_columns, (list, tuple, set)):
            self.critical_columns = list(critical_columns)
        else:
            self.critical_columns = [critical_columns]
        self.is_custom_imputer = is_custom_imputer
        self.custom_imputer = custom_imputer

    def _is_spark_df(self, df):
        return isinstance(df, SparkDataFrame)


    def convert(self, df, critical_features={
        'TotalCharges': 'numeric'
    }):
        if self._is_spark_df(df):
            for col, dtype in critical_features.items():
                if dtype == 'numeric' and col in df.columns:
                    df = df.withColumn(
                        col,
                        F.when(F.trim(F.col(col).cast('string')) == '', None)
                        .otherwise(F.col(col).cast('double'))
                    )
            logging.info("Converted critical features to Spark numeric columns where needed.")
            df.printSchema()
            return df

        for col, dtype in critical_features.items():
            if dtype == 'numeric' and col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logging.info("Converted critical features to pandas numeric columns where needed.")
        logging.info(df.info())
        return df

    def handle(self,df):
        if self.is_custom_imputer:
            return self.custom_imputer.impute(df)

        if self._is_spark_df(df):
            if not self.critical_columns:
                return df
            fill_map = {col: self.fill_value for col in self.critical_columns if col in df.columns}
            if fill_map:
                df = df.fillna(fill_map)
            logging.info(f"Missing values filled in columns {list(fill_map.keys())}.")
            df.printSchema()
            return df

        if not self.critical_columns:
            return df

        existing_columns = [col for col in self.critical_columns if col in df.columns]
        if len(existing_columns) == 1:
            df[existing_columns[0]] = df[existing_columns[0]].fillna(self.fill_value)
        elif existing_columns:
            df[existing_columns] = df[existing_columns].fillna(self.fill_value)
        logging.info(f'Missing values filled in columns {existing_columns}.')
        logging.info(df.info())
        return df
    
