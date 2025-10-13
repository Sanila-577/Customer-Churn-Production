import joblib, os
from typing import Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod
from sklearn.linear_model import LogisticRegression

class BaseModelBuilder(ABC):
    def __init__(
                self,
                model_name:str,
                **kwargs
                ):
        self.model_name = model_name
        self.model = None 
        self.model_params = kwargs
    
    @abstractmethod
    def build_model(self):
        pass 

    def save_model(self, filepath):
        if self.model is None:
            raise ValueError("No model to save. Build the model first.")
        
        joblib.dump(self.model, filepath)
        
    def load_model(self, filepath):
        if not os.path.exists(filepath):
            raise ValueError("Can't load. File not found.")
        
        self.model = joblib.load(filepath)

class LogisticRegressionModelBuilder(BaseModelBuilder):
    def __init__(self, **kwargs):
        default_params = {
                        'max_iter': 100,
                        'class_weight': 'balanced'
                        }
        default_params.update(kwargs)
        super().__init__('LogisticRegression', **default_params)

    def build_model(self):
        self.model = LogisticRegression(**self.model_params)
        return self.model