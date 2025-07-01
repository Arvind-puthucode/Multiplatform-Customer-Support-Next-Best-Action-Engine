
import os
from typing import Dict
from .engines.base_engine import BaseDataEngine
from .engines.pandas_engine import PandasDataEngine
from .engines.spark_engine import SparkDataEngine

class DataEngineFactory:
    @staticmethod
    def get_engine(config: Dict = None) -> BaseDataEngine:
        data_engine_type = os.environ.get("DATA_ENGINE", "pandas").lower()
        if data_engine_type == "pandas":
            return PandasDataEngine()
        elif data_engine_type == "spark":
            return SparkDataEngine(config=config)
        else:
            raise ValueError(f"Unsupported data engine type: {data_engine_type}")
