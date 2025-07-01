
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd

class BaseDataEngine(ABC):
    @abstractmethod
    def read_data(self, file_path: str, last_processed_timestamp: Optional[str] = None) -> pd.DataFrame:
        pass
    
    @abstractmethod  
    def normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def quality_check(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def get_records(self, df: pd.DataFrame) -> List[Dict]:
        pass
