
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

class DatabaseConnector(ABC):
    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def create_tables(self) -> bool:
        pass

    @abstractmethod
    def batch_insert(self, records: List[Dict]) -> bool:
        pass

    @abstractmethod
    def get_last_watermark(self) -> Optional[str]:
        pass

    @abstractmethod
    def update_watermark(self, value: str) -> bool:
        pass

    @abstractmethod
    def health_check(self) -> bool:
        pass
