"""
CSV data loading with incremental processing
"""

import pandas as pd
import logging
from typing import Iterator, Optional, Dict
from pathlib import Path

logger = logging.getLogger(__name__)


class CSVLoader:
    """Handles CSV loading with checkpointing for incremental processing"""
    
    def __init__(self, csv_path: str,sample: bool = False):
        # Always resolve relative to project root (backend/) if not absolute
        path = Path(csv_path)
        if not path.is_absolute():
            # Find backend/ as the project root
            project_root = Path(__file__).parent.parent
            path = project_root / path
        self.csv_path = path.resolve()
        self.current_position = 0
        self._total_rows = None
        self.sample = sample
    
    def validate_file(self) -> bool:
        """Check if CSV file exists and is readable"""
        if not self.csv_path.exists():
            logger.error(f"CSV file not found: {self.csv_path}")
            return False
        
        try:
            # Quick validation - read just the header
            pd.read_csv(self.csv_path, nrows=1)
            logger.info(f"CSV file validated: {self.csv_path}")
            return True
        except Exception as e:
            logger.error(f"CSV file validation failed: {e}")
            return False
    
    def get_total_rows(self) -> int:
        """Get total number of rows in CSV (cached)"""
        if self._total_rows is None:
            try:
                # Count lines efficiently
                with open(self.csv_path, 'r') as f:
                    self._total_rows = sum(1 for _ in f) - 1  # Subtract header
                logger.info(f"Total rows in CSV: {self._total_rows}")
            except Exception as e:
                logger.error(f"Failed to count rows: {e}")
                self._total_rows = 0
        return self._total_rows
    
    def load_incremental(self, last_processed_id: Optional[str] = None) -> pd.DataFrame:
        """Load data starting from last processed record"""
        try:
            # Load full dataset for now - can optimize later with chunking
            df = pd.read_csv(self.csv_path)
            
            if last_processed_id:
                # Find starting position based on last processed ID
                if 'tweet_id' in df.columns:
                    mask = df['tweet_id'].astype(str) > str(last_processed_id)
                    df = df[mask]
                    logger.info(f"Loaded {len(df)} new records after ID {last_processed_id}")
                else:
                    logger.warning("No tweet_id column found, loading all data")
            else:
                logger.info(f"Loaded {len(df)} total records")
            
            if self.sample:
                df = df.sample(n=100)
                logger.info(f"Sampled {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load CSV data: {e}")
            return pd.DataFrame()
    
    def get_batch_iterator(self, batch_size: int, last_processed_id: Optional[str] = None) -> Iterator[pd.DataFrame]:
        """Get iterator that yields batches of data"""
        try:
            # Use chunking for memory efficiency
            chunk_iter = pd.read_csv(self.csv_path, chunksize=batch_size)
            
            skip_until_found = bool(last_processed_id)
            
            for chunk in chunk_iter:
                if skip_until_found and 'tweet_id' in chunk.columns:
                    # Skip chunks until we find our starting point
                    if not any(chunk['tweet_id'].astype(str) == str(last_processed_id)):
                        continue
                    else:
                        # Found the chunk with our last processed ID
                        mask = chunk['tweet_id'].astype(str) > str(last_processed_id)
                        chunk = chunk[mask]
                        skip_until_found = False
                
                if len(chunk) > 0:
                    yield chunk
                    
        except Exception as e:
            logger.error(f"Failed to create batch iterator: {e}")
            return
    
    def get_sample_data(self, n_rows: int = 100) -> pd.DataFrame:
        """Get a sample of data for testing"""
        try:
            return pd.read_csv(self.csv_path, nrows=n_rows)
        except Exception as e:
            logger.error(f"Failed to load sample data: {e}")
            return pd.DataFrame()
    
    def get_column_info(self) -> Dict:
        """Get information about CSV columns"""
        try:
            sample = pd.read_csv(self.csv_path, nrows=1000)
            return {
                'columns': list(sample.columns),
                'dtypes': sample.dtypes.to_dict(),
                'sample_values': {col: sample[col].head(3).tolist() for col in sample.columns},
                'null_counts': sample.isnull().sum().to_dict()
            }
        except Exception as e:
            logger.error(f"Failed to get column info: {e}")
            return {}