"""
Data processing and normalization
Clean, validate, and prepare data for storage
"""

import pandas as pd
import re
import hashlib
import logging
from typing import Dict, List, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data cleaning, normalization, and basic validation"""
    
    def __init__(self, max_text_length: int = 1000, min_text_length: int = 5):
        self.max_text_length = max_text_length
        self.min_text_length = min_text_length
        self.processed_hashes: Set[str] = set()
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if pd.isna(text) or text is None:
            return ""
        
        text = str(text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Handle encoding issues
        text = text.encode('utf-8', errors='ignore').decode('utf-8')
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # Truncate if too long
        if len(text) > self.max_text_length:
            text = text[:self.max_text_length].rsplit(' ', 1)[0] + '...'
        
        return text
    
    def normalize_timestamp(self, timestamp_str: str) -> str:
        """Normalize timestamp to ISO format"""
        if pd.isna(timestamp_str) or not timestamp_str:
            return datetime.utcnow().isoformat()
        
        try:
            parsed_ts = pd.to_datetime(timestamp_str)
            return parsed_ts.isoformat()
        except Exception as e:
            logger.warning(f"Could not parse timestamp '{timestamp_str}': {e}")
            return datetime.utcnow().isoformat()
    
    def extract_mentions(self, text: str) -> List[str]:
        """Extract @mentions from text"""
        if not text:
            return []
        return re.findall(r'@(\w+)', text)
    
    def extract_hashtags(self, text: str) -> List[str]:
        """Extract #hashtags from text"""
        if not text:
            return []
        return re.findall(r'#(\w+)', text)
    
    def generate_record_hash(self, record: Dict) -> str:
        """Generate unique hash for deduplication"""
        # Use key fields to create hash
        hash_input = f"{record.get('tweet_id', '')}{record.get('author_id', '')}{record.get('text', '')}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def validate_record(self, record: Dict) -> bool:
        """Basic record validation"""
        # Check required fields
        if not record.get('tweet_id'):
            return False
        
        # Check text length
        text = record.get('text', '')
        if len(text) < self.min_text_length:
            return False
        
        # Check if timestamp is valid
        try:
            pd.to_datetime(record.get('created_at'))
        except:
            return False
        
        return True
    
    def normalize_record(self, record: Dict) -> Dict:
        """Normalize a single record"""
        normalized = {
            'tweet_id': str(record.get('tweet_id', '')),
            'author_id': str(record.get('author_id', '')),
            'text': self.clean_text(record.get('text', '')),
            'created_at': self.normalize_timestamp(record.get('created_at', '')),
            'in_response_to_status_id': str(record.get('in_response_to_status_id', '')) if pd.notna(record.get('in_response_to_status_id')) else None,
            'mentions': self.extract_mentions(record.get('text', '')),
            'hashtags': self.extract_hashtags(record.get('text', '')),
            'processed_at': datetime.utcnow().isoformat()
        }
        
        # Add hash for deduplication
        normalized['record_hash'] = self.generate_record_hash(normalized)
        
        return normalized
    
    def process_batch(self, df: pd.DataFrame) -> List[Dict]:
        """Process a batch of records"""
        processed_records = []
        duplicate_count = 0
        invalid_count = 0
        
        for _, row in df.iterrows():
            try:
                # Normalize the record
                normalized = self.normalize_record(row.to_dict())
                
                # Validate
                if not self.validate_record(normalized):
                    invalid_count += 1
                    continue
                
                # Check for duplicates within this batch
                record_hash = normalized['record_hash']
                if record_hash in self.processed_hashes:
                    duplicate_count += 1
                    continue
                
                self.processed_hashes.add(record_hash)
                processed_records.append(normalized)
                
            except Exception as e:
                logger.error(f"Failed to process record: {e}")
                invalid_count += 1
                continue
        
        logger.info(f"Processed batch: {len(processed_records)} valid, {duplicate_count} duplicates, {invalid_count} invalid")
        
        return processed_records
    
    def get_processing_stats(self) -> Dict:
        """Get processing statistics"""
        return {
            'total_processed_hashes': len(self.processed_hashes),
            'max_text_length': self.max_text_length,
            'min_text_length': self.min_text_length
        }