"""
Data normalization and cleaning utilities
"""

import pandas as pd
import re
from typing import List
import logging

logger = logging.getLogger(__name__)

class TwitterDataNormalizer:
    """Handles data cleaning and normalization for Twitter data"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize tweet text"""
        if pd.isna(text) or text is None:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', str(text)).strip()
        
        # Handle encoding issues
        text = text.encode('utf-8', errors='ignore').decode('utf-8')
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        return text
    
    @staticmethod
    def extract_mentions(text: str) -> List[str]:
        """Extract @mentions from tweet text"""
        if not text:
            return []
        return re.findall(r'@(\w+)', text)
    
    @staticmethod
    def extract_hashtags(text: str) -> List[str]:
        """Extract #hashtags from tweet text"""
        if not text:
            return []
        return re.findall(r'#(\w+)', text)
    
    @staticmethod
    def detect_language(text: str) -> str:
        """Simple language detection"""
        if not text:
            return "unknown"
        
        # Simple heuristic - check for common English words
        english_indicators = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'with']
        text_lower = text.lower()
        
        english_word_count = sum(1 for word in english_indicators if word in text_lower)
        
        if english_word_count >= 2:
            return "en"
        elif any(char in text for char in ['á', 'é', 'í', 'ó', 'ú', 'ñ']):
            return "es"
        elif any(char in text for char in ['à', 'ç', 'è', 'é', 'ê']):
            return "fr"
        else:
            return "unknown"
    
    @staticmethod
    def normalize_timestamp(timestamp_str: str) -> str:
        """Normalize timestamp to ISO format"""
        try:
            # Parse and convert to standard format
            parsed_ts = pd.to_datetime(timestamp_str)
            return parsed_ts.isoformat()
        except:
            logger.warning(f"Could not parse timestamp: {timestamp_str}")
            return timestamp_str
    
    @staticmethod
    def extract_urls(text: str) -> List[str]:
        """Extract URLs from text"""
        if not text:
            return []
        
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        return re.findall(url_pattern, text)
    
    def normalize_record(self, record: dict) -> dict:
        """Normalize a complete record"""
        normalized = {
            'tweet_id': str(record.get('tweet_id', '')),
            'author_id': str(record.get('author_id', '')),
            'text': self.clean_text(record.get('text', '')),
            'created_at': self.normalize_timestamp(record.get('created_at', '')),
            'in_response_to_status_id': str(record.get('in_response_to_status_id', '')) if pd.notna(record.get('in_response_to_status_id')) else None,
            'language': self.detect_language(record.get('text', '')),
            'mentions': self.extract_mentions(record.get('text', '')),
            'hashtags': self.extract_hashtags(record.get('text', '')),
            'urls': self.extract_urls(record.get('text', ''))
        }
        
        return normalized