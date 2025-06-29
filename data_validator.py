"""
Data quality validation and scoring
"""

import pandas as pd
from typing import Dict, List, Tuple
import re
import logging

logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Validates data quality and flags issues"""
    
    def __init__(self, config=None):
        self.config = config or {}
        self.min_text_length = self.config.get('min_text_length', 5)
        self.max_text_length = self.config.get('max_text_length', 1000)
        self.quality_threshold = self.config.get('quality_threshold', 0.8)
    
    def validate_record(self, record: Dict) -> Tuple[bool, List[str], float]:
        """
        Validate individual record quality
        Returns: (is_valid, issues_list, quality_score)
        """
        issues = []
        quality_score = 1.0
        
        # Required fields validation
        required_fields = ['tweet_id', 'author_id', 'text', 'created_at']
        missing_fields = [field for field in required_fields if not record.get(field)]
        
        if missing_fields:
            issues.extend([f"Missing required field: {field}" for field in missing_fields])
            quality_score -= 0.3 * len(missing_fields)
        
        # Tweet ID validation
        tweet_id = record.get('tweet_id', '')
        if tweet_id and not self._is_valid_tweet_id(tweet_id):
            issues.append("Invalid tweet ID format")
            quality_score -= 0.2
        
        # Text quality validation
        text = record.get('text', '')
        text_issues, text_score = self._validate_text_quality(text)
        issues.extend(text_issues)
        quality_score = min(quality_score, text_score)
        
        # Timestamp validation
        timestamp = record.get('created_at', '')
        if timestamp and not self._is_valid_timestamp(timestamp):
            issues.append("Invalid timestamp format")
            quality_score -= 0.1
        
        # Author ID validation
        author_id = record.get('author_id', '')
        if author_id and not self._is_valid_author_id(author_id):
            issues.append("Invalid author ID format")
            quality_score -= 0.1
        
        # Cross-field validation
        cross_issues, cross_score = self._validate_cross_fields(record)
        issues.extend(cross_issues)
        quality_score = min(quality_score, cross_score)
        
        # Ensure quality score is between 0 and 1
        quality_score = max(0.0, min(1.0, quality_score))
        
        # Determine if record is valid
        is_valid = quality_score >= self.quality_threshold and len(issues) == 0
        
        return is_valid, issues, quality_score
    
    def _validate_text_quality(self, text: str) -> Tuple[List[str], float]:
        """Validate text content quality"""
        issues = []
        score = 1.0
        
        if not text:
            issues.append("Empty text content")
            return issues, 0.0
        
        # Length validation
        if len(text) < self.min_text_length:
            issues.append(f"Text too short (< {self.min_text_length} chars)")
            score -= 0.3
        
        if len(text) > self.max_text_length:
            issues.append(f"Text too long (> {self.max_text_length} chars)")
            score -= 0.2
        
        # Content quality checks
        if text.strip() == text.upper() and len(text) > 20:
            issues.append("Text appears to be all caps (spam indicator)")
            score -= 0.2
        
        # Check for excessive special characters
        special_char_ratio = len(re.findall(r'[^a-zA-Z0-9\s]', text)) / len(text)
        if special_char_ratio > 0.5:
            issues.append("Excessive special characters")
            score -= 0.3
        
        # Check for repeated characters (spam indicator)
        if re.search(r'(.)\1{5,}', text):
            issues.append("Excessive character repetition")
            score -= 0.2
        
        # Check for URLs (not necessarily bad, but worth noting)
        url_count = len(re.findall(r'http[s]?://\S+', text))
        if url_count > 3:
            issues.append("Multiple URLs detected")
            score -= 0.1
        
        return issues, max(0.0, score)
    
    def _is_valid_tweet_id(self, tweet_id: str) -> bool:
        """Validate tweet ID format"""
        if not tweet_id:
            return False
        
        # Tweet IDs should be numeric strings
        try:
            int(tweet_id)
            return len(tweet_id) >= 10  # Twitter IDs are typically long
        except ValueError:
            return False
    
    def _is_valid_author_id(self, author_id: str) -> bool:
        """Validate author ID format"""
        if not author_id:
            return False
        
        # Author IDs can be numeric or alphanumeric
        # Should not contain special characters except underscores
        return bool(re.match(r'^[a-zA-Z0-9_]+$', author_id))
    
    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """Validate timestamp format with improved parsing"""
        if not timestamp:
            return False
        
        try:
            pd.to_datetime(timestamp, utc=True)
            return True
        except:
            return False
    
    def _validate_cross_fields(self, record: Dict) -> Tuple[List[str], float]:
        """Validate relationships between fields"""
        issues = []
        score = 1.0
        
        # Check if reply relationship makes sense
        in_reply_to = record.get('in_response_to_status_id')
        text = record.get('text', '')
        
        if in_reply_to and not text.startswith('@'):
            # Replies typically start with mentions
            issues.append("Reply tweet doesn't start with mention")
            score -= 0.1
        
        # Check for self-replies (usually unusual)
        if in_reply_to == record.get('tweet_id'):
            issues.append("Tweet replies to itself")
            score -= 0.3
        
        # Validate conversation consistency
        # (This would be enhanced with more context about the conversation)
        
        return issues, score
    
    def validate_batch(self, records: List[Dict]) -> Dict:
        """Validate a batch of records and return summary statistics"""
        total_records = len(records)
        valid_records = 0
        invalid_records = 0
        quality_scores = []
        all_issues = []
        
        record_results = []
        
        for i, record in enumerate(records):
            is_valid, issues, quality_score = self.validate_record(record)
            
            record_results.append({
                'record_index': i,
                'is_valid': is_valid,
                'quality_score': quality_score,
                'issues': issues
            })
            
            quality_scores.append(quality_score)
            all_issues.extend(issues)
            
            if is_valid:
                valid_records += 1
            else:
                invalid_records += 1
        
        # Calculate summary statistics
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Count issue types
        issue_counts = {}
        for issue in all_issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': invalid_records,
            'avg_quality_score': avg_quality_score,
            'quality_scores': quality_scores,
            'issue_counts': issue_counts,
            'record_results': record_results
        }