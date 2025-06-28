"""
Conversation threading logic for grouping related tweets
"""

import pandas as pd
from typing import Dict, Set
import logging

logger = logging.getLogger(__name__)

class ConversationThreader:
    """Groups tweets into conversation threads"""
    
    def __init__(self):
        self.brand_accounts = set()  # Will be populated with known brand accounts
        self.conversation_cache = {}
    
    def load_brand_accounts(self, brand_account_list: list = None):
        """Load known brand/support account IDs"""
        if brand_account_list:
            self.brand_accounts.update(brand_account_list)
        
        # Add some common patterns for brand accounts
        # In production, this would be loaded from a database
        self.brand_accounts.update([
            'support', 'help', 'service', 'care', 'assist'
        ])
    
    def is_brand_account(self, author_id: str, text: str = "") -> bool:
        """
        Detect if account is likely a brand/support account
        Uses heuristics and known account patterns
        """
        if not author_id:
            return False
        
        author_lower = author_id.lower()
        
        # Check against known brand account patterns
        brand_patterns = ['support', 'help', 'service', 'care', 'assist', 'team']
        if any(pattern in author_lower for pattern in brand_patterns):
            return True
        
        # Check text patterns that indicate support responses
        if text:
            text_lower = text.lower()
            support_phrases = [
                'thank you for contacting',
                'we apologize',
                'please dm us',
                'sorry for the inconvenience',
                'we can help',
                'please reach out',
                'our team will',
                'thanks for reaching out'
            ]
            if any(phrase in text_lower for phrase in support_phrases):
                return True
        
        return False
    
    def thread_conversations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Thread tweets into conversations based on:
        1. in_response_to_status_id relationships
        2. Mention patterns between users
        3. Temporal proximity for same user interactions
        """
        df = df.copy()
        
        # Initialize threading columns
        df['conversation_id'] = None
        df['thread_position'] = 0
        df['is_customer_message'] = True
        df['conversation_type'] = 'unknown'  # customer_inquiry, brand_response, etc.
        
        # Sort by timestamp for proper threading
        df['created_at_parsed'] = pd.to_datetime(df['created_at'], errors='coerce')
        df = df.sort_values('created_at_parsed')
        
        conversation_counter = 0
        reply_map = {}  # Maps tweet_id to conversation_id
        
        for idx, row in df.iterrows():
            tweet_id = row['tweet_id']
            author_id = row['author_id']
            text = row['text']
            in_reply_to = row.get('in_response_to_status_id')
            
            # Check if this is a direct reply to an existing tweet
            if pd.notna(in_reply_to) and in_reply_to in reply_map:
                # This is part of an existing conversation
                conv_id = reply_map[in_reply_to]
                df.loc[idx, 'conversation_id'] = conv_id
                
                # Calculate thread position
                parent_position = df[
                    (df['conversation_id'] == conv_id) & 
                    (df['tweet_id'] == in_reply_to)
                ]['thread_position'].iloc[0] if len(df[
                    (df['conversation_id'] == conv_id) & 
                    (df['tweet_id'] == in_reply_to)
                ]) > 0 else 0
                
                df.loc[idx, 'thread_position'] = parent_position + 1
                
                # Determine if this is a brand response
                if self.is_brand_account(author_id, text):
                    df.loc[idx, 'is_customer_message'] = False
                    df.loc[idx, 'conversation_type'] = 'brand_response'
                else:
                    df.loc[idx, 'conversation_type'] = 'customer_followup'
                
                # Add to reply map
                reply_map[tweet_id] = conv_id
                
            else:
                # Start a new conversation thread
                conversation_counter += 1
                conv_id = f"conv_{conversation_counter:08d}"
                
                df.loc[idx, 'conversation_id'] = conv_id
                df.loc[idx, 'thread_position'] = 0
                
                # Determine conversation type
                if self.is_brand_account(author_id, text):
                    df.loc[idx, 'is_customer_message'] = False
                    df.loc[idx, 'conversation_type'] = 'brand_initiated'
                else:
                    df.loc[idx, 'conversation_type'] = 'customer_inquiry'
                
                # Add to reply map
                reply_map[tweet_id] = conv_id
        
        # Post-process: Group mentions-based conversations
        df = self._group_mention_conversations(df)
        
        # Add conversation metadata
        df = self._add_conversation_metadata(df)
        
        return df
    
    def _group_mention_conversations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group tweets that mention each other but aren't direct replies"""
        
        # Find tweets that mention each other within time windows
        for conv_id in df['conversation_id'].unique():
            if pd.isna(conv_id):
                continue
                
            conv_tweets = df[df['conversation_id'] == conv_id].copy()
            
            # Look for nearby tweets from the same users
            for idx, row in conv_tweets.iterrows():
                author_id = row['author_id']
                timestamp = row['created_at_parsed']
                
                # Find nearby tweets from same author that might be related
                time_window = pd.Timedelta(hours=24)
                nearby_tweets = df[
                    (df['author_id'] == author_id) &
                    (df['created_at_parsed'].between(
                        timestamp - time_window, 
                        timestamp + time_window
                    )) &
                    (df['conversation_id'] != conv_id)
                ]
                
                # Simple heuristic: if tweets mention similar topics, group them
                # This could be enhanced with semantic similarity
                for nearby_idx, nearby_row in nearby_tweets.iterrows():
                    if self._are_tweets_related(row['text'], nearby_row['text']):
                        df.loc[nearby_idx, 'conversation_id'] = conv_id
                        df.loc[nearby_idx, 'thread_position'] = len(conv_tweets)
        
        return df
    
    def _are_tweets_related(self, text1: str, text2: str) -> bool:
        """Simple heuristic to check if two tweets are related"""
        if not text1 or not text2:
            return False
        
        # Convert to lowercase for comparison
        text1_lower = text1.lower()
        text2_lower = text2.lower()
        
        # Check for common keywords (simple approach)
        words1 = set(text1_lower.split())
        words2 = set(text2_lower.split())
        
        # Remove common words
        stop_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'with', 'by'}
        words1 = words1 - stop_words
        words2 = words2 - stop_words
        
        # Calculate overlap
        if len(words1) == 0 or len(words2) == 0:
            return False
        
        overlap = len(words1.intersection(words2))
        similarity = overlap / min(len(words1), len(words2))
        
        return similarity > 0.3  # 30% word overlap threshold
    
    def _add_conversation_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata about conversations"""
        
        conversation_stats = df.groupby('conversation_id').agg({
            'tweet_id': 'count',
            'is_customer_message': 'sum',
            'created_at_parsed': ['min', 'max'],
            'author_id': 'nunique'
        }).round(2)
        
        conversation_stats.columns = [
            'total_messages',
            'customer_messages',
            'start_time',
            'end_time',
            'unique_participants'
        ]
        
        # Calculate conversation duration
        conversation_stats['duration_hours'] = (
            conversation_stats['end_time'] - conversation_stats['start_time']
        ).dt.total_seconds() / 3600
        
        # Merge back to main dataframe
        df = df.merge(
            conversation_stats.reset_index(),
            on='conversation_id',
            how='left'
        )
        
        return df
    
    def get_conversation_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate summary statistics for conversations"""
        summary = df.groupby('conversation_id').agg({
            'tweet_id': 'count',
            'is_customer_message': ['sum', lambda x: len(x) - sum(x)],
            'author_id': 'nunique',
            'created_at_parsed': ['min', 'max'],
            'conversation_type': lambda x: x.iloc[0]  # First message type
        })
        
        summary.columns = [
            'total_messages',
            'customer_messages',
            'brand_messages',
            'unique_participants',
            'start_time',
            'end_time',
            'conversation_type'
        ]
        
        summary['duration_hours'] = (
            summary['end_time'] - summary['start_time']
        ).dt.total_seconds() / 3600
        
        return summary.reset_index()