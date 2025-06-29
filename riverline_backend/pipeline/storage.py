"""
Supabase storage operations
Database connection and data operations
"""

import logging
from typing import List, Dict, Optional
from supabase import create_client, Client
import json

logger = logging.getLogger(__name__)


class SupabaseStorage:
    """Handles all Supabase database operations"""
    
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.client: Optional[Client] = None
    
    def connect(self) -> bool:
        """Establish connection to Supabase"""
        try:
            self.client = create_client(self.supabase_url, self.supabase_key)
            
            # Test connection
            response = self.client.table('_test_connection').select('*').limit(1).execute()
            logger.info("Connected to Supabase successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {e}")
            return False

    def insert_batch(self, records: List[Dict]) -> Dict:
        """Insert a batch of records with conflict handling"""
        if not self.client:
            raise RuntimeError("Not connected to Supabase")
        
        if not records:
            return {'inserted': 0, 'duplicates': 0, 'errors': 0}
        
        try:
            # Prepare records for insertion
            db_records = []
            for record in records:
                db_record = {
                    'tweet_id': record['tweet_id'],
                    'author_id': record['author_id'],
                    'text': record['text'],
                    'created_at': record['created_at'],
                    'in_response_to_status_id': record.get('in_response_to_status_id'),
                    'mentions': json.dumps(record.get('mentions', [])),
                    'hashtags': json.dumps(record.get('hashtags', [])),
                    'record_hash': record['record_hash'],
                    'processed_at': record['processed_at']
                }
                db_records.append(db_record)
            
            # Insert with upsert to handle duplicates
            response = self.client.table('interactions').upsert(
                db_records,
                on_conflict='record_hash'
            ).execute()
            
            inserted_count = len(response.data) if response.data else 0
            
            logger.info(f"Inserted {inserted_count} records to Supabase")
            
            return {
                'inserted': inserted_count,
                'duplicates': len(records) - inserted_count,
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            return {
                'inserted': 0,
                'duplicates': 0,
                'errors': len(records)
            }
    
    def get_last_processed_id(self) -> Optional[str]:
        """Get the last processed tweet ID"""
        if not self.client:
            return None
        
        try:
            response = self.client.table('pipeline_status').select('last_processed_tweet_id').order('last_run_at', desc=True).limit(1).execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0]['last_processed_tweet_id']
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get last processed ID: {e}")
            return None
    
    def update_pipeline_status(self, last_processed_id: str, records_processed: int, status: str = 'completed', error_message: str = None) -> bool:
        """Update pipeline status"""
        if not self.client:
            return False
        
        try:
            status_record = {
                'last_processed_tweet_id': last_processed_id,
                'records_processed': records_processed,
                'status': status,
                'error_message': error_message,
                'updated_at': 'NOW()'
            }
            
            response = self.client.table('pipeline_status').insert(status_record).execute()
            
            logger.info(f"Updated pipeline status: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update pipeline status: {e}")
            return False
    
    def get_record_count(self) -> int:
        """Get total number of records in interactions table"""
        if not self.client:
            return 0
        
        try:
            response = self.client.table('interactions').select('id', count='exact').execute()
            return response.count if response.count else 0
            
        except Exception as e:
            logger.error(f"Failed to get record count: {e}")
            return 0
    
    def get_recent_records(self, limit: int = 10) -> List[Dict]:
        """Get recent records for verification"""
        if not self.client:
            return []
        
        try:
            response = self.client.table('interactions').select('*').order('created_at_db', desc=True).limit(limit).execute()
            
            return response.data if response.data else []
            
        except Exception as e:
            logger.error(f"Failed to get recent records: {e}")
            return []
    
    def health_check(self) -> Dict:
        """Perform health check on database connection"""
        try:
            if not self.client:
                return {'status': 'error', 'message': 'Not connected'}
            
            # Simple query to test connection
            response = self.client.table('interactions').select('id').limit(1).execute()
            
            return {
                'status': 'healthy',
                'total_records': self.get_record_count(),
                'connection': 'active'
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e),
                'connection': 'failed'
            }