"""
ClickHouse database management and operations
"""

import clickhouse_connect
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import json

logger = logging.getLogger(__name__)

class ClickHouseManager:
    """Manages ClickHouse database operations"""
    
    def __init__(self, config):
        self.config = config
        self.client = None
    
    def connect(self):
        """Establish ClickHouse connection"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config.clickhouse_host,
                username=self.config.clickhouse_user,
                password=self.config.clickhouse_password,
                database=self.config.clickhouse_database,
                secure=self.config.clickhouse_secure
            )
            
            # Test connection
            result = self.client.query("SELECT 1")
            logger.info("Connected to ClickHouse successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def create_tables(self):
        """Create all necessary tables if they don't exist"""
        
        tables = {
            'interactions': self._get_interactions_ddl(),
            'ingestion_watermarks': self._get_watermarks_ddl(),
            'data_quality_metrics': self._get_quality_metrics_ddl(),
            'conversation_summaries': self._get_conversation_summaries_ddl()
        }
        
        for table_name, ddl in tables.items():
            try:
                self.client.command(ddl)
                logger.info(f"Table '{table_name}' created/verified successfully")
            except Exception as e:
                logger.error(f"Failed to create table '{table_name}': {e}")
                raise
    
    def _get_interactions_ddl(self) -> str:
        """Get DDL for main interactions table"""
        return """
        CREATE TABLE IF NOT EXISTS interactions (
            tweet_id String,
            conversation_id String,
            thread_position UInt32,
            author_id String,
            text String,
            created_at DateTime64(3),
            in_response_to_status_id Nullable(String),
            is_customer_message Bool,
            conversation_type String DEFAULT 'unknown',
            language String,
            mentions Array(String),
            hashtags Array(String),
            urls Array(String),
            sentiment_score Nullable(Float32),
            data_source String DEFAULT 'twitter_csv',
            ingestion_timestamp DateTime64(3) DEFAULT now64(),
            record_hash String,
            data_quality_score Float32 DEFAULT 1.0,
            quality_issues Array(String),
            
            -- Conversation metadata
            total_messages UInt32 DEFAULT 0,
            customer_messages UInt32 DEFAULT 0,
            unique_participants UInt32 DEFAULT 0,
            duration_hours Float32 DEFAULT 0.0,
            
            INDEX idx_conversation_id conversation_id TYPE bloom_filter GRANULARITY 1,
            INDEX idx_author_id author_id TYPE bloom_filter GRANULARITY 1,
            INDEX idx_created_at created_at TYPE minmax GRANULARITY 1
        ) ENGINE = MergeTree()
        ORDER BY (conversation_id, thread_position, created_at)
        PARTITION BY toYYYYMM(created_at)
        SETTINGS index_granularity = 8192
        """
    
    def _get_watermarks_ddl(self) -> str:
        """Get DDL for ingestion watermarks table"""
        return """
        CREATE TABLE IF NOT EXISTS ingestion_watermarks (
            data_source String,
            last_processed_timestamp DateTime64(3),
            last_processed_id String,
            records_processed UInt64,
            processing_timestamp DateTime64(3) DEFAULT now64(),
            batch_info String DEFAULT ''
        ) ENGINE = ReplacingMergeTree(processing_timestamp)
        ORDER BY (data_source)
        """
    
    def _get_quality_metrics_ddl(self) -> str:
        """Get DDL for data quality metrics table"""
        return """
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            batch_id String,
            data_source String,
            total_records UInt64,
            valid_records UInt64,
            invalid_records UInt64,
            duplicate_records UInt64,
            avg_quality_score Float32,
            processing_timestamp DateTime64(3) DEFAULT now64(),
            quality_issues Array(String),
            issue_counts String DEFAULT '{}'
        ) ENGINE = MergeTree()
        ORDER BY (processing_timestamp, data_source)
        PARTITION BY toYYYYMM(processing_timestamp)
        """
    
    def _get_conversation_summaries_ddl(self) -> str:
        """Get DDL for conversation summaries table"""
        return """
        CREATE TABLE IF NOT EXISTS conversation_summaries (
            conversation_id String,
            total_messages UInt32,
            customer_messages UInt32,
            brand_messages UInt32,
            unique_participants UInt32,
            start_time DateTime64(3),
            end_time DateTime64(3),
            duration_hours Float32,
            conversation_type String,
            last_customer_message String DEFAULT '',
            last_brand_message String DEFAULT '',
            is_resolved Bool DEFAULT false,
            resolution_reason String DEFAULT '',
            created_at DateTime64(3) DEFAULT now64()
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY conversation_id
        """
    
    def get_last_watermark(self, data_source: str) -> Optional[Dict]:
        """Get the last processed watermark for idempotent processing"""
        try:
            result = self.client.query(
                """
                SELECT * FROM ingestion_watermarks 
                WHERE data_source = %(source)s 
                ORDER BY processing_timestamp DESC 
                LIMIT 1
                """,
                {"source": data_source}
            )
            
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    'data_source': row[0],
                    'last_processed_timestamp': row[1],
                    'last_processed_id': row[2],
                    'records_processed': row[3],
                    'processing_timestamp': row[4],
                    'batch_info': row[5] if len(row) > 5 else ''
                }
            return None
            
        except Exception as e:
            logger.warning(f"Could not retrieve watermark for {data_source}: {e}")
            return None
    
    def update_watermark(self, data_source: str, last_timestamp: str, 
                        last_id: str, records_count: int, batch_info: str = ""):
        """Update processing watermark"""
        try:
            self.client.insert(
                'ingestion_watermarks',
                [[data_source, last_timestamp, last_id, records_count, 
                  datetime.now(timezone.utc), batch_info]],
                column_names=[
                    'data_source', 'last_processed_timestamp', 'last_processed_id', 
                    'records_processed', 'processing_timestamp', 'batch_info'
                ]
            )
            logger.info(f"Updated watermark for {data_source}: {records_count} records")
            
        except Exception as e:
            logger.error(f"Failed to update watermark for {data_source}: {e}")
            raise
    
    def insert_interactions_batch(self, records: List[Dict], 
                                 handle_duplicates: bool = True) -> Dict:
        """Insert batch of interaction records with deduplication"""
        if not records:
            return {'inserted': 0, 'duplicates': 0, 'errors': 0}
        
        try:
            # Prepare data for insertion
            data_rows = []
            column_names = [
                'tweet_id', 'conversation_id', 'thread_position', 'author_id',
                'text', 'created_at', 'in_response_to_status_id', 'is_customer_message',
                'conversation_type', 'language', 'mentions', 'hashtags', 'urls',
                'sentiment_score', 'data_source', 'ingestion_timestamp', 
                'record_hash', 'data_quality_score', 'quality_issues',
                'total_messages', 'customer_messages', 'unique_participants', 'duration_hours'
            ]
            
            for record in records:
                row = [
                    record.get('tweet_id', ''),
                    record.get('conversation_id', ''),
                    record.get('thread_position', 0),
                    record.get('author_id', ''),
                    record.get('text', ''),
                    record.get('created_at', ''),
                    record.get('in_response_to_status_id'),
                    record.get('is_customer_message', True),
                    record.get('conversation_type', 'unknown'),
                    record.get('language', 'unknown'),
                    record.get('mentions', []),
                    record.get('hashtags', []),
                    record.get('urls', []),
                    record.get('sentiment_score'),
                    record.get('data_source', 'twitter_csv'),
                    datetime.now(timezone.utc),
                    record.get('record_hash', ''),
                    record.get('data_quality_score', 1.0),
                    record.get('quality_issues', []),
                    record.get('total_messages', 0),
                    record.get('customer_messages', 0),
                    record.get('unique_participants', 0),
                    record.get('duration_hours', 0.0)
                ]
                data_rows.append(row)
            
            # Check for existing records if deduplication is enabled
            duplicates_count = 0
            if handle_duplicates:
                existing_hashes = self._get_existing_hashes([r.get('record_hash', '') for r in records])
                
                # Filter out duplicates
                filtered_rows = []
                for i, row in enumerate(data_rows):
                    record_hash = records[i].get('record_hash', '')
                    if record_hash not in existing_hashes:
                        filtered_rows.append(row)
                    else:
                        duplicates_count += 1
                
                data_rows = filtered_rows
            
            # Insert the data
            if data_rows:
                self.client.insert('interactions', data_rows, column_names=column_names)
                logger.info(f"Inserted {len(data_rows)} records, skipped {duplicates_count} duplicates")
            
            return {
                'inserted': len(data_rows),
                'duplicates': duplicates_count,
                'errors': 0
            }
            
        except Exception as e:
            logger.error(f"Failed to insert interactions batch: {e}")
            return {
                'inserted': 0,
                'duplicates': 0,
                'errors': len(records)
            }
    
    def _get_existing_hashes(self, hashes: List[str]) -> set:
        """Get existing record hashes to prevent duplicates"""
        if not hashes:
            return set()
        
        try:
            # Filter out empty hashes
            valid_hashes = [h for h in hashes if h]
            if not valid_hashes:
                return set()
            
            # Query existing hashes
            hash_list = "', '".join(valid_hashes)
            query = f"SELECT DISTINCT record_hash FROM interactions WHERE record_hash IN ('{hash_list}')"
            
            result = self.client.query(query)
            return {row[0] for row in result.result_rows}
            
        except Exception as e:
            logger.warning(f"Could not check for existing hashes: {e}")
            return set()
    
    def insert_quality_metrics(self, batch_id: str, data_source: str, 
                              metrics: Dict) -> None:
        """Insert data quality metrics for a batch"""
        try:
            data = [[
                batch_id,
                data_source,
                metrics.get('total_records', 0),
                metrics.get('valid_records', 0),
                metrics.get('invalid_records', 0),
                metrics.get('duplicates', 0),
                metrics.get('avg_quality_score', 0.0),
                datetime.now(timezone.utc),
                list(metrics.get('issue_counts', {}).keys()),
                json.dumps(metrics.get('issue_counts', {}))
            ]]
            
            column_names = [
                'batch_id', 'data_source', 'total_records', 'valid_records',
                'invalid_records', 'duplicate_records', 'avg_quality_score',
                'processing_timestamp', 'quality_issues', 'issue_counts'
            ]
            
            self.client.insert('data_quality_metrics', data, column_names=column_names)
            logger.info(f"Inserted quality metrics for batch {batch_id}")
            
        except Exception as e:
            logger.error(f"Failed to insert quality metrics: {e}")
    
    def get_conversation_stats(self, limit: int = 100) -> List[Dict]:
        """Get conversation statistics"""
        try:
            query = """
            SELECT 
                conversation_id,
                COUNT(*) as total_messages,
                SUM(CASE WHEN is_customer_message THEN 1 ELSE 0 END) as customer_messages,
                SUM(CASE WHEN NOT is_customer_message THEN 1 ELSE 0 END) as brand_messages,
                COUNT(DISTINCT author_id) as unique_participants,
                MIN(created_at) as start_time,
                MAX(created_at) as end_time,
                dateDiff('hour', MIN(created_at), MAX(created_at)) as duration_hours,
                any(conversation_type) as conversation_type
            FROM interactions 
            GROUP BY conversation_id
            ORDER BY total_messages DESC
            LIMIT %(limit)s
            """
            
            result = self.client.query(query, {'limit': limit})
            
            conversations = []
            for row in result.result_rows:
                conversations.append({
                    'conversation_id': row[0],
                    'total_messages': row[1],
                    'customer_messages': row[2],
                    'brand_messages': row[3],
                    'unique_participants': row[4],
                    'start_time': row[5],
                    'end_time': row[6],
                    'duration_hours': row[7],
                    'conversation_type': row[8]
                })
            
            return conversations
            
        except Exception as e:
            logger.error(f"Failed to get conversation stats: {e}")
            return []
    
    def get_ingestion_summary(self, days: int = 7) -> Dict:
        """Get ingestion summary for the last N days"""
        try:
            query = """
            SELECT 
                data_source,
                COUNT(*) as total_records,
                COUNT(DISTINCT conversation_id) as unique_conversations,
                AVG(data_quality_score) as avg_quality_score,
                MIN(created_at) as earliest_record,
                MAX(created_at) as latest_record
            FROM interactions 
            WHERE created_at >= now() - INTERVAL %(days)s DAY
            GROUP BY data_source
            """
            
            result = self.client.query(query, {'days': days})
            
            summary = {}
            for row in result.result_rows:
                summary[row[0]] = {
                    'total_records': row[1],
                    'unique_conversations': row[2],
                    'avg_quality_score': float(row[3]) if row[3] else 0.0,
                    'earliest_record': row[4],
                    'latest_record': row[5]
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get ingestion summary: {e}")
            return {}
    
    def cleanup_old_data(self, retention_days: int = 90) -> Dict:
        """Clean up old data beyond retention period"""
        try:
            # Clean up old interactions
            interactions_query = """
            ALTER TABLE interactions 
            DELETE WHERE created_at < now() - INTERVAL %(days)s DAY
            """
            
            # Clean up old quality metrics
            metrics_query = """
            ALTER TABLE data_quality_metrics 
            DELETE WHERE processing_timestamp < now() - INTERVAL %(days)s DAY
            """
            
            self.client.command(interactions_query, {'days': retention_days})
            self.client.command(metrics_query, {'days': retention_days})
            
            logger.info(f"Cleaned up data older than {retention_days} days")
            
            return {'status': 'success', 'retention_days': retention_days}
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def disconnect(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            logger.info("Disconnected from ClickHouse")


if __name__ == "__main__":
    # check and see if connector is connecting
    config = IngestionConfig()
    db_manager = ClickHouseManager(config)
    db_manager.connect()
    db_manager.disconnect()