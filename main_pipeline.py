"""
Main ingestion pipeline orchestrator
Coordinates all components for end-to-end data processing
"""

import pandas as pd
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from pathlib import Path
import os

from config.config import IngestionConfig
from data_normalizer import TwitterDataNormalizer
from conversation_threader import ConversationThreader
from data_validator import DataQualityValidator
from database_manager import ClickHouseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TwitterIngestionPipeline:
    """Main ingestion pipeline orchestrator"""
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.db_manager = ClickHouseManager(config)
        self.normalizer = TwitterDataNormalizer()
        self.threader = ConversationThreader()
        self.validator = DataQualityValidator({
            'min_text_length': config.min_text_length,
            'max_text_length': config.max_text_length,
            'quality_threshold': config.quality_threshold
        })
        
        # Pipeline state
        self.total_processed = 0
        self.total_errors = 0
        self.batch_counter = 0
    
    def setup(self):
        """Initialize pipeline components"""
        logger.info("Setting up ingestion pipeline...")
        
        # Create logs directory
        log_dir = Path(self.config.log_file).parent
        log_dir.mkdir(exist_ok=True)
        
        # Setup database connection and tables
        self.db_manager.connect()
        self.db_manager.create_tables()
        
        # Load brand accounts for conversation threading
        self.threader.load_brand_accounts()
        
        logger.info("Pipeline setup completed successfully")
    
    def generate_record_hash(self, record: Dict) -> str:
        """Generate deterministic hash for deduplication"""
        # Use tweet_id + author_id + text for hash to handle potential duplicates
        hash_input = f"{record.get('tweet_id', '')}{record.get('author_id', '')}{record.get('text', '')}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load CSV data with incremental processing support"""
        logger.info(f"Loading data from {file_path}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Load data in chunks for memory efficiency
        try:
            chunk_list = []
            chunk_size = min(self.config.batch_size, 50000)  # Limit chunk size
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                chunk_list.append(chunk)
                
                # Log progress for large files
                if len(chunk_list) % 10 == 0:
                    logger.info(f"Loaded {len(chunk_list)} chunks so far...")
            
            df = pd.concat(chunk_list, ignore_index=True)
            logger.info(f"Loaded {len(df)} total records from {len(chunk_list)} chunks")
            
        except Exception as e:
            logger.error(f"Failed to load data from {file_path}: {e}")
            raise
        
        # Check for incremental processing
        df = self._filter_for_incremental_processing(df)
        
        return df
    
    def _filter_for_incremental_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter data for incremental processing based on watermarks"""
        
        # Get last watermark
        watermark = self.db_manager.get_last_watermark(self.config.data_source_type)
        
        if watermark and watermark.get('last_processed_timestamp'):
            try:
                # Parse timestamps
                df['created_at_parsed'] = pd.to_datetime(df['created_at'], errors='coerce')
                last_timestamp = pd.to_datetime(watermark['last_processed_timestamp'])
                
                # Filter for records after last processed timestamp
                original_count = len(df)
                df = df[df['created_at_parsed'] > last_timestamp].copy()
                
                logger.info(
                    f"Incremental processing: {len(df)} new records out of {original_count} "
                    f"(last processed: {last_timestamp})"
                )
                
            except Exception as e:
                logger.warning(f"Could not apply incremental filtering: {e}")
                logger.info("Proceeding with full dataset")
        
        return df
    
    def process_batch(self, df_batch: pd.DataFrame, batch_id: str) -> Tuple[List[Dict], Dict]:
        """Process a batch of records through the full pipeline"""
        logger.info(f"Processing batch {batch_id} with {len(df_batch)} records")
        
        try:
            # Step 1: Thread conversations
            logger.debug(f"Threading conversations for batch {batch_id}")
            df_batch = self.threader.thread_conversations(df_batch)
            
            # Step 2: Normalize and validate records
            processed_records = []
            validation_results = {
                'total': len(df_batch),
                'valid': 0,
                'invalid': 0,
                'duplicates': 0,
                'quality_scores': [],
                'issue_counts': {}
            }
            
            for _, row in df_batch.iterrows():
                try:
                    # Normalize the record
                    normalized_record = self.normalizer.normalize_record(row.to_dict())
                    
                    # Add conversation threading data
                    normalized_record.update({
                        'conversation_id': row.get('conversation_id', ''),
                        'thread_position': int(row.get('thread_position', 0)),
                        'is_customer_message': bool(row.get('is_customer_message', True)),
                        'conversation_type': row.get('conversation_type', 'unknown'),
                        'total_messages': int(row.get('total_messages', 0)),
                        'customer_messages': int(row.get('customer_messages', 0)),
                        'unique_participants': int(row.get('unique_participants', 0)),
                        'duration_hours': float(row.get('duration_hours', 0.0))
                    })
                    
                    # Generate hash for deduplication
                    normalized_record['record_hash'] = self.generate_record_hash(normalized_record)
                    normalized_record['data_source'] = self.config.data_source_type
                    
                    # Validate record quality
                    is_valid, issues, quality_score = self.validator.validate_record(normalized_record)
                    
                    normalized_record['data_quality_score'] = quality_score
                    normalized_record['quality_issues'] = issues
                    
                    # Update validation metrics
                    validation_results['quality_scores'].append(quality_score)
                    
                    for issue in issues:
                        validation_results['issue_counts'][issue] = (
                            validation_results['issue_counts'].get(issue, 0) + 1
                        )
                    
                    if is_valid:
                        validation_results['valid'] += 1
                        processed_records.append(normalized_record)
                    else:
                        validation_results['invalid'] += 1
                        logger.debug(f"Invalid record {normalized_record.get('tweet_id', 'unknown')}: {issues}")
                        
                except Exception as e:
                    logger.error(f"Error processing record in batch {batch_id}: {e}")
                    validation_results['invalid'] += 1
            
            # Calculate average quality score
            if validation_results['quality_scores']:
                validation_results['avg_quality_score'] = (
                    sum(validation_results['quality_scores']) / len(validation_results['quality_scores'])
                )
            else:
                validation_results['avg_quality_score'] = 0.0
            
            logger.info(
                f"Batch {batch_id} processed: {validation_results['valid']} valid, "
                f"{validation_results['invalid']} invalid records"
            )
            
            return processed_records, validation_results
            
        except Exception as e:
            logger.error(f"Failed to process batch {batch_id}: {e}")
            raise
    
    def insert_batch(self, records: List[Dict], batch_id: str, 
                    validation_metrics: Dict) -> Dict:
        """Insert processed batch into database"""
        if not records:
            logger.warning(f"No valid records to insert for batch {batch_id}")
            return {'inserted': 0, 'duplicates': 0, 'errors': 0}
        
        try:
            # Insert interaction records
            insert_result = self.db_manager.insert_interactions_batch(records)
            
            # Insert quality metrics
            self.db_manager.insert_quality_metrics(
                batch_id=batch_id,
                data_source=self.config.data_source_type,
                metrics=validation_metrics
            )
            
            logger.info(
                f"Batch {batch_id} inserted: {insert_result['inserted']} records, "
                f"{insert_result['duplicates']} duplicates skipped"
            )
            
            return insert_result
            
        except Exception as e:
            logger.error(f"Failed to insert batch {batch_id}: {e}")
            return {'inserted': 0, 'duplicates': 0, 'errors': len(records)}
    
    def update_pipeline_watermark(self, df: pd.DataFrame, total_processed: int):
        """Update watermark after successful batch processing"""
        if len(df) == 0:
            return
        
        try:
            # Get the latest timestamp and ID from processed data
            df_sorted = df.sort_values('created_at_parsed')
            last_record = df_sorted.iloc[-1]
            
            last_timestamp = last_record['created_at_parsed'].isoformat()
            last_id = str(last_record['tweet_id'])
            
            # Create batch info
            batch_info = f"Processed {total_processed} records in {self.batch_counter} batches"
            
            # Update watermark
            self.db_manager.update_watermark(
                data_source=self.config.data_source_type,
                last_timestamp=last_timestamp,
                last_id=last_id,
                records_count=total_processed,
                batch_info=batch_info
            )
            
        except Exception as e:
            logger.error(f"Failed to update pipeline watermark: {e}")
    
    def run_ingestion(self, file_path: str = None) -> Dict:
        """Run the complete ingestion pipeline"""
        start_time = datetime.now()
        file_path = file_path or self.config.data_source_path
        
        logger.info(f"Starting ingestion pipeline for {file_path}")
        
        try:
            # Reset counters
            self.total_processed = 0
            self.total_errors = 0
            self.batch_counter = 0
            
            # Load data
            df = self.load_data(file_path)
            
            if len(df) == 0:
                logger.info("No new records to process")
                return {
                    'status': 'success',
                    'records_processed': 0,
                    'batches_processed': 0,
                    'duration_seconds': 0,
                    'message': 'No new records found'
                }
            
            # Process data in batches
            total_validation_metrics = {
                'total': 0, 'valid': 0, 'invalid': 0, 'duplicates': 0,
                'quality_scores': [], 'issue_counts': {}
            }
            
            for i in range(0, len(df), self.config.batch_size):
                self.batch_counter += 1
                batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.batch_counter:04d}"
                
                # Get batch data
                batch_df = df.iloc[i:i + self.config.batch_size].copy()
                
                logger.info(f"Processing batch {self.batch_counter}/{(len(df) + self.config.batch_size - 1) // self.config.batch_size}")
                
                # Process batch
                processed_records, validation_metrics = self.process_batch(batch_df, batch_id)
                
                # Insert batch
                insert_result = self.insert_batch(processed_records, batch_id, validation_metrics)
                
                # Update totals
                self.total_processed += insert_result['inserted']
                self.total_errors += insert_result['errors']
                
                # Accumulate validation metrics
                total_validation_metrics['total'] += validation_metrics['total']
                total_validation_metrics['valid'] += validation_metrics['valid']
                total_validation_metrics['invalid'] += validation_metrics['invalid']
                total_validation_metrics['duplicates'] += insert_result['duplicates']
                total_validation_metrics['quality_scores'].extend(validation_metrics['quality_scores'])
                
                # Merge issue counts
                for issue, count in validation_metrics['issue_counts'].items():
                    total_validation_metrics['issue_counts'][issue] = (
                        total_validation_metrics['issue_counts'].get(issue, 0) + count
                    )
            
            # Ensure 'created_at_parsed' exists before updating watermark
            if 'created_at_parsed' not in df.columns:
                if 'created_at' in df.columns:
                    df['created_at_parsed'] = pd.to_datetime(df['created_at'], errors='coerce')
                else:
                    logger.warning("'created_at' column missing; cannot create 'created_at_parsed'. Watermark update may fail.")
            # Update watermark after successful processing
            self.update_pipeline_watermark(df, self.total_processed)
            
            # Calculate final metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Calculate overall quality score
            if total_validation_metrics['quality_scores']:
                avg_quality_score = (
                    sum(total_validation_metrics['quality_scores']) / 
                    len(total_validation_metrics['quality_scores'])
                )
            else:
                avg_quality_score = 0.0
            
            result = {
                'status': 'success',
                'records_processed': self.total_processed,
                'batches_processed': self.batch_counter,
                'duration_seconds': duration,
                'quality_score': avg_quality_score,
                'validation_metrics': total_validation_metrics,
                'errors': self.total_errors,
                'throughput_records_per_second': self.total_processed / duration if duration > 0 else 0
            }
            
            logger.info(
                f"Ingestion completed successfully: {self.total_processed} records processed "
                f"in {self.batch_counter} batches, {duration:.1f}s "
                f"(Quality: {avg_quality_score:.3f})"
            )
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Ingestion pipeline failed after {duration:.1f}s: {e}")
            
            return {
                'status': 'error',
                'error': str(e),
                'records_processed': self.total_processed,
                'batches_processed': self.batch_counter,
                'duration_seconds': duration,
                'errors': self.total_errors
            }
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status and statistics"""
        try:
            # Get recent ingestion summary
            ingestion_summary = self.db_manager.get_ingestion_summary(days=7)
            
            # Get conversation statistics
            conversation_stats = self.db_manager.get_conversation_stats(limit=10)
            
            # Get last watermark
            watermark = self.db_manager.get_last_watermark(self.config.data_source_type)
            
            return {
                'pipeline_ready': True,
                'last_watermark': watermark,
                'recent_ingestion': ingestion_summary,
                'top_conversations': conversation_stats,
                'database_connected': self.db_manager.client is not None
            }
            
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            return {
                'pipeline_ready': False,
                'error': str(e),
                'database_connected': False
            }
    
    def cleanup_and_shutdown(self):
        """Clean up resources and shutdown pipeline"""
        logger.info("Shutting down ingestion pipeline...")
        
        try:
            # Disconnect from database
            if self.db_manager:
                self.db_manager.disconnect()
            
            logger.info("Pipeline shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during pipeline shutdown: {e}")

def main():
    """Example usage of the ingestion pipeline"""
    try:
        # Create configuration from environment
        config = IngestionConfig()

        # Initialize pipeline
        pipeline = TwitterIngestionPipeline(config)
        pipeline.setup()
        
        # Run ingestion
        result = pipeline.run_ingestion()
        
        # Print results
        print("\n" + "="*50)
        print("INGESTION PIPELINE RESULTS")
        print("="*50)
        print(f"Status: {result['status']}")
        print(f"Records Processed: {result.get('records_processed', 0)}")
        print(f"Batches Processed: {result.get('batches_processed', 0)}")
        print(f"Duration: {result.get('duration_seconds', 0):.1f} seconds")
        print(f"Quality Score: {result.get('quality_score', 0):.3f}")
        
        if result['status'] == 'success':
            print(f"Throughput: {result.get('throughput_records_per_second', 0):.1f} records/sec")
            
            # Print validation metrics
            metrics = result.get('validation_metrics', {})
            if metrics:
                print(f"\nValidation Summary:")
                print(f"  Valid Records: {metrics.get('valid', 0)}")
                print(f"  Invalid Records: {metrics.get('invalid', 0)}")
                print(f"  Duplicates Skipped: {metrics.get('duplicates', 0)}")
                
                # Top issues
                issues = metrics.get('issue_counts', {})
                if issues:
                    print(f"\nTop Data Quality Issues:")
                    for issue, count in sorted(issues.items(), key=lambda x: x[1], reverse=True)[:5]:
                        print(f"  {issue}: {count}")
        else:
            print(f"Error: {result.get('error', 'Unknown error')}")
        
        # Get pipeline status
        status = pipeline.get_pipeline_status()
        if status.get('pipeline_ready'):
            print(f"\nPipeline Status: Ready")
            print(f"Database Connected: {status.get('database_connected', False)}")
        
        # Cleanup
        pipeline.cleanup_and_shutdown()
        
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()