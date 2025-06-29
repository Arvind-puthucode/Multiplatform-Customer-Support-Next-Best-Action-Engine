"""
Main pipeline orchestrator
Coordinates data loading, processing, and storage
"""

import logging
from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from riverline_backend.pipeline.config import PipelineConfig
from riverline_backend.pipeline.data_loader import CSVLoader
from riverline_backend.pipeline.data_processor import DataProcessor
from riverline_backend.pipeline.storage import SupabaseStorage

logger = logging.getLogger(__name__)
load_dotenv()


class PipelineRunner:
    """Main pipeline orchestrator that coordinates all components"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.loader = CSVLoader(config.csv_path,sample=True)
        self.processor = DataProcessor(
            max_text_length=config.max_text_length,
            min_text_length=config.min_text_length
        )
        self.storage = SupabaseStorage(config.supabase_url, config.supabase_key)

        # Pipeline state
        self.total_processed = 0
        self.total_errors = 0
        self.start_time = None

    def setup(self) -> bool:
        """Initialize all pipeline components"""
        logger.info("Setting up pipeline components...")

        try:
            # Validate configuration (already done by Pydantic on instantiation)

            # Validate CSV file
            if not self.loader.validate_file():
                return False

            # Connect to database
            if not self.storage.connect():
                return False


            logger.info("Pipeline setup completed successfully")
            return True

        except Exception as e:
            logger.error(f"Pipeline setup failed: {e}")
            return False

    def run_once(self, force_full_reload: bool = False) -> Dict:
        """Run the pipeline once with incremental processing"""
        self.start_time = datetime.utcnow()

        logger.info("Starting pipeline run...")

        try:
            # Get last processed ID unless forcing full reload
            last_processed_id = None
            if not force_full_reload:
                last_processed_id = self.storage.get_last_processed_id()
                if last_processed_id:
                    logger.info(f"Resuming from last processed ID: {last_processed_id}")

            # Load data incrementally
            batch_iterator = self.loader.get_batch_iterator(
                batch_size=self.config.batch_size,
                last_processed_id=last_processed_id
            )

            # Process batches
            batch_count = 0
            last_tweet_id = last_processed_id

            for batch_df in batch_iterator:
                batch_count += 1
                logger.info(f"Processing batch {batch_count} with {len(batch_df)} records")

                # Process the batch
                processed_records = self.processor.process_batch(batch_df)

                if processed_records:
                    # Insert to database
                    insert_result = self.storage.insert_batch(processed_records)

                    # Update counters
                    self.total_processed += insert_result['inserted']
                    self.total_errors += insert_result['errors']

                    # Track last processed ID
                    if processed_records:
                        last_tweet_id = processed_records[-1]['tweet_id']

                    logger.info(f"Batch {batch_count} completed: {insert_result}")
                else:
                    logger.warning(f"Batch {batch_count} produced no valid records")

            # Update pipeline status
            if last_tweet_id:
                self.storage.update_pipeline_status(
                    last_processed_id=last_tweet_id,
                    records_processed=self.total_processed,
                    status='completed'
                )

            # Calculate duration
            duration = (datetime.utcnow() - self.start_time).total_seconds()

            result = {
                'status': 'success',
                'records_processed': self.total_processed,
                'batches_processed': batch_count,
                'errors': self.total_errors,
                'duration_seconds': duration,
                'last_processed_id': last_tweet_id,
                'processing_rate': self.total_processed / duration if duration > 0 else 0
            }

            logger.info(f"Pipeline run completed: {result}")
            return result

        except Exception as e:
            # Log error and update status
            error_msg = str(e)
            logger.error(f"Pipeline run failed: {error_msg}")

            self.storage.update_pipeline_status(
                last_processed_id=last_processed_id or '',
                records_processed=self.total_processed,
                status='failed',
                error_message=error_msg
            )

            duration = (datetime.utcnow() - self.start_time).total_seconds() if self.start_time else 0

            return {
                'status': 'error',
                'records_processed': self.total_processed,
                'errors': self.total_errors,
                'duration_seconds': duration,
                'error_message': error_msg
            }

    def run_sample(self, n_rows: int = 100) -> Dict:
        """Run pipeline on a small sample for testing"""
        logger.info(f"Running pipeline on sample of {n_rows} rows")

        try:
            # Load sample data
            sample_df = self.loader.get_sample_data(n_rows)

            if sample_df.empty:
                return {'status': 'error', 'message': 'No sample data loaded'}

            # Process sample
            processed_records = self.processor.process_batch(sample_df)

            if not processed_records:
                return {'status': 'error', 'message': 'No valid records in sample'}

            # Insert to database
            insert_result = self.storage.insert_batch(processed_records)

            return {
                'status': 'success',
                'sample_size': len(sample_df),
                'processed_records': len(processed_records),
                'insert_result': insert_result
            }

        except Exception as e:
            logger.error(f"Sample run failed: {e}")
            return {'status': 'error', 'error_message': str(e)}

    def get_status(self) -> Dict:
        """Get current pipeline status"""
        try:
            # Get database health
            db_health = self.storage.health_check()

            # Get CSV info
            csv_info = self.loader.get_column_info()

            # Get processing stats
            processing_stats = self.processor.get_processing_stats()

            return {
                'pipeline_ready': db_health['status'] == 'healthy',
                'database': db_health,
                'csv_file': {
                    'path': str(self.loader.csv_path),
                    'exists': self.loader.csv_path.exists(),
                    'total_rows': self.loader.get_total_rows(),
                    'columns': csv_info.get('columns', [])
                },
                'processor': processing_stats,
                'last_run': self._get_last_run_info()
            }

        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            return {
                'pipeline_ready': False,
                'error': str(e)
            }

    def _get_last_run_info(self) -> Dict:
        """Get information about the last pipeline run"""
        try:
            # This would typicalrun_samplely come from a pipeline_runs table
            # For now, just return basic info
            return {
                'total_processed': self.total_processed,
                'total_errors': self.total_errors,
                'last_run_time': self.start_time.isoformat() if self.start_time else None
            }
        except Exception:
            return {}

    def cleanup(self):
        """Cleanup resources"""
        try:
            # Clear processor cache if needed
            self.processor.processed_hashes.clear()
            logger.info("Pipeline cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")


def main():
    """Main entry point for running the pipeline"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # Load configuration
        config = PipelineConfig()

        # Create and setup pipeline
        pipeline = PipelineRunner(config)

        if not pipeline.setup():
            logger.error("Pipeline setup failed")
            return

        # Run the pipeline
        result = pipeline.run_sample()

        print(f"Pipeline completed with status: {result['status']}")
        print(f"Records processed: {result['records_processed']}")

        if result['status'] == 'error':
            print(f"Error: {result.get('error_message', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Main execution failed: {e}")

    finally:
        # Cleanup
        if 'pipeline' in locals():
            pipeline.cleanup()


if __name__ == "__main__":
    main()