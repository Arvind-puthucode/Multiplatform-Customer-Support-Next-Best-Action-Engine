"""
Automated scheduler for running the ingestion pipeline
"""

import schedule
import time
import logging
from datetime import datetime
from pathlib import Path
import sys
import os
from typing import Dict, Any
import json

from config import IngestionConfig
from main_pipeline import TwitterIngestionPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IngestionScheduler:
    """Handles scheduled execution of the ingestion pipeline"""
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.pipeline = None
        self.last_run_status = None
        self.run_count = 0
        self.is_running = False
        
        # Create logs directory
        Path('logs').mkdir(exist_ok=True)
    
    def setup(self):
        """Initialize the pipeline"""
        try:
            self.pipeline = TwitterIngestionPipeline(self.config)
            self.pipeline.setup()
            logger.info("Pipeline scheduler initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            raise
    
    def run_scheduled_ingestion(self):
        """Execute the ingestion pipeline with error handling"""
        if self.is_running:
            logger.warning("Ingestion already running, skipping scheduled run")
            return
        
        self.is_running = True
        self.run_count += 1
        
        logger.info(f"Starting scheduled ingestion run #{self.run_count}")
        
        try:
            start_time = datetime.now()
            
            # Reinitialize pipeline if needed
            if not self.pipeline:
                self.pipeline = TwitterIngestionPipeline(self.config)
                self.pipeline.setup()
            
            # Run the ingestion pipeline
            result = self.pipeline.run_ingestion()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Store run status
            self.last_run_status = {
                'run_number': self.run_count,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'status': result['status'],
                'records_processed': result.get('records_processed', 0),
                'quality_score': result.get('quality_score', 0.0),
                'batches_processed': result.get('batches_processed', 0),
                'errors': result.get('errors', 0)
            }
            
            # Log results
            if result['status'] == 'success':
                logger.info(
                    f"Ingestion run #{self.run_count} completed successfully: "
                    f"{result.get('records_processed', 0)} records in {duration:.1f}s "
                    f"(Quality: {result.get('quality_score', 0):.3f})"
                )
                
                # Save detailed results
                self._save_run_results(result)
                
            else:
                logger.error(
                    f"Ingestion run #{self.run_count} failed: "
                    f"{result.get('error', 'Unknown error')}"
                )
                
        except Exception as e:
            logger.error(f"Unexpected error in scheduled ingestion run #{self.run_count}: {e}")
            
            self.last_run_status = {
                'run_number': self.run_count,
                'start_time': datetime.now().isoformat(),
                'status': 'error',
                'error': str(e),
                'records_processed': 0
            }
            
        finally:
            self.is_running = False
    
    def _save_run_results(self, result: Dict):
        """Save detailed run results to file"""
        try:
            results_dir = Path('logs/run_results')
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = results_dir / f"ingestion_run_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(result, f, indent=2, default=str)
                
            logger.info(f"Run results saved to {filename}")
            
        except Exception as e:
            logger.warning(f"Could not save run results: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        return {
            'scheduler_active': True,
            'is_currently_running': self.is_running,
            'total_runs': self.run_count,
            'last_run': self.last_run_status,
            'next_run': schedule.next_run().isoformat() if schedule.next_run() else None,
            'pipeline_config': {
                'data_source': self.config.data_source_path,
                'batch_size': self.config.batch_size,
                'database': self.config.clickhouse_database
            }
        }
    
    def manual_run(self) -> Dict:
        """Manually trigger an ingestion run"""
        logger.info("Manual ingestion run triggered")
        self.run_scheduled_ingestion()
        return self.last_run_status or {'status': 'error', 'error': 'No run status available'}
    
    def health_check(self) -> Dict:
        """Perform health check on pipeline components"""
        try:
            if not self.pipeline:
                self.pipeline = TwitterIngestionPipeline(self.config)
                self.pipeline.setup()
            
            status = self.pipeline.get_pipeline_status()
            
            health = {
                'overall_health': 'healthy' if status.get('pipeline_ready') else 'unhealthy',
                'database_connected': status.get('database_connected', False),
                'last_watermark': status.get('last_watermark'),
                'data_source_exists': os.path.exists(self.config.data_source_path),
                'recent_activity': status.get('recent_ingestion', {}),
                'timestamp': datetime.now().isoformat()
            }
            
            return health
            
        except Exception as e:
            return {
                'overall_health': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

def create_scheduler_from_env() -> IngestionScheduler:
    """Create scheduler with configuration from environment"""
    config = IngestionConfig.from_env()
    config.validate()
    
    return IngestionScheduler(config)

def main():
    """Main scheduler execution"""
    logger.info("Starting Riverline Ingestion Pipeline Scheduler")
    
    try:
        # Initialize scheduler
        scheduler = create_scheduler_from_env()
        scheduler.setup()
        
        # Schedule regular jobs
        # Every 6 hours for continuous ingestion
        schedule.every(6).hours.do(scheduler.run_scheduled_ingestion)
        
        # Daily health check at 2 AM
        schedule.every().day.at("02:00").do(scheduler.health_check)
        
        # Weekly cleanup (optional)
        schedule.every().sunday.at("03:00").do(
            lambda: logger.info("Weekly maintenance would run here")
        )
        
        # Run immediately on startup if configured
        if os.getenv('RUN_ON_STARTUP', 'false').lower() == 'true':
            logger.info("Running initial ingestion on startup")
            scheduler.run_scheduled_ingestion()
        
        # Print initial status
        status = scheduler.get_status()
        logger.info(f"Scheduler initialized: {status['total_runs']} previous runs")
        
        # Health check
        health = scheduler.health_check()
        logger.info(f"Initial health check: {health['overall_health']}")
        
        logger.info("Scheduler started. Waiting for scheduled runs...")
        logger.info("Press Ctrl+C to stop the scheduler")
        
        # Main loop
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        
    except Exception as e:
        logger.error(f"Scheduler crashed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup
        try:
            if 'scheduler' in locals() and scheduler.pipeline:
                scheduler.pipeline.cleanup_and_shutdown()
        except:
            pass
        
        logger.info("Scheduler shutdown complete")

if __name__ == "__main__":
    main()