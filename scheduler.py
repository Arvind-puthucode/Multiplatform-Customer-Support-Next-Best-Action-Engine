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
        logging.FileHandler('riverline_backend/logs/scheduler.log'),
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
        Path('riverline_backend/logs').mkdir(exist_ok=True)
    
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
            
            # Run the ingestion
            result = self.pipeline.run_ingestion()
            
            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Update status
            self.last_run_status = {
                'run_number': self.run_count,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'status': result['status'],
                'records_processed': result.get('records_processed', 0),
                'error': result.get('error', None)
            }
            
            # Log results
            if result['status'] == 'success':
                logger.info(
                    f"Scheduled run #{self.run_count} completed successfully: "
                    f"{result.get('records_processed', 0)} records processed in {duration:.1f}s"
                )
            else:
                logger.error(
                    f"Scheduled run #{self.run_count} failed: {result.get('error', 'Unknown error')}"
                )
            
            # Save run results
            self._save_run_results(self.last_run_status)
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Scheduled run #{self.run_count} crashed after {duration:.1f}s: {e}")
            
            self.last_run_status = {
                'run_number': self.run_count,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'status': 'error',
                'records_processed': 0,
                'error': str(e)
            }
            
            self._save_run_results(self.last_run_status)
            
        finally:
            self.is_running = False
            
        return self.last_run_status
    
    def _save_run_results(self, results: Dict):
        """Save run results to file"""
        try:
            results_dir = Path('riverline_backend/logs/run_results')
            results_dir.mkdir(exist_ok=True)
            
            filename = f"run_{results['run_number']:04d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            results_file = results_dir / filename
            
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save run results: {e}")
    
    def get_status(self) -> Dict:
        """Get current scheduler status"""
        return {
            'is_running': self.is_running,
            'total_runs': self.run_count,
            'last_run': self.last_run_status,
            'pipeline_ready': self.pipeline is not None
        }
    
    def health_check(self) -> Dict:
        """Perform health check on the pipeline and environment"""
        try:
            health = {
                'overall_health': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'checks': {}
            }
            
            # Check if pipeline is initialized
            health['checks']['pipeline_initialized'] = self.pipeline is not None
            
            # Check if data source exists
            data_source_exists = os.path.exists(self.config.data_source_path)
            health['checks']['data_source_exists'] = data_source_exists
            
            # Check database connectivity
            if self.pipeline:
                try:
                    status = self.pipeline.get_pipeline_status()
                    health['checks']['database_connected'] = status.get('database_connected', False)
                    health['checks']['pipeline_ready'] = status.get('pipeline_ready', False)
                except Exception as e:
                    health['checks']['database_connected'] = False
                    health['checks']['pipeline_ready'] = False
                    health['checks']['database_error'] = str(e)
            
            # Check recent activity
            if self.last_run_status:
                last_run_time = datetime.fromisoformat(self.last_run_status['start_time'])
                hours_since_last_run = (datetime.now() - last_run_time).total_seconds() / 3600
                health['checks']['hours_since_last_run'] = hours_since_last_run
                health['checks']['last_run_successful'] = self.last_run_status.get('status') == 'success'
            
            # Check disk space (basic check)
            try:
                import shutil
                disk_usage = shutil.disk_usage('.')
                free_space_gb = disk_usage.free / (1024**3)
                health['checks']['free_disk_space_gb'] = round(free_space_gb, 2)
                health['checks']['sufficient_disk_space'] = free_space_gb > 1.0  # At least 1GB free
            except:
                health['checks']['disk_space_check_failed'] = True
            
            # Determine overall health
            critical_checks = [
                'pipeline_initialized',
                'data_source_exists', 
                'database_connected',
                'pipeline_ready'
            ]
            
            failed_critical = [check for check in critical_checks 
                             if not health['checks'].get(check, False)]
            
            if failed_critical:
                health['overall_health'] = 'unhealthy'
                health['failed_checks'] = failed_critical
            elif health['checks'].get('hours_since_last_run', 0) > 24:
                health['overall_health'] = 'warning'
                health['warning'] = 'No recent ingestion activity'
            
            logger.info(f"Health check completed: {health['overall_health']}")
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