"""
Utility functions and helpers
Common functionality shared across pipeline components
"""

import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any
import json
from datetime import datetime


def setup_logging(log_level: str = "INFO", log_file: str = None) -> logging.Logger:
    """Setup logging configuration"""
    
    # Create logs directory if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Setup file handler if specified
    handlers = [console_handler]
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=handlers,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    return logging.getLogger(__name__)


def load_env_file(env_path: str = ".env") -> Dict[str, str]:
    """Load environment variables from .env file"""
    env_vars = {}
    env_file = Path(env_path)
    
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip().strip('"\'')
                    os.environ[key.strip()] = value.strip().strip('"\'')
    
    return env_vars


def create_project_structure() -> bool:
    """Create necessary project directories"""
    directories = [
        'data',
        'logs',
        'config',
        'tests'
    ]
    
    try:
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)
            print(f"✓ Created directory: {directory}")
        return True
    except Exception as e:
        print(f"✗ Failed to create directories: {e}")
        return False


def save_run_result(result: Dict[str, Any], run_id: str = None) -> str:
    """Save pipeline run results to file"""
    try:
        # Create results directory
        results_dir = Path('logs/run_results')
        results_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        if not run_id:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            run_id = f"run_{timestamp}"
        
        filename = results_dir / f"{run_id}.json"
        
        # Add metadata
        result['run_metadata'] = {
            'run_id': run_id,
            'saved_at': datetime.utcnow().isoformat(),
            'hostname': os.uname().nodename if hasattr(os, 'uname') else 'unknown'
        }
        
        # Save to file
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        print(f"✓ Run results saved to: {filename}")
        return str(filename)
        
    except Exception as e:
        print(f"✗ Failed to save run results: {e}")
        return ""


def validate_csv_structure(csv_path: str, required_columns: list = None) -> Dict[str, Any]:
    """Validate CSV file structure"""
    import pandas as pd
    
    if not required_columns:
        required_columns = ['tweet_id', 'author_id', 'text', 'created_at']
    
    try:
        # Check if file exists
        csv_file = Path(csv_path)
        if not csv_file.exists():
            return {
                'valid': False,
                'error': f"CSV file not found: {csv_path}"
            }
        
        # Read sample to check structure
        sample_df = pd.read_csv(csv_path, nrows=100)
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in sample_df.columns]
        
        if missing_columns:
            return {
                'valid': False,
                'error': f"Missing required columns: {missing_columns}",
                'found_columns': list(sample_df.columns)
            }
        
        # Get file stats
        total_rows = sum(1 for _ in open(csv_path)) - 1  # Subtract header
        
        return {
            'valid': True,
            'total_rows': total_rows,
            'columns': list(sample_df.columns),
            'sample_data': sample_df.head(3).to_dict('records'),
            'file_size_mb': csv_file.stat().st_size / (1024 * 1024)
        }
        
    except Exception as e:
        return {
            'valid': False,
            'error': f"Failed to validate CSV: {e}"
        }


def check_dependencies() -> Dict[str, bool]:
    """Check if required dependencies are installed"""
    required_packages = {
        'pandas': 'pandas',
        'supabase': 'supabase',
        'logging': 'logging'  # Built-in
    }
    
    results = {}
    
    for package_name, import_name in required_packages.items():
        try:
            __import__(import_name)
            results[package_name] = True
        except ImportError:
            results[package_name] = False
    
    return results


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def format_processing_rate(records: int, seconds: float) -> str:
    """Format processing rate in human-readable format"""
    if seconds <= 0:
        return "N/A"
    
    rate = records / seconds
    
    if rate < 1:
        return f"{rate:.2f} records/second"
    elif rate < 60:
        return f"{rate:.1f} records/second"
    else:
        rate_per_minute = rate * 60
        return f"{rate_per_minute:.0f} records/minute"


def print_pipeline_summary(result: Dict[str, Any]) -> None:
    """Print a formatted summary of pipeline results"""
    print("\n" + "="*50)
    print("PIPELINE RUN SUMMARY")
    print("="*50)
    
    status = result.get('status', 'unknown')
    print(f"Status: {status.upper()}")
    
    if status == 'success':
        print(f"✓ Records processed: {result.get('records_processed', 0):,}")
        print(f"✓ Batches processed: {result.get('batches_processed', 0)}")
        
        duration = result.get('duration_seconds', 0)
        print(f"✓ Duration: {format_duration(duration)}")
        
        if result.get('records_processed', 0) > 0 and duration > 0:
            rate = format_processing_rate(result['records_processed'], duration)
            print(f"✓ Processing rate: {rate}")
        
        if result.get('last_processed_id'):
            print(f"✓ Last processed ID: {result['last_processed_id']}")
    
    else:
        print(f"✗ Error: {result.get('error_message', 'Unknown error')}")
        print(f"✗ Records processed before error: {result.get('records_processed', 0)}")
    
    errors = result.get('errors', 0)
    if errors > 0:
        print(f"⚠ Errors encountered: {errors}")
    
    print("="*50)


def create_sample_env_file() -> bool:
    """Create a sample .env file with required variables"""
    env_content = """# Supabase Configuration
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key

# Data Configuration
CSV_PATH=data/customer_support_twitter.csv
BATCH_SIZE=1000

# Text Processing
MAX_TEXT_LENGTH=1000
MIN_TEXT_LENGTH=5

# Logging
LOG_LEVEL=INFO
"""
    
    try:
        env_file = Path('.env.example')
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        print(f"✓ Sample environment file created: {env_file}")
        print("  Copy this to .env and update with your actual values")
        return True
        
    except Exception as e:
        print(f"✗ Failed to create sample .env file: {e}")
        return False


def validate_environment() -> Dict[str, Any]:
    """Validate environment configuration"""
    required_vars = ['SUPABASE_URL', 'SUPABASE_KEY']
    optional_vars = ['CSV_PATH', 'BATCH_SIZE', 'LOG_LEVEL']
    
    validation_result = {
        'valid': True,
        'missing_required': [],
        'missing_optional': [],
        'found_vars': {}
    }
    
    # Check required variables
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            validation_result['missing_required'].append(var)
            validation_result['valid'] = False
        else:
            validation_result['found_vars'][var] = '***' if 'key' in var.lower() else value
    
    # Check optional variables
    for var in optional_vars:
        value = os.getenv(var)
        if not value:
            validation_result['missing_optional'].append(var)
        else:
            validation_result['found_vars'][var] = value
    
    return validation_result


class ProgressTracker:
    """Simple progress tracking utility"""
    
    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = datetime.now()
    
    def update(self, increment: int = 1) -> None:
        """Update progress"""
        self.current += increment
        self._print_progress()
    
    def _print_progress(self) -> None:
        """Print current progress"""
        if self.total <= 0:
            return
        
        percentage = (self.current / self.total) * 100
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        if elapsed > 0 and self.current > 0:
            rate = self.current / elapsed
            eta_seconds = (self.total - self.current) / rate if rate > 0 else 0
            eta_str = format_duration(eta_seconds)
        else:
            eta_str = "Unknown"
        
        print(f"\r{self.description}: {self.current:,}/{self.total:,} ({percentage:.1f}%) - ETA: {eta_str}", end='', flush=True)
    
    def finish(self) -> None:
        """Mark progress as complete"""
        print()  # New line
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print(f"✓ Completed {self.current:,} items in {format_duration(elapsed)}")


def setup_project() -> bool:
    """Setup the entire project structure and files"""
    print("Setting up Riverline Pipeline Project...")
    
    # Create directories
    if not create_project_structure():
        return False
    
    # Create sample .env file
    if not create_sample_env_file():
        return False
    
    # Check dependencies
    deps = check_dependencies()
    missing_deps = [pkg for pkg, available in deps.items() if not available]
    
    if missing_deps:
        print(f"\n⚠ Missing dependencies: {missing_deps}")
        print("Install them with: pip install pandas supabase")
    else:
        print("✓ All dependencies available")
    
    print("\n" + "="*50)
    print("PROJECT SETUP COMPLETE")
    print("="*50)
    print("Next steps:")
    print("1. Copy .env.example to .env")
    print("2. Update .env with your Supabase credentials")
    print("3. Place your CSV data in data/customer_support_twitter.csv")
    print("4. Run: python pipeline_runner.py")
    print("="*50)
    
    return True


# Quick test functions
def test_csv_loading(csv_path: str = "data/customer_support_twitter.csv") -> bool:
    """Quick test of CSV loading functionality"""
    print(f"Testing CSV loading: {csv_path}")
    
    validation = validate_csv_structure(csv_path)
    
    if validation['valid']:
        print(f"✓ CSV validation passed")
        print(f"  - Total rows: {validation['total_rows']:,}")
        print(f"  - Columns: {validation['columns']}")
        print(f"  - File size: {validation['file_size_mb']:.1f} MB")
        return True
    else:
        print(f"✗ CSV validation failed: {validation['error']}")
        return False


def test_supabase_connection(supabase_url: str, supabase_key: str) -> bool:
    """Quick test of Supabase connection"""
    try:
        from storage import SupabaseStorage
        
        print("Testing Supabase connection...")
        storage = SupabaseStorage(supabase_url, supabase_key)
        
        if storage.connect():
            health = storage.health_check()
            print(f"✓ Supabase connection successful")
            print(f"  - Status: {health['status']}")
            return True
        else:
            print("✗ Supabase connection failed")
            return False
            
    except Exception as e:
        print(f"✗ Supabase test failed: {e}")
        return False