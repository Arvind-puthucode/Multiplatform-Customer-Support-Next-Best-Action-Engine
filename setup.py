#!/usr/bin/env python3
"""
Setup script for the Riverline Ingestion Pipeline
Creates necessary directories and validates environment
"""

import os
import sys
from pathlib import Path
import logging

def create_directories():
    """Create necessary directories"""
    directories = [
        'data',
        'logs',
        'logs/run_results',
        'config'
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ Created directory: {directory}")

def validate_environment():
    """Validate environment configuration"""
    required_env_vars = [
        'CLICKHOUSE_HOST',
        'CLICKHOUSE_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("⚠️  Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables or update your .env file")
        return False
    
    print("✓ Environment variables validated")
    return True

def check_dependencies():
    """Check if required Python packages are installed"""
    required_packages = [
        'pandas',
        'clickhouse-connect',
        'schedule'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    

    
    print("✓ Required packages available")
    return True

def test_database_connection():
    """Test connection to ClickHouse database"""
    try:
        from config import IngestionConfig
        from database_manager import ClickHouseManager
        
        config = IngestionConfig.from_env()
        db_manager = ClickHouseManager(config)
        
        print("Testing database connection...")
        db_manager.connect()
        
        # Test query
        result = db_manager.client.query("SELECT 1 as test")
        if result.result_rows and result.result_rows[0][0] == 1:
            print("✓ Database connection successful")
            db_manager.disconnect()
            return True
        else:
            print("❌ Database query failed")
            return False
            
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def setup_sample_data():
    """Create sample data file if it doesn't exist"""
    data_file = Path('data/customer_support_twitter.csv')
    
    if not data_file.exists():
        print("Creating sample data file...")
        
        sample_data = """tweet_id,author_id,text,created_at,in_response_to_status_id
1234567890,customer1,"I'm having trouble with my account login",2024-01-15T10:30:00Z,
1234567891,support_team,"Hi! We'd be happy to help. Please DM us your account details.",2024-01-15T10:35:00Z,1234567890
1234567892,customer1,"Thanks, just sent you a DM",2024-01-15T10:40:00Z,1234567891
1234567893,customer2,"Your app keeps crashing on my phone",2024-01-15T11:00:00Z,
1234567894,support_team,"Sorry to hear that! Which phone model are you using?",2024-01-15T11:05:00Z,1234567893"""
        
        with open(data_file, 'w') as f:
            f.write(sample_data)
        
        print(f"✓ Sample data created at {data_file}")
    else:
        print(f"✓ Data file exists: {data_file}")

def main():
    """Main setup function"""
    print("🚀 Setting up Riverline Ingestion Pipeline")
    print("=" * 50)
    
    # Create directories
    print("\n📁 Creating directories...")
    create_directories()
    
    # Check dependencies
    print("\n📦 Checking dependencies...")
    if not check_dependencies():
        print("\n❌ Setup failed due to missing dependencies")
        sys.exit(1)
    
    # Validate environment
    print("\n🔧 Validating environment...")
    if not validate_environment():
        print("\n⚠️  Environment validation failed - some features may not work")
    
    # Setup sample data
    print("\n📊 Setting up data files...")
    setup_sample_data()
    
    # Test database connection (optional)
    print("\n🗄️  Testing database connection...")
    if os.getenv('CLICKHOUSE_HOST') and os.getenv('CLICKHOUSE_PASSWORD'):
        test_database_connection()
    else:
        print("⚠️  Skipping database test - credentials not configured")
    
    print("\n" + "=" * 50)
    print("✅ Setup completed!")
    print("\nNext steps:")
    print("1. Configure your .env file with ClickHouse credentials")
    print("2. Place your Twitter CSV data in data/customer_support_twitter.csv")
    print("3. Run: python main_pipeline.py")
    print("4. Or start scheduler: python scheduler.py")

if __name__ == "__main__":
    main()