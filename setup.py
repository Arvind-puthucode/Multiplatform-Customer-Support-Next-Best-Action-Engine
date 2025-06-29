#!/usr/bin/env python3
"""
Simple setup script for Riverline Pipeline
"""

import sys
from pathlib import Path

# Add current directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent))

from utils import setup_project, validate_environment, load_env_file


def main():
    """Main setup function"""
    print("🚀 Riverline Pipeline Setup")
    print("=" * 40)
    
    # Setup project structure
    if not setup_project():
        print("❌ Project setup failed")
        return False
    
    # Load environment if .env exists
    if Path('.env').exists():
        print("\n📋 Loading environment variables...")
        load_env_file('.env')
        
        # Validate environment
        env_validation = validate_environment()
        
        if env_validation['valid']:
            print("✅ Environment validation passed")
        else:
            print("⚠️  Environment validation issues:")
            if env_validation['missing_required']:
                print(f"   Missing required: {env_validation['missing_required']}")
            if env_validation['missing_optional']:
                print(f"   Missing optional: {env_validation['missing_optional']}")
    else:
        print("\n📝 No .env file found - using .env.example as template")
    
    print("\n🎉 Setup complete! Ready to run pipeline.")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)