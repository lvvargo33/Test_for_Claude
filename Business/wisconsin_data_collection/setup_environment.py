#!/usr/bin/env python3
"""
Environment Setup Script
========================

Sets up the Wisconsin data collection environment with proper dependency management,
error handling, and validation.
"""

import sys
import subprocess
import importlib
import os
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True

def install_requirements():
    """Install requirements with better error handling"""
    print("\n🔧 Installing requirements...")
    
    requirements_file = Path(__file__).parent / "requirements.txt"
    if not requirements_file.exists():
        print("❌ requirements.txt not found")
        return False
    
    try:
        # Install core requirements
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", 
            "-r", str(requirements_file),
            "--upgrade"
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ Core requirements installed successfully")
            return True
        else:
            print(f"❌ Failed to install requirements: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Installation timed out. Try installing manually:")
        print("   pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Installation error: {e}")
        return False

def test_imports():
    """Test critical imports"""
    print("\n🧪 Testing imports...")
    
    critical_modules = [
        ('yaml', 'PyYAML'),
        ('pydantic', 'Pydantic'),
        ('requests', 'Requests'),
        ('bs4', 'BeautifulSoup4'),
        ('dotenv', 'python-dotenv'),
        ('tenacity', 'Tenacity')
    ]
    
    all_imports_ok = True
    
    for module_name, package_name in critical_modules:
        try:
            importlib.import_module(module_name)
            print(f"✅ {package_name}")
        except ImportError as e:
            print(f"❌ {package_name}: {e}")
            all_imports_ok = False
    
    return all_imports_ok

def test_yaml_configuration():
    """Test YAML configuration loading"""
    print("\n📄 Testing configuration...")
    
    try:
        import yaml
        config_file = Path(__file__).parent / "data_sources.yaml"
        
        if not config_file.exists():
            print("❌ data_sources.yaml not found")
            return False
            
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Basic validation
        if 'states' not in config:
            print("❌ Invalid configuration: 'states' section missing")
            return False
            
        if 'wisconsin' not in config['states']:
            print("❌ Invalid configuration: Wisconsin configuration missing")
            return False
            
        print("✅ Configuration file loaded and validated")
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

def test_data_models():
    """Test Pydantic data models"""
    print("\n🏗️  Testing data models...")
    
    try:
        from models import BusinessEntity, BusinessType, BusinessStatus, DataSource
        
        # Test basic model creation
        test_business = BusinessEntity(
            business_id="TEST001",
            source_id="TEST_SRC_001",
            business_name="Test Business LLC",
            business_type=BusinessType.RESTAURANT,
            status=BusinessStatus.ACTIVE,
            city="Madison",
            state="WI",
            data_source=DataSource.STATE_REGISTRATION
        )
        
        print("✅ Data models working properly")
        return True
        
    except Exception as e:
        print(f"❌ Data model test failed: {e}")
        return False

def setup_logging_directory():
    """Create logs directory if it doesn't exist"""
    print("\n📁 Setting up logging...")
    
    logs_dir = Path(__file__).parent / "logs"
    try:
        logs_dir.mkdir(exist_ok=True)
        print(f"✅ Logs directory: {logs_dir}")
        return True
    except Exception as e:
        print(f"❌ Failed to create logs directory: {e}")
        return False

def create_environment_file():
    """Create .env template if it doesn't exist"""
    print("\n⚙️  Setting up environment...")
    
    env_file = Path(__file__).parent / ".env"
    env_template = Path(__file__).parent / ".env.template"
    
    if not env_file.exists():
        template_content = """# Wisconsin Data Collection Environment Variables
# =============================================

# BigQuery Configuration (optional)
# GOOGLE_CLOUD_PROJECT=your-project-id
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# API Keys (if needed)
# MILWAUKEE_API_KEY=your-milwaukee-api-key
# MADISON_API_KEY=your-madison-api-key

# Processing Configuration
BATCH_SIZE=1000
MAX_CONCURRENT_REQUESTS=5
REQUEST_TIMEOUT_SECONDS=30

# Logging Level
LOG_LEVEL=INFO
"""
        try:
            with open(env_file, 'w') as f:
                f.write(template_content)
            print(f"✅ Created environment template: {env_file}")
            print("   Edit this file with your specific configuration")
            return True
        except Exception as e:
            print(f"❌ Failed to create environment file: {e}")
            return False
    else:
        print(f"✅ Environment file exists: {env_file}")
        return True

def run_offline_test():
    """Run the offline architecture test"""
    print("\n🚀 Running offline architecture test...")
    
    try:
        test_file = Path(__file__).parent / "test_architecture_offline.py"
        if not test_file.exists():
            print("❌ test_architecture_offline.py not found")
            return False
            
        result = subprocess.run([
            sys.executable, str(test_file)
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Offline architecture test passed")
            # Print last few lines of output for summary
            lines = result.stdout.strip().split('\n')
            for line in lines[-5:]:
                if line.strip():
                    print(f"   {line}")
            return True
        else:
            print(f"❌ Offline test failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Test timed out")
        return False
    except Exception as e:
        print(f"❌ Test execution error: {e}")
        return False

def main():
    """Main setup function"""
    print("🎯 Wisconsin Data Collection - Environment Setup")
    print("=" * 50)
    
    setup_steps = [
        ("Python Version Check", check_python_version),
        ("Install Requirements", install_requirements),
        ("Test Imports", test_imports),
        ("Test Configuration", test_yaml_configuration),
        ("Test Data Models", test_data_models),
        ("Setup Logging", setup_logging_directory),
        ("Create Environment File", create_environment_file),
        ("Run Offline Test", run_offline_test)
    ]
    
    results = []
    
    for step_name, step_func in setup_steps:
        print(f"\n📋 {step_name}")
        print("-" * (len(step_name) + 5))
        try:
            result = step_func()
            results.append((step_name, result))
        except Exception as e:
            print(f"❌ {step_name} crashed: {e}")
            results.append((step_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 SETUP SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for step_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {step_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} steps completed successfully")
    
    if passed == total:
        print("\n🎉 Environment setup complete!")
        print("\n📋 Next steps:")
        print("   1. Edit .env file with your configuration")
        print("   2. For BigQuery: pip install google-cloud-bigquery pandas pyarrow")
        print("   3. Run: python main.py --setup")
        print("   4. Run: python main.py --collect")
    else:
        print(f"\n⚠️  {total - passed} steps failed. Please address the issues above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)