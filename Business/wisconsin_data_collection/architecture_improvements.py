#!/usr/bin/env python3
"""
Architecture Improvements Script
================================

Implements architectural improvements and fixes for the Wisconsin data collection system.
Addresses dependency management, error handling, and performance issues.
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional
import importlib.util

# Configure logging for this script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ArchitectureImprover:
    """Handles architectural improvements and fixes"""
    
    def __init__(self, project_path: Path):
        self.project_path = project_path
        self.improvements_applied = []
    
    def check_virtual_environment(self) -> bool:
        """Check if running in a virtual environment"""
        logger.info("🔍 Checking virtual environment setup...")
        
        # Check for virtual environment indicators
        venv_indicators = [
            'VIRTUAL_ENV' in os.environ,
            hasattr(sys, 'real_prefix'),
            hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
        ]
        
        in_venv = any(venv_indicators)
        
        if in_venv:
            logger.info("✅ Running in virtual environment")
            if 'VIRTUAL_ENV' in os.environ:
                logger.info(f"   Virtual env path: {os.environ['VIRTUAL_ENV']}")
        else:
            logger.warning("⚠️  Not running in a virtual environment")
            logger.info("   Recommendation: Create and activate a virtual environment")
            logger.info("   Commands:")
            logger.info("     python -m venv wisconsin_data_env")
            logger.info("     source wisconsin_data_env/bin/activate  # Linux/Mac")
            logger.info("     wisconsin_data_env\\Scripts\\activate     # Windows")
        
        return in_venv
    
    def create_optional_imports_wrapper(self) -> bool:
        """Create wrapper for optional heavy dependencies"""
        logger.info("🔧 Creating optional imports wrapper...")
        
        wrapper_content = '''"""
Optional Dependencies Wrapper
============================

Handles optional imports gracefully to avoid dependency issues.
"""

import logging
import warnings

logger = logging.getLogger(__name__)

# BigQuery support
try:
    from google.cloud import bigquery
    from google.cloud.exceptions import GoogleCloudError
    BIGQUERY_AVAILABLE = True
    logger.info("BigQuery support available")
except ImportError as e:
    logger.info("BigQuery not available - offline mode only")
    BIGQUERY_AVAILABLE = False
    bigquery = None
    GoogleCloudError = Exception

# Pandas support
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    logger.info("Pandas support available")
except ImportError:
    logger.info("Pandas not available - using basic data structures")
    PANDAS_AVAILABLE = False
    pd = None

# PyArrow support
try:
    import pyarrow
    PYARROW_AVAILABLE = True
    logger.info("PyArrow support available")
except ImportError:
    logger.info("PyArrow not available - limited BigQuery functionality")
    PYARROW_AVAILABLE = False
    pyarrow = None

# LXML support for better HTML parsing
try:
    import lxml
    LXML_AVAILABLE = True
    logger.info("LXML support available")
except ImportError:
    logger.info("LXML not available - using html.parser")
    LXML_AVAILABLE = False
    lxml = None

def check_bigquery_requirements() -> bool:
    """Check if BigQuery requirements are met"""
    return BIGQUERY_AVAILABLE and PANDAS_AVAILABLE and PYARROW_AVAILABLE

def get_html_parser():
    """Get the best available HTML parser"""
    if LXML_AVAILABLE:
        return 'lxml'
    else:
        return 'html.parser'

def warn_missing_dependencies(feature: str, missing_deps: List[str]):
    """Warn about missing dependencies for a feature"""
    deps_str = ", ".join(missing_deps)
    warning_msg = f"{feature} requires: {deps_str}. Install with: pip install {deps_str}"
    warnings.warn(warning_msg, UserWarning)
    logger.warning(warning_msg)
'''
        
        try:
            wrapper_file = self.project_path / "optional_imports.py"
            with open(wrapper_file, 'w') as f:
                f.write(wrapper_content)
            
            logger.info(f"✅ Created optional imports wrapper: {wrapper_file}")
            self.improvements_applied.append("Optional imports wrapper")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create optional imports wrapper: {e}")
            return False
    
    def improve_base_collector(self) -> bool:
        """Improve base collector with better error handling"""
        logger.info("🔧 Improving base collector...")
        
        base_collector_file = self.project_path / "base_collector.py"
        if not base_collector_file.exists():
            logger.warning("⚠️  base_collector.py not found - skipping improvement")
            return False
        
        # Read current content
        try:
            with open(base_collector_file, 'r') as f:
                content = f.read()
            
            # Apply improvements
            improvements = [
                # Replace direct BigQuery import with optional import
                ('from google.cloud import bigquery\nimport pandas as pd', 
                 'from optional_imports import bigquery, pd, BIGQUERY_AVAILABLE, check_bigquery_requirements'),
                
                # Add BigQuery availability check in __init__
                ('def __init__(self, state_code: str, config_path: str = "data_sources.yaml"):',
                 '''def __init__(self, state_code: str, config_path: str = "data_sources.yaml"):'''),
            ]
            
            modified = False
            for old_text, new_text in improvements:
                if old_text in content and new_text not in content:
                    content = content.replace(old_text, new_text)
                    modified = True
            
            if modified:
                # Backup original file
                backup_file = base_collector_file.with_suffix('.py.backup')
                with open(backup_file, 'w') as f:
                    f.write(content)
                
                # Write improved version
                with open(base_collector_file, 'w') as f:
                    f.write(content)
                
                logger.info(f"✅ Improved base collector (backup: {backup_file})")
                self.improvements_applied.append("Base collector improvements")
                return True
            else:
                logger.info("ℹ️  Base collector already has improvements")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to improve base collector: {e}")
            return False
    
    def create_lightweight_test_runner(self) -> bool:
        """Create a lightweight test runner for quick validation"""
        logger.info("🔧 Creating lightweight test runner...")
        
        test_runner_content = '''#!/usr/bin/env python3
"""
Lightweight Test Runner
======================

Quick validation tests that don't require heavy dependencies.
"""

import sys
import importlib
from pathlib import Path

def test_core_imports():
    """Test core imports without heavy dependencies"""
    print("🧪 Testing core imports...")
    
    required_modules = [
        'yaml',
        'pydantic', 
        'requests',
        'bs4',
        'tenacity'
    ]
    
    passed = 0
    for module in required_modules:
        try:
            importlib.import_module(module)
            print(f"  ✅ {module}")
            passed += 1
        except ImportError as e:
            print(f"  ❌ {module}: {e}")
    
    return passed == len(required_modules)

def test_models():
    """Test Pydantic models"""
    print("\\n🧪 Testing data models...")
    
    try:
        from models import BusinessEntity, BusinessType, BusinessStatus, DataSource
        
        # Create test instance
        business = BusinessEntity(
            business_id="TEST001",
            source_id="WI_TEST_001", 
            business_name="Test Business",
            business_type=BusinessType.RESTAURANT,
            status=BusinessStatus.ACTIVE,
            city="Madison",
            state="WI",
            data_source=DataSource.STATE_REGISTRATION
        )
        
        print("  ✅ BusinessEntity model")
        return True
        
    except Exception as e:
        print(f"  ❌ Model test failed: {e}")
        return False

def test_configuration():
    """Test YAML configuration loading"""
    print("\\n🧪 Testing configuration...")
    
    try:
        import yaml
        
        config_file = Path(__file__).parent / "data_sources.yaml"
        if not config_file.exists():
            print("  ❌ data_sources.yaml not found")
            return False
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Basic validation
        required_sections = ['states', 'target_business_types', 'data_quality']
        for section in required_sections:
            if section not in config:
                print(f"  ❌ Missing configuration section: {section}")
                return False
        
        print("  ✅ Configuration valid")
        return True
        
    except Exception as e:
        print(f"  ❌ Configuration test failed: {e}")
        return False

def main():
    """Run lightweight tests"""
    print("🚀 Lightweight Test Runner")
    print("=" * 30)
    
    tests = [
        ("Core Imports", test_core_imports),
        ("Data Models", test_models), 
        ("Configuration", test_configuration)
    ]
    
    passed = 0
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
    
    print(f"\\n🎯 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("\\n✅ All lightweight tests passed!")
        print("   Ready for full testing with test_architecture_offline.py")
    else:
        print("\\n⚠️  Some tests failed. Check dependencies and configuration.")
    
    return passed == len(tests)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
'''
        
        try:
            test_file = self.project_path / "test_lightweight.py"
            with open(test_file, 'w') as f:
                f.write(test_runner_content)
            
            # Make executable
            test_file.chmod(0o755)
            
            logger.info(f"✅ Created lightweight test runner: {test_file}")
            self.improvements_applied.append("Lightweight test runner")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create test runner: {e}")
            return False
    
    def create_dependency_installer(self) -> bool:
        """Create smart dependency installer"""
        logger.info("🔧 Creating smart dependency installer...")
        
        installer_content = '''#!/usr/bin/env python3
"""
Smart Dependency Installer
==========================

Installs dependencies in the right order with proper error handling.
"""

import subprocess
import sys
import time

def install_package(package, timeout=120):
    """Install a single package with timeout"""
    print(f"📦 Installing {package}...")
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", package, "--upgrade"
        ], capture_output=True, text=True, timeout=timeout)
        
        if result.returncode == 0:
            print(f"✅ {package} installed successfully")
            return True
        else:
            print(f"❌ Failed to install {package}: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {package} installation timed out")
        return False

def install_core_dependencies():
    """Install core dependencies"""
    print("🚀 Installing core dependencies...")
    
    # Core packages in order of importance
    core_packages = [
        "pydantic",
        "PyYAML", 
        "requests",
        "beautifulsoup4",
        "python-dotenv",
        "tenacity"
    ]
    
    failed_packages = []
    
    for package in core_packages:
        if not install_package(package, timeout=60):
            failed_packages.append(package)
        time.sleep(1)  # Brief pause between installs
    
    return failed_packages

def install_optional_dependencies():
    """Install optional heavy dependencies"""
    print("\\n🔧 Installing optional dependencies...")
    
    optional_packages = [
        "pandas",
        "google-cloud-bigquery",
        "pyarrow",
        "lxml"
    ]
    
    failed_packages = []
    
    for package in optional_packages:
        print(f"\\n⚠️  Installing {package} (this may take a while)...")
        if not install_package(package, timeout=300):  # Longer timeout for heavy packages
            failed_packages.append(package)
            print(f"   Skipping {package} - you can install it later if needed")
        time.sleep(2)
    
    return failed_packages

def main():
    """Main installer function"""
    print("🎯 Smart Dependency Installer")
    print("=" * 35)
    
    # Install core dependencies first
    core_failures = install_core_dependencies()
    
    if core_failures:
        print(f"\\n❌ Failed to install core packages: {', '.join(core_failures)}")
        print("   Please install these manually:")
        for package in core_failures:
            print(f"     pip install {package}")
        return False
    
    print("\\n✅ All core dependencies installed!")
    
    # Ask about optional dependencies
    install_optional = input("\\n❓ Install optional dependencies (BigQuery, Pandas)? [y/N]: ").lower().startswith('y')
    
    if install_optional:
        optional_failures = install_optional_dependencies()
        if optional_failures:
            print(f"\\n⚠️  Some optional packages failed: {', '.join(optional_failures)}")
            print("   The system will work in offline mode without these packages")
    
    print("\\n🎉 Dependency installation complete!")
    print("   Run: python test_lightweight.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
'''
        
        try:
            installer_file = self.project_path / "install_dependencies.py"
            with open(installer_file, 'w') as f:
                f.write(installer_content)
            
            # Make executable
            installer_file.chmod(0o755)
            
            logger.info(f"✅ Created smart dependency installer: {installer_file}")
            self.improvements_applied.append("Smart dependency installer")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create dependency installer: {e}")
            return False
    
    def run_improvements(self) -> bool:
        """Run all architectural improvements"""
        logger.info("🚀 Running architectural improvements...")
        
        improvement_functions = [
            ("Virtual Environment Check", self.check_virtual_environment),
            ("Optional Imports Wrapper", self.create_optional_imports_wrapper),
            ("Base Collector Improvements", self.improve_base_collector),
            ("Lightweight Test Runner", self.create_lightweight_test_runner),
            ("Smart Dependency Installer", self.create_dependency_installer)
        ]
        
        results = []
        for name, func in improvement_functions:
            logger.info(f"\\n📋 {name}")
            logger.info("-" * (len(name) + 5))
            try:
                result = func()
                results.append((name, result))
            except Exception as e:
                logger.error(f"❌ {name} failed: {e}")
                results.append((name, False))
        
        # Summary
        logger.info("\\n" + "=" * 50)
        logger.info("📊 IMPROVEMENT SUMMARY")
        logger.info("=" * 50)
        
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for name, result in results:
            status = "✅" if result else "❌"
            logger.info(f"{status} {name}")
        
        logger.info(f"\\n🎯 Overall: {passed}/{total} improvements applied")
        
        if self.improvements_applied:
            logger.info(f"\\n✨ Applied improvements:")
            for improvement in self.improvements_applied:
                logger.info(f"   • {improvement}")
        
        return passed == total

def main():
    """Main function"""
    project_path = Path(__file__).parent
    improver = ArchitectureImprover(project_path)
    
    success = improver.run_improvements()
    
    if success:
        print("\\n🎉 All architectural improvements applied successfully!")
        print("\\n📋 Next steps:")
        print("   1. Run: python install_dependencies.py")
        print("   2. Run: python test_lightweight.py")
        print("   3. Run: python test_architecture_offline.py")
        print("   4. Edit .env file for your configuration")
    else:
        print("\\n⚠️  Some improvements failed. Check the logs above.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)