#!/usr/bin/env python3
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
    print("\n🧪 Testing data models...")
    
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
    print("\n🧪 Testing configuration...")
    
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
    
    print(f"\n🎯 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("\n✅ All lightweight tests passed!")
        print("   Ready for full testing with test_architecture_offline.py")
    else:
        print("\n⚠️  Some tests failed. Check dependencies and configuration.")
    
    return passed == len(tests)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
