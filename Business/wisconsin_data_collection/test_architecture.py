"""
Test New Architecture with Sample Data
=====================================

Tests the new Wisconsin data collection architecture with sample data
to verify everything works before connecting to real data sources.
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from models import BusinessEntity, SBALoanRecord, BusinessLicense, BusinessType, BusinessStatus, DataSource
from wisconsin_collector import WisconsinDataCollector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_models():
    """Test Pydantic data models"""
    print("🧪 Testing Data Models")
    print("=" * 30)
    
    try:
        # Test BusinessEntity model
        business = BusinessEntity(
            business_id="TEST001",
            source_id="WI_DFI_001",
            business_name="Test Restaurant LLC",
            business_type=BusinessType.RESTAURANT,
            status=BusinessStatus.ACTIVE,
            city="Milwaukee",
            state="WI",
            data_source=DataSource.STATE_REGISTRATION
        )
        print("✅ BusinessEntity model works")
        
        # Test SBA Loan model
        loan = SBALoanRecord(
            loan_id="SBA001",
            borrower_name="Test Borrower",
            loan_amount=150000.0,
            approval_date=datetime.now().date(),
            program_type="7(a)",
            borrower_city="Madison",
            borrower_state="WI"
        )
        print("✅ SBALoanRecord model works")
        
        # Test Business License model
        license = BusinessLicense(
            license_id="LIC001",
            business_name="Test Business",
            license_type="Restaurant License",
            status="Active",
            city="Green Bay",
            state="WI",
            data_source="Test_Source"
        )
        print("✅ BusinessLicense model works")
        
        return True
        
    except Exception as e:
        print(f"❌ Data model test failed: {e}")
        return False

def test_wisconsin_collector():
    """Test Wisconsin collector with sample data"""
    print("\n🧪 Testing Wisconsin Collector")
    print("=" * 35)
    
    try:
        # Initialize collector (this will use sample data)
        collector = WisconsinDataCollector()
        print("✅ Wisconsin collector initialized")
        
        # Test business registration collection
        businesses = collector.collect_business_registrations(days_back=30)
        print(f"✅ Collected {len(businesses)} business registrations")
        
        # Test SBA loan collection
        loans = collector.collect_sba_loans(days_back=60)
        print(f"✅ Collected {len(loans)} SBA loans")
        
        # Test license collection
        licenses = collector.collect_business_licenses(days_back=15)
        print(f"✅ Collected {len(licenses)} business licenses")
        
        # Test data validation
        valid_businesses = [b for b in businesses if collector.validate_business_entity(b)]
        print(f"✅ {len(valid_businesses)}/{len(businesses)} businesses passed validation")
        
        # Test confidence scoring
        for business in businesses[:3]:
            score = collector.calculate_confidence_score(business)
            print(f"   📊 {business.business_name}: {score}% confidence")
        
        return True
        
    except Exception as e:
        print(f"❌ Wisconsin collector test failed: {e}")
        return False

def test_configuration_loading():
    """Test configuration system"""
    print("\n🧪 Testing Configuration System")
    print("=" * 35)
    
    try:
        # Test YAML loading
        import yaml
        with open('data_sources.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        print("✅ Configuration file loaded")
        
        # Check Wisconsin config
        wi_config = config['states']['wisconsin']
        print(f"✅ Wisconsin config found: {wi_config['name']}")
        
        # Check data sources
        business_reg = wi_config['business_registrations']['primary']
        print(f"✅ Business registration source: {business_reg['name']}")
        
        # Check target business types
        target_types = config['target_business_types']['high_priority']
        print(f"✅ Found {len(target_types)} high-priority business types")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

def test_data_processing():
    """Test data processing and transformation"""
    print("\n🧪 Testing Data Processing")
    print("=" * 30)
    
    try:
        collector = WisconsinDataCollector()
        
        # Test business type standardization
        test_types = [
            "Fast Food Restaurant",
            "Retail Clothing Store", 
            "Hair Salon",
            "Auto Repair Shop",
            "Fitness Center"
        ]
        
        for raw_type in test_types:
            standardized = collector.standardize_business_type(raw_type)
            print(f"   📝 '{raw_type}' → {standardized.value}")
        
        print("✅ Business type standardization works")
        
        # Test data cleaning
        businesses = collector.collect_business_registrations(days_back=10)
        
        # Check data quality
        complete_records = sum(1 for b in businesses if b.phone and b.email)
        print(f"✅ {complete_records}/{len(businesses)} records have complete contact info")
        
        return True
        
    except Exception as e:
        print(f"❌ Data processing test failed: {e}")
        return False

def generate_sample_report():
    """Generate a sample analysis report"""
    print("\n📊 Generating Sample Report")
    print("=" * 30)
    
    try:
        collector = WisconsinDataCollector()
        
        # Collect sample data
        businesses = collector.collect_business_registrations(days_back=30)
        loans = collector.collect_sba_loans(days_back=60)
        licenses = collector.collect_business_licenses(days_back=15)
        
        # Summary statistics
        print(f"📈 Data Collection Summary (Last 30 Days)")
        print(f"   • Business Registrations: {len(businesses)}")
        print(f"   • SBA Loan Approvals: {len(loans)}")
        print(f"   • Business Licenses: {len(licenses)}")
        print(f"   • Total Records: {len(businesses) + len(loans) + len(licenses)}")
        
        # Business type breakdown
        type_counts = {}
        for business in businesses:
            btype = business.business_type.value
            type_counts[btype] = type_counts.get(btype, 0) + 1
        
        print(f"\n🏢 Business Types:")
        for btype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {btype.title()}: {count}")
        
        # Geographic distribution
        city_counts = {}
        for business in businesses:
            city = business.city
            city_counts[city] = city_counts.get(city, 0) + 1
        
        print(f"\n🌍 Geographic Distribution:")
        for city, count in sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"   • {city}: {count}")
        
        # High-value prospects
        high_value_loans = [l for l in loans if l.loan_amount >= 200000]
        print(f"\n💰 High-Value Prospects (>$200K loans): {len(high_value_loans)}")
        
        for loan in high_value_loans[:3]:
            print(f"   • {loan.borrower_name}: ${loan.loan_amount:,.0f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        return False

def main():
    """Main test function"""
    print("🎯 Location Optimizer - Architecture Test")
    print("=" * 50)
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    tests = [
        ("Data Models", test_data_models),
        ("Configuration Loading", test_configuration_loading),
        ("Wisconsin Collector", test_wisconsin_collector),
        ("Data Processing", test_data_processing),
        ("Sample Report", generate_sample_report)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Architecture is working correctly.")
        print("\n📋 Next steps:")
        print("   1. Set up Google Cloud authentication")
        print("   2. Run: python main.py --setup")
        print("   3. Run: python main.py --collect")
    else:
        print(f"\n⚠️  {total - passed} tests failed. Check errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)