"""
Test New Architecture - Offline Mode
===================================

Tests the new Wisconsin data collection architecture without BigQuery
to verify the core functionality works before setting up authentication.
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from models import BusinessEntity, SBALoanRecord, BusinessLicense, BusinessType, BusinessStatus, DataSource
import yaml
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
        print(f"   📝 Sample: {business.business_name} in {business.city}")
        
        # Test validation
        assert business.state == "WI"
        assert business.business_type == BusinessType.RESTAURANT
        print("✅ Data validation works")
        
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
        print(f"   💰 Sample: {loan.borrower_name} - ${loan.loan_amount:,.0f}")
        
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
        print(f"   📄 Sample: {license.business_name} - {license.license_type}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data model test failed: {e}")
        return False

def test_configuration_loading():
    """Test configuration system"""
    print("\n🧪 Testing Configuration System")
    print("=" * 35)
    
    try:
        # Test YAML loading
        with open('data_sources.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        print("✅ Configuration file loaded")
        
        # Check Wisconsin config
        wi_config = config['states']['wisconsin']
        print(f"✅ Wisconsin config found: {wi_config['name']}")
        
        # Check data sources
        business_reg = wi_config['business_registrations']['primary']
        print(f"✅ Business registration source: {business_reg['name']}")
        print(f"   🔗 URL: {business_reg['url']}")
        
        # Check SBA config
        sba_config = wi_config['sba_loans']['primary']
        print(f"✅ SBA loan source: {sba_config['name']}")
        
        # Check license sources
        milwaukee_config = wi_config['business_licenses']['milwaukee']
        print(f"✅ Milwaukee licenses: {milwaukee_config['name']}")
        
        # Check target business types
        target_types = config['target_business_types']['high_priority']
        print(f"✅ Found {len(target_types)} high-priority business types:")
        for btype in target_types[:3]:
            print(f"   • {btype}")
        
        # Check BigQuery config
        bq_config = config['bigquery']
        print(f"✅ BigQuery project: {bq_config['project_id']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

def test_data_generation():
    """Test sample data generation methods"""
    print("\n🧪 Testing Data Generation")
    print("=" * 30)
    
    try:
        # Import the collector class but don't initialize it
        # (to avoid BigQuery authentication issues)
        from wisconsin_collector import WisconsinDataCollector
        
        # Create instance without calling parent __init__
        collector = object.__new__(WisconsinDataCollector)
        
        # Set up minimal attributes
        collector.county_mappings = {
            'Milwaukee': 'Milwaukee',
            'Madison': 'Dane',
            'Green Bay': 'Brown'
        }
        
        # Test sample business generation
        businesses = collector._generate_realistic_wi_businesses(30)
        print(f"✅ Generated {len(businesses)} sample businesses")
        
        # Check data quality
        restaurant_count = sum(1 for b in businesses if b.business_type == BusinessType.RESTAURANT)
        retail_count = sum(1 for b in businesses if b.business_type == BusinessType.RETAIL)
        print(f"   🍽️  Restaurants: {restaurant_count}")
        print(f"   🛍️  Retail: {retail_count}")
        
        # Test SBA loan generation
        loans = collector._generate_sample_sba_loans(60)
        print(f"✅ Generated {len(loans)} sample SBA loans")
        
        # Check loan amounts
        avg_amount = sum(l.loan_amount for l in loans) / len(loans)
        print(f"   💰 Average loan: ${avg_amount:,.0f}")
        
        # Test license generation
        milwaukee_licenses = collector._generate_sample_milwaukee_licenses(30)
        madison_licenses = collector._generate_sample_madison_licenses(30)
        print(f"✅ Generated {len(milwaukee_licenses)} Milwaukee licenses")
        print(f"✅ Generated {len(madison_licenses)} Madison licenses")
        
        return True
        
    except Exception as e:
        print(f"❌ Data generation test failed: {e}")
        return False

def test_business_type_standardization():
    """Test business type standardization"""
    print("\n🧪 Testing Business Type Standardization")
    print("=" * 42)
    
    try:
        # Import the Wisconsin collector which has the method implemented
        from wisconsin_collector import WisconsinDataCollector
        
        # Create a mock collector instance
        collector = object.__new__(WisconsinDataCollector)
        
        test_types = [
            "Fast Food Restaurant",
            "Retail Clothing Store", 
            "Hair Salon and Spa",
            "Auto Repair Shop",
            "Fitness Center & Gym",
            "Dental Clinic",
            "Law Office",
            "McDonald's Franchise",
            "Unknown Business Type"
        ]
        
        for raw_type in test_types:
            standardized = collector.standardize_business_type(raw_type)
            print(f"   📝 '{raw_type}' → {standardized.value}")
        
        print("✅ Business type standardization works")
        
        return True
        
    except Exception as e:
        print(f"❌ Business type standardization test failed: {e}")
        return False

def test_data_quality_rules():
    """Test data quality and validation rules"""
    print("\n🧪 Testing Data Quality Rules")
    print("=" * 32)
    
    try:
        # Load quality rules from config
        with open('data_sources.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        quality_rules = config['data_quality']
        
        # Check required fields
        required_fields = quality_rules['required_fields']['business_entity']
        print(f"✅ Required fields for business entity: {len(required_fields)}")
        for field in required_fields:
            print(f"   • {field}")
        
        # Check validation rules
        validation_rules = quality_rules['validation_rules']
        print(f"✅ Validation rules defined: {len(validation_rules)}")
        
        # Test phone validation
        import re
        phone_pattern = validation_rules['phone_number']
        test_phones = [
            "(414) 555-1234",
            "414-555-1234",
            "414.555.1234",
            "4145551234",
            "invalid-phone"
        ]
        
        for phone in test_phones:
            is_valid = bool(re.match(phone_pattern, phone))
            status = "✅" if is_valid else "❌"
            print(f"   {status} {phone}")
        
        # Check confidence scoring rules
        confidence_rules = quality_rules['confidence_scoring']
        print(f"✅ Confidence scoring rules: {len(confidence_rules)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data quality rules test failed: {e}")
        return False

def generate_sample_analysis():
    """Generate a sample analysis report"""
    print("\n📊 Generating Sample Analysis")
    print("=" * 30)
    
    try:
        # Generate sample data
        from wisconsin_collector import WisconsinDataCollector
        
        # Create instance without BigQuery initialization
        collector = object.__new__(WisconsinDataCollector)
        collector.county_mappings = {
            'Milwaukee': 'Milwaukee',
            'Madison': 'Dane',
            'Green Bay': 'Brown',
            'Kenosha': 'Kenosha',
            'Racine': 'Racine'
        }
        
        # Generate sample data
        businesses = collector._generate_realistic_wi_businesses(100)
        loans = collector._generate_sample_sba_loans(50)
        licenses = collector._generate_sample_milwaukee_licenses(75)
        
        print(f"📈 Sample Data Summary:")
        print(f"   • Business Registrations: {len(businesses)}")
        print(f"   • SBA Loan Approvals: {len(loans)}")
        print(f"   • Business Licenses: {len(licenses)}")
        print(f"   • Total Records: {len(businesses) + len(loans) + len(licenses)}")
        
        # Business type analysis
        type_counts = {}
        for business in businesses:
            btype = business.business_type.value
            type_counts[btype] = type_counts.get(btype, 0) + 1
        
        print(f"\n🏢 Business Type Distribution:")
        for btype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            percentage = (count / len(businesses)) * 100
            print(f"   • {btype.replace('_', ' ').title()}: {count} ({percentage:.1f}%)")
        
        # Geographic analysis
        city_counts = {}
        for business in businesses:
            city = business.city
            city_counts[city] = city_counts.get(city, 0) + 1
        
        print(f"\n🌍 Top 5 Cities by Business Activity:")
        for city, count in sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"   • {city}: {count} new businesses")
        
        # Loan analysis
        high_value_loans = [l for l in loans if l.loan_amount >= 200000]
        franchise_loans = [l for l in loans if l.franchise_name]
        
        print(f"\n💰 SBA Loan Analysis:")
        print(f"   • Total loans: {len(loans)}")
        print(f"   • High-value loans (>$200K): {len(high_value_loans)}")
        print(f"   • Franchise loans: {len(franchise_loans)}")
        
        total_funding = sum(l.loan_amount for l in loans)
        avg_loan = total_funding / len(loans)
        print(f"   • Total funding: ${total_funding:,.0f}")
        print(f"   • Average loan: ${avg_loan:,.0f}")
        
        # Top prospects
        print(f"\n🎯 Top Prospect Categories:")
        print(f"   • Recent high-value SBA loans: {len(high_value_loans)}")
        print(f"   • New restaurant registrations: {type_counts.get('restaurant', 0)}")
        print(f"   • New retail registrations: {type_counts.get('retail', 0)}")
        print(f"   • Active business licenses: {len(licenses)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Sample analysis failed: {e}")
        return False

def main():
    """Main test function"""
    print("🎯 Location Optimizer - Offline Architecture Test")
    print("=" * 55)
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🔧 Mode: Offline (No BigQuery authentication required)")
    print("=" * 55)
    
    tests = [
        ("Data Models", test_data_models),
        ("Configuration System", test_configuration_loading),
        ("Data Generation", test_data_generation),
        ("Business Type Standardization", test_business_type_standardization),
        ("Data Quality Rules", test_data_quality_rules),
        ("Sample Analysis", generate_sample_analysis)
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
    print("\n" + "=" * 55)
    print("📋 TEST SUMMARY")
    print("=" * 55)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All offline tests passed! Core architecture is working.")
        print("\n📋 Ready for next steps:")
        print("   1. Set up Google Cloud authentication")
        print("   2. Run: python main.py --setup")
        print("   3. Run: python main.py --collect")
        print("   4. Run: python main.py --analyze")
    else:
        print(f"\n⚠️  {total - passed} tests failed. Check errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)