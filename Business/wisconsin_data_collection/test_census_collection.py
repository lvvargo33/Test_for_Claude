"""
Test Census Data Collection
===========================

Test script to validate Census API integration and demographic data collection.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add current directory to Python path
sys.path.append(str(Path(__file__).parent))

from census_collector import CensusDataCollector
from models import CensusGeography

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('census_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def test_census_api_connection():
    """Test basic Census API connectivity"""
    print("🔍 Testing Census API Connection...")
    
    try:
        collector = CensusDataCollector()
        
        # Test API with a simple county-level request for Wisconsin (FIPS 55)
        test_variables = ['B01003_001E']  # Total population
        
        data = collector._make_census_api_request(
            acs_year=2022,
            variables=test_variables,
            geography="county:*",
            state="55"  # Wisconsin
        )
        
        if data and len(data) > 1:
            print(f"✅ Census API connection successful!")
            print(f"   Retrieved data for {len(data)-1} Wisconsin counties")
            print(f"   Sample data: {data[1][:3]}...")  # Show first few fields
            return True
        else:
            print("❌ Census API connection failed - no data returned")
            return False
            
    except Exception as e:
        print(f"❌ Census API connection failed: {str(e)}")
        return False


def test_variable_mapping():
    """Test Census variable parsing and mapping"""
    print("\n🧪 Testing Variable Mapping...")
    
    try:
        collector = CensusDataCollector()
        
        # Create test data row (simulating API response)
        test_row = [
            "50000",     # B01003_001E - Total population
            "35.5",      # B01002_001E - Median age
            "65000",     # B19013_001E - Median household income
            "1500",      # B23025_005E - Unemployed
            "25000",     # B23025_002E - Labor force
            "8000",      # B15003_022E - Bachelor's degree
            "20000",     # B15003_001E - Total education pop
            "22000",     # B25001_001E - Total housing units
            "15000",     # B25003_002E - Owner occupied
            "20000",     # B25003_001E - Total occupied
            "18000",     # B08303_001E - Total commuters
            "2000",      # B08303_013E - Commute 60+ min
            "1000",      # B08301_010E - Public transport
            "18000",     # B08301_001E - Total transport pop
            "55",        # State FIPS
            "025"        # County FIPS (Dane County)
        ]
        
        variables = collector._build_variable_list()
        
        # Test parsing
        record = collector._parse_census_response(
            test_row, variables, 'county', 2022
        )
        
        if record:
            print("✅ Variable mapping successful!")
            print(f"   Population: {record.total_population:,}")
            print(f"   Median Income: ${record.median_household_income:,}")
            print(f"   Unemployment Rate: {record.unemployment_rate}%")
            print(f"   Bachelor's Degree %: {record.bachelor_degree_pct}%")
            print(f"   Data Quality Score: {record.data_quality_score}")
            return True
        else:
            print("❌ Variable mapping failed")
            return False
            
    except Exception as e:
        print(f"❌ Variable mapping test failed: {str(e)}")
        return False


def test_wisconsin_county_collection():
    """Test collecting data for Wisconsin counties"""
    print("\n🏡 Testing Wisconsin County Collection...")
    
    try:
        collector = CensusDataCollector()
        
        # Collect county-level data for Wisconsin
        summary = collector.collect_wisconsin_demographics(
            geographic_levels=['county'],
            acs_year=2022
        )
        
        if summary.success:
            print("✅ Wisconsin county collection successful!")
            print(f"   Counties collected: {summary.counties_collected}")
            print(f"   API requests made: {summary.api_requests_made}")
            print(f"   Processing time: {summary.processing_time_seconds:.1f}s")
            print(f"   Average data quality: {summary.avg_data_quality_score:.1f}")
            return True
        else:
            print(f"❌ Wisconsin county collection failed")
            print(f"   API errors: {summary.api_errors}")
            return False
            
    except Exception as e:
        print(f"❌ Wisconsin county collection test failed: {str(e)}")
        return False


def test_tract_level_collection():
    """Test collecting tract-level data for priority counties"""
    print("\n🗺️  Testing Tract Level Collection...")
    
    try:
        collector = CensusDataCollector()
        
        # Test tract collection for Milwaukee County only
        print("   Testing tract collection for Milwaukee County...")
        
        data = collector._make_census_api_request(
            acs_year=2022,
            variables=['B01003_001E', 'B19013_001E'],  # Population and income
            geography="tract:*",
            state="55",
            county="079"  # Milwaukee County
        )
        
        if data and len(data) > 1:
            print(f"✅ Tract collection successful!")
            print(f"   Milwaukee County tracts: {len(data)-1}")
            print(f"   Sample tract data: {data[1][:3]}...")
            return True
        else:
            print("❌ Tract collection failed")
            return False
            
    except Exception as e:
        print(f"❌ Tract collection test failed: {str(e)}")
        return False


def test_demographic_analysis():
    """Test demographic analysis functionality"""
    print("\n📊 Testing Demographic Analysis...")
    
    try:
        collector = CensusDataCollector()
        
        # Get demographic summary for Milwaukee area
        summary = collector.get_demographic_summary(
            location="Milwaukee, WI",
            radius_miles=5.0
        )
        
        if summary:
            print("✅ Demographic analysis framework ready!")
            print(f"   Location: {summary['location']}")
            print(f"   Radius: {summary['radius_miles']} miles")
            print(f"   Framework includes: population, economic, education, housing, transportation")
            return True
        else:
            print("❌ Demographic analysis failed")
            return False
            
    except Exception as e:
        print(f"❌ Demographic analysis test failed: {str(e)}")
        return False


def run_comprehensive_test():
    """Run all Census collection tests"""
    print("🧭 Census Data Collection - Comprehensive Test")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    tests = [
        ("API Connection", test_census_api_connection),
        ("Variable Mapping", test_variable_mapping),
        ("Wisconsin Counties", test_wisconsin_county_collection),
        ("Tract Level Data", test_tract_level_collection),
        ("Demographic Analysis", test_demographic_analysis)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} test crashed: {str(e)}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:.<30} {status}")
    
    print("-" * 60)
    print(f"Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All Census collection tests passed!")
        print("\n📋 Ready for production use:")
        print("   • Census API integration working")
        print("   • Demographic data collection ready")
        print("   • Wisconsin geographic coverage complete")
        print("   • Data quality validation implemented")
    else:
        print(f"\n⚠️  {total - passed} tests failed - review logs for details")
    
    print("=" * 60)
    return passed == total


if __name__ == "__main__":
    success = run_comprehensive_test()
    
    if success:
        print("\n🚀 Next steps:")
        print("   python main.py --collect  # Run full collection with demographics")
        print("   python main.py --analyze  # Analyze collected data")
        sys.exit(0)
    else:
        print("\n🔧 Fix issues before proceeding to production")
        sys.exit(1)