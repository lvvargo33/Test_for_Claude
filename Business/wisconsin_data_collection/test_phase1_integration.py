"""
Test Phase 1 Integration
========================

Test script to verify Phase 1 data collectors work with existing infrastructure.
"""

import logging
import sys
import os
from datetime import datetime
import json

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from traffic_data_collector import WisconsinTrafficDataCollector
from zoning_data_collector import WisconsinZoningDataCollector
from consumer_spending_collector import BEAConsumerSpendingCollector


def test_traffic_collector():
    """Test traffic data collector"""
    print("\\n" + "="*60)
    print("TESTING TRAFFIC DATA COLLECTOR")
    print("="*60)
    
    try:
        collector = WisconsinTrafficDataCollector()
        print("✓ Traffic collector initialized successfully")
        
        # Test data collection (small sample)
        print("Testing traffic data collection...")
        records = collector.collect_highway_traffic_data(max_records=100)
        print(f"✓ Collected {len(records)} traffic records")
        
        if records:
            sample_record = records[0]
            print(f"✓ Sample record: {sample_record.route_name} in {sample_record.county} County")
            print(f"  AADT: {sample_record.aadt:,}, Quality Score: {sample_record.data_quality_score}")
        
        return True, len(records)
        
    except Exception as e:
        print(f"✗ Traffic collector test failed: {e}")
        return False, 0


def test_zoning_collector():
    """Test zoning data collector"""
    print("\\n" + "="*60)
    print("TESTING ZONING DATA COLLECTOR")
    print("="*60)
    
    try:
        collector = WisconsinZoningDataCollector()
        print("✓ Zoning collector initialized successfully")
        
        # Test with a single county (limited records)
        print("Testing zoning data collection for Dane County...")
        records = collector.collect_county_zoning_data('Dane', max_records=50)
        print(f"✓ Collected {len(records)} zoning records")
        
        if records:
            sample_record = records[0]
            print(f"✓ Sample record: {sample_record.zoning_code} - {sample_record.zoning_description}")
            print(f"  Commercial allowed: {sample_record.commercial_allowed}, Quality Score: {sample_record.data_quality_score}")
        
        return True, len(records)
        
    except Exception as e:
        print(f"✗ Zoning collector test failed: {e}")
        return False, 0


def test_consumer_spending_collector():
    """Test consumer spending data collector"""
    print("\\n" + "="*60)
    print("TESTING CONSUMER SPENDING DATA COLLECTOR")
    print("="*60)
    
    try:
        collector = BEAConsumerSpendingCollector()
        print("✓ Consumer spending collector initialized successfully")
        
        # Test data collection (recent year only)
        print("Testing consumer spending data collection...")
        current_year = datetime.now().year
        test_years = [current_year - 2]  # Just one recent year
        
        records = collector.collect_state_consumer_spending(years=test_years)
        print(f"✓ Collected {len(records)} consumer spending records")
        
        if records:
            sample_record = records[0]
            print(f"✓ Sample record: {sample_record.state_name} {sample_record.data_year}")
            print(f"  Total PCE: ${sample_record.total_pce:,.0f}M, Quality Score: {sample_record.data_quality_score}")
        
        return True, len(records)
        
    except Exception as e:
        print(f"✗ Consumer spending collector test failed: {e}")
        return False, 0


def test_configuration_loading():
    """Test that configuration loads properly"""
    print("\\n" + "="*60)
    print("TESTING CONFIGURATION LOADING")
    print("="*60)
    
    try:
        import yaml
        
        with open('data_sources.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Check Phase 1 sections exist
        wisconsin_config = config.get('states', {}).get('wisconsin', {})
        
        if 'traffic_data' in wisconsin_config:
            print("✓ Traffic data configuration found")
        else:
            print("✗ Traffic data configuration missing")
            
        if 'zoning_data' in wisconsin_config:
            print("✓ Zoning data configuration found")
        else:
            print("✗ Zoning data configuration missing")
            
        if 'consumer_spending' in wisconsin_config:
            print("✓ Consumer spending configuration found")
        else:
            print("✗ Consumer spending configuration missing")
        
        # Check BigQuery table configuration
        bq_tables = config.get('bigquery', {}).get('tables', {})
        phase1_tables = ['traffic_counts', 'zoning_data', 'consumer_spending']
        
        for table in phase1_tables:
            if table in bq_tables:
                print(f"✓ BigQuery table '{table}' configured")
            else:
                print(f"✗ BigQuery table '{table}' missing")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False


def main():
    """Run all Phase 1 integration tests"""
    # Set up logging
    logging.basicConfig(level=logging.WARNING)  # Reduce noise for testing
    
    print("PHASE 1 DATA COLLECTION INTEGRATION TESTS")
    print("=" * 80)
    print(f"Test started at: {datetime.now()}")
    
    test_results = {
        'configuration': False,
        'traffic': False,
        'zoning': False,
        'consumer_spending': False,
        'total_records': 0
    }
    
    # Test 1: Configuration
    test_results['configuration'] = test_configuration_loading()
    
    # Test 2: Traffic Collector
    success, records = test_traffic_collector()
    test_results['traffic'] = success
    test_results['total_records'] += records
    
    # Test 3: Zoning Collector  
    success, records = test_zoning_collector()
    test_results['zoning'] = success
    test_results['total_records'] += records
    
    # Test 4: Consumer Spending Collector
    success, records = test_consumer_spending_collector()
    test_results['consumer_spending'] = success
    test_results['total_records'] += records
    
    # Summary
    print("\\n" + "="*80)
    print("INTEGRATION TEST SUMMARY")
    print("="*80)
    
    total_tests = len(test_results) - 1  # Exclude total_records
    passed_tests = sum(1 for key, value in test_results.items() 
                      if key != 'total_records' and value)
    
    print(f"Tests Passed: {passed_tests}/{total_tests}")
    print(f"Success Rate: {passed_tests/total_tests:.1%}")
    print(f"Total Records Collected: {test_results['total_records']}")
    
    print("\\nDetailed Results:")
    for test_name, result in test_results.items():
        if test_name == 'total_records':
            continue
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status} {test_name.replace('_', ' ').title()}")
    
    if passed_tests == total_tests:
        print("\\n🎉 ALL TESTS PASSED! Phase 1 integration is ready.")
        return 0
    else:
        print(f"\\n⚠️  {total_tests - passed_tests} test(s) failed. Review errors above.")
        return 1


if __name__ == "__main__":
    exit(main())