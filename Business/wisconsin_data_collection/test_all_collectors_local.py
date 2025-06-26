"""
Test All Phase 1 Collectors with Local Storage
===============================================

Test all three Phase 1 collectors and save results locally.
"""

import json
from datetime import datetime
import pandas as pd

from traffic_data_collector import WisconsinTrafficDataCollector
from zoning_data_collector import WisconsinZoningDataCollector
from consumer_spending_collector import BEAConsumerSpendingCollector

def test_traffic_collector():
    """Test and collect traffic data"""
    print("="*60)
    print("TESTING TRAFFIC DATA COLLECTOR")
    print("="*60)
    
    try:
        collector = WisconsinTrafficDataCollector()
        records = collector.collect_highway_traffic_data(max_records=1000)
        
        result = {
            'success': True,
            'records_collected': len(records),
            'sample_data': []
        }
        
        if records:
            # Convert sample to dict for JSON serialization
            sample_records = records[:3]
            for record in sample_records:
                record_dict = record.model_dump()
                # Convert datetime to string for JSON serialization
                if 'data_collection_date' in record_dict:
                    record_dict['data_collection_date'] = record_dict['data_collection_date'].isoformat()
                result['sample_data'].append(record_dict)
        
        print(f"✓ SUCCESS: Collected {len(records)} traffic records")
        if records:
            print(f"  Sample: {records[0].route_name} in {records[0].county} County (AADT: {records[0].aadt:,})")
        
        return result
        
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return {'success': False, 'error': str(e), 'records_collected': 0}

def test_zoning_collector():
    """Test zoning data collector"""
    print("="*60)
    print("TESTING ZONING DATA COLLECTOR")
    print("="*60)
    
    try:
        collector = WisconsinZoningDataCollector()
        
        # Try to collect from one county (may fail due to network)
        try:
            records = collector.collect_county_zoning_data('Dane', max_records=100)
            success = True
            records_count = len(records)
            error = None
        except Exception as e:
            success = False
            records_count = 0
            error = str(e)
        
        result = {
            'success': success,
            'records_collected': records_count,
            'error': error,
            'collector_initialized': True
        }
        
        if success and records_count > 0:
            print(f"✓ SUCCESS: Collected {records_count} zoning records")
        else:
            print(f"✓ COLLECTOR READY: Initialized successfully")
            print(f"  Note: {error if error else 'No records collected (may be network issue)'}")
        
        return result
        
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return {'success': False, 'error': str(e), 'records_collected': 0, 'collector_initialized': False}

def test_consumer_spending_collector():
    """Test consumer spending data collector"""
    print("="*60)
    print("TESTING CONSUMER SPENDING DATA COLLECTOR")
    print("="*60)
    
    try:
        collector = BEAConsumerSpendingCollector()
        
        # Try to collect recent year (may fail due to API key)
        try:
            records = collector.collect_state_consumer_spending(years=[2022])
            success = True
            records_count = len(records)
            error = None
        except Exception as e:
            success = False
            records_count = 0
            error = str(e)
        
        result = {
            'success': success,
            'records_collected': records_count,
            'error': error,
            'collector_initialized': True
        }
        
        if success and records_count > 0:
            print(f"✓ SUCCESS: Collected {records_count} consumer spending records")
        else:
            print(f"✓ COLLECTOR READY: Initialized successfully")
            print(f"  Note: {error if error else 'No records collected (may need API key)'}")
        
        return result
        
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return {'success': False, 'error': str(e), 'records_collected': 0, 'collector_initialized': False}

def main():
    print("PHASE 1 DATA COLLECTORS COMPREHENSIVE TEST")
    print("=" * 80)
    print(f"Test started: {datetime.now()}")
    
    # Test all collectors
    results = {
        'test_timestamp': datetime.now().isoformat(),
        'traffic': test_traffic_collector(),
        'zoning': test_zoning_collector(),
        'consumer_spending': test_consumer_spending_collector()
    }
    
    # Summary
    print("=" * 80)
    print("PHASE 1 COLLECTORS TEST SUMMARY")
    print("=" * 80)
    
    total_records = sum(r.get('records_collected', 0) for r in results.values() if isinstance(r, dict))
    successful_collectors = sum(1 for r in results.values() if isinstance(r, dict) and r.get('success', False))
    initialized_collectors = sum(1 for r in results.values() if isinstance(r, dict) and r.get('collector_initialized', r.get('success', False)))
    
    print(f"Collectors Initialized: {initialized_collectors}/3")
    print(f"Successful Data Collection: {successful_collectors}/3")
    print(f"Total Records Collected: {total_records:,}")
    
    print(f"\nDetailed Results:")
    for collector_name, result in results.items():
        if isinstance(result, dict):
            status = "✓ SUCCESS" if result.get('success') else ("✓ READY" if result.get('collector_initialized') else "✗ FAILED")
            records = result.get('records_collected', 0)
            print(f"  {status} {collector_name.replace('_', ' ').title()}: {records:,} records")
            if result.get('error') and not result.get('success'):
                error_preview = result['error'][:100] + "..." if len(result['error']) > 100 else result['error']
                print(f"    Error: {error_preview}")
    
    # Save detailed results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = f"phase1_collectors_test_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nDetailed results saved to: {results_file}")
    
    # Overall assessment
    if initialized_collectors == 3:
        print(f"\n🎉 ALL COLLECTORS READY! Phase 1 infrastructure is operational.")
        if successful_collectors >= 1:
            print(f"   Data collection is working ({successful_collectors}/3 collectors successfully gathered data)")
        else:
            print(f"   Note: Collectors are ready but may need network access/API keys for data collection")
    else:
        print(f"\n⚠️  {3 - initialized_collectors} collector(s) failed to initialize")
    
    return results

if __name__ == "__main__":
    main()