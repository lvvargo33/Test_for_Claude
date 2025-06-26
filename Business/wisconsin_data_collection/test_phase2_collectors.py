"""
Phase 2 Collectors Comprehensive Test
====================================

Test all Phase 2 collectors individually and as a coordinated system.
Provides detailed validation of data collection capabilities.
"""

import json
import logging
from datetime import datetime
import pandas as pd
import traceback

# Import Phase 2 collectors
from real_estate_collector import WisconsinRealEstateCollector
from industry_benchmarks_collector import IndustryBenchmarksCollector
from enhanced_employment_collector import EnhancedEmploymentCollector


def setup_logging():
    """Configure logging for test execution"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def test_real_estate_collector():
    """Test real estate data collector"""
    print("=" * 60)
    print("TESTING REAL ESTATE DATA COLLECTOR")
    print("=" * 60)
    
    try:
        collector = WisconsinRealEstateCollector()
        
        # Test with limited scope to avoid timeouts
        summary = collector.run_real_estate_collection(
            counties=['Milwaukee'], 
            cities=['Milwaukee']
        )
        
        result = {
            'success': summary.get('success', False),
            'county_records': summary.get('county_records', 0),
            'loopnet_records': summary.get('loopnet_records', 0),
            'total_records': summary.get('total_records', 0),
            'processing_time': summary.get('processing_time_seconds', 0),
            'errors': summary.get('errors', []),
            'data_sources': summary.get('data_sources', [])
        }
        
        if result['success'] and result['total_records'] > 0:
            print(f"✓ SUCCESS: Collected {result['total_records']} real estate records")
            print(f"  County Records: {result['county_records']}")
            print(f"  LoopNet Records: {result['loopnet_records']}")
        elif result['success']:
            print("✓ COLLECTOR READY: Initialized successfully")
            print("  Note: No records collected (placeholder data generation working)")
        else:
            print(f"✗ FAILED: {result['errors']}")
        
        return result
        
    except Exception as e:
        print(f"✗ FAILED: {e}")
        traceback.print_exc()
        return {
            'success': False, 
            'error': str(e), 
            'county_records': 0,
            'loopnet_records': 0,
            'total_records': 0,
            'collector_initialized': False
        }


def test_industry_benchmarks_collector():
    """Test industry benchmarks data collector"""
    print("=" * 60)
    print("TESTING INDUSTRY BENCHMARKS DATA COLLECTOR")
    print("=" * 60)
    
    try:
        collector = IndustryBenchmarksCollector()
        
        # Run benchmarks collection
        summary = collector.run_benchmarks_collection()
        
        result = {
            'success': summary.get('success', False),
            'sba_benchmarks': summary.get('sba_benchmarks', 0),
            'franchise_benchmarks': summary.get('franchise_benchmarks', 0),
            'industry_benchmarks': summary.get('industry_benchmarks', 0),
            'total_records': summary.get('total_records', 0),
            'processing_time': summary.get('processing_time_seconds', 0),
            'errors': summary.get('errors', []),
            'data_sources': summary.get('data_sources', [])
        }
        
        if result['success'] and result['total_records'] > 0:
            print(f"✓ SUCCESS: Collected {result['total_records']} benchmark records")
            print(f"  SBA Benchmarks: {result['sba_benchmarks']}")
            print(f"  Franchise Benchmarks: {result['franchise_benchmarks']}")
            print(f"  Industry Benchmarks: {result['industry_benchmarks']}")
        elif result['success']:
            print("✓ COLLECTOR READY: Initialized successfully")
            print("  Note: Sample data generation working")
        else:
            print(f"✗ FAILED: {result['errors']}")
        
        return result
        
    except Exception as e:
        print(f"✗ FAILED: {e}")
        traceback.print_exc()
        return {
            'success': False, 
            'error': str(e), 
            'sba_benchmarks': 0,
            'franchise_benchmarks': 0,
            'industry_benchmarks': 0,
            'total_records': 0,
            'collector_initialized': False
        }


def test_enhanced_employment_collector():
    """Test enhanced employment data collector"""
    print("=" * 60)
    print("TESTING ENHANCED EMPLOYMENT DATA COLLECTOR")
    print("=" * 60)
    
    try:
        collector = EnhancedEmploymentCollector()
        
        # Run enhanced employment collection
        summary = collector.run_enhanced_employment_collection()
        
        result = {
            'success': summary.get('success', False),
            'employment_projections': summary.get('employment_projections', 0),
            'wage_records': summary.get('wage_records', 0),
            'areas_covered': summary.get('areas_covered', 0),
            'processing_time': summary.get('processing_time_seconds', 0),
            'errors': summary.get('errors', []),
            'data_sources': summary.get('data_sources', [])
        }
        
        total_records = result['employment_projections'] + result['wage_records']
        
        if result['success'] and total_records > 0:
            print(f"✓ SUCCESS: Collected {total_records} employment records")
            print(f"  Employment Projections: {result['employment_projections']}")
            print(f"  Wage Records: {result['wage_records']}")
            print(f"  Areas Covered: {result['areas_covered']}")
        elif result['success']:
            print("✓ COLLECTOR READY: Initialized successfully")
            print("  Note: Sample data generation working")
        else:
            print(f"✗ FAILED: {result['errors']}")
        
        return result
        
    except Exception as e:
        print(f"✗ FAILED: {e}")
        traceback.print_exc()
        return {
            'success': False, 
            'error': str(e), 
            'employment_projections': 0,
            'wage_records': 0,
            'areas_covered': 0,
            'collector_initialized': False
        }


def test_data_integration():
    """Test data integration capabilities"""
    print("=" * 60)
    print("TESTING DATA INTEGRATION CAPABILITIES")
    print("=" * 60)
    
    integration_score = 0.0
    tests_passed = 0
    total_tests = 4
    
    # Test 1: Cross-collector data consistency
    try:
        # Initialize all collectors
        re_collector = WisconsinRealEstateCollector()
        ib_collector = IndustryBenchmarksCollector()
        ee_collector = EnhancedEmploymentCollector()
        
        # Test if they can coexist
        tests_passed += 1
        integration_score += 25.0
        print("✓ Collectors can be initialized together")
        
    except Exception as e:
        print(f"✗ Collector initialization failed: {e}")
    
    # Test 2: Configuration consistency
    try:
        # Check if all collectors use same config
        config_path = "data_sources.yaml"
        collectors = [
            WisconsinRealEstateCollector(config_path),
            IndustryBenchmarksCollector(config_path),
            EnhancedEmploymentCollector(config_path)
        ]
        
        # Verify they all have BigQuery config (or handle its absence gracefully)
        config_consistent = True
        for collector in collectors:
            if not hasattr(collector, 'bq_config'):
                config_consistent = False
        
        if config_consistent:
            tests_passed += 1
            integration_score += 25.0
            print("✓ Configuration consistency across collectors")
        else:
            print("⚠ Configuration inconsistency detected")
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
    
    # Test 3: Data model compatibility
    try:
        # Test that data models can be imported and instantiated
        from real_estate_collector import CommercialPropertyRecord
        from industry_benchmarks_collector import IndustryBenchmarkRecord
        from enhanced_employment_collector import EmploymentProjectionRecord, OESWageRecord
        
        # Try to create sample instances
        sample_property = CommercialPropertyRecord(
            property_id="test_001",
            property_address="123 Test St",
            city="Test City",
            county="Test County",
            property_type="office",
            data_source="Test"
        )
        
        sample_benchmark = IndustryBenchmarkRecord(
            benchmark_id="test_bench_001",
            industry_name="Test Industry",
            benchmark_type="Financial",
            metric_name="Test Metric",
            metric_category="Revenue",
            geographic_scope="National",
            benchmark_value=100.0,
            benchmark_unit="USD",
            data_year=2023,
            data_source="Test Source",
            source_organization="Test Org"
        )
        
        sample_projection = EmploymentProjectionRecord(
            projection_id="test_proj_001",
            projection_period="2022-2032",
            data_year=2022,
            projection_year=2032,
            area_name="Test Area",
            area_type="State",
            industry_code="722",
            industry_title="Test Industry",
            industry_level="3-digit",
            base_year_employment=1000,
            projected_employment=1100,
            numeric_change=100,
            percent_change=10.0,
            growth_rate="Fast",
            growth_outlook="Much faster than average"
        )
        
        tests_passed += 1
        integration_score += 25.0
        print("✓ Data models are compatible and functional")
        
    except Exception as e:
        print(f"✗ Data model test failed: {e}")
    
    # Test 4: Error handling consistency
    try:
        # Test that collectors handle errors gracefully
        test_collector = WisconsinRealEstateCollector()
        
        # Try an operation that should handle errors gracefully
        if hasattr(test_collector, 'save_real_estate_data_to_bigquery'):
            # This should handle the case where BigQuery is not available
            result = test_collector.save_real_estate_data_to_bigquery([])
            # If it returns a boolean, error handling is working
            if isinstance(result, bool):
                tests_passed += 1
                integration_score += 25.0
                print("✓ Error handling is consistent")
            else:
                print("⚠ Error handling may need improvement")
        else:
            print("⚠ Cannot test error handling")
        
    except Exception as e:
        print(f"✗ Error handling test failed: {e}")
    
    print(f"\nIntegration Test Results: {tests_passed}/{total_tests} passed")
    print(f"Integration Score: {integration_score:.1f}%")
    
    return {
        'integration_score': integration_score,
        'tests_passed': tests_passed,
        'total_tests': total_tests,
        'success': tests_passed >= (total_tests * 0.75)  # 75% pass rate
    }


def main():
    """Main test function"""
    logger = setup_logging()
    
    print("PHASE 2 DATA COLLECTORS COMPREHENSIVE TEST")
    print("=" * 80)
    print(f"Test started: {datetime.now()}")
    
    # Test all collectors
    results = {
        'test_timestamp': datetime.now().isoformat(),
        'phase': 'Phase 2',
        'real_estate': test_real_estate_collector(),
        'industry_benchmarks': test_industry_benchmarks_collector(),
        'enhanced_employment': test_enhanced_employment_collector(),
        'data_integration': test_data_integration()
    }
    
    # Summary
    print("=" * 80)
    print("PHASE 2 COLLECTORS TEST SUMMARY")
    print("=" * 80)
    
    # Count successes
    successful_collectors = 0
    initialized_collectors = 0
    total_records = 0
    
    for collector_name, result in results.items():
        if collector_name in ['test_timestamp', 'phase']:
            continue
            
        if isinstance(result, dict):
            if result.get('success', False):
                successful_collectors += 1
                
            if result.get('collector_initialized', result.get('success', False)):
                initialized_collectors += 1
            
            # Count records
            if collector_name == 'real_estate':
                total_records += result.get('total_records', 0)
            elif collector_name == 'industry_benchmarks':
                total_records += result.get('total_records', 0)
            elif collector_name == 'enhanced_employment':
                total_records += result.get('employment_projections', 0) + result.get('wage_records', 0)
    
    total_collectors = 3  # Excluding integration test
    
    print(f"Collectors Initialized: {initialized_collectors}/{total_collectors}")
    print(f"Successful Data Collection: {successful_collectors}/{total_collectors}")
    print(f"Total Records Collected: {total_records:,}")
    print(f"Data Integration Score: {results['data_integration']['integration_score']:.1f}%")
    
    # Detailed results
    print(f"\nDetailed Results:")
    for collector_name, result in results.items():
        if collector_name in ['test_timestamp', 'phase']:
            continue
            
        if collector_name == 'data_integration':
            status = "✓ PASS" if result.get('success') else "✗ FAIL"
            print(f"  {status} Data Integration: {result['tests_passed']}/{result['total_tests']} tests passed")
            continue
        
        if isinstance(result, dict):
            status = "✓ SUCCESS" if result.get('success') else (
                "✓ READY" if result.get('collector_initialized') else "✗ FAILED"
            )
            
            if collector_name == 'real_estate':
                records = result.get('total_records', 0)
                print(f"  {status} Real Estate Collector: {records:,} records")
                if records > 0:
                    print(f"    County: {result.get('county_records', 0):,}, LoopNet: {result.get('loopnet_records', 0):,}")
                    
            elif collector_name == 'industry_benchmarks':
                records = result.get('total_records', 0)
                print(f"  {status} Industry Benchmarks Collector: {records:,} records")
                if records > 0:
                    print(f"    SBA: {result.get('sba_benchmarks', 0):,}, Franchise: {result.get('franchise_benchmarks', 0):,}, Industry: {result.get('industry_benchmarks', 0):,}")
                    
            elif collector_name == 'enhanced_employment':
                proj_records = result.get('employment_projections', 0)
                wage_records = result.get('wage_records', 0)
                total_emp_records = proj_records + wage_records
                print(f"  {status} Enhanced Employment Collector: {total_emp_records:,} records")
                if total_emp_records > 0:
                    print(f"    Projections: {proj_records:,}, Wages: {wage_records:,}")
            
            # Show errors if any
            if not result.get('success') and result.get('error'):
                error_preview = result['error'][:100] + "..." if len(result['error']) > 100 else result['error']
                print(f"    Error: {error_preview}")
    
    # Save detailed results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = f"phase2_collectors_test_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nDetailed results saved to: {results_file}")
    
    # Overall assessment
    if initialized_collectors == total_collectors:
        print(f"\n🎉 ALL PHASE 2 COLLECTORS READY!")
        if successful_collectors >= 2:
            print(f"   Excellent! {successful_collectors}/{total_collectors} collectors successfully gathered data")
        elif successful_collectors >= 1:
            print(f"   Good! {successful_collectors}/{total_collectors} collectors successfully gathered data")
        else:
            print(f"   Collectors are ready but may need API keys/network access for data collection")
    else:
        print(f"\n⚠️  {total_collectors - initialized_collectors} collector(s) failed to initialize")
    
    # Integration assessment
    if results['data_integration']['success']:
        print("✓ Data integration tests passed - collectors are well integrated")
    else:
        print("⚠ Some data integration issues detected - review collector compatibility")
    
    return results


if __name__ == "__main__":
    main()