#!/usr/bin/env python3
"""
Test script for Census Economic Census data collector
"""

import logging
import json
from datetime import datetime

from census_economic_collector import CensusEconomicCollector, EconomicCensusRecord


def test_economic_census_collector():
    """Test the Census Economic Census collector functionality"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize collector
        logger.info("Initializing Census Economic Collector...")
        collector = CensusEconomicCollector()
        
        # Test 1: Collect state-level data for a few industries
        logger.info("\n=== Test 1: State-level Economic Census data ===")
        state_records = collector.collect_economic_census_data(
            census_year=2017,
            geographic_levels=['state'],
            naics_codes=['72', '44-45', '54']  # Food services, Retail, Professional services
        )
        
        logger.info(f"Collected {len(state_records)} state-level records")
        
        if state_records:
            # Display sample record
            sample = state_records[0]
            logger.info(f"\nSample state record:")
            logger.info(f"  Industry: {sample.naics_code} - {sample.naics_title}")
            logger.info(f"  Establishments: {sample.establishments_total:,}" if sample.establishments_total else "  Establishments: N/A")
            logger.info(f"  Employees: {sample.employees_total:,}" if sample.employees_total else "  Employees: N/A")
            logger.info(f"  Revenue: ${sample.revenue_total:,.0f}k" if sample.revenue_total else "  Revenue: N/A")
            logger.info(f"  Data Quality Score: {sample.data_quality_score}")
        
        # Test 2: Collect county-level data
        logger.info("\n=== Test 2: County-level Economic Census data ===")
        county_records = collector.collect_economic_census_data(
            census_year=2017,
            geographic_levels=['county'],
            naics_codes=['722']  # Restaurants
        )
        
        logger.info(f"Collected {len(county_records)} county-level records")
        
        if county_records:
            # Show records for major counties
            major_counties = ['55079', '55025', '55009']  # Milwaukee, Dane, Brown
            for record in county_records:
                if record.county_fips in major_counties:
                    logger.info(f"\n{record.county_name} County:")
                    logger.info(f"  Industry: {record.naics_code} - {record.naics_title}")
                    logger.info(f"  Establishments: {record.establishments_total:,}" if record.establishments_total else "  Establishments: N/A")
                    logger.info(f"  Revenue per establishment: ${record.revenue_per_establishment:,.0f}" if record.revenue_per_establishment else "  Revenue per establishment: N/A")
        
        # Test 3: County Business Patterns (annual data)
        logger.info("\n=== Test 3: County Business Patterns data ===")
        cbp_records = collector.collect_county_business_patterns(year=2022)
        
        logger.info(f"Collected {len(cbp_records)} County Business Patterns records")
        
        if cbp_records:
            sample = cbp_records[0]
            logger.info(f"\nSample CBP record (2022):")
            logger.info(f"  Industry: {sample.naics_code} - {sample.naics_title}")
            logger.info(f"  Establishments: {sample.establishments_total:,}" if sample.establishments_total else "  Establishments: N/A")
            logger.info(f"  Employees: {sample.employees_total:,}" if sample.employees_total else "  Employees: N/A")
        
        # Test 4: Calculate industry benchmarks
        logger.info("\n=== Test 4: Industry Benchmark Calculations ===")
        
        # Group by NAICS code and calculate benchmarks
        benchmarks = {}
        all_records = state_records + county_records
        
        for record in all_records:
            if record.naics_code not in benchmarks:
                benchmarks[record.naics_code] = {
                    'industry': record.naics_title,
                    'records': [],
                    'total_establishments': 0,
                    'total_revenue': 0,
                    'total_employees': 0
                }
            
            benchmarks[record.naics_code]['records'].append(record)
            if record.establishments_total:
                benchmarks[record.naics_code]['total_establishments'] += record.establishments_total
            if record.revenue_total:
                benchmarks[record.naics_code]['total_revenue'] += record.revenue_total
            if record.employees_total:
                benchmarks[record.naics_code]['total_employees'] += record.employees_total
        
        # Display benchmarks
        for naics, data in benchmarks.items():
            if data['total_establishments'] > 0:
                logger.info(f"\nBenchmarks for {naics} - {data['industry']}:")
                logger.info(f"  Total establishments: {data['total_establishments']:,}")
                logger.info(f"  Total employees: {data['total_employees']:,}")
                logger.info(f"  Total revenue: ${data['total_revenue']:,.0f}k")
                
                if data['total_employees'] > 0:
                    avg_revenue_per_employee = data['total_revenue'] / data['total_employees']
                    logger.info(f"  Average revenue per employee: ${avg_revenue_per_employee:,.0f}k")
        
        # Test 5: Save sample data to JSON for inspection
        logger.info("\n=== Test 5: Saving sample data ===")
        
        sample_data = {
            'collection_timestamp': datetime.now().isoformat(),
            'state_records_count': len(state_records),
            'county_records_count': len(county_records),
            'cbp_records_count': len(cbp_records),
            'sample_state_record': state_records[0].model_dump() if state_records else None,
            'sample_county_record': county_records[0].model_dump() if county_records else None,
            'sample_cbp_record': cbp_records[0].model_dump() if cbp_records else None,
            'benchmarks_summary': {
                naics: {
                    'industry': data['industry'],
                    'establishments': data['total_establishments'],
                    'revenue_thousands': data['total_revenue'],
                    'employees': data['total_employees']
                }
                for naics, data in benchmarks.items()
                if data['total_establishments'] > 0
            }
        }
        
        with open('census_economic_test_results.json', 'w') as f:
            json.dump(sample_data, f, indent=2, default=str)
        
        logger.info("Sample data saved to census_economic_test_results.json")
        
        # Test 6: Run full collection
        logger.info("\n=== Test 6: Full collection run ===")
        summary = collector.run_collection(census_year=2017, include_cbp=False)
        
        logger.info(f"\nCollection Summary:")
        logger.info(f"  Economic Census Records: {summary['economic_census_records']}")
        logger.info(f"  CBP Records: {summary['cbp_records']}")
        logger.info(f"  Total Records: {summary['total_records']}")
        logger.info(f"  Industries: {len(summary['industries_collected'])}")
        logger.info(f"  Success: {summary['success']}")
        logger.info(f"  Time: {summary['processing_time_seconds']:.1f}s")
        
        if summary['errors']:
            logger.error(f"  Errors: {summary['errors']}")
        
        logger.info("\n✅ All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    test_economic_census_collector()