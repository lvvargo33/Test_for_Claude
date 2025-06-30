#!/usr/bin/env python3
"""
Test script for BLS CPI Data Collection
"""

import logging
import json
import os
from datetime import datetime

# Set up environment
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-96b6102d3548.json'

from bls_cpi_collector import BLSCPICollector


def test_cpi_data_collection():
    """Test the BLS CPI collector functionality"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize collector
        logger.info("Initializing BLS CPI Collector...")
        collector = BLSCPICollector()
        
        # Test 1: Collect sample recent data
        logger.info("\n=== Test 1: Recent CPI Data Collection ===")
        recent_records = collector.collect_cpi_data(start_year=2023, end_year=2024)
        
        logger.info(f"Collected {len(recent_records)} recent CPI records")
        
        if recent_records:
            # Display sample records by category
            categories = {}
            for record in recent_records:
                cat = record.category
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(record)
            
            logger.info(f"\nSample records by category:")
            for category, records in list(categories.items())[:5]:  # Show first 5 categories
                latest = max(records, key=lambda x: (x.year, x.period))
                logger.info(f"  {category}: Index {latest.index_value:.1f} ({latest.year}-{latest.period})")
                if latest.annual_change_pct:
                    logger.info(f"    Annual inflation: {latest.annual_change_pct:+.1f}% ({latest.inflation_trend})")
        
        # Test 2: Inflation analysis
        logger.info("\n=== Test 2: Inflation Analysis ===")
        inflation_summary = collector.get_inflation_summary(recent_records)
        
        logger.info(f"Categories tracked: {len(inflation_summary['categories_tracked'])}")
        logger.info(f"Geographic areas: {inflation_summary['geographic_areas']}")
        logger.info(f"Latest year: {inflation_summary['latest_year']}")
        
        logger.info("\nInflation trends by category:")
        for category, analysis in inflation_summary['category_analysis'].items():
            change = analysis['avg_annual_inflation_pct']
            trend = analysis['trend']
            index = analysis['latest_index']
            logger.info(f"  {category}: {change:+.1f}% ({trend}) - Index: {index:.1f}")
        
        # Test 3: Geographic comparison
        logger.info("\n=== Test 3: Geographic Analysis ===")
        national_records = [r for r in recent_records if r.geographic_area == 'US National' and r.category == 'All Items']
        midwest_records = [r for r in recent_records if r.geographic_area == 'Midwest Region']
        milwaukee_records = [r for r in recent_records if r.geographic_area == 'Milwaukee Metro']
        
        logger.info(f"National CPI records: {len(national_records)}")
        logger.info(f"Midwest CPI records: {len(midwest_records)}")
        logger.info(f"Milwaukee CPI records: {len(milwaukee_records)}")
        
        if national_records and midwest_records:
            latest_national = max(national_records, key=lambda x: (x.year, x.period))
            latest_midwest = max(midwest_records, key=lambda x: (x.year, x.period))
            
            logger.info(f"\nGeographic comparison (latest):")
            logger.info(f"  National: {latest_national.index_value:.1f} ({latest_national.annual_change_pct:+.1f}%)")
            logger.info(f"  Midwest: {latest_midwest.index_value:.1f} ({latest_midwest.annual_change_pct:+.1f}%)")
        
        # Test 4: Historical trend analysis
        logger.info("\n=== Test 4: Historical Trend Analysis ===")
        
        # Get housing CPI trend (important for business costs)
        housing_records = [r for r in recent_records if r.category == 'Housing' and r.geographic_area == 'US National']
        if housing_records:
            housing_records.sort(key=lambda x: (x.year, x.period))
            
            logger.info(f"Housing CPI trend analysis:")
            logger.info(f"  Records collected: {len(housing_records)}")
            
            # Show recent housing inflation
            recent_housing = [r for r in housing_records if r.year >= 2023]
            for record in recent_housing[-6:]:  # Last 6 data points
                change_str = f", {record.annual_change_pct:+.1f}% inflation" if record.annual_change_pct else ""
                logger.info(f"    {record.year}-{record.period}: {record.index_value:.1f}{change_str}")
        
        # Test 5: Energy price volatility
        logger.info("\n=== Test 5: Energy Price Volatility ===")
        energy_records = [r for r in recent_records if r.category == 'Energy']
        gasoline_records = [r for r in recent_records if 'Gasoline' in r.series_title]
        
        if energy_records:
            energy_records.sort(key=lambda x: (x.year, x.period))
            logger.info(f"Energy CPI records: {len(energy_records)}")
            
            # Calculate volatility
            annual_changes = [r.annual_change_pct for r in energy_records if r.annual_change_pct is not None]
            if annual_changes:
                avg_change = sum(annual_changes) / len(annual_changes)
                logger.info(f"  Average energy inflation: {avg_change:+.1f}%")
                
                # Show volatility
                latest_energy = max(energy_records, key=lambda x: (x.year, x.period))
                logger.info(f"  Latest energy index: {latest_energy.index_value:.1f} ({latest_energy.inflation_trend})")
        
        # Test 6: Save sample data for analysis
        logger.info("\n=== Test 6: Saving Sample Data ===")
        
        sample_data = {
            'collection_timestamp': datetime.now().isoformat(),
            'recent_records_count': len(recent_records),
            'inflation_summary': inflation_summary,
            'sample_records': [
                record.model_dump() for record in recent_records[:10]
            ]
        }
        
        with open('cpi_test_results.json', 'w') as f:
            json.dump(sample_data, f, indent=2, default=str)
        
        logger.info("Sample data saved to cpi_test_results.json")
        
        logger.info("\n✅ All CPI collection tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    test_cpi_data_collection()