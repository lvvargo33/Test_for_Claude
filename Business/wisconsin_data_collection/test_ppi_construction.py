#!/usr/bin/env python3
"""
Test script for BLS PPI Construction Materials collector
"""

import logging
import json
import os
from datetime import datetime

# Set up environment
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-96b6102d3548.json'

from bls_ppi_construction_collector import BLSPPIConstructionCollector


def test_ppi_construction_collector():
    """Test the BLS PPI Construction collector functionality"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize collector
        logger.info("Initializing BLS PPI Construction Collector...")
        collector = BLSPPIConstructionCollector()
        
        # Test 1: Collect recent data (2022-2024)
        logger.info("\n=== Test 1: Recent Construction Materials Data ===")
        recent_records = collector.collect_construction_materials_data(
            start_year=2022,
            end_year=2024
        )
        
        logger.info(f"Collected {len(recent_records)} recent records")
        
        if recent_records:
            # Display sample records by category
            categories = {}
            for record in recent_records:
                cat = record.material_category
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(record)
            
            logger.info(f"\nSample records by category:")
            for category, records in list(categories.items())[:5]:  # Show first 5 categories
                latest = max(records, key=lambda x: (x.year, x.period))
                logger.info(f"  {category}: Index {latest.index_value:.1f} ({latest.year}-{latest.period})")
                if latest.yearly_change_pct:
                    logger.info(f"    Yearly change: {latest.yearly_change_pct:+.1f}%")
        
        # Test 2: Cost summary analysis
        logger.info("\n=== Test 2: Construction Cost Analysis ===")
        cost_summary = collector.get_construction_cost_summary(recent_records)
        
        logger.info(f"Materials tracked: {len(cost_summary['materials_tracked'])}")
        logger.info(f"Latest year: {cost_summary['latest_year']}")
        
        logger.info("\nCost trends by material:")
        for material, trend_data in cost_summary['cost_trends'].items():
            change = trend_data['avg_yearly_change_pct']
            trend = trend_data['trend']
            index = trend_data['latest_index']
            logger.info(f"  {material}: {change:+.1f}% ({trend}) - Index: {index:.1f}")
        
        # Identify materials with significant price changes
        if cost_summary['cost_increases']:
            logger.info(f"\nMaterials with significant cost increases:")
            for material, change in cost_summary['cost_increases'].items():
                logger.info(f"  {material}: +{change:.1f}%")
        
        if cost_summary['cost_decreases']:
            logger.info(f"\nMaterials with significant cost decreases:")
            for material, change in cost_summary['cost_decreases'].items():
                logger.info(f"  {material}: {change:.1f}%")
        
        # Test 3: Specific material analysis
        logger.info("\n=== Test 3: Lumber Price Analysis ===")
        lumber_records = [r for r in recent_records if r.material_category == 'Lumber']
        
        if lumber_records:
            # Sort by date and show trend
            lumber_records.sort(key=lambda x: (x.year, x.period))
            
            logger.info(f"Lumber records collected: {len(lumber_records)}")
            
            # Show recent lumber prices
            recent_lumber = [r for r in lumber_records if r.year >= 2023]
            for record in recent_lumber[-6:]:  # Last 6 data points
                change_str = f", {record.yearly_change_pct:+.1f}% YoY" if record.yearly_change_pct else ""
                logger.info(f"  {record.year}-{record.period}: {record.index_value:.1f}{change_str}")
        
        # Test 4: Steel price analysis  
        logger.info("\n=== Test 4: Steel Price Analysis ===")
        steel_records = [r for r in recent_records if r.material_category == 'Steel']
        
        if steel_records:
            steel_records.sort(key=lambda x: (x.year, x.period))
            logger.info(f"Steel records collected: {len(steel_records)}")
            
            # Show steel price volatility
            recent_steel = [r for r in steel_records if r.year >= 2023]
            for record in recent_steel[-6:]:
                change_str = f", {record.yearly_change_pct:+.1f}% YoY" if record.yearly_change_pct else ""
                logger.info(f"  {record.series_title}: {record.index_value:.1f}{change_str}")
        
        # Test 5: Full collection run
        logger.info("\n=== Test 5: Full Collection Run ===")
        summary = collector.run_collection(start_year=2023, end_year=2024)
        
        logger.info(f"\nCollection Summary:")
        logger.info(f"  Records Collected: {summary['records_collected']:,}")
        logger.info(f"  Materials Tracked: {summary['materials_tracked']}")
        logger.info(f"  Series Collected: {summary['series_collected']}")
        logger.info(f"  Success: {summary['success']}")
        logger.info(f"  Processing Time: {summary['processing_time_seconds']:.1f}s")
        
        # Test 6: Save sample data
        logger.info("\n=== Test 6: Saving Sample Data ===")
        
        sample_data = {
            'collection_timestamp': datetime.now().isoformat(),
            'recent_records_count': len(recent_records),
            'cost_summary': cost_summary,
            'collection_summary': summary,
            'sample_records': [
                record.model_dump() for record in recent_records[:10]
            ]
        }
        
        with open('ppi_construction_test_results.json', 'w') as f:
            json.dump(sample_data, f, indent=2, default=str)
        
        logger.info("Sample data saved to ppi_construction_test_results.json")
        
        logger.info("\n✅ All PPI construction tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    test_ppi_construction_collector()