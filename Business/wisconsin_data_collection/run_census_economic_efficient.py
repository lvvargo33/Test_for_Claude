#!/usr/bin/env python3
"""
Efficient Census Economic data collection focusing on available years
"""

import os
import sys
import logging
import time
from datetime import datetime
import json

# Set up environment
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-96b6102d3548.json'

from census_economic_collector import CensusEconomicCollector


def main():
    """Run efficient Census Economic data collection"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('census_economic_efficient_collection.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Starting Efficient Census Economic Data Collection")
        logger.info("=" * 80)
        
        # Initialize collector
        collector = CensusEconomicCollector()
        
        all_records = []
        collection_summary = {
            'start_time': datetime.now(),
            'records_collected': {},
            'total_records': 0,
            'industries': set(),
            'errors': []
        }
        
        # Step 1: Collect 2017 Economic Census (most recent available)
        logger.info("\n--- Collecting 2017 Economic Census Data ---")
        try:
            ec_2017 = collector.collect_economic_census_data(
                census_year=2017,
                geographic_levels=['state', 'county'],
                naics_codes=['72', '44-45', '54', '81', '722', '445', '541', '812']  # Focus on key industries
            )
            
            if ec_2017:
                all_records.extend(ec_2017)
                collection_summary['records_collected']['EC_2017'] = len(ec_2017)
                for record in ec_2017:
                    collection_summary['industries'].add(record.naics_code)
                logger.info(f"✓ Collected {len(ec_2017)} records from 2017 Economic Census")
                
        except Exception as e:
            logger.error(f"Error collecting 2017 data: {e}")
            collection_summary['errors'].append(str(e))
        
        # Step 2: Collect recent County Business Patterns (2022, 2021, 2020)
        logger.info("\n--- Collecting Recent County Business Patterns Data ---")
        
        for year in [2022, 2021, 2020, 2019, 2018]:
            try:
                logger.info(f"Collecting {year} CBP data...")
                cbp_records = collector.collect_county_business_patterns(year=year)
                
                if cbp_records:
                    all_records.extend(cbp_records)
                    collection_summary['records_collected'][f'CBP_{year}'] = len(cbp_records)
                    for record in cbp_records:
                        collection_summary['industries'].add(record.naics_code)
                    logger.info(f"✓ Collected {len(cbp_records)} records for {year}")
                    
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Failed to collect {year} CBP data: {e}")
                continue
        
        # Step 3: Try 2012 Economic Census with different API approach
        logger.info("\n--- Attempting 2012 Economic Census Data ---")
        try:
            # For 2012, we need to use different variable names
            collector.api_variables_2012 = [
                'NAICS2012',  # Different NAICS variable for 2012
                'NAICS2012_TTL',  # Title field
                'ESTAB',
                'EMP',
                'PAYANN',
                'RCPTOT'
            ]
            
            # Note: 2012 API may have different structure, skip if not working
            logger.info("Note: 2012 Economic Census API has different structure, may be limited")
            
        except Exception as e:
            logger.info(f"2012 Economic Census not available or incompatible: {e}")
        
        # Step 4: Save to BigQuery
        logger.info("\n--- Uploading to BigQuery ---")
        
        if all_records:
            logger.info(f"Uploading {len(all_records)} total records...")
            success = collector.save_to_bigquery(all_records)
            
            if success:
                logger.info("✓ Successfully uploaded to BigQuery")
            else:
                logger.error("✗ Failed to upload to BigQuery")
                collection_summary['errors'].append("BigQuery upload failed")
        
        # Generate summary
        collection_summary['end_time'] = datetime.now()
        collection_summary['total_records'] = len(all_records)
        collection_summary['industries'] = sorted(list(collection_summary['industries']))
        collection_summary['processing_time_seconds'] = (
            collection_summary['end_time'] - collection_summary['start_time']
        ).total_seconds()
        
        # Display summary
        logger.info("\n" + "=" * 80)
        logger.info("COLLECTION SUMMARY")
        logger.info("=" * 80)
        
        logger.info(f"\nTotal Records Collected: {collection_summary['total_records']:,}")
        logger.info(f"Industries Covered: {len(collection_summary['industries'])}")
        logger.info(f"Processing Time: {collection_summary['processing_time_seconds']:.1f} seconds")
        
        logger.info(f"\nBreakdown by Source:")
        for source, count in collection_summary['records_collected'].items():
            logger.info(f"  - {source}: {count:,} records")
        
        # Save summary
        summary_file = f"census_economic_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(collection_summary, f, indent=2, default=str)
        
        logger.info(f"\nSummary saved to: {summary_file}")
        
        # Also check what's in BigQuery
        logger.info("\n--- Checking BigQuery Table ---")
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project="location-optimizer-1")
            
            # Query to check record counts
            query = """
            SELECT 
                census_year,
                geo_level,
                COUNT(*) as record_count,
                COUNT(DISTINCT naics_code) as industry_count,
                COUNT(DISTINCT county_fips) as county_count
            FROM `location-optimizer-1.raw_business_data.census_economic_benchmarks`
            WHERE state_fips = '55'
            GROUP BY census_year, geo_level
            ORDER BY census_year DESC, geo_level
            """
            
            results = client.query(query).result()
            
            logger.info("\nCurrent BigQuery Table Contents:")
            for row in results:
                logger.info(f"  Year {row.census_year} ({row.geo_level}): "
                           f"{row.record_count} records, "
                           f"{row.industry_count} industries, "
                           f"{row.county_count or 0} counties")
                           
        except Exception as e:
            logger.warning(f"Could not check BigQuery contents: {e}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())