#!/usr/bin/env python3
"""
Run Census Economic data collection for 10 years of historical data
Combines Economic Census (2012, 2017) with County Business Patterns (2013-2022)
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
    """Run comprehensive 10-year Census Economic data collection"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('census_economic_10year_collection.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 80)
        logger.info("Starting 10-Year Census Economic Data Collection for Wisconsin")
        logger.info("=" * 80)
        
        # Initialize collector
        collector = CensusEconomicCollector()
        
        all_records = []
        collection_summary = {
            'start_time': datetime.now(),
            'economic_census_years': [],
            'cbp_years': [],
            'total_records': 0,
            'records_by_year': {},
            'industries_collected': set(),
            'counties_collected': set(),
            'errors': []
        }
        
        # Step 1: Collect Economic Census data (2012 and 2017)
        logger.info("\n--- PHASE 1: Economic Census Data Collection ---")
        
        for census_year in [2017, 2012]:
            logger.info(f"\nCollecting {census_year} Economic Census data...")
            try:
                ec_records = collector.collect_economic_census_data(
                    census_year=census_year,
                    geographic_levels=['state', 'county']
                )
                
                if ec_records:
                    all_records.extend(ec_records)
                    collection_summary['economic_census_years'].append(census_year)
                    collection_summary['records_by_year'][f'EC_{census_year}'] = len(ec_records)
                    
                    # Track industries and counties
                    for record in ec_records:
                        collection_summary['industries_collected'].add(record.naics_code)
                        if record.county_fips:
                            collection_summary['counties_collected'].add(record.county_fips)
                    
                    logger.info(f"  ✓ Collected {len(ec_records)} records for {census_year}")
                else:
                    logger.warning(f"  ⚠ No records collected for {census_year}")
                    
                # Brief pause between years
                time.sleep(2)
                
            except Exception as e:
                error_msg = f"Error collecting {census_year} Economic Census: {str(e)}"
                logger.error(f"  ✗ {error_msg}")
                collection_summary['errors'].append(error_msg)
        
        # Step 2: Collect County Business Patterns data (2013-2022)
        logger.info("\n--- PHASE 2: County Business Patterns Annual Data Collection ---")
        
        # Collect CBP data year by year
        for cbp_year in range(2022, 2012, -1):  # Start with most recent
            logger.info(f"\nCollecting {cbp_year} County Business Patterns data...")
            try:
                cbp_records = collector.collect_county_business_patterns(year=cbp_year)
                
                if cbp_records:
                    all_records.extend(cbp_records)
                    collection_summary['cbp_years'].append(cbp_year)
                    collection_summary['records_by_year'][f'CBP_{cbp_year}'] = len(cbp_records)
                    
                    # Track industries
                    for record in cbp_records:
                        collection_summary['industries_collected'].add(record.naics_code)
                    
                    logger.info(f"  ✓ Collected {len(cbp_records)} records for {cbp_year}")
                else:
                    logger.warning(f"  ⚠ No records collected for {cbp_year}")
                
                # Rate limiting between years
                time.sleep(1)
                
            except Exception as e:
                error_msg = f"Error collecting {cbp_year} CBP data: {str(e)}"
                logger.error(f"  ✗ {error_msg}")
                collection_summary['errors'].append(error_msg)
                
                # Continue with next year even if one fails
                continue
        
        # Step 3: Save all data to BigQuery
        logger.info("\n--- PHASE 3: BigQuery Upload ---")
        
        if all_records:
            logger.info(f"Uploading {len(all_records)} total records to BigQuery...")
            success = collector.save_to_bigquery(all_records)
            
            if success:
                logger.info("  ✓ Successfully uploaded all records to BigQuery")
            else:
                logger.error("  ✗ Failed to upload records to BigQuery")
                collection_summary['errors'].append("BigQuery upload failed")
        else:
            logger.warning("No records to upload")
        
        # Step 4: Generate comprehensive summary
        collection_summary['end_time'] = datetime.now()
        collection_summary['total_records'] = len(all_records)
        collection_summary['processing_time_minutes'] = (
            collection_summary['end_time'] - collection_summary['start_time']
        ).total_seconds() / 60
        
        # Convert sets to lists for JSON serialization
        collection_summary['industries_collected'] = sorted(list(collection_summary['industries_collected']))
        collection_summary['counties_collected'] = sorted(list(collection_summary['counties_collected']))
        
        # Display summary
        logger.info("\n" + "=" * 80)
        logger.info("COLLECTION COMPLETE - 10 YEAR SUMMARY")
        logger.info("=" * 80)
        
        logger.info(f"\nTime Period Coverage:")
        logger.info(f"  - Economic Census Years: {collection_summary['economic_census_years']}")
        logger.info(f"  - County Business Patterns Years: {sorted(collection_summary['cbp_years'])}")
        
        logger.info(f"\nRecords Collected:")
        logger.info(f"  - Total Records: {collection_summary['total_records']:,}")
        logger.info(f"  - Industries Covered: {len(collection_summary['industries_collected'])}")
        logger.info(f"  - Counties Covered: {len(collection_summary['counties_collected'])}")
        
        logger.info(f"\nBreakdown by Year:")
        for year_type, count in sorted(collection_summary['records_by_year'].items()):
            logger.info(f"  - {year_type}: {count:,} records")
        
        logger.info(f"\nProcessing Time: {collection_summary['processing_time_minutes']:.1f} minutes")
        
        if collection_summary['errors']:
            logger.warning(f"\nErrors Encountered: {len(collection_summary['errors'])}")
            for error in collection_summary['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  - {error}")
        
        # Save detailed summary to file
        summary_file = f"census_economic_10year_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(collection_summary, f, indent=2, default=str)
        
        logger.info(f"\nDetailed summary saved to: {summary_file}")
        
        # Create a simple text summary too
        text_summary_file = summary_file.replace('.json', '.txt')
        with open(text_summary_file, 'w') as f:
            f.write("10-Year Census Economic Data Collection Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Collection Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Records: {collection_summary['total_records']:,}\n")
            f.write(f"Industries: {len(collection_summary['industries_collected'])}\n")
            f.write(f"Counties: {len(collection_summary['counties_collected'])}\n")
            f.write(f"Processing Time: {collection_summary['processing_time_minutes']:.1f} minutes\n\n")
            
            f.write("Year-by-Year Breakdown:\n")
            for year_type, count in sorted(collection_summary['records_by_year'].items()):
                f.write(f"  {year_type}: {count:,} records\n")
            
            f.write(f"\nData Period: {min(collection_summary['cbp_years'] + collection_summary['economic_census_years'])} - {max(collection_summary['cbp_years'] + collection_summary['economic_census_years'])}\n")
            
            if collection_summary['errors']:
                f.write(f"\nErrors: {len(collection_summary['errors'])}\n")
        
        logger.info(f"Text summary saved to: {text_summary_file}")
        
        return 0 if collection_summary['total_records'] > 0 else 1
        
    except Exception as e:
        logger.error(f"Fatal error in collection: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())