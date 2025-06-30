#!/usr/bin/env python3
"""
Run Census Economic data collection and upload to BigQuery
"""

import os
import sys
import logging
from datetime import datetime

# Set up environment
if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-96b6102d3548.json'

from census_economic_collector import CensusEconomicCollector


def main():
    """Run Census Economic data collection"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('census_economic_collection.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Census Economic data collection")
        logger.info("=" * 60)
        
        # Initialize collector
        collector = CensusEconomicCollector()
        
        # Run collection for 2017 Economic Census
        logger.info("Collecting 2017 Economic Census data...")
        summary_2017 = collector.run_collection(
            census_year=2017,
            include_cbp=False  # Don't include CBP in this run
        )
        
        logger.info(f"2017 Collection Summary:")
        logger.info(f"  - Records collected: {summary_2017['total_records']}")
        logger.info(f"  - Industries: {len(summary_2017['industries_collected'])}")
        logger.info(f"  - Success: {summary_2017['success']}")
        
        # Also collect recent County Business Patterns data
        logger.info("\nCollecting County Business Patterns data...")
        cbp_records = collector.collect_county_business_patterns(year=2022)
        
        if cbp_records:
            # Save CBP data
            success = collector.save_to_bigquery(cbp_records)
            logger.info(f"CBP data saved to BigQuery: {success}")
            logger.info(f"CBP records collected: {len(cbp_records)}")
        
        # Summary report
        logger.info("\n" + "=" * 60)
        logger.info("COLLECTION COMPLETE")
        logger.info("=" * 60)
        
        total_records = summary_2017['total_records'] + len(cbp_records)
        logger.info(f"Total records collected: {total_records}")
        
        if summary_2017['industries_collected']:
            logger.info(f"\nIndustries collected:")
            for naics in sorted(summary_2017['industries_collected'])[:10]:
                logger.info(f"  - {naics}")
            
            if len(summary_2017['industries_collected']) > 10:
                logger.info(f"  ... and {len(summary_2017['industries_collected']) - 10} more")
        
        # Write summary to file
        summary_file = f"census_economic_collection_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(summary_file, 'w') as f:
            f.write(f"Census Economic Data Collection Summary\n")
            f.write(f"{'=' * 40}\n")
            f.write(f"Collection Date: {datetime.now().isoformat()}\n\n")
            f.write(f"2017 Economic Census:\n")
            f.write(f"  - Records: {summary_2017['total_records']}\n")
            f.write(f"  - Industries: {len(summary_2017['industries_collected'])}\n")
            f.write(f"  - Processing Time: {summary_2017['processing_time_seconds']:.1f}s\n\n")
            f.write(f"County Business Patterns:\n")
            f.write(f"  - Records: {len(cbp_records)}\n\n")
            f.write(f"Total Records: {total_records}\n")
            f.write(f"Data uploaded to BigQuery: {summary_2017['success']}\n")
        
        logger.info(f"\nSummary written to: {summary_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in collection: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())