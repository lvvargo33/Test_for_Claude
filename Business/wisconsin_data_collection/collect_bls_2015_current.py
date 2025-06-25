#!/usr/bin/env python3
"""
Collect BLS Data 2015-Current
=============================

Collects BLS QCEW and LAUS data for Wisconsin counties from 2015 to current year.
Implements proper rate limiting and batch processing for API limits.
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
import argparse

# Set credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json'

# Add current directory to path
sys.path.append('.')

from bls_collector import BLSDataCollector


class BLSHistoricalCollector:
    """Collects historical BLS data from 2015 to current with proper batching"""
    
    def __init__(self):
        self.collector = BLSDataCollector()
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def collect_by_year(self, start_year: int = 2015, end_year: int = None):
        """Collect data year by year to manage API limits"""
        if end_year is None:
            end_year = datetime.now().year
        
        self.logger.info(f"Starting BLS historical collection: {start_year}-{end_year}")
        
        all_results = {
            'qcew_data': [],
            'laus_data': [],
            'total_records': 0,
            'years_collected': []
        }
        
        for year in range(start_year, end_year + 1):
            try:
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"Collecting BLS data for year {year}")
                self.logger.info(f"{'='*60}")
                
                # Collect data for this year only
                year_results = self.collector.collect_wisconsin_bls_data(
                    start_year=year, 
                    end_year=year
                )
                
                # Aggregate results
                all_results['qcew_data'].extend(year_results['qcew_data'])
                all_results['laus_data'].extend(year_results['laus_data'])
                all_results['years_collected'].append(year)
                
                year_total = len(year_results['qcew_data']) + len(year_results['laus_data'])
                all_results['total_records'] += year_total
                
                self.logger.info(f"Year {year} complete: {year_total} records collected")
                
                # Store data for this year immediately
                if year_total > 0:
                    self.logger.info(f"Storing {year} data to BigQuery...")
                    self.collector.store_bls_data(year_results)
                
                # Wait between years to respect API limits
                if year < end_year:
                    self.logger.info("Waiting 60 seconds before next year...")
                    time.sleep(60)  # 1 minute between years
                
            except Exception as e:
                self.logger.error(f"Error collecting data for year {year}: {str(e)}")
                continue
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Historical collection complete!")
        self.logger.info(f"Years collected: {all_results['years_collected']}")
        self.logger.info(f"Total QCEW records: {len(all_results['qcew_data'])}")
        self.logger.info(f"Total LAUS records: {len(all_results['laus_data'])}")
        self.logger.info(f"Grand total: {all_results['total_records']} records")
        
        return all_results
    
    def collect_recent_years_only(self):
        """Collect just the most recent years for testing"""
        current_year = datetime.now().year
        start_year = current_year - 2  # Last 3 years
        
        self.logger.info(f"Collecting recent years only: {start_year}-{current_year}")
        
        return self.collect_by_year(start_year, current_year)


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Collect BLS historical data 2015-current')
    parser.add_argument('--start-year', type=int, default=2015,
                       help='Starting year (default: 2015)')
    parser.add_argument('--end-year', type=int, 
                       help='Ending year (default: current year)')
    parser.add_argument('--recent-only', action='store_true',
                       help='Collect only recent years for testing')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - collect single year only')
    
    args = parser.parse_args()
    
    collector = BLSHistoricalCollector()
    
    if args.test:
        print("🧪 Test Mode - Collecting 2023 data only")
        results = collector.collect_by_year(2023, 2023)
        
    elif args.recent_only:
        print("📅 Recent Years Mode - Collecting last 3 years")
        results = collector.collect_recent_years_only()
        
    else:
        print("📊 Full Historical Mode - Collecting 2015-current")
        results = collector.collect_by_year(args.start_year, args.end_year)
    
    print(f"\n✅ BLS historical collection completed!")
    print(f"📈 Total records collected: {results['total_records']}")


if __name__ == "__main__":
    main()