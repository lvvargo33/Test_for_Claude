#!/usr/bin/env python3
"""
Historical Census Data Collection Script
=======================================

Collects census data from 2013-2024 including:
- American Community Survey (ACS) 5-year estimates for each year
- Population Estimates Program (PEP) data where available
- County-level data for all Wisconsin counties
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
import argparse

# Add the current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from census_collector import CensusDataCollector
from models import CensusGeography

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HistoricalCensusCollector:
    """Collects historical census data from 2013-2024"""
    
    def __init__(self):
        self.collector = CensusDataCollector()
        self.years_to_collect = list(range(2013, 2025))  # 2013-2024
        self.collected_data = []
        
    def collect_all_years(self, geographic_levels: List[str] = None):
        """Collect census data for all years"""
        if geographic_levels is None:
            geographic_levels = ['county']  # Start with county level
            
        logger.info(f"Starting historical census collection for years {self.years_to_collect[0]}-{self.years_to_collect[-1]}")
        logger.info(f"Geographic levels: {geographic_levels}")
        
        for year in self.years_to_collect:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Collecting data for year {year}")
                logger.info(f"{'='*60}")
                
                # Check if ACS 5-year data is available for this year
                # ACS 5-year estimates are available starting from 2009
                if year >= 2009:
                    self._collect_acs_year(year, geographic_levels)
                else:
                    logger.warning(f"ACS 5-year data not available for {year}")
                
                # Add delay between years to avoid rate limiting
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Failed to collect data for year {year}: {str(e)}")
                continue
        
        logger.info(f"\nCollection complete! Total records collected: {len(self.collected_data)}")
        return self.collected_data
    
    def _collect_acs_year(self, year: int, geographic_levels: List[str]):
        """Collect ACS data for a specific year"""
        try:
            # Collect Wisconsin demographics for this year
            summary = self.collector.collect_wisconsin_demographics(
                geographic_levels=geographic_levels,
                acs_year=year,
                include_population_estimates=True
            )
            
            logger.info(f"Year {year} collection summary:")
            logger.info(f"  - Counties collected: {summary.counties_collected}")
            logger.info(f"  - Tracts collected: {summary.tracts_collected}")
            logger.info(f"  - Block groups collected: {summary.block_groups_collected}")
            logger.info(f"  - Total records: {summary.total_records}")
            logger.info(f"  - Collection time: {summary.collection_time_seconds:.2f} seconds")
            
            # Add year marker to collected data
            if summary.total_records > 0:
                self.collected_data.extend(summary.records if hasattr(summary, 'records') else [])
                
        except Exception as e:
            logger.error(f"Error collecting ACS data for {year}: {str(e)}")
    
    def collect_population_estimates(self):
        """Collect population estimates data for multiple years"""
        logger.info("\nCollecting Population Estimates Program data...")
        
        # PEP data availability:
        # - County-level: 2010-2019 (API discontinued county support after 2019)
        # - We'll collect 2019 data and use Census 2020 data for 2020+
        
        pep_years = [2019]  # Most recent year with county-level API support
        
        for year in pep_years:
            try:
                logger.info(f"Collecting PEP data for {year}...")
                estimates = self.collector._collect_population_estimates(pep_year=year)
                logger.info(f"Collected population estimates for {len(estimates)} counties")
                
            except Exception as e:
                logger.error(f"Failed to collect PEP data for {year}: {str(e)}")
    
    def save_summary_report(self):
        """Generate and save a summary report of collected data"""
        report_path = "historical_census_collection_report.txt"
        
        with open(report_path, 'w') as f:
            f.write("Historical Census Data Collection Report\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Collection Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Years Collected: {self.years_to_collect[0]}-{self.years_to_collect[-1]}\n")
            f.write(f"Total Records: {len(self.collected_data)}\n\n")
            
            # Count records by year
            year_counts = {}
            for record in self.collected_data:
                if hasattr(record, 'acs_year'):
                    year = record.acs_year
                    year_counts[year] = year_counts.get(year, 0) + 1
            
            f.write("Records by Year:\n")
            for year in sorted(year_counts.keys()):
                f.write(f"  {year}: {year_counts[year]} records\n")
            
            f.write(f"\nReport saved to: {report_path}\n")
        
        logger.info(f"Summary report saved to {report_path}")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Collect historical census data from 2013-2024')
    parser.add_argument('--years', nargs='+', type=int, 
                       help='Specific years to collect (default: 2013-2024)')
    parser.add_argument('--levels', nargs='+', 
                       choices=['county', 'tract', 'block_group'],
                       default=['county'],
                       help='Geographic levels to collect')
    parser.add_argument('--pep', action='store_true',
                       help='Also collect Population Estimates Program data')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - collect only 2022-2023 data')
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = HistoricalCensusCollector()
    
    # Override years if specified
    if args.test:
        collector.years_to_collect = [2022, 2023]
        logger.info("Running in test mode - collecting only 2022-2023 data")
    elif args.years:
        collector.years_to_collect = args.years
    
    # Collect ACS data
    collector.collect_all_years(geographic_levels=args.levels)
    
    # Collect PEP data if requested
    if args.pep:
        collector.collect_population_estimates()
    
    # Save summary report
    collector.save_summary_report()
    
    logger.info("\nHistorical census collection complete!")


if __name__ == "__main__":
    main()