#!/usr/bin/env python3
"""
Comprehensive Data Refresh System
=================================

A unified script to refresh all data sources for the Wisconsin business intelligence platform.
Includes DFI registrations, census data, SBA loans, and business licenses.

Usage:
    python comprehensive_data_refresh.py --weekly        # Weekly DFI refresh (default)
    python comprehensive_data_refresh.py --monthly       # Monthly comprehensive refresh
    python comprehensive_data_refresh.py --quarterly     # Quarterly census update
    python comprehensive_data_refresh.py --annual        # Annual full data refresh
    python comprehensive_data_refresh.py --dfi-only      # DFI registrations only
    python comprehensive_data_refresh.py --census-only   # Census data only
    python comprehensive_data_refresh.py --all           # Force refresh everything
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add current directory to path
sys.path.append('.')

# Import data collectors
from weekly_dfi_collection import weekly_dfi_collection
from census_collector import CensusDataCollector
from collect_census_2013_2023 import collect_acs_year, collect_pep_2019, store_to_bigquery
from bls_collector import BLSDataCollector
from google.cloud import bigquery


class ComprehensiveDataRefresh:
    """Manages all data source refreshes in one place"""
    
    def __init__(self):
        self.setup_logging()
        self.client = bigquery.Client(project="location-optimizer-1")
        self.refresh_results = {}
        
    def setup_logging(self):
        """Setup comprehensive logging"""
        log_dir = '/workspaces/Test_for_Claude/Business/wisconsin_data_collection/logs'
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{log_dir}/data_refresh.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def print_header(self, title: str):
        """Print formatted section header"""
        print(f"\n{'='*80}")
        print(f"🔄 {title}")
        print(f"{'='*80}")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def refresh_dfi_registrations(self) -> bool:
        """Refresh DFI business registrations (weekly)"""
        self.print_header("DFI Business Registrations - Weekly Refresh")
        
        try:
            success = weekly_dfi_collection()
            self.refresh_results['dfi'] = {
                'success': success,
                'timestamp': datetime.now(),
                'type': 'weekly'
            }
            return success
        except Exception as e:
            self.logger.error(f"DFI refresh failed: {str(e)}")
            self.refresh_results['dfi'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def refresh_census_current_year(self) -> bool:
        """Refresh current year census data (quarterly)"""
        self.print_header("Census Data - Current Year Refresh")
        
        try:
            current_year = datetime.now().year
            # For ACS data, use previous year as current year data isn't available yet
            data_year = current_year - 1
            
            print(f"📊 Collecting ACS {data_year} data...")
            
            # Collect current year ACS data
            records = collect_acs_year(data_year)
            
            if records:
                print(f"💾 Storing {len(records)} census records...")
                store_to_bigquery(records)
                
                self.refresh_results['census_current'] = {
                    'success': True,
                    'records': len(records),
                    'year': data_year,
                    'timestamp': datetime.now()
                }
                
                print(f"✅ Census {data_year} data refresh complete!")
                return True
            else:
                self.refresh_results['census_current'] = {
                    'success': False,
                    'error': 'No records collected',
                    'timestamp': datetime.now()
                }
                return False
                
        except Exception as e:
            self.logger.error(f"Census refresh failed: {str(e)}")
            self.refresh_results['census_current'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def refresh_historical_census(self) -> bool:
        """Refresh historical census data (annual)"""
        self.print_header("Census Data - Historical Refresh (2013-2023)")
        
        try:
            from collect_census_2013_2023 import main as collect_historical
            
            print("📈 Collecting historical census data...")
            print("⚠️  Note: This may take 15-20 minutes due to API rate limits")
            
            # Run historical collection
            # Note: This will collect all years 2013-2023
            collect_historical()
            
            self.refresh_results['census_historical'] = {
                'success': True,
                'timestamp': datetime.now(),
                'note': 'Historical collection completed'
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"Historical census refresh failed: {str(e)}")
            self.refresh_results['census_historical'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def refresh_population_estimates(self) -> bool:
        """Refresh population estimates (annual)"""
        self.print_header("Population Estimates - Annual Refresh")
        
        try:
            print("📊 Collecting Population Estimates Program data...")
            
            # Collect 2019 PEP data (most recent available for counties)
            pep_data = collect_pep_2019()
            
            if pep_data:
                print(f"📈 Population estimates collected for {len(pep_data)} counties")
                
                # Note: PEP data gets merged with census records automatically
                # during the census collection process
                
                self.refresh_results['population_estimates'] = {
                    'success': True,
                    'counties': len(pep_data),
                    'timestamp': datetime.now()
                }
                
                print("✅ Population estimates refresh complete!")
                return True
            else:
                self.refresh_results['population_estimates'] = {
                    'success': False,
                    'error': 'No PEP data collected',
                    'timestamp': datetime.now()
                }
                return False
                
        except Exception as e:
            self.logger.error(f"Population estimates refresh failed: {str(e)}")
            self.refresh_results['population_estimates'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def refresh_bls_current_year(self) -> bool:
        """Refresh current year BLS data (monthly)"""
        self.print_header("BLS Data - Current Year Refresh")
        
        try:
            current_year = datetime.now().year
            
            print(f"📊 Collecting BLS {current_year} data...")
            
            # Initialize BLS collector
            bls_collector = BLSDataCollector()
            
            # Collect current year data
            results = bls_collector.collect_wisconsin_bls_data(
                start_year=current_year,
                end_year=current_year
            )
            
            if results['collection_summary']['qcew_records'] > 0 or results['collection_summary']['laus_records'] > 0:
                print(f"💾 Storing BLS data...")
                bls_collector.store_bls_data(results)
                
                self.refresh_results['bls_current'] = {
                    'success': True,
                    'qcew_records': results['collection_summary']['qcew_records'],
                    'laus_records': results['collection_summary']['laus_records'],
                    'year': current_year,
                    'timestamp': datetime.now()
                }
                
                print(f"✅ BLS {current_year} data refresh complete!")
                return True
            else:
                self.refresh_results['bls_current'] = {
                    'success': False,
                    'error': 'No records collected',
                    'timestamp': datetime.now()
                }
                return False
                
        except Exception as e:
            self.logger.error(f"BLS current year refresh failed: {str(e)}")
            self.refresh_results['bls_current'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def refresh_bls_historical(self) -> bool:
        """Refresh historical BLS data (annual)"""
        self.print_header("BLS Data - Historical Refresh (2015-Current)")
        
        try:
            print("📈 Collecting historical BLS data...")
            print("⚠️  Note: This may take 30-45 minutes due to API rate limits")
            
            # Run historical BLS collection
            from collect_bls_2015_current import BLSHistoricalCollector
            historical_collector = BLSHistoricalCollector()
            
            # Collect all historical data
            results = historical_collector.collect_by_year(start_year=2015)
            
            self.refresh_results['bls_historical'] = {
                'success': True,
                'total_records': results['total_records'],
                'years_collected': results['years_collected'],
                'timestamp': datetime.now()
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"Historical BLS refresh failed: {str(e)}")
            self.refresh_results['bls_historical'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def check_sba_loans_update(self) -> bool:
        """Check if SBA loans need updating (monthly)"""
        self.print_header("SBA Loans - Update Check")
        
        try:
            # Check when SBA data was last updated
            query = """
            SELECT 
                MAX(data_extraction_date) as last_update,
                COUNT(*) as total_records
            FROM `location-optimizer-1.raw_business_data.sba_loan_approvals`
            """
            
            results = list(self.client.query(query).result())
            
            if results:
                last_update = results[0].last_update
                total_records = results[0].total_records
                
                print(f"📊 Current SBA data:")
                print(f"   Last updated: {last_update}")
                print(f"   Total records: {total_records:,}")
                
                # SBA typically releases quarterly updates
                if last_update:
                    days_since_update = (datetime.now().date() - last_update.date()).days
                    print(f"   Days since update: {days_since_update}")
                    
                    if days_since_update > 90:  # More than 3 months
                        print("⚠️  SBA data may need updating (90+ days old)")
                        print("   Check https://www.sba.gov/about-sba/open-government/foia for new data")
                    else:
                        print("✅ SBA data is current")
                else:
                    print("⚠️  No SBA data found")
                
                self.refresh_results['sba_check'] = {
                    'success': True,
                    'last_update': str(last_update) if last_update else None,
                    'total_records': total_records,
                    'days_since_update': days_since_update if last_update else None,
                    'timestamp': datetime.now()
                }
                
                return True
            
        except Exception as e:
            self.logger.error(f"SBA check failed: {str(e)}")
            self.refresh_results['sba_check'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def check_business_licenses_update(self) -> bool:
        """Check business licenses status (monthly)"""
        self.print_header("Business Licenses - Update Check")
        
        try:
            # Check business licenses data
            query = """
            SELECT 
                MAX(data_extraction_date) as last_update,
                COUNT(*) as total_records,
                COUNT(DISTINCT city) as cities_covered
            FROM `location-optimizer-1.raw_business_data.business_licenses`
            """
            
            results = list(self.client.query(query).result())
            
            if results:
                last_update = results[0].last_update
                total_records = results[0].total_records
                cities_covered = results[0].cities_covered
                
                print(f"📊 Current business licenses data:")
                print(f"   Last updated: {last_update}")
                print(f"   Total records: {total_records:,}")
                print(f"   Cities covered: {cities_covered}")
                
                if last_update:
                    days_since_update = (datetime.now().date() - last_update.date()).days
                    print(f"   Days since update: {days_since_update}")
                    
                    if days_since_update > 30:  # More than 1 month
                        print("⚠️  Business licenses may need updating (30+ days old)")
                    else:
                        print("✅ Business licenses data is current")
                
                self.refresh_results['licenses_check'] = {
                    'success': True,
                    'last_update': str(last_update) if last_update else None,
                    'total_records': total_records,
                    'cities_covered': cities_covered,
                    'timestamp': datetime.now()
                }
                
                return True
            
        except Exception as e:
            self.logger.error(f"Business licenses check failed: {str(e)}")
            self.refresh_results['licenses_check'] = {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now()
            }
            return False
    
    def print_refresh_summary(self):
        """Print summary of all refresh operations"""
        self.print_header("Data Refresh Summary")
        
        for source, result in self.refresh_results.items():
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            print(f"{source.upper():20} | {status}")
            
            if result['success']:
                if 'records' in result:
                    print(f"{'':22} | Records: {result['records']}")
                if 'counties' in result:
                    print(f"{'':22} | Counties: {result['counties']}")
                if 'total_records' in result:
                    print(f"{'':22} | Total: {result['total_records']:,}")
            else:
                if 'error' in result:
                    print(f"{'':22} | Error: {result['error']}")
        
        print(f"\n⏰ Refresh completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Comprehensive Data Refresh System')
    
    # Refresh type options
    parser.add_argument('--weekly', action='store_true', 
                       help='Weekly refresh: DFI registrations only (default)')
    parser.add_argument('--monthly', action='store_true',
                       help='Monthly refresh: DFI + current census + data checks')
    parser.add_argument('--quarterly', action='store_true',
                       help='Quarterly refresh: All monthly + current year census')
    parser.add_argument('--annual', action='store_true',
                       help='Annual refresh: Everything including historical data')
    
    # Individual source options
    parser.add_argument('--dfi-only', action='store_true',
                       help='Refresh DFI registrations only')
    parser.add_argument('--census-only', action='store_true',
                       help='Refresh current year census data only')
    parser.add_argument('--historical-census', action='store_true',
                       help='Refresh historical census data (2013-2023)')
    parser.add_argument('--population-estimates', action='store_true',
                       help='Refresh population estimates only')
    parser.add_argument('--bls-current', action='store_true',
                       help='Refresh current year BLS data only')
    parser.add_argument('--bls-historical', action='store_true',
                       help='Refresh historical BLS data (2015-current)')
    parser.add_argument('--all', action='store_true',
                       help='Force refresh all data sources')
    
    # Utility options
    parser.add_argument('--check-status', action='store_true',
                       help='Check status of all data sources without refreshing')
    
    args = parser.parse_args()
    
    # Initialize refresh system
    refresh_system = ComprehensiveDataRefresh()
    
    # Determine refresh type
    if args.check_status:
        refresh_system.print_header("Data Sources Status Check")
        refresh_system.check_sba_loans_update()
        refresh_system.check_business_licenses_update()
        
    elif args.dfi_only:
        refresh_system.refresh_dfi_registrations()
        
    elif args.census_only:
        refresh_system.refresh_census_current_year()
        
    elif args.historical_census:
        refresh_system.refresh_historical_census()
        
    elif args.population_estimates:
        refresh_system.refresh_population_estimates()
        
    elif args.bls_current:
        refresh_system.refresh_bls_current_year()
        
    elif args.bls_historical:
        refresh_system.refresh_bls_historical()
        
    elif args.all:
        refresh_system.print_header("FULL DATA REFRESH - ALL SOURCES")
        print("⚠️  This will take 60-90 minutes to complete")
        refresh_system.refresh_dfi_registrations()
        refresh_system.refresh_census_current_year()
        refresh_system.refresh_historical_census()
        refresh_system.refresh_population_estimates()
        refresh_system.refresh_bls_current_year()
        refresh_system.refresh_bls_historical()
        refresh_system.check_sba_loans_update()
        refresh_system.check_business_licenses_update()
        
    elif args.annual:
        refresh_system.print_header("ANNUAL DATA REFRESH")
        print("⚠️  This will take 60-90 minutes to complete")
        refresh_system.refresh_dfi_registrations()
        refresh_system.refresh_census_current_year()
        refresh_system.refresh_historical_census()
        refresh_system.refresh_population_estimates()
        refresh_system.refresh_bls_historical()
        refresh_system.check_sba_loans_update()
        refresh_system.check_business_licenses_update()
        
    elif args.quarterly:
        refresh_system.print_header("QUARTERLY DATA REFRESH")
        refresh_system.refresh_dfi_registrations()
        refresh_system.refresh_census_current_year()
        refresh_system.refresh_bls_current_year()
        refresh_system.check_sba_loans_update()
        refresh_system.check_business_licenses_update()
        
    elif args.monthly:
        refresh_system.print_header("MONTHLY DATA REFRESH")
        refresh_system.refresh_dfi_registrations()
        refresh_system.refresh_bls_current_year()
        refresh_system.check_sba_loans_update()
        refresh_system.check_business_licenses_update()
        
    else:  # Default: weekly
        refresh_system.print_header("WEEKLY DATA REFRESH (Default)")
        refresh_system.refresh_dfi_registrations()
    
    # Print summary
    refresh_system.print_refresh_summary()


if __name__ == "__main__":
    main()