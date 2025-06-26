"""
Phase 1 Data Collection Runner
==============================

Orchestrates the collection of Phase 1 data sources:
1. Wisconsin DOT Traffic Data
2. County GIS Zoning Data  
3. BEA Consumer Spending Data

Integrates with existing BigQuery infrastructure.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from traffic_data_collector import WisconsinTrafficDataCollector
from zoning_data_collector import WisconsinZoningDataCollector  
from consumer_spending_collector import BEAConsumerSpendingCollector


class Phase1DataCollectionOrchestrator:
    """
    Orchestrates Phase 1 data collection across all new collectors
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.collectors = {}
        self.results = {}
        
        # Initialize collectors
        try:
            self.collectors['traffic'] = WisconsinTrafficDataCollector()
            self.collectors['zoning'] = WisconsinZoningDataCollector()
            self.collectors['consumer_spending'] = BEAConsumerSpendingCollector()
            
            self.logger.info("Phase 1 collectors initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing collectors: {e}")
            raise
    
    def run_traffic_collection(self, max_records: int = 10000) -> Dict[str, Any]:
        """Run traffic data collection"""
        self.logger.info("Starting traffic data collection")
        
        try:
            result = self.collectors['traffic'].run_traffic_collection(max_records=max_records)
            self.results['traffic'] = result
            
            if result['success']:
                self.logger.info(f"Traffic collection completed: {result['total_records']} records")
            else:
                self.logger.warning(f"Traffic collection had issues: {result.get('errors', [])}")
            
            return result
            
        except Exception as e:
            error_msg = f"Traffic collection failed: {e}"
            self.logger.error(error_msg)
            self.results['traffic'] = {'success': False, 'error': error_msg}
            return self.results['traffic']
    
    def run_zoning_collection(self, counties: List[str] = None, max_records_per_county: int = 2000) -> Dict[str, Any]:
        """Run zoning data collection"""
        self.logger.info("Starting zoning data collection")
        
        try:
            result = self.collectors['zoning'].run_zoning_collection(
                counties=counties, 
                max_records_per_county=max_records_per_county
            )
            self.results['zoning'] = result
            
            if result['success']:
                self.logger.info(f"Zoning collection completed: {result['total_records']} records across {result['counties_processed']} counties")
            else:
                self.logger.warning(f"Zoning collection had issues: {result.get('errors', [])}")
            
            return result
            
        except Exception as e:
            error_msg = f"Zoning collection failed: {e}"
            self.logger.error(error_msg)
            self.results['zoning'] = {'success': False, 'error': error_msg}
            return self.results['zoning']
    
    def run_consumer_spending_collection(self, years: List[int] = None) -> Dict[str, Any]:
        """Run consumer spending data collection"""
        self.logger.info("Starting consumer spending data collection")
        
        try:
            result = self.collectors['consumer_spending'].run_consumer_spending_collection(years=years)
            self.results['consumer_spending'] = result
            
            if result['success']:
                self.logger.info(f"Consumer spending collection completed: {result['total_records']} records for years {result['years_collected']}")
            else:
                self.logger.warning(f"Consumer spending collection had issues: {result.get('errors', [])}")
            
            return result
            
        except Exception as e:
            error_msg = f"Consumer spending collection failed: {e}"
            self.logger.error(error_msg)
            self.results['consumer_spending'] = {'success': False, 'error': error_msg}
            return self.results['consumer_spending']
    
    def run_all_phase1_collections(self, 
                                  traffic_max_records: int = 10000,
                                  zoning_counties: List[str] = None,
                                  zoning_max_per_county: int = 2000,
                                  spending_years: List[int] = None) -> Dict[str, Any]:
        """
        Run all Phase 1 data collections
        
        Args:
            traffic_max_records: Max traffic records to collect
            zoning_counties: Counties for zoning data (None for all configured)
            zoning_max_per_county: Max zoning records per county  
            spending_years: Years for consumer spending data
            
        Returns:
            Summary of all collection results
        """
        start_time = time.time()
        self.logger.info("Starting Phase 1 data collection orchestration")
        
        # Set defaults
        if spending_years is None:
            current_year = datetime.now().year
            spending_years = list(range(current_year - 4, current_year))
        
        if zoning_counties is None:
            zoning_counties = ['Dane', 'Milwaukee', 'Brown']  # Start with major counties
        
        summary = {
            'phase': 'Phase 1',
            'start_time': datetime.now().isoformat(),
            'collections': {},
            'overall_success': True,
            'total_processing_time': 0,
            'total_records_collected': 0,
            'errors': []
        }
        
        # Collection 1: Traffic Data
        self.logger.info("=== Starting Traffic Data Collection ===")
        try:
            traffic_result = self.run_traffic_collection(max_records=traffic_max_records)
            summary['collections']['traffic'] = traffic_result
            
            if traffic_result['success']:
                summary['total_records_collected'] += traffic_result.get('total_records', 0)
            else:
                summary['overall_success'] = False
                summary['errors'].extend(traffic_result.get('errors', []))
            
        except Exception as e:
            error_msg = f"Traffic collection orchestration error: {e}"
            self.logger.error(error_msg)
            summary['collections']['traffic'] = {'success': False, 'error': error_msg}
            summary['overall_success'] = False
            summary['errors'].append(error_msg)
        
        # Wait between collections
        time.sleep(5)
        
        # Collection 2: Zoning Data
        self.logger.info("=== Starting Zoning Data Collection ===")
        try:
            zoning_result = self.run_zoning_collection(
                counties=zoning_counties,
                max_records_per_county=zoning_max_per_county
            )
            summary['collections']['zoning'] = zoning_result
            
            if zoning_result['success']:
                summary['total_records_collected'] += zoning_result.get('total_records', 0)
            else:
                summary['overall_success'] = False
                summary['errors'].extend(zoning_result.get('errors', []))
            
        except Exception as e:
            error_msg = f"Zoning collection orchestration error: {e}"
            self.logger.error(error_msg)
            summary['collections']['zoning'] = {'success': False, 'error': error_msg}
            summary['overall_success'] = False
            summary['errors'].append(error_msg)
        
        # Wait between collections
        time.sleep(5)
        
        # Collection 3: Consumer Spending Data
        self.logger.info("=== Starting Consumer Spending Data Collection ===")
        try:
            spending_result = self.run_consumer_spending_collection(years=spending_years)
            summary['collections']['consumer_spending'] = spending_result
            
            if spending_result['success']:
                summary['total_records_collected'] += spending_result.get('total_records', 0)
            else:
                summary['overall_success'] = False
                summary['errors'].extend(spending_result.get('errors', []))
            
        except Exception as e:
            error_msg = f"Consumer spending collection orchestration error: {e}"
            self.logger.error(error_msg)
            summary['collections']['consumer_spending'] = {'success': False, 'error': error_msg}
            summary['overall_success'] = False
            summary['errors'].append(error_msg)
        
        # Final summary
        summary['total_processing_time'] = time.time() - start_time
        summary['end_time'] = datetime.now().isoformat()
        
        # Success metrics
        successful_collections = sum(1 for result in summary['collections'].values() if result.get('success'))
        total_collections = len(summary['collections'])
        summary['success_rate'] = successful_collections / total_collections if total_collections > 0 else 0
        
        self.logger.info(f"Phase 1 collection complete: {successful_collections}/{total_collections} successful")
        self.logger.info(f"Total records collected: {summary['total_records_collected']}")
        self.logger.info(f"Total processing time: {summary['total_processing_time']:.2f} seconds")
        
        return summary
    
    def save_collection_summary(self, summary: Dict[str, Any], output_file: str = None) -> str:
        """Save collection summary to file"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"phase1_collection_summary_{timestamp}.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            self.logger.info(f"Collection summary saved to {output_file}")
            return output_file
            
        except Exception as e:
            self.logger.error(f"Error saving summary: {e}")
            return ""


def main():
    """Run Phase 1 data collection"""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('phase1_collection.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize orchestrator
        orchestrator = Phase1DataCollectionOrchestrator()
        
        # Configure collection parameters
        collection_config = {
            'traffic_max_records': 5000,  # Start smaller for testing
            'zoning_counties': ['Dane', 'Milwaukee'],  # Major counties first
            'zoning_max_per_county': 1000,
            'spending_years': [2021, 2022, 2023]  # Recent years
        }
        
        logger.info("Starting Phase 1 data collection with configuration:")
        logger.info(json.dumps(collection_config, indent=2))
        
        # Run all collections
        summary = orchestrator.run_all_phase1_collections(**collection_config)
        
        # Save summary
        summary_file = orchestrator.save_collection_summary(summary)
        
        # Print final results
        print("\\n" + "="*80)
        print("PHASE 1 DATA COLLECTION SUMMARY")
        print("="*80)
        print(f"Overall Success: {summary['overall_success']}")
        print(f"Success Rate: {summary['success_rate']:.1%}")
        print(f"Total Records: {summary['total_records_collected']:,}")
        print(f"Processing Time: {summary['total_processing_time']:.1f} seconds")
        print(f"Summary File: {summary_file}")
        
        if summary['errors']:
            print(f"\\nErrors ({len(summary['errors'])}):")
            for error in summary['errors']:
                print(f"  - {error}")
        
        print("\\nCollection Details:")
        for collection_name, result in summary['collections'].items():
            status = "✓" if result.get('success') else "✗"
            records = result.get('total_records', 0)
            print(f"  {status} {collection_name.title()}: {records:,} records")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Phase 1 collection failed: {e}")
        print(f"\\nFATAL ERROR: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())