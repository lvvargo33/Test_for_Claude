"""
Phase 2 Data Collection Orchestrator
===================================

Orchestrates all Phase 2 data collectors:
1. Real Estate Data (county records + LoopNet)
2. Industry Benchmarks (SBA + franchise data)
3. Enhanced Employment Data (BLS projections + OES wages)

Manages parallel execution, error handling, and comprehensive reporting.
"""

import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

# Import Phase 2 collectors
from real_estate_collector import WisconsinRealEstateCollector
from industry_benchmarks_collector import IndustryBenchmarksCollector
from enhanced_employment_collector import EnhancedEmploymentCollector


class Phase2Orchestrator:
    """
    Phase 2 Data Collection Orchestrator
    
    Coordinates execution of all Phase 2 data collectors with parallel processing,
    comprehensive error handling, and detailed reporting.
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config_path = config_path
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize collectors
        self.collectors = {}
        self._initialize_collectors()
        
        # Execution configuration
        self.max_concurrent_collectors = 3
        self.timeout_minutes = 30
        self.retry_failed_collectors = True
        self.save_intermediate_results = True
        
        self.logger.info("Phase 2 Orchestrator initialized")
    
    def _initialize_collectors(self):
        """Initialize all Phase 2 data collectors"""
        try:
            self.collectors = {
                'real_estate': WisconsinRealEstateCollector(self.config_path),
                'industry_benchmarks': IndustryBenchmarksCollector(self.config_path),
                'enhanced_employment': EnhancedEmploymentCollector(self.config_path)
            }
            self.logger.info(f"Initialized {len(self.collectors)} Phase 2 collectors")
            
        except Exception as e:
            self.logger.error(f"Error initializing collectors: {e}")
            self.collectors = {}
    
    def run_real_estate_collection(self) -> Dict[str, Any]:
        """Run real estate data collection"""
        start_time = time.time()
        self.logger.info("Starting Real Estate data collection")
        
        try:
            collector = self.collectors['real_estate']
            
            # Configure collection parameters
            counties = ['Milwaukee', 'Dane', 'Brown', 'Waukesha', 'Racine']
            cities = ['Milwaukee', 'Madison', 'Green Bay', 'Waukesha', 'Racine', 'Kenosha']
            
            # Run collection
            summary = collector.run_real_estate_collection(counties=counties, cities=cities)
            
            # Add orchestrator metadata
            summary.update({
                'orchestrator_start_time': start_time,
                'orchestrator_end_time': time.time(),
                'collection_scope': {
                    'counties': counties,
                    'cities': cities
                }
            })
            
            self.logger.info(f"Real Estate collection completed: {summary['total_records']} records")
            return summary
            
        except Exception as e:
            error_msg = f"Real Estate collection failed: {e}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'orchestrator_start_time': start_time,
                'orchestrator_end_time': time.time(),
                'total_records': 0
            }
    
    def run_industry_benchmarks_collection(self) -> Dict[str, Any]:
        """Run industry benchmarks data collection"""
        start_time = time.time()
        self.logger.info("Starting Industry Benchmarks data collection")
        
        try:
            collector = self.collectors['industry_benchmarks']
            
            # Run collection
            summary = collector.run_benchmarks_collection()
            
            # Add orchestrator metadata
            summary.update({
                'orchestrator_start_time': start_time,
                'orchestrator_end_time': time.time()
            })
            
            self.logger.info(f"Industry Benchmarks collection completed: {summary['total_records']} records")
            return summary
            
        except Exception as e:
            error_msg = f"Industry Benchmarks collection failed: {e}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'orchestrator_start_time': start_time,
                'orchestrator_end_time': time.time(),
                'total_records': 0
            }
    
    def run_enhanced_employment_collection(self) -> Dict[str, Any]:
        """Run enhanced employment data collection"""
        start_time = time.time()
        self.logger.info("Starting Enhanced Employment data collection")
        
        try:
            collector = self.collectors['enhanced_employment']
            
            # Run collection
            summary = collector.run_enhanced_employment_collection()
            
            # Add orchestrator metadata
            summary.update({
                'orchestrator_start_time': start_time,
                'orchestrator_end_time': time.time()
            })
            
            self.logger.info(f"Enhanced Employment collection completed: {summary['employment_projections']} projections, {summary['wage_records']} wage records")
            return summary
            
        except Exception as e:
            error_msg = f"Enhanced Employment collection failed: {e}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'orchestrator_start_time': start_time,
                'orchestrator_end_time': time.time(),
                'employment_projections': 0,
                'wage_records': 0
            }
    
    def run_parallel_collection(self) -> Dict[str, Any]:
        """
        Run all Phase 2 collectors in parallel
        
        Returns:
            Comprehensive collection summary
        """
        start_time = time.time()
        self.logger.info("Starting parallel Phase 2 data collection")
        
        # Initialize summary
        orchestrator_summary = {
            'phase': 'Phase 2',
            'orchestrator_start_time': datetime.now(),
            'collectors_run': list(self.collectors.keys()),
            'parallel_execution': True,
            'max_concurrent': self.max_concurrent_collectors,
            'timeout_minutes': self.timeout_minutes,
            'results': {},
            'success_count': 0,
            'failure_count': 0,
            'total_records_collected': 0,
            'total_processing_time': 0,
            'overall_success': False
        }
        
        # Collection functions mapping
        collection_functions = {
            'real_estate': self.run_real_estate_collection,
            'industry_benchmarks': self.run_industry_benchmarks_collection,
            'enhanced_employment': self.run_enhanced_employment_collection
        }
        
        # Execute collectors in parallel
        with ThreadPoolExecutor(max_workers=self.max_concurrent_collectors) as executor:
            # Submit all collection tasks
            future_to_collector = {
                executor.submit(func): collector_name 
                for collector_name, func in collection_functions.items()
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_collector, timeout=self.timeout_minutes * 60):
                collector_name = future_to_collector[future]
                
                try:
                    result = future.result()
                    orchestrator_summary['results'][collector_name] = result
                    
                    if result.get('success', False):
                        orchestrator_summary['success_count'] += 1
                        # Count total records (different fields for different collectors)
                        total_records = result.get('total_records', 0)
                        if total_records == 0:
                            # For employment collector, sum projections and wages
                            total_records = result.get('employment_projections', 0) + result.get('wage_records', 0)
                        orchestrator_summary['total_records_collected'] += total_records
                    else:
                        orchestrator_summary['failure_count'] += 1
                    
                    self.logger.info(f"Collector '{collector_name}' completed: {result.get('success', False)}")
                    
                except Exception as e:
                    orchestrator_summary['failure_count'] += 1
                    error_result = {
                        'success': False,
                        'error': str(e),
                        'timeout': True,
                        'collector_name': collector_name
                    }
                    orchestrator_summary['results'][collector_name] = error_result
                    self.logger.error(f"Collector '{collector_name}' failed with timeout/error: {e}")
        
        # Calculate final metrics
        orchestrator_summary['total_processing_time'] = time.time() - start_time
        orchestrator_summary['orchestrator_end_time'] = datetime.now()
        orchestrator_summary['overall_success'] = orchestrator_summary['success_count'] > 0
        
        # Detailed summary
        self.logger.info("=" * 80)
        self.logger.info("PHASE 2 PARALLEL COLLECTION COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"Successful Collectors: {orchestrator_summary['success_count']}/{len(self.collectors)}")
        self.logger.info(f"Failed Collectors: {orchestrator_summary['failure_count']}")
        self.logger.info(f"Total Records Collected: {orchestrator_summary['total_records_collected']:,}")
        self.logger.info(f"Total Processing Time: {orchestrator_summary['total_processing_time']:.1f} seconds")
        
        # Individual collector results
        for collector_name, result in orchestrator_summary['results'].items():
            status = "✓ SUCCESS" if result.get('success') else "✗ FAILED"
            records = result.get('total_records', result.get('employment_projections', 0) + result.get('wage_records', 0))
            self.logger.info(f"  {status} {collector_name}: {records:,} records")
            if not result.get('success') and result.get('error'):
                error_preview = result['error'][:100] + "..." if len(result['error']) > 100 else result['error']
                self.logger.info(f"    Error: {error_preview}")
        
        return orchestrator_summary
    
    def run_sequential_collection(self) -> Dict[str, Any]:
        """
        Run all Phase 2 collectors sequentially (fallback option)
        
        Returns:
            Comprehensive collection summary
        """
        start_time = time.time()
        self.logger.info("Starting sequential Phase 2 data collection")
        
        # Initialize summary
        orchestrator_summary = {
            'phase': 'Phase 2',
            'orchestrator_start_time': datetime.now(),
            'collectors_run': list(self.collectors.keys()),
            'parallel_execution': False,
            'results': {},
            'success_count': 0,
            'failure_count': 0,
            'total_records_collected': 0,
            'total_processing_time': 0,
            'overall_success': False
        }
        
        # Collection functions mapping
        collection_functions = {
            'real_estate': self.run_real_estate_collection,
            'industry_benchmarks': self.run_industry_benchmarks_collection,
            'enhanced_employment': self.run_enhanced_employment_collection
        }
        
        # Execute collectors sequentially
        for collector_name, func in collection_functions.items():
            self.logger.info(f"Running {collector_name} collector...")
            
            try:
                result = func()
                orchestrator_summary['results'][collector_name] = result
                
                if result.get('success', False):
                    orchestrator_summary['success_count'] += 1
                    # Count total records
                    total_records = result.get('total_records', 0)
                    if total_records == 0:
                        total_records = result.get('employment_projections', 0) + result.get('wage_records', 0)
                    orchestrator_summary['total_records_collected'] += total_records
                else:
                    orchestrator_summary['failure_count'] += 1
                
                self.logger.info(f"Collector '{collector_name}' completed: {result.get('success', False)}")
                
            except Exception as e:
                orchestrator_summary['failure_count'] += 1
                error_result = {
                    'success': False,
                    'error': str(e),
                    'collector_name': collector_name
                }
                orchestrator_summary['results'][collector_name] = error_result
                self.logger.error(f"Collector '{collector_name}' failed: {e}")
        
        # Calculate final metrics
        orchestrator_summary['total_processing_time'] = time.time() - start_time
        orchestrator_summary['orchestrator_end_time'] = datetime.now()
        orchestrator_summary['overall_success'] = orchestrator_summary['success_count'] > 0
        
        return orchestrator_summary
    
    def save_orchestrator_results(self, summary: Dict[str, Any], filename_prefix: str = "phase2_orchestrator"):
        """
        Save orchestrator results to JSON file
        
        Args:
            summary: Collection summary dictionary
            filename_prefix: Prefix for output filename
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.json"
        
        # Convert datetime objects to strings for JSON serialization
        json_summary = self._prepare_for_json(summary)
        
        try:
            with open(filename, 'w') as f:
                json.dump(json_summary, f, indent=2, default=str)
            
            self.logger.info(f"Orchestrator results saved to: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error saving orchestrator results: {e}")
            return None
    
    def _prepare_for_json(self, obj):
        """Recursively prepare object for JSON serialization"""
        if isinstance(obj, dict):
            return {key: self._prepare_for_json(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._prepare_for_json(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
    
    def generate_phase2_report(self, summary: Dict[str, Any]) -> str:
        """
        Generate a comprehensive Phase 2 collection report
        
        Args:
            summary: Collection summary from orchestrator
            
        Returns:
            Formatted report string
        """
        report_lines = []
        
        # Header
        report_lines.append("="*80)
        report_lines.append("WISCONSIN BUSINESS INTELLIGENCE - PHASE 2 COLLECTION REPORT")
        report_lines.append("="*80)
        report_lines.append(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Collection Started: {summary['orchestrator_start_time']}")
        report_lines.append(f"Collection Ended: {summary['orchestrator_end_time']}")
        report_lines.append(f"Total Processing Time: {summary['total_processing_time']:.1f} seconds")
        report_lines.append("")
        
        # Executive Summary
        report_lines.append("EXECUTIVE SUMMARY")
        report_lines.append("-" * 40)
        report_lines.append(f"Overall Success: {'✓ YES' if summary['overall_success'] else '✗ NO'}")
        report_lines.append(f"Collectors Successful: {summary['success_count']}/{len(summary['results'])}")
        report_lines.append(f"Total Data Records: {summary['total_records_collected']:,}")
        report_lines.append(f"Execution Mode: {'Parallel' if summary.get('parallel_execution') else 'Sequential'}")
        report_lines.append("")
        
        # Detailed Results by Collector
        report_lines.append("DETAILED RESULTS BY COLLECTOR")
        report_lines.append("-" * 40)
        
        for collector_name, result in summary['results'].items():
            status = "SUCCESS" if result.get('success') else "FAILED"
            report_lines.append(f"\n{collector_name.replace('_', ' ').title()} Collector: {status}")
            
            if result.get('success'):
                # Success metrics
                if collector_name == 'real_estate':
                    report_lines.append(f"  County Records: {result.get('county_records', 0):,}")
                    report_lines.append(f"  LoopNet Records: {result.get('loopnet_records', 0):,}")
                    report_lines.append(f"  Total Properties: {result.get('total_records', 0):,}")
                elif collector_name == 'industry_benchmarks':
                    report_lines.append(f"  SBA Benchmarks: {result.get('sba_benchmarks', 0):,}")
                    report_lines.append(f"  Franchise Benchmarks: {result.get('franchise_benchmarks', 0):,}")
                    report_lines.append(f"  Industry Benchmarks: {result.get('industry_benchmarks', 0):,}")
                    report_lines.append(f"  Total Benchmarks: {result.get('total_records', 0):,}")
                elif collector_name == 'enhanced_employment':
                    report_lines.append(f"  Employment Projections: {result.get('employment_projections', 0):,}")
                    report_lines.append(f"  Wage Records: {result.get('wage_records', 0):,}")
                    report_lines.append(f"  Areas Covered: {result.get('areas_covered', 0)}")
                
                report_lines.append(f"  Processing Time: {result.get('processing_time_seconds', 0):.1f} seconds")
                
                if result.get('errors'):
                    report_lines.append(f"  Warnings: {len(result['errors'])} non-critical errors")
            else:
                # Failure details
                error_msg = result.get('error', 'Unknown error')
                report_lines.append(f"  Error: {error_msg}")
                if result.get('timeout'):
                    report_lines.append(f"  Cause: Collection timeout")
        
        # Data Quality Assessment
        report_lines.append("\n\nDATA QUALITY ASSESSMENT")
        report_lines.append("-" * 40)
        
        total_expected_collectors = 3
        quality_score = (summary['success_count'] / total_expected_collectors) * 100
        
        if quality_score >= 100:
            quality_rating = "EXCELLENT"
        elif quality_score >= 67:
            quality_rating = "GOOD"
        elif quality_score >= 33:
            quality_rating = "FAIR"
        else:
            quality_rating = "POOR"
        
        report_lines.append(f"Data Collection Score: {quality_score:.1f}% ({quality_rating})")
        report_lines.append(f"Records Collected: {summary['total_records_collected']:,}")
        
        # Next Steps
        report_lines.append("\n\nNEXT STEPS")
        report_lines.append("-" * 40)
        
        if summary['overall_success']:
            report_lines.append("✓ Phase 2 data collection successful")
            report_lines.append("• Data is ready for business intelligence analysis")
            report_lines.append("• Proceed with site analysis report generation")
            report_lines.append("• Monitor data freshness for future updates")
        else:
            report_lines.append("⚠ Phase 2 data collection had issues")
            report_lines.append("• Review failed collectors and retry if needed")
            report_lines.append("• Check network connectivity and API access")
            report_lines.append("• Consider manual data source verification")
        
        if summary['failure_count'] > 0:
            report_lines.append("• Investigate failed collector configurations")
            report_lines.append("• Check data source availability and access permissions")
        
        report_lines.append("\n" + "="*80)
        
        return "\n".join(report_lines)


def main():
    """Main function to run Phase 2 orchestrator"""
    
    # Configure logging for main execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Phase 2 Data Collection Orchestrator")
    
    try:
        # Initialize orchestrator
        orchestrator = Phase2Orchestrator()
        
        # Run parallel collection (preferred method)
        logger.info("Executing parallel data collection...")
        summary = orchestrator.run_parallel_collection()
        
        # Save results
        results_file = orchestrator.save_orchestrator_results(summary)
        
        # Generate and save report
        report = orchestrator.generate_phase2_report(summary)
        
        # Save report to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"phase2_collection_report_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        # Print report to console
        print("\n" + report)
        
        logger.info(f"Phase 2 orchestration complete. Report saved to: {report_file}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Error in Phase 2 orchestrator: {e}")
        return None


if __name__ == "__main__":
    main()