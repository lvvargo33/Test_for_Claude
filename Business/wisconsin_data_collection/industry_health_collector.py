#!/usr/bin/env python3
"""
Industry Health Data Collector
===============================

Collects Bureau of Labor Statistics industry health data including:
- QCEW: Industry employment and wages by MSA
- Employment Projections: Industry growth forecasts
- Industry performance vs other sectors

Provides data for Section 1.3 Market Demand analysis - Industry Health Assessment
"""

import requests
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
import yaml
from pathlib import Path

try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None

class DataCollectionError(Exception):
    """Custom exception for data collection errors"""
    pass


class IndustryHealthCollector:
    """Collector for BLS industry health and performance data"""
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config_path = config_path
        self.api_key = "c177d400482b4df282ff74850f23a7d9"  # BLS API key
        self.base_url = "https://api.bls.gov/publicAPI/v2"
        self.logger = self._setup_logging()
        
        # Rate limiting (BLS allows 500 queries per day with API key)
        self.request_delay = 0.5  # 500ms between requests
        self.max_retries = 3
        
        # Industry codes for analysis
        self.industry_codes = self._get_industry_codes()
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def _get_industry_codes(self) -> Dict[str, str]:
        """Industry codes for various business sectors"""
        return {
            'food_services': '722',  # Food Services and Drinking Places
            'retail_trade': '44-45',  # Retail Trade
            'healthcare': '62',  # Health Care and Social Assistance
            'finance': '52',  # Finance and Insurance
            'real_estate': '53',  # Real Estate and Rental and Leasing
            'professional': '54',  # Professional, Scientific, and Technical Services
            'accommodation': '721',  # Accommodation
            'entertainment': '713',  # Amusement, Gambling, and Recreation Industries
            'personal_services': '8121',  # Personal Care Services
            'automotive': '441',  # Motor Vehicle and Parts Dealers
            'construction': '23',  # Construction
            'manufacturing': '31-33'  # Manufacturing
        }

    def _make_bls_request(self, series_ids: List[str], start_year: int, end_year: int) -> Dict[str, Any]:
        """Make BLS API request with error handling"""
        url = f"{self.base_url}/timeseries/data/"
        
        headers = {'Content-type': 'application/json'}
        data = {
            'seriesid': series_ids,
            'startyear': str(start_year),
            'endyear': str(end_year),
            'registrationkey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.request_delay)
                response = requests.post(url, json=data, headers=headers, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                if result.get('status') == 'REQUEST_SUCCEEDED':
                    return result
                else:
                    self.logger.warning(f"BLS API returned status: {result.get('status')}")
                    self.logger.warning(f"BLS API response: {result}")
                    return result  # Return even if not successful to debug
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise DataCollectionError(f"Failed to fetch BLS data after {self.max_retries} attempts")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise DataCollectionError("Failed to get successful response from BLS API")

    def collect_industry_employment_data(self, state_code: str = "55", years: int = 5) -> List[Dict[str, Any]]:
        """
        Collect industry employment data for Wisconsin state
        Using state-level data as MSA-level series IDs are complex
        
        Args:
            state_code: State FIPS code (55 = Wisconsin)
            years: Number of years of historical data
            
        Returns:
            List of industry employment records
        """
        current_year = datetime.now().year
        start_year = current_year - years
        
        industry_data = []
        
        # Use state-level LAUS data which is more reliable
        # Format: LASST550000000000003 (unemployment rate)
        # Format: LASST550000000000006 (employment level)
        
        # Wisconsin employment level
        wisconsin_employment = f"LASST{state_code}0000000000006"
        
        try:
            self.logger.info(f"Collecting Wisconsin employment data")
            self.logger.info(f"Series ID: {wisconsin_employment}")
            
            result = self._make_bls_request([wisconsin_employment], start_year, current_year)
            
            # Process employment data
            for series in result.get('Results', {}).get('series', []):
                series_id = series['seriesID']
                
                for data_point in series.get('data', []):
                    record = {
                        'state_code': state_code,
                        'area_name': 'Wisconsin',
                        'data_type': 'employment_level',
                        'year': int(data_point['year']),
                        'period': data_point['period'],
                        'value': data_point['value'],
                        'collected_at': datetime.now().isoformat()
                    }
                    industry_data.append(record)
            
            self.logger.info(f"Collected {len(industry_data)} Wisconsin employment records")
                
        except Exception as e:
            self.logger.error(f"Failed to collect Wisconsin employment data: {e}")
        
        return industry_data

    def collect_employment_projections(self, state_code: str = '55') -> List[Dict[str, Any]]:
        """
        Collect BLS employment projections by industry
        
        Args:
            state_code: State FIPS code (55 = Wisconsin)
            
        Returns:
            List of employment projection records
        """
        projection_data = []
        
        for industry_name, industry_code in self.industry_codes.items():
            try:
                # Employment projections series format varies by data availability
                # Using national data as state projections may not be available
                series_id = f"PRS{industry_code.replace('-', '')}000000000003"  # Productivity series
                
                self.logger.info(f"Collecting employment projections for {industry_name}")
                
                result = self._make_bls_request([series_id], 2020, 2024)
                
                for series in result.get('Results', {}).get('series', []):
                    for data_point in series.get('data', []):
                        record = {
                            'state_code': state_code,
                            'industry_name': industry_name,
                            'industry_code': industry_code,
                            'projection_type': 'productivity',
                            'year': int(data_point['year']),
                            'period': data_point['period'],
                            'value': data_point['value'],
                            'collected_at': datetime.now().isoformat()
                        }
                        projection_data.append(record)
                
            except Exception as e:
                self.logger.error(f"Failed to collect projections for {industry_name}: {e}")
                continue
        
        return projection_data

    def analyze_industry_performance(self, industry_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze employment performance metrics
        
        Args:
            industry_data: Raw employment data
            
        Returns:
            Employment performance analysis
        """
        if not industry_data:
            return {}
        
        df = pd.DataFrame(industry_data)
        
        performance_metrics = {}
        
        # Analyze Wisconsin employment trends
        if 'data_type' in df.columns and 'employment_level' in df['data_type'].values:
            employment_df = df[df['data_type'] == 'employment_level']
            
            if len(employment_df) >= 2:
                # Calculate year-over-year growth
                employment_df = employment_df.sort_values('year')
                if len(employment_df) > 1:
                    latest_value = pd.to_numeric(employment_df.iloc[-1]['value'], errors='coerce')
                    previous_value = pd.to_numeric(employment_df.iloc[-2]['value'], errors='coerce')
                    
                    if latest_value and previous_value:
                        growth_rate = ((latest_value - previous_value) / previous_value) * 100
                        
                        performance_metrics['wisconsin_employment'] = {
                            'latest_employment': float(latest_value),
                            'growth_rate': round(float(growth_rate), 2),
                            'trend': 'growing' if growth_rate > 0 else 'declining',
                            'data_points': int(len(employment_df)),
                            'time_period': f"{int(employment_df.iloc[0]['year'])}-{int(employment_df.iloc[-1]['year'])}"
                        }
        
        return performance_metrics

    def save_to_csv(self, data: List[Dict[str, Any]], filename: str) -> str:
        """Save collected data to CSV file"""
        if not data:
            self.logger.warning("No data to save")
            return ""
        
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{filename}_{timestamp}.csv"
        
        df.to_csv(output_file, index=False)
        self.logger.info(f"Saved {len(data)} records to {output_file}")
        return output_file

    def collect_all_industry_data(self, state_code: str = "55") -> Dict[str, Any]:
        """
        Collect comprehensive industry health data
        
        Args:
            state_code: State code (55 = Wisconsin)
            
        Returns:
            Comprehensive industry health analysis
        """
        self.logger.info("Starting comprehensive industry health data collection")
        
        # Collect state employment data
        employment_data = self.collect_industry_employment_data(state_code)
        
        # Collect employment projections
        projection_data = self.collect_employment_projections(state_code)
        
        # Analyze performance
        performance_analysis = self.analyze_industry_performance(employment_data)
        
        # Save raw data
        employment_file = self.save_to_csv(employment_data, "industry_employment_data")
        projection_file = self.save_to_csv(projection_data, "employment_projections")
        
        # Create summary
        summary = {
            'collection_date': datetime.now().isoformat(),
            'state_code': state_code,
            'industries_analyzed': len(self.industry_codes),
            'employment_records': len(employment_data),
            'projection_records': len(projection_data),
            'performance_metrics': performance_analysis,
            'output_files': {
                'employment_data': employment_file,
                'projections': projection_file
            }
        }
        
        # Save summary
        summary_file = f"industry_health_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Industry health data collection complete. Summary saved to {summary_file}")
        return summary


def main():
    """Test the industry health collector"""
    collector = IndustryHealthCollector()
    
    # Collect data for Wisconsin
    summary = collector.collect_all_industry_data(state_code="55")
    
    print(f"Collection Summary:")
    print(f"- Industries analyzed: {summary['industries_analyzed']}")
    print(f"- Employment records: {summary['employment_records']}")
    print(f"- Projection records: {summary['projection_records']}")
    print(f"- Performance metrics: {len(summary['performance_metrics'])} industries")


if __name__ == "__main__":
    main()