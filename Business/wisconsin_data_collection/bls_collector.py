#!/usr/bin/env python3
"""
BLS Data Collector
==================

Collects Bureau of Labor Statistics data for Wisconsin counties including:
- QCEW: Quarterly Census of Employment and Wages
- LAUS: Local Area Unemployment Statistics

Phase 1 Implementation: 2015-current data for all Wisconsin counties
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

from google.cloud import bigquery


class BLSDataCollector:
    """Collector for Bureau of Labor Statistics data"""
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config = self._load_config(config_path)
        self.api_key = "c177d400482b4df282ff74850f23a7d9"  # BLS API key for higher limits
        self.base_url = "https://api.bls.gov/publicAPI/v2"
        self.logger = self._setup_logging()
        
        # Rate limiting (BLS allows 500 queries per day with API key)
        self.request_delay = 0.5  # 500ms between requests
        self.max_retries = 3
        
        # Wisconsin county FIPS codes (55 prefix)
        self.wisconsin_counties = self._get_wisconsin_county_fips()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for BLS data collection"""
        logger = logging.getLogger('bls_collector')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.FileHandler('location_optimizer.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _get_wisconsin_county_fips(self) -> Dict[str, str]:
        """Get Wisconsin county FIPS codes and names"""
        # Wisconsin counties with their FIPS codes
        counties = {
            '55001': 'Adams County',
            '55003': 'Ashland County', 
            '55005': 'Barron County',
            '55007': 'Bayfield County',
            '55009': 'Brown County',
            '55011': 'Buffalo County',
            '55013': 'Burnett County',
            '55015': 'Calumet County',
            '55017': 'Chippewa County',
            '55019': 'Clark County',
            '55021': 'Columbia County',
            '55023': 'Crawford County',
            '55025': 'Dane County',
            '55027': 'Dodge County',
            '55029': 'Door County',
            '55031': 'Douglas County',
            '55033': 'Dunn County',
            '55035': 'Eau Claire County',
            '55037': 'Florence County',
            '55039': 'Fond du Lac County',
            '55041': 'Forest County',
            '55043': 'Grant County',
            '55045': 'Green County',
            '55047': 'Green Lake County',
            '55049': 'Iowa County',
            '55051': 'Iron County',
            '55053': 'Jackson County',
            '55055': 'Jefferson County',
            '55057': 'Juneau County',
            '55059': 'Kenosha County',
            '55061': 'Kewaunee County',
            '55063': 'La Crosse County',
            '55065': 'Lafayette County',
            '55067': 'Langlade County',
            '55069': 'Lincoln County',
            '55071': 'Manitowoc County',
            '55073': 'Marathon County',
            '55075': 'Marinette County',
            '55077': 'Marquette County',
            '55078': 'Menominee County',
            '55079': 'Milwaukee County',
            '55081': 'Monroe County',
            '55083': 'Oconto County',
            '55085': 'Oneida County',
            '55087': 'Outagamie County',
            '55089': 'Ozaukee County',
            '55091': 'Pepin County',
            '55093': 'Pierce County',
            '55095': 'Polk County',
            '55097': 'Portage County',
            '55099': 'Price County',
            '55101': 'Racine County',
            '55103': 'Richland County',
            '55105': 'Rock County',
            '55107': 'Rusk County',
            '55109': 'St. Croix County',
            '55111': 'Sauk County',
            '55113': 'Sawyer County',
            '55115': 'Shawano County',
            '55117': 'Sheboygan County',
            '55119': 'Taylor County',
            '55121': 'Trempealeau County',
            '55123': 'Vernon County',
            '55125': 'Vilas County',
            '55127': 'Walworth County',
            '55129': 'Washburn County',
            '55131': 'Washington County',
            '55133': 'Waukesha County',
            '55135': 'Waupaca County',
            '55137': 'Waushara County',
            '55139': 'Winnebago County',
            '55141': 'Wood County'
        }
        return counties
    
    def collect_wisconsin_bls_data(self, start_year: int = 2015, end_year: int = None) -> Dict[str, Any]:
        """
        Collect QCEW and LAUS data for Wisconsin counties
        
        Args:
            start_year: Starting year for data collection
            end_year: Ending year (defaults to current year)
            
        Returns:
            Dictionary with collection results
        """
        if end_year is None:
            end_year = datetime.now().year
            
        self.logger.info(f"Starting BLS data collection for Wisconsin ({start_year}-{end_year})")
        
        start_time = time.time()
        results = {
            'qcew_data': [],
            'laus_data': [],
            'collection_summary': {
                'start_year': start_year,
                'end_year': end_year,
                'counties_processed': 0,
                'qcew_records': 0,
                'laus_records': 0,
                'errors': []
            }
        }
        
        # Collect QCEW data (quarterly employment and wages)
        self.logger.info("Collecting QCEW data...")
        qcew_data = self._collect_qcew_data(start_year, end_year)
        results['qcew_data'] = qcew_data
        results['collection_summary']['qcew_records'] = len(qcew_data)
        
        # Collect LAUS data (monthly unemployment statistics)
        self.logger.info("Collecting LAUS data...")
        laus_data = self._collect_laus_data(start_year, end_year)
        results['laus_data'] = laus_data
        results['collection_summary']['laus_records'] = len(laus_data)
        
        # Summary
        results['collection_summary']['counties_processed'] = len(self.wisconsin_counties)
        results['collection_summary']['collection_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"BLS collection complete: {len(qcew_data)} QCEW + {len(laus_data)} LAUS records")
        
        return results
    
    def _collect_qcew_data(self, start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """
        Collect QCEW (Quarterly Census of Employment and Wages) data
        
        QCEW provides quarterly employment, wage, and establishment data by industry and location
        """
        qcew_records = []
        
        # QCEW series ID format: ENU + area_code + data_type + size + ownership + industry
        # For county total employment: ENU + county_fips + 1 + 0 + 5 + 000000 (all industries, private)
        
        for county_fips, county_name in self.wisconsin_counties.items():
            try:
                self.logger.info(f"Collecting QCEW data for {county_name} ({county_fips})")
                
                # Build series IDs for this county (all industries, private sector)
                series_ids = [
                    f"ENU{county_fips}105000000",  # Employment, all industries, private
                    f"ENU{county_fips}205000000",  # Average weekly wages, all industries, private
                    f"ENU{county_fips}305000000",  # Total quarterly wages, all industries, private
                ]
                
                # Request data for all years at once
                county_data = self._make_bls_api_request(series_ids, start_year, end_year)
                
                if county_data:
                    # Parse and format the data
                    for series in county_data.get('Results', {}).get('series', []):
                        series_id = series['seriesID']
                        
                        # Determine data type based on series ID pattern
                        if '105000000' in series_id:
                            data_type = 'employment'
                        elif '205000000' in series_id:
                            data_type = 'average_weekly_wages'
                        elif '305000000' in series_id:
                            data_type = 'total_quarterly_wages'
                        else:
                            continue
                        
                        # Process each data point
                        for data_point in series.get('data', []):
                            record = {
                                'county_fips': county_fips,
                                'county_name': county_name,
                                'year': int(data_point.get('year', 0)),
                                'period': data_point.get('period', ''),
                                'period_name': data_point.get('periodName', ''),
                                'value': self._safe_float(data_point.get('value')),
                                'data_type': data_type,
                                'series_id': series_id,
                                'data_source': 'BLS_QCEW',
                                'data_extraction_date': datetime.now()
                            }
                            
                            # Add quarter information
                            if record['period'].startswith('Q'):
                                record['quarter'] = int(record['period'][1:])
                            
                            qcew_records.append(record)
                
                # Rate limiting
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Error collecting QCEW data for {county_name}: {str(e)}")
                continue
        
        return qcew_records
    
    def _collect_laus_data(self, start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """
        Collect LAUS (Local Area Unemployment Statistics) data
        
        LAUS provides monthly unemployment rates, labor force, and employment data
        """
        laus_records = []
        
        # LAUS series ID format: LAUCN + county_fips + zeros + measure_code
        # Measure codes: 03=unemployment rate, 04=unemployment level, 05=employment level, 06=labor force
        
        for county_fips, county_name in self.wisconsin_counties.items():
            try:
                self.logger.info(f"Collecting LAUS data for {county_name} ({county_fips})")
                
                # Build series IDs for this county
                # Format: LAUCN + 5-digit FIPS + 9 zeros + 2-digit measure code
                series_ids = [
                    f"LAUCN{county_fips}0000000003",    # Unemployment rate
                    f"LAUCN{county_fips}0000000004",    # Unemployment level
                    f"LAUCN{county_fips}0000000005",    # Employment level
                    f"LAUCN{county_fips}0000000006"     # Labor force
                ]
                
                # Request data for all years at once
                county_data = self._make_bls_api_request(series_ids, start_year, end_year)
                
                if county_data:
                    # Parse and format the data
                    for series in county_data.get('Results', {}).get('series', []):
                        series_id = series['seriesID']
                        
                        # Determine measure type
                        if series_id.endswith('0000000003'):
                            measure_type = 'unemployment_rate'
                        elif series_id.endswith('0000000004'):
                            measure_type = 'unemployment_level'
                        elif series_id.endswith('0000000005'):
                            measure_type = 'employment_level'
                        elif series_id.endswith('0000000006'):
                            measure_type = 'labor_force'
                        else:
                            continue
                        
                        # Process each data point
                        for data_point in series.get('data', []):
                            record = {
                                'county_fips': county_fips,
                                'county_name': county_name,
                                'year': int(data_point.get('year', 0)),
                                'period': data_point.get('period', ''),
                                'period_name': data_point.get('periodName', ''),
                                'value': self._safe_float(data_point.get('value')),
                                'measure_type': measure_type,
                                'series_id': series_id,
                                'data_source': 'BLS_LAUS',
                                'data_extraction_date': datetime.now()
                            }
                            
                            # Add month information
                            if record['period'].startswith('M'):
                                record['month'] = int(record['period'][1:])
                            
                            laus_records.append(record)
                
                # Rate limiting
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Error collecting LAUS data for {county_name}: {str(e)}")
                continue
        
        return laus_records
    
    def _make_bls_api_request(self, series_ids: List[str], start_year: int, end_year: int) -> Optional[Dict[str, Any]]:
        """Make API request to BLS"""
        
        # BLS API v2 allows up to 50 series per request
        if len(series_ids) > 50:
            series_ids = series_ids[:50]
        
        data = {
            'seriesid': series_ids,
            'startyear': str(start_year),
            'endyear': str(end_year),
            'registrationkey': self.api_key if self.api_key else None
        }
        
        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}
        
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/timeseries/data/",
                    json=data,
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                
                # Check for API errors
                if result.get('status') != 'REQUEST_SUCCEEDED':
                    self.logger.warning(f"BLS API warning: {result.get('message', 'Unknown error')}")
                
                return result
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"BLS API request attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"All BLS API request attempts failed")
                    return None
                    
        return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '' or value == '-':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def store_bls_data(self, bls_data: Dict[str, Any]):
        """Store BLS data in BigQuery"""
        try:
            client = bigquery.Client(project="location-optimizer-1")
            
            # Store QCEW data
            if bls_data['qcew_data']:
                self._store_qcew_data(client, bls_data['qcew_data'])
            
            # Store LAUS data  
            if bls_data['laus_data']:
                self._store_laus_data(client, bls_data['laus_data'])
                
            self.logger.info("Successfully stored BLS data to BigQuery")
            
        except Exception as e:
            self.logger.error(f"Failed to store BLS data to BigQuery: {str(e)}")
            raise
    
    def _store_qcew_data(self, client: bigquery.Client, qcew_data: List[Dict[str, Any]]):
        """Store QCEW data to BigQuery"""
        if not qcew_data:
            return
            
        table_id = "location-optimizer-1.raw_business_data.bls_qcew_data"
        
        # Convert to DataFrame
        df = pd.DataFrame(qcew_data)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        # Load data
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        self.logger.info(f"Stored {len(qcew_data)} QCEW records to BigQuery")
    
    def _store_laus_data(self, client: bigquery.Client, laus_data: List[Dict[str, Any]]):
        """Store LAUS data to BigQuery"""
        if not laus_data:
            return
            
        table_id = "location-optimizer-1.raw_business_data.bls_laus_data"
        
        # Convert to DataFrame
        df = pd.DataFrame(laus_data)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        # Load data
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        self.logger.info(f"Stored {len(laus_data)} LAUS records to BigQuery")


def main():
    """Test the BLS collector"""
    print("🏭 BLS Data Collector - Phase 1 Implementation")
    print("=" * 60)
    
    collector = BLSDataCollector()
    
    # Test with recent years first
    print("Testing with 2022-2023 data...")
    results = collector.collect_wisconsin_bls_data(start_year=2022, end_year=2023)
    
    print(f"\nCollection Summary:")
    print(f"QCEW Records: {results['collection_summary']['qcew_records']}")
    print(f"LAUS Records: {results['collection_summary']['laus_records']}")
    print(f"Counties Processed: {results['collection_summary']['counties_processed']}")
    print(f"Collection Time: {results['collection_summary']['collection_time_seconds']:.1f} seconds")
    
    # Store to BigQuery
    print("\nStoring data to BigQuery...")
    collector.store_bls_data(results)
    
    print("✅ BLS Phase 1 test completed successfully!")


if __name__ == "__main__":
    main()