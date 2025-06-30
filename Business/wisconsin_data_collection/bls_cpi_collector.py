#!/usr/bin/env python3
"""
BLS Consumer Price Index (CPI) Collector
========================================

Collects Bureau of Labor Statistics Consumer Price Index data for economic
analysis and business intelligence. Tracks inflation trends across key
consumer categories for business planning and cost analysis.

Key Features:
- Overall CPI and Core CPI (excluding food and energy)
- Category-specific inflation (food, energy, housing, transportation)
- Regional CPI data for Midwest and major metro areas
- Historical inflation trends for economic forecasting
- Integration with existing Wisconsin business intelligence data

Coverage:
- National CPI for all items and major categories
- Core CPI (excludes volatile food and energy)
- Regional CPI for Midwest region
- Metro area CPI for Milwaukee-Waukesha-West Allis
- Historical data back to 2015 for trend analysis
"""

import requests
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from pydantic import BaseModel, Field, validator
import yaml
from pathlib import Path

from google.cloud import bigquery


class CPIRecord(BaseModel):
    """Model for BLS Consumer Price Index data"""
    
    # Series identification
    series_id: str = Field(..., description="BLS CPI series identifier")
    series_title: str = Field(..., description="Full series title/description")
    
    # Category information
    category: str = Field(..., description="CPI category (All Items, Food, Energy, etc.)")
    subcategory: Optional[str] = Field(None, description="Specific subcategory")
    geographic_area: str = Field(..., description="Geographic area (US, Midwest, Milwaukee, etc.)")
    
    # Time period
    year: int = Field(..., description="Data year")
    period: str = Field(..., description="Period code (M01-M12, etc.)")
    period_name: str = Field(..., description="Period name (January, February, etc.)")
    month: Optional[int] = Field(None, description="Month number (1-12)")
    
    # Index values
    index_value: float = Field(..., description="Consumer price index value")
    base_period: str = Field(default="1982-84=100", description="Base period for index")
    
    # Calculated metrics
    monthly_change_pct: Optional[float] = Field(None, description="Month-over-month percentage change")
    annual_change_pct: Optional[float] = Field(None, description="12-month percentage change (inflation rate)")
    
    # Economic implications
    inflation_trend: Optional[str] = Field(None, description="Inflation trend classification")
    volatility_level: Optional[str] = Field(None, description="Price volatility assessment")
    
    # Data quality and source
    data_source: str = Field(default="BLS_CPI", description="Data source identifier")
    data_extraction_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    seasonally_adjusted: bool = Field(default=True, description="Whether data is seasonally adjusted")
    
    @validator('period')
    def extract_month(cls, v, values):
        """Extract month from period code"""
        if v.startswith('M') and len(v) == 3:
            try:
                month = int(v[1:])
                if 1 <= month <= 12:
                    values['month'] = month
            except ValueError:
                pass
        return v
    
    def calculate_inflation_metrics(self, previous_month_value: Optional[float] = None, 
                                  previous_year_value: Optional[float] = None):
        """Calculate inflation metrics"""
        if previous_month_value and previous_month_value > 0:
            self.monthly_change_pct = ((self.index_value - previous_month_value) / previous_month_value) * 100
            
        if previous_year_value and previous_year_value > 0:
            self.annual_change_pct = ((self.index_value - previous_year_value) / previous_year_value) * 100
            
        # Classify inflation trend
        if self.annual_change_pct:
            if self.annual_change_pct >= 4.0:
                self.inflation_trend = "High Inflation"
            elif self.annual_change_pct >= 2.5:
                self.inflation_trend = "Moderate Inflation"
            elif self.annual_change_pct >= 0.5:
                self.inflation_trend = "Low Inflation"
            elif self.annual_change_pct >= -0.5:
                self.inflation_trend = "Stable Prices"
            else:
                self.inflation_trend = "Deflation"


class BLSCPICollector:
    """
    BLS Consumer Price Index Collector
    
    Collects CPI data from BLS for economic analysis and business intelligence
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config = self._load_config(config_path)
        self.api_key = "c177d400482b4df282ff74850f23a7d9"  # Same as existing BLS collector
        self.base_url = "https://api.bls.gov/publicAPI/v2"
        self.logger = self._setup_logging()
        
        # Rate limiting (500 queries per day with API key)
        self.request_delay = 0.5
        self.max_retries = 3
        
        # CPI series definitions
        self.cpi_series = self._get_cpi_series_definitions()
        
        self.logger.info("BLS CPI Collector initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for CPI data collection"""
        logger = logging.getLogger('bls_cpi_collector')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.FileHandler('location_optimizer.log')
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _get_cpi_series_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Define CPI series to collect"""
        
        return {
            # National CPI - All Items
            'cpi_all_items_national': {
                'series_id': 'CUUR0000SA0',
                'title': 'Consumer Price Index - All Urban Consumers, All Items',
                'category': 'All Items',
                'subcategory': 'All Items',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Core CPI (excludes food and energy)
            'cpi_core_national': {
                'series_id': 'CUUR0000SA0L1E',
                'title': 'Consumer Price Index - All Urban Consumers, All Items Less Food and Energy',
                'category': 'Core CPI',
                'subcategory': 'Excludes Food and Energy',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Food CPI
            'cpi_food': {
                'series_id': 'CUUR0000SAF1',
                'title': 'Consumer Price Index - All Urban Consumers, Food',
                'category': 'Food',
                'subcategory': 'All Food',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_food_home': {
                'series_id': 'CUUR0000SAF11',
                'title': 'Consumer Price Index - All Urban Consumers, Food at Home',
                'category': 'Food',
                'subcategory': 'Food at Home',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_food_away': {
                'series_id': 'CUUR0000SEFV',
                'title': 'Consumer Price Index - All Urban Consumers, Food Away from Home',
                'category': 'Food',
                'subcategory': 'Food Away from Home',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Energy CPI
            'cpi_energy': {
                'series_id': 'CUUR0000SA0E',
                'title': 'Consumer Price Index - All Urban Consumers, Energy',
                'category': 'Energy',
                'subcategory': 'All Energy',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_gasoline': {
                'series_id': 'CUUR0000SETB01',
                'title': 'Consumer Price Index - All Urban Consumers, Gasoline (All Types)',
                'category': 'Energy',
                'subcategory': 'Gasoline',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_electricity': {
                'series_id': 'CUUR0000SEHF01',
                'title': 'Consumer Price Index - All Urban Consumers, Electricity',
                'category': 'Energy',
                'subcategory': 'Electricity',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Housing CPI
            'cpi_housing': {
                'series_id': 'CUUR0000SAH1',
                'title': 'Consumer Price Index - All Urban Consumers, Housing',
                'category': 'Housing',
                'subcategory': 'All Housing',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_rent': {
                'series_id': 'CUUR0000SEHA',
                'title': 'Consumer Price Index - All Urban Consumers, Rent of Primary Residence',
                'category': 'Housing',
                'subcategory': 'Rent',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Transportation CPI
            'cpi_transportation': {
                'series_id': 'CUUR0000SAT1',
                'title': 'Consumer Price Index - All Urban Consumers, Transportation',
                'category': 'Transportation',
                'subcategory': 'All Transportation',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_new_vehicles': {
                'series_id': 'CUUR0000SETA01',
                'title': 'Consumer Price Index - All Urban Consumers, New Vehicles',
                'category': 'Transportation',
                'subcategory': 'New Vehicles',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            'cpi_used_vehicles': {
                'series_id': 'CUUR0000SETA02',
                'title': 'Consumer Price Index - All Urban Consumers, Used Cars and Trucks',
                'category': 'Transportation',
                'subcategory': 'Used Vehicles',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Healthcare CPI
            'cpi_medical': {
                'series_id': 'CUUR0000SAM',
                'title': 'Consumer Price Index - All Urban Consumers, Medical Care',
                'category': 'Medical Care',
                'subcategory': 'All Medical',
                'geographic_area': 'US National',
                'seasonally_adjusted': True
            },
            
            # Regional CPI - Midwest
            'cpi_midwest_all': {
                'series_id': 'CUUR0200SA0',
                'title': 'Consumer Price Index - All Urban Consumers, All Items, Midwest',
                'category': 'All Items',
                'subcategory': 'All Items',
                'geographic_area': 'Midwest Region',
                'seasonally_adjusted': True
            },
            
            # Milwaukee Metro Area CPI
            'cpi_milwaukee_all': {
                'series_id': 'CUUR0330SA0',
                'title': 'Consumer Price Index - All Urban Consumers, All Items, Milwaukee-Waukesha-West Allis',
                'category': 'All Items',
                'subcategory': 'All Items',
                'geographic_area': 'Milwaukee Metro',
                'seasonally_adjusted': True
            }
        }
    
    def collect_cpi_data(self, start_year: int = None, end_year: int = None) -> List[CPIRecord]:
        """
        Collect CPI data for all series
        
        Args:
            start_year: Starting year (defaults to 10 years ago)
            end_year: Ending year (defaults to current year)
            
        Returns:
            List of CPIRecord objects
        """
        if start_year is None:
            start_year = datetime.now().year - 10
        if end_year is None:
            end_year = datetime.now().year
            
        self.logger.info(f"Collecting CPI data ({start_year}-{end_year})")
        
        all_records = []
        
        # Collect data for each CPI series
        for series_key, series_info in self.cpi_series.items():
            try:
                self.logger.info(f"Collecting {series_info['title']} data...")
                
                # Make API request for this series
                series_data = self._make_bls_api_request(
                    [series_info['series_id']], 
                    start_year, 
                    end_year
                )
                
                if series_data and series_data.get('Results', {}).get('series'):
                    # Parse the response
                    for series in series_data['Results']['series']:
                        records = self._parse_cpi_series_data(
                            series, 
                            series_info,
                            series_key
                        )
                        all_records.extend(records)
                
                # Rate limiting
                time.sleep(self.request_delay)
                
            except Exception as e:
                self.logger.error(f"Error collecting {series_key} data: {e}")
                continue
        
        # Calculate inflation metrics for all records
        self._calculate_inflation_metrics_for_records(all_records)
        
        self.logger.info(f"Collected {len(all_records)} CPI records")
        return all_records
    
    def _make_bls_api_request(self, series_ids: List[str], start_year: int, 
                             end_year: int) -> Optional[Dict[str, Any]]:
        """Make API request to BLS"""
        
        data = {
            'seriesid': series_ids,
            'startyear': str(start_year),
            'endyear': str(end_year),
            'registrationkey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/timeseries/data/",
                    json=data,
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                
                if result.get('status') != 'REQUEST_SUCCEEDED':
                    self.logger.warning(f"BLS API warning: {result.get('message', 'Unknown error')}")
                
                return result
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"BLS API request attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
                    
        return None
    
    def _parse_cpi_series_data(self, series_data: Dict[str, Any], 
                              series_info: Dict[str, Any],
                              series_key: str) -> List[CPIRecord]:
        """Parse CPI series data into records"""
        
        records = []
        
        for data_point in series_data.get('data', []):
            try:
                # Skip if no value
                value = data_point.get('value')
                if not value or value == '-':
                    continue
                
                index_value = float(value)
                
                record = CPIRecord(
                    series_id=series_data.get('seriesID', ''),
                    series_title=series_info['title'],
                    category=series_info['category'],
                    subcategory=series_info['subcategory'],
                    geographic_area=series_info['geographic_area'],
                    year=int(data_point.get('year', 0)),
                    period=data_point.get('period', ''),
                    period_name=data_point.get('periodName', ''),
                    index_value=index_value,
                    seasonally_adjusted=series_info['seasonally_adjusted']
                )
                
                records.append(record)
                
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error parsing data point for {series_key}: {e}")
                continue
        
        return records
    
    def _calculate_inflation_metrics_for_records(self, records: List[CPIRecord]):
        """Calculate inflation metrics for all records"""
        
        # Group records by series and sort by date
        series_groups = {}
        for record in records:
            key = f"{record.series_id}_{record.geographic_area}"
            if key not in series_groups:
                series_groups[key] = []
            series_groups[key].append(record)
        
        # Sort each series by year and period
        for series_id, series_records in series_groups.items():
            series_records.sort(key=lambda x: (x.year, x.period))
            
            # Calculate inflation metrics
            for i, record in enumerate(series_records):
                # Monthly change (previous month)
                if i > 0:
                    prev_record = series_records[i - 1]
                    record.calculate_inflation_metrics(previous_month_value=prev_record.index_value)
                
                # Annual change (same month previous year)
                for j in range(i):
                    other_record = series_records[j]
                    if (other_record.year == record.year - 1 and 
                        other_record.period == record.period):
                        record.calculate_inflation_metrics(
                            previous_month_value=record.monthly_change_pct and series_records[i-1].index_value,
                            previous_year_value=other_record.index_value
                        )
                        break
    
    def get_inflation_summary(self, records: List[CPIRecord]) -> Dict[str, Any]:
        """Generate inflation summary from collected data"""
        
        summary = {
            'collection_date': datetime.now(),
            'total_records': len(records),
            'categories_tracked': set(),
            'geographic_areas': set(),
            'latest_year': 0,
            'inflation_trends': {},
            'category_analysis': {},
            'regional_comparison': {}
        }
        
        # Analyze by category
        category_data = {}
        
        for record in records:
            summary['categories_tracked'].add(record.category)
            summary['geographic_areas'].add(record.geographic_area)
            summary['latest_year'] = max(summary['latest_year'], record.year)
            
            cat = record.category
            if cat not in category_data:
                category_data[cat] = []
            category_data[cat].append(record)
        
        # Calculate trends by category
        for category, cat_records in category_data.items():
            # Get latest records
            latest_records = [r for r in cat_records if r.year == summary['latest_year']]
            
            if latest_records:
                # Average annual inflation
                annual_rates = [r.annual_change_pct for r in latest_records 
                              if r.annual_change_pct is not None]
                
                if annual_rates:
                    avg_inflation = sum(annual_rates) / len(annual_rates)
                    summary['category_analysis'][category] = {
                        'avg_annual_inflation_pct': round(avg_inflation, 2),
                        'trend': latest_records[-1].inflation_trend if latest_records else 'Unknown',
                        'latest_index': round(latest_records[-1].index_value, 1) if latest_records else 0
                    }
        
        # Convert sets to lists for JSON serialization
        summary['categories_tracked'] = sorted(list(summary['categories_tracked']))
        summary['geographic_areas'] = sorted(list(summary['geographic_areas']))
        
        return summary
    
    def save_to_bigquery(self, records: List[CPIRecord]) -> bool:
        """Save CPI data to BigQuery"""
        try:
            client = bigquery.Client(project="location-optimizer-1")
            
            # Convert to DataFrame
            data = [record.model_dump() for record in records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No CPI data to save")
                return True
            
            # Ensure proper data types
            df['data_extraction_date'] = pd.to_datetime(df['data_extraction_date'])
            df['year'] = df['year'].astype(int)
            df['index_value'] = pd.to_numeric(df['index_value'])
            
            # Set table reference
            table_id = "location-optimizer-1.raw_business_data.bls_cpi_data"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extraction_date"
                ),
                clustering_fields=["category", "geographic_area", "year", "series_id"]
            )
            
            # Load data to BigQuery
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} CPI records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving CPI data to BigQuery: {e}")
            return False
    
    def run_collection(self, start_year: int = None, end_year: int = None) -> Dict[str, Any]:
        """
        Run complete CPI data collection
        
        Args:
            start_year: Starting year for data collection
            end_year: Ending year for data collection
            
        Returns:
            Collection summary
        """
        start_time = time.time()
        
        summary = {
            'collection_date': datetime.now(),
            'start_year': start_year or (datetime.now().year - 10),
            'end_year': end_year or datetime.now().year,
            'records_collected': 0,
            'categories_tracked': 0,
            'series_collected': 0,
            'success': False,
            'processing_time_seconds': 0,
            'inflation_summary': {},
            'errors': []
        }
        
        try:
            # Collect CPI data
            records = self.collect_cpi_data(start_year, end_year)
            summary['records_collected'] = len(records)
            summary['series_collected'] = len(set(r.series_id for r in records))
            
            # Generate inflation summary
            inflation_summary = self.get_inflation_summary(records)
            summary['inflation_summary'] = inflation_summary
            summary['categories_tracked'] = len(inflation_summary['categories_tracked'])
            
            # Save to BigQuery
            if records:
                success = self.save_to_bigquery(records)
                summary['success'] = success
            else:
                summary['success'] = False
                summary['errors'].append("No records collected")
            
        except Exception as e:
            error_msg = f"Error in CPI collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"CPI collection complete: {summary}")
        return summary


def main():
    """Test the BLS CPI collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = BLSCPICollector()
        
        # Run collection for recent years
        summary = collector.run_collection(start_year=2020, end_year=2024)
        
        print(f"\nCPI Collection Summary:")
        print(f"- Records Collected: {summary['records_collected']:,}")
        print(f"- Categories Tracked: {summary['categories_tracked']}")
        print(f"- Series Collected: {summary['series_collected']}")
        print(f"- Success: {summary['success']}")
        print(f"- Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        
        if summary['inflation_summary']:
            print(f"\nInflation Analysis:")
            for category, analysis in summary['inflation_summary']['category_analysis'].items():
                print(f"  {category}: {analysis['avg_annual_inflation_pct']:+.1f}% ({analysis['trend']})")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()