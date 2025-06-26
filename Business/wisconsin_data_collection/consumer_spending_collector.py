"""
BEA Consumer Spending Data Collector
====================================

Collects consumer spending data from the Bureau of Economic Analysis (BEA) API.
Integrates with existing BigQuery infrastructure and follows base collector pattern.
"""

import requests
import pandas as pd
import logging
import time
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass
from pydantic import BaseModel, Field, validator
import os

from base_collector import BaseDataCollector, DataCollectionError


class ConsumerSpendingRecord(BaseModel):
    """Pydantic model for consumer spending data records"""
    
    # Geographic identifiers
    geo_fips: str = Field(..., description="Geographic FIPS code")
    geo_name: str = Field(..., description="Geographic area name")
    state_fips: str = Field(..., description="State FIPS code")
    state_name: str = Field(..., description="State name")
    
    # Time identifiers
    data_year: int = Field(..., description="Data year")
    data_period: str = Field(..., description="Data period (annual/quarterly)")
    
    # Spending categories and amounts
    total_pce: Optional[float] = Field(None, description="Total Personal Consumption Expenditures (millions)")
    
    # Major spending categories
    goods_total: Optional[float] = Field(None, description="Total spending on goods (millions)")
    goods_durable: Optional[float] = Field(None, description="Durable goods spending (millions)")
    goods_nondurable: Optional[float] = Field(None, description="Nondurable goods spending (millions)")
    services_total: Optional[float] = Field(None, description="Total spending on services (millions)")
    
    # Detailed categories
    food_beverages: Optional[float] = Field(None, description="Food and beverages spending (millions)")
    housing_utilities: Optional[float] = Field(None, description="Housing and utilities spending (millions)")
    transportation: Optional[float] = Field(None, description="Transportation spending (millions)")
    healthcare: Optional[float] = Field(None, description="Healthcare spending (millions)")
    recreation: Optional[float] = Field(None, description="Recreation spending (millions)")
    education: Optional[float] = Field(None, description="Education spending (millions)")
    restaurants_hotels: Optional[float] = Field(None, description="Restaurants and hotels spending (millions)")
    other_services: Optional[float] = Field(None, description="Other services spending (millions)")
    
    # Per capita calculations
    total_pce_per_capita: Optional[float] = Field(None, description="PCE per capita (dollars)")
    goods_per_capita: Optional[float] = Field(None, description="Goods spending per capita (dollars)")
    services_per_capita: Optional[float] = Field(None, description="Services spending per capita (dollars)")
    
    # Growth rates (year-over-year)
    total_pce_growth_rate: Optional[float] = Field(None, description="Total PCE growth rate (%)")
    goods_growth_rate: Optional[float] = Field(None, description="Goods spending growth rate (%)")
    services_growth_rate: Optional[float] = Field(None, description="Services spending growth rate (%)")
    
    # Population data (for per capita calculations)
    population: Optional[int] = Field(None, description="Population count")
    
    # Seasonal adjustments
    seasonally_adjusted: bool = Field(default=True, description="Data is seasonally adjusted")
    
    # Business relevant metrics
    retail_relevant_spending: Optional[float] = Field(None, description="Retail-relevant spending categories combined")
    restaurant_relevant_spending: Optional[float] = Field(None, description="Restaurant-relevant spending categories")
    services_business_relevant: Optional[float] = Field(None, description="Services relevant to business location")
    
    # Metadata
    data_source: str = Field(default="BEA", description="Bureau of Economic Analysis")
    api_dataset: str = Field(..., description="BEA API dataset used")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data completeness score (0-100)")
    
    @validator('state_fips')
    def validate_state_fips(cls, v):
        """Ensure state FIPS is 2 digits"""
        if v and len(v) <= 2 and v.isdigit():
            return v.zfill(2)
        return v
    
    def calculate_per_capita_metrics(self):
        """Calculate per capita spending metrics"""
        if not self.population or self.population <= 0:
            return
        
        # Convert millions to dollars and divide by population
        if self.total_pce:
            self.total_pce_per_capita = round((self.total_pce * 1_000_000) / self.population, 2)
        
        if self.goods_total:
            self.goods_per_capita = round((self.goods_total * 1_000_000) / self.population, 2)
        
        if self.services_total:
            self.services_per_capita = round((self.services_total * 1_000_000) / self.population, 2)
    
    def calculate_business_relevant_metrics(self):
        """Calculate spending metrics relevant to business location analysis"""
        # Retail-relevant spending (goods + some services)
        retail_components = [
            self.goods_total,
            # Exclude housing/utilities from retail relevance
        ]
        
        if any(retail_components):
            self.retail_relevant_spending = sum(x for x in retail_components if x is not None)
        
        # Restaurant-relevant spending
        if self.restaurants_hotels:
            self.restaurant_relevant_spending = self.restaurants_hotels
        
        # Services relevant to business location
        service_components = [
            self.recreation,
            self.education,
            self.other_services,
            # Exclude healthcare as it's less location-flexible
        ]
        
        if any(service_components):
            self.services_business_relevant = sum(x for x in service_components if x is not None)
    
    def calculate_data_quality_score(self):
        """Calculate data completeness score"""
        score = 0.0
        
        # Core required fields (40 points)
        if self.geo_fips and self.data_year:
            score += 20
        if self.total_pce:
            score += 20
        
        # Major categories (30 points)
        if self.goods_total:
            score += 10
        if self.services_total:
            score += 10
        if self.population:
            score += 10
        
        # Detailed categories (20 points)
        detailed_fields = [
            self.food_beverages, self.housing_utilities, self.transportation,
            self.healthcare, self.recreation, self.restaurants_hotels
        ]
        populated_detailed = sum(1 for field in detailed_fields if field is not None)
        score += (populated_detailed / len(detailed_fields)) * 20
        
        # Per capita calculations (10 points)
        if self.total_pce_per_capita:
            score += 5
        if self.goods_per_capita or self.services_per_capita:
            score += 5
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class BEAConsumerSpendingCollector(BaseDataCollector):
    """
    Bureau of Economic Analysis Consumer Spending Data Collector
    
    Collects consumer spending data from BEA API
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # BEA API configuration
        self.bea_api_base = "https://apps.bea.gov/api/data"
        self.bea_api_key = os.getenv('BEA_API_KEY')  # Set in environment or config
        
        # BEA dataset configurations
        self.datasets = {
            'regional_pce': {
                'dataset_name': 'Regional',
                'table_name': 'CAINC30',  # Personal consumption expenditures by state
                'line_codes': {
                    '1': 'total_pce',
                    '2': 'goods_total',
                    '3': 'goods_durable',
                    '4': 'goods_nondurable',
                    '5': 'services_total',
                    '6': 'housing_utilities',
                    '7': 'healthcare',
                    '8': 'transportation',
                    '9': 'recreation',
                    '10': 'food_beverages',
                    '11': 'restaurants_hotels',
                    '12': 'other_services'
                }
            },
            'state_quarterly': {
                'dataset_name': 'Regional',
                'table_name': 'SQGDP2',  # Quarterly GDP by state (includes PCE components)
                'frequency': 'quarterly'
            }
        }
        
        # Wisconsin FIPS codes
        self.wisconsin_fips = {
            'state': '55',
            'counties': {
                'Dane': '55025',
                'Milwaukee': '55079',
                'Brown': '55009',
                'Racine': '55101',
                'Kenosha': '55059',
                'Rock': '55105',
                'Winnebago': '55139',
                'Outagamie': '55087',
                'Washington': '55131',
                'Waukesha': '55133'
            }
        }
        
        self.logger.info("BEA Consumer Spending Collector initialized")
        
        if not self.bea_api_key:
            self.logger.warning("BEA API key not found in environment. Using demo data mode.")
    
    def collect_state_consumer_spending(self, years: List[int] = None) -> List[ConsumerSpendingRecord]:
        """
        Collect state-level consumer spending data
        
        Args:
            years: List of years to collect (defaults to last 5 years)
            
        Returns:
            List of ConsumerSpendingRecord objects
        """
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        spending_records = []
        
        try:
            self.logger.info(f"Collecting BEA consumer spending data for Wisconsin, years: {years}")
            
            # Collect annual PCE data
            for year in years:
                try:
                    records = self._collect_annual_pce_data(year)
                    spending_records.extend(records)
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    self.logger.warning(f"Error collecting data for year {year}: {e}")
                    continue
            
            self.logger.info(f"Successfully collected {len(spending_records)} consumer spending records")
            
        except Exception as e:
            self.logger.error(f"Error collecting consumer spending data: {e}")
            raise DataCollectionError(f"Failed to collect consumer spending data: {e}")
        
        return spending_records
    
    def collect_county_consumer_spending(self, counties: List[str] = None, years: List[int] = None) -> List[ConsumerSpendingRecord]:
        """
        Collect county-level consumer spending data (if available)
        
        Args:
            counties: List of county names
            years: List of years to collect
            
        Returns:
            List of ConsumerSpendingRecord objects
        """
        if counties is None:
            counties = list(self.wisconsin_fips['counties'].keys())
        
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 3, current_year))
        
        county_records = []
        
        # Note: County-level PCE data is limited in BEA
        # This would primarily collect income data and estimate spending patterns
        self.logger.info("County-level consumer spending data collection (limited availability)")
        
        for county in counties:
            if county not in self.wisconsin_fips['counties']:
                continue
                
            try:
                # For now, this is a placeholder for future implementation
                # when county-level PCE data becomes available
                self.logger.info(f"County-level collection for {county} (placeholder)")
                
            except Exception as e:
                self.logger.warning(f"Error collecting county data for {county}: {e}")
                continue
        
        return county_records
    
    def _collect_annual_pce_data(self, year: int) -> List[ConsumerSpendingRecord]:
        """Collect annual PCE data for Wisconsin"""
        records = []
        
        try:
            # Build BEA API request
            params = {
                'UserID': self.bea_api_key or 'DEMO_KEY',
                'method': 'GetData',
                'datasetname': 'Regional',
                'TableName': 'CAINC30',
                'LineCode': 'ALL',
                'GeoFips': self.wisconsin_fips['state'],
                'Year': str(year),
                'ResultFormat': 'json'
            }
            
            response = self._make_request(self.bea_api_base, params=params)
            data = response.json()
            
            if 'BEAAPI' not in data or 'Results' not in data['BEAAPI']:
                self.logger.warning(f"Invalid response format for year {year}")
                return records
            
            results = data['BEAAPI']['Results']['Data']
            
            # Group by line code to build complete records
            spending_data = {}
            
            for item in results:
                line_code = item.get('LineCode')
                data_value = item.get('DataValue')
                
                if line_code not in spending_data:
                    spending_data[line_code] = {
                        'geo_fips': item.get('GeoFips'),
                        'geo_name': item.get('GeoName'),
                        'year': year,
                        'values': {}
                    }
                
                # Parse data value (remove commas, handle (D) for data not available)
                if data_value and data_value != '(D)':
                    try:
                        value = float(data_value.replace(',', ''))
                        spending_data[line_code]['values'][line_code] = value
                    except ValueError:
                        continue
            
            # Create spending record
            record = self._build_spending_record(spending_data, year)
            if record:
                records.append(record)
            
        except Exception as e:
            self.logger.error(f"Error collecting annual PCE data for {year}: {e}")
        
        return records
    
    def _build_spending_record(self, spending_data: Dict, year: int) -> Optional[ConsumerSpendingRecord]:
        """Build a ConsumerSpendingRecord from BEA data"""
        try:
            if not spending_data:
                return None
            
            # Get base information from any line code
            base_info = next(iter(spending_data.values()))
            
            # Aggregate all spending categories
            all_values = {}
            for line_data in spending_data.values():
                all_values.update(line_data['values'])
            
            # Map line codes to spending categories
            line_code_mapping = self.datasets['regional_pce']['line_codes']
            
            record_data = {
                'geo_fips': base_info['geo_fips'],
                'geo_name': base_info['geo_name'],
                'state_fips': self.wisconsin_fips['state'],
                'state_name': 'Wisconsin',
                'data_year': year,
                'data_period': 'annual',
                'api_dataset': 'Regional_CAINC30'
            }
            
            # Map spending categories
            for line_code, field_name in line_code_mapping.items():
                if line_code in all_values:
                    record_data[field_name] = all_values[line_code]
            
            record = ConsumerSpendingRecord(**record_data)
            
            # Calculate derived metrics
            record.calculate_business_relevant_metrics()
            record.calculate_data_quality_score()
            
            return record
            
        except Exception as e:
            self.logger.warning(f"Error building spending record: {e}")
            return None
    
    def save_consumer_spending_to_bigquery(self, spending_records: List[ConsumerSpendingRecord]) -> bool:
        """
        Save consumer spending data to BigQuery
        
        Args:
            spending_records: List of ConsumerSpendingRecord objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert to DataFrame
            data = [record.dict() for record in spending_records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No consumer spending data to save")
                return True
            
            # Ensure proper data types
            df['data_collection_date'] = pd.to_datetime(df['data_collection_date'])
            df['data_year'] = df['data_year'].astype(int)
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'consumer_spending'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_collection_date"
                ),
                clustering_fields=["state_fips", "data_year", "data_period"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} consumer spending records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving consumer spending data to BigQuery: {e}")
            return False
    
    def run_consumer_spending_collection(self, years: List[int] = None) -> Dict[str, Any]:
        """
        Run complete consumer spending data collection
        
        Args:
            years: List of years to collect
            
        Returns:
            Collection summary dictionary
        """
        start_time = time.time()
        self.logger.info("Starting BEA consumer spending data collection")
        
        summary = {
            'collection_date': datetime.now(),
            'state': 'WI',
            'data_source': 'BEA',
            'years_collected': [],
            'state_records': 0,
            'county_records': 0,
            'total_records': 0,
            'success': False,
            'processing_time_seconds': 0,
            'errors': []
        }
        
        try:
            # Collect state-level data
            state_records = self.collect_state_consumer_spending(years)
            summary['state_records'] = len(state_records)
            
            # Collect county-level data (if available)
            county_records = self.collect_county_consumer_spending()
            summary['county_records'] = len(county_records)
            
            # Combine all records
            all_records = state_records + county_records
            summary['total_records'] = len(all_records)
            
            if all_records:
                summary['years_collected'] = sorted(list(set(record.data_year for record in all_records)))
            
            # Save to BigQuery
            if all_records:
                success = self.save_consumer_spending_to_bigquery(all_records)
                summary['success'] = success
            else:
                self.logger.warning("No consumer spending records collected")
                summary['success'] = False
            
        except Exception as e:
            error_msg = f"Error in consumer spending collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"Consumer spending collection complete: {summary}")
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        """Not applicable for consumer spending collector"""
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        """Not applicable for consumer spending collector"""
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        """Not applicable for consumer spending collector"""
        return []


def main():
    """Test the consumer spending data collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = BEAConsumerSpendingCollector()
        
        # Test with recent years
        current_year = datetime.now().year
        test_years = [current_year - 2, current_year - 1]
        
        summary = collector.run_consumer_spending_collection(years=test_years)
        print(f"Collection Summary: {summary}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()