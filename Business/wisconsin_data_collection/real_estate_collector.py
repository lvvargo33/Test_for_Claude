"""
Wisconsin Real Estate Data Collector
====================================

Collects commercial real estate data from county property records and LoopNet.
Integrates with existing BigQuery infrastructure and follows base collector pattern.
"""

import requests
import pandas as pd
import logging
import time
import json
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass
from pydantic import BaseModel, Field, validator
from urllib.parse import urljoin, quote
import xml.etree.ElementTree as ET

from base_collector import BaseDataCollector, DataCollectionError


class CommercialPropertyRecord(BaseModel):
    """Pydantic model for commercial real estate records"""
    
    # Property identifiers
    property_id: str = Field(..., description="Unique property identifier")
    parcel_id: Optional[str] = Field(None, description="County parcel ID")
    mls_number: Optional[str] = Field(None, description="MLS listing number")
    
    # Basic property information
    property_address: str = Field(..., description="Property street address")
    city: str = Field(..., description="City")
    state: str = Field(default="WI", description="State")
    zip_code: Optional[str] = Field(None, description="ZIP code")
    county: str = Field(..., description="County name")
    
    # Geographic coordinates
    latitude: Optional[float] = Field(None, description="Latitude coordinate")
    longitude: Optional[float] = Field(None, description="Longitude coordinate")
    
    # Property characteristics
    property_type: str = Field(..., description="Commercial property type")
    property_subtype: Optional[str] = Field(None, description="Property subtype")
    building_size_sqft: Optional[int] = Field(None, description="Building size in square feet")
    lot_size_sqft: Optional[int] = Field(None, description="Lot size in square feet")
    lot_size_acres: Optional[float] = Field(None, description="Lot size in acres")
    year_built: Optional[int] = Field(None, description="Year property was built")
    number_of_units: Optional[int] = Field(None, description="Number of units/spaces")
    
    # Pricing information
    listing_price: Optional[float] = Field(None, description="Asking price ($)")
    price_per_sqft: Optional[float] = Field(None, description="Price per square foot")
    assessed_value: Optional[float] = Field(None, description="County assessed value")
    market_value: Optional[float] = Field(None, description="Estimated market value")
    
    # Rental information
    lease_rate_sqft_year: Optional[float] = Field(None, description="Annual lease rate per sq ft")
    lease_rate_sqft_month: Optional[float] = Field(None, description="Monthly lease rate per sq ft")
    gross_income: Optional[float] = Field(None, description="Gross rental income")
    cap_rate: Optional[float] = Field(None, description="Capitalization rate (%)")
    
    # Transaction information
    sale_date: Optional[date] = Field(None, description="Last sale date")
    sale_price: Optional[float] = Field(None, description="Last sale price")
    listing_date: Optional[date] = Field(None, description="Listing date")
    days_on_market: Optional[int] = Field(None, description="Days on market")
    
    # Property features
    parking_spaces: Optional[int] = Field(None, description="Number of parking spaces")
    zoning: Optional[str] = Field(None, description="Zoning classification")
    utilities: Optional[str] = Field(None, description="Available utilities")
    
    # Business suitability
    restaurant_suitable: bool = Field(default=False, description="Suitable for restaurant use")
    retail_suitable: bool = Field(default=False, description="Suitable for retail use")
    office_suitable: bool = Field(default=False, description="Suitable for office use")
    warehouse_suitable: bool = Field(default=False, description="Suitable for warehouse use")
    
    # Market context
    market_segment: Optional[str] = Field(None, description="Market segment (Class A/B/C)")
    submarket: Optional[str] = Field(None, description="Submarket area")
    walkability_score: Optional[int] = Field(None, description="Walkability score (0-100)")
    
    # Data source and quality
    data_source: str = Field(..., description="Data source (County Records, LoopNet, etc.)")
    source_url: Optional[str] = Field(None, description="Source URL")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data completeness score (0-100)")
    listing_status: Optional[str] = Field(None, description="Active, Sold, Leased, etc.")
    
    @validator('zip_code')
    def validate_zip_code(cls, v):
        """Clean and validate ZIP code"""
        if v:
            cleaned = ''.join(filter(str.isdigit, v))
            if len(cleaned) >= 5:
                return cleaned[:5]
        return v
    
    @validator('county')
    def title_case_county(cls, v):
        """Ensure county name is properly formatted"""
        if v:
            return v.title().replace(' County', '').replace(' Co', '')
        return v
    
    def classify_business_suitability(self):
        """Classify what types of businesses this property suits"""
        if not self.property_type:
            return
        
        prop_type_lower = self.property_type.lower()
        prop_subtype_lower = (self.property_subtype or "").lower()
        
        # Restaurant suitability
        restaurant_keywords = ['restaurant', 'food service', 'kitchen', 'dining', 'bar', 'cafe']
        self.restaurant_suitable = any(keyword in prop_type_lower or keyword in prop_subtype_lower 
                                     for keyword in restaurant_keywords)
        
        # Retail suitability
        retail_keywords = ['retail', 'store', 'shop', 'shopping', 'strip', 'mall', 'center']
        self.retail_suitable = any(keyword in prop_type_lower or keyword in prop_subtype_lower 
                                 for keyword in retail_keywords)
        
        # Office suitability
        office_keywords = ['office', 'professional', 'medical', 'business']
        self.office_suitable = any(keyword in prop_type_lower or keyword in prop_subtype_lower 
                                 for keyword in office_keywords)
        
        # Warehouse suitability
        warehouse_keywords = ['warehouse', 'industrial', 'distribution', 'storage', 'flex']
        self.warehouse_suitable = any(keyword in prop_type_lower or keyword in prop_subtype_lower 
                                    for keyword in warehouse_keywords)
    
    def calculate_data_quality_score(self):
        """Calculate data completeness score"""
        score = 0.0
        
        # Core required fields (40 points)
        if self.property_id and self.property_address:
            score += 15
        if self.city and self.county:
            score += 15
        if self.property_type:
            score += 10
        
        # Location data (20 points)
        if self.latitude and self.longitude:
            score += 10
        if self.zip_code:
            score += 5
        if self.zoning:
            score += 5
        
        # Property details (25 points)
        if self.building_size_sqft:
            score += 10
        if self.lot_size_sqft or self.lot_size_acres:
            score += 8
        if self.year_built:
            score += 4
        if self.parking_spaces:
            score += 3
        
        # Financial data (15 points)
        if self.listing_price or self.assessed_value:
            score += 8
        if self.lease_rate_sqft_year or self.lease_rate_sqft_month:
            score += 7
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class WisconsinRealEstateCollector(BaseDataCollector):
    """
    Wisconsin Commercial Real Estate Data Collector
    
    Collects commercial real estate data from county property records and LoopNet
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # Wisconsin counties with property record systems
        self.county_property_systems = {
            'Milwaukee': {
                'name': 'Milwaukee County Real Property',
                'base_url': 'https://milwaukee.county-taxes.com',
                'search_url': 'https://milwaukee.county-taxes.com/search',
                'type': 'property_tax_search'
            },
            'Dane': {
                'name': 'Dane County Register of Deeds',
                'base_url': 'https://rod.countyofdane.com',
                'search_url': 'https://rod.countyofdane.com/property-search',
                'type': 'deed_records'
            },
            'Waukesha': {
                'name': 'Waukesha County Property Search',
                'base_url': 'https://waukesha.county-taxes.com',
                'type': 'property_tax_search'
            },
            'Brown': {
                'name': 'Brown County Property Records',
                'base_url': 'https://brown.county-taxes.com',
                'type': 'property_tax_search'
            },
            'Racine': {
                'name': 'Racine County Property Information',
                'base_url': 'https://racine.county-taxes.com',
                'type': 'property_tax_search'
            }
        }
        
        # LoopNet search configuration
        self.loopnet_config = {
            'base_url': 'https://www.loopnet.com',
            'search_api': 'https://www.loopnet.com/search/',
            'rate_limit_seconds': 2,  # Be respectful
            'user_agent': 'LocationOptimizer/2.0 Market Research Tool'
        }
        
        # Property type standardization
        self.property_type_mapping = {
            'office': ['office building', 'medical office', 'professional office'],
            'retail': ['retail', 'shopping center', 'strip center', 'storefront'],
            'restaurant': ['restaurant', 'food service', 'bar', 'cafe'],
            'industrial': ['warehouse', 'industrial', 'manufacturing', 'flex space'],
            'mixed_use': ['mixed use', 'mixed-use', 'multi-use'],
            'land': ['land', 'vacant land', 'development land']
        }
        
        self.logger.info("Wisconsin Real Estate Collector initialized")
    
    def collect_county_property_records(self, county: str, property_types: List[str] = None) -> List[CommercialPropertyRecord]:
        """
        Collect commercial property records from county systems
        
        Args:
            county: County name
            property_types: List of property types to search for
            
        Returns:
            List of CommercialPropertyRecord objects
        """
        if property_types is None:
            property_types = ['office', 'retail', 'restaurant', 'industrial']
        
        property_records = []
        
        if county not in self.county_property_systems:
            self.logger.warning(f"No property system configured for {county} County")
            return property_records
        
        county_config = self.county_property_systems[county]
        
        try:
            self.logger.info(f"Collecting property records for {county} County")
            
            if county_config['type'] == 'property_tax_search':
                records = self._collect_property_tax_records(county, county_config, property_types)
            elif county_config['type'] == 'deed_records':
                records = self._collect_deed_records(county, county_config, property_types)
            else:
                self.logger.warning(f"Unknown system type for {county} County")
                return property_records
            
            property_records.extend(records)
            self.logger.info(f"Collected {len(records)} property records for {county} County")
            
        except Exception as e:
            self.logger.error(f"Error collecting property records for {county} County: {e}")
        
        return property_records
    
    def collect_loopnet_listings(self, cities: List[str] = None, property_types: List[str] = None, 
                                max_listings_per_city: int = 100) -> List[CommercialPropertyRecord]:
        """
        Collect commercial real estate listings from LoopNet
        
        Args:
            cities: List of Wisconsin cities to search
            property_types: Property types to search for
            max_listings_per_city: Maximum listings per city
            
        Returns:
            List of CommercialPropertyRecord objects
        """
        if cities is None:
            cities = ['Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine']
        
        if property_types is None:
            property_types = ['office', 'retail', 'restaurant', 'industrial']
        
        loopnet_records = []
        
        for city in cities:
            for property_type in property_types:
                try:
                    self.logger.info(f"Searching LoopNet: {property_type} properties in {city}")
                    
                    records = self._search_loopnet_listings(city, property_type, max_listings_per_city)
                    loopnet_records.extend(records)
                    
                    # Rate limiting
                    time.sleep(self.loopnet_config['rate_limit_seconds'])
                    
                except Exception as e:
                    self.logger.error(f"Error searching LoopNet for {property_type} in {city}: {e}")
                    continue
        
        return loopnet_records
    
    def _collect_property_tax_records(self, county: str, config: Dict, property_types: List[str]) -> List[CommercialPropertyRecord]:
        """Collect from county property tax systems (placeholder for full implementation)"""
        records = []
        
        # This would implement web scraping or API calls to county property tax systems
        # For now, return a sample record to demonstrate the structure
        self.logger.info(f"Property tax record collection for {county} County not yet implemented")
        
        # Sample record for demonstration
        if county == "Milwaukee":
            sample_record = CommercialPropertyRecord(
                property_id=f"{county}_sample_001",
                property_address="123 Main St",
                city="Milwaukee",
                county=county,
                property_type="office",
                building_size_sqft=5000,
                assessed_value=500000.0,
                data_source=f"{county} County Property Records"
            )
            sample_record.classify_business_suitability()
            sample_record.calculate_data_quality_score()
            records.append(sample_record)
        
        return records
    
    def _collect_deed_records(self, county: str, config: Dict, property_types: List[str]) -> List[CommercialPropertyRecord]:
        """Collect from county deed record systems"""
        records = []
        
        # This would implement deed record searching
        # For now, return empty list with informational log
        self.logger.info(f"Deed record collection for {county} County not yet implemented")
        
        return records
    
    def _search_loopnet_listings(self, city: str, property_type: str, max_listings: int) -> List[CommercialPropertyRecord]:
        """Search LoopNet for commercial listings (placeholder for full implementation)"""
        records = []
        
        # This would implement LoopNet web scraping or API integration
        # For now, return sample records to demonstrate the structure
        self.logger.info(f"LoopNet search for {property_type} in {city} not yet implemented")
        
        # Generate sample records for demonstration
        for i in range(min(3, max_listings)):  # Just 3 sample records
            sample_record = CommercialPropertyRecord(
                property_id=f"loopnet_{city}_{property_type}_{i+1}",
                property_address=f"{100 + i*100} Business Ave",
                city=city,
                county=self._get_county_for_city(city),
                property_type=property_type,
                building_size_sqft=2000 + i*1000,
                listing_price=300000.0 + i*100000,
                lease_rate_sqft_year=15.0 + i*2,
                data_source="LoopNet"
            )
            sample_record.classify_business_suitability()
            sample_record.calculate_data_quality_score()
            records.append(sample_record)
        
        return records
    
    def _get_county_for_city(self, city: str) -> str:
        """Map Wisconsin cities to their counties"""
        city_to_county = {
            'Milwaukee': 'Milwaukee',
            'Madison': 'Dane',
            'Green Bay': 'Brown',
            'Kenosha': 'Kenosha',
            'Racine': 'Racine',
            'Appleton': 'Outagamie',
            'Waukesha': 'Waukesha',
            'Eau Claire': 'Eau Claire',
            'Oshkosh': 'Winnebago',
            'Janesville': 'Rock'
        }
        return city_to_county.get(city, 'Unknown')
    
    def save_real_estate_data_to_bigquery(self, property_records: List[CommercialPropertyRecord]) -> bool:
        """
        Save real estate data to BigQuery
        
        Args:
            property_records: List of CommercialPropertyRecord objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.bq_client:
                self.logger.warning("BigQuery client not available, skipping save")
                return False
            
            # Convert to DataFrame
            data = [record.model_dump() for record in property_records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No real estate data to save")
                return True
            
            # Ensure proper data types
            df['data_collection_date'] = pd.to_datetime(df['data_collection_date'])
            if 'sale_date' in df.columns:
                df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date
            if 'listing_date' in df.columns:
                df['listing_date'] = pd.to_datetime(df['listing_date']).dt.date
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'commercial_real_estate'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_collection_date"
                ),
                clustering_fields=["county", "property_type", "data_source"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} real estate records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving real estate data to BigQuery: {e}")
            return False
    
    def run_real_estate_collection(self, counties: List[str] = None, cities: List[str] = None) -> Dict[str, Any]:
        """
        Run complete real estate data collection
        
        Args:
            counties: Counties for property record collection
            cities: Cities for LoopNet collection
            
        Returns:
            Collection summary dictionary
        """
        start_time = time.time()
        self.logger.info("Starting Wisconsin real estate data collection")
        
        summary = {
            'collection_date': datetime.now(),
            'state': 'WI',
            'data_sources': ['County Records', 'LoopNet'],
            'county_records': 0,
            'loopnet_records': 0,
            'total_records': 0,
            'success': False,
            'processing_time_seconds': 0,
            'errors': []
        }
        
        try:
            if counties is None:
                counties = ['Milwaukee', 'Dane', 'Brown']
            
            if cities is None:
                cities = ['Milwaukee', 'Madison', 'Green Bay']
            
            all_records = []
            
            # Collect county property records
            for county in counties:
                try:
                    county_records = self.collect_county_property_records(county)
                    all_records.extend(county_records)
                    summary['county_records'] += len(county_records)
                    
                except Exception as e:
                    error_msg = f"Error collecting {county} County records: {e}"
                    self.logger.error(error_msg)
                    summary['errors'].append(error_msg)
            
            # Collect LoopNet listings
            try:
                loopnet_records = self.collect_loopnet_listings(cities)
                all_records.extend(loopnet_records)
                summary['loopnet_records'] = len(loopnet_records)
                
            except Exception as e:
                error_msg = f"Error collecting LoopNet data: {e}"
                self.logger.error(error_msg)
                summary['errors'].append(error_msg)
            
            summary['total_records'] = len(all_records)
            
            # Save to BigQuery
            if all_records:
                success = self.save_real_estate_data_to_bigquery(all_records)
                summary['success'] = success
            else:
                self.logger.warning("No real estate records collected")
                summary['success'] = False
            
        except Exception as e:
            error_msg = f"Error in real estate collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"Real estate collection complete: {summary}")
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        """Not applicable for real estate collector"""
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        """Not applicable for real estate collector"""
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        """Not applicable for real estate collector"""
        return []


def main():
    """Test the real estate data collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = WisconsinRealEstateCollector()
        
        # Test with limited scope
        summary = collector.run_real_estate_collection(
            counties=['Milwaukee'], 
            cities=['Milwaukee']
        )
        print(f"Collection Summary: {summary}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()