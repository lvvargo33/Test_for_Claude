"""
Wisconsin County Zoning Data Collector
======================================

Collects zoning and regulatory data from Wisconsin county GIS systems.
Integrates with existing BigQuery infrastructure and follows base collector pattern.
"""

import requests
import pandas as pd
import logging
import time
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from pydantic import BaseModel, Field, validator
from urllib.parse import urljoin, urlparse
import re

from base_collector import BaseDataCollector, DataCollectionError


class ZoningDataRecord(BaseModel):
    """Pydantic model for zoning data records"""
    
    # Location identifiers
    parcel_id: str = Field(..., description="Unique parcel identifier")
    county: str = Field(..., description="County name")
    municipality: Optional[str] = Field(None, description="City/town/village name")
    
    # Geographic information
    latitude: Optional[float] = Field(None, description="Latitude coordinate")
    longitude: Optional[float] = Field(None, description="Longitude coordinate")
    address: Optional[str] = Field(None, description="Property address")
    
    # Zoning information
    zoning_code: str = Field(..., description="Zoning classification code")
    zoning_description: str = Field(..., description="Zoning classification description")
    zoning_district: Optional[str] = Field(None, description="Zoning district name")
    
    # Land use information
    current_land_use: Optional[str] = Field(None, description="Current land use classification")
    future_land_use: Optional[str] = Field(None, description="Future land use designation")
    comprehensive_plan_designation: Optional[str] = Field(None, description="Comprehensive plan designation")
    
    # Property characteristics
    lot_size_acres: Optional[float] = Field(None, description="Lot size in acres")
    lot_size_sqft: Optional[float] = Field(None, description="Lot size in square feet")
    frontage_feet: Optional[float] = Field(None, description="Street frontage in feet")
    
    # Business permissions
    commercial_allowed: bool = Field(default=False, description="Commercial use allowed")
    retail_allowed: bool = Field(default=False, description="Retail use allowed")
    restaurant_allowed: bool = Field(default=False, description="Restaurant use allowed")
    mixed_use_allowed: bool = Field(default=False, description="Mixed use allowed")
    
    # Regulatory information
    overlay_districts: Optional[str] = Field(None, description="Overlay districts (comma-separated)")
    special_districts: Optional[str] = Field(None, description="Special districts")
    flood_zone: Optional[str] = Field(None, description="FEMA flood zone designation")
    
    # Development requirements
    setback_front: Optional[float] = Field(None, description="Front setback requirement (feet)")
    setback_side: Optional[float] = Field(None, description="Side setback requirement (feet)")
    setback_rear: Optional[float] = Field(None, description="Rear setback requirement (feet)")
    max_building_height: Optional[float] = Field(None, description="Maximum building height (feet)")
    max_lot_coverage: Optional[float] = Field(None, description="Maximum lot coverage (%)")
    min_parking_spaces: Optional[int] = Field(None, description="Minimum parking spaces required")
    
    # Permit information
    building_permit_required: bool = Field(default=True, description="Building permit required")
    conditional_use_permit_required: bool = Field(default=False, description="Conditional use permit required")
    variance_required: bool = Field(default=False, description="Variance may be required")
    
    # Metadata
    data_source: str = Field(..., description="County GIS data source")
    source_url: Optional[str] = Field(None, description="Source URL")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data completeness score (0-100)")
    last_updated: Optional[date] = Field(None, description="Last update date from source")
    
    @validator('county')
    def title_case_county(cls, v):
        """Ensure county name is properly formatted"""
        if v:
            return v.title().replace(' County', '').replace(' Co', '')
        return v
    
    def classify_business_permissions(self):
        """Classify what types of businesses are allowed based on zoning"""
        if not self.zoning_code or not self.zoning_description:
            return
        
        zoning_text = f"{self.zoning_code} {self.zoning_description}".lower()
        
        # Commercial classifications
        commercial_keywords = ['commercial', 'business', 'c-', 'cb-', 'cc-', 'cg-', 'retail', 'office']
        self.commercial_allowed = any(keyword in zoning_text for keyword in commercial_keywords)
        
        # Retail specific
        retail_keywords = ['retail', 'shopping', 'store', 'market']
        self.retail_allowed = any(keyword in zoning_text for keyword in retail_keywords) or self.commercial_allowed
        
        # Restaurant specific
        restaurant_keywords = ['restaurant', 'food service', 'dining', 'hospitality']
        self.restaurant_allowed = any(keyword in zoning_text for keyword in restaurant_keywords) or self.commercial_allowed
        
        # Mixed use
        mixed_use_keywords = ['mixed', 'mixed-use', 'mixed use', 'mu-', 'pud']
        self.mixed_use_allowed = any(keyword in zoning_text for keyword in mixed_use_keywords)
    
    def calculate_data_quality_score(self):
        """Calculate data completeness score"""
        score = 0.0
        
        # Core required fields (40 points)
        if self.parcel_id:
            score += 10
        if self.county:
            score += 10
        if self.zoning_code:
            score += 10
        if self.zoning_description:
            score += 10
        
        # Location data (30 points)
        if self.latitude and self.longitude:
            score += 15
        if self.address:
            score += 10
        if self.municipality:
            score += 5
        
        # Property details (20 points)
        if self.lot_size_acres or self.lot_size_sqft:
            score += 10
        if self.current_land_use:
            score += 5
        if self.frontage_feet:
            score += 5
        
        # Regulatory details (10 points)
        if any([self.setback_front, self.setback_side, self.setback_rear]):
            score += 5
        if self.max_building_height:
            score += 3
        if self.max_lot_coverage:
            score += 2
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class WisconsinZoningDataCollector(BaseDataCollector):
    """
    Wisconsin County Zoning Data Collector
    
    Collects zoning and regulatory data from Wisconsin county GIS systems
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # Major Wisconsin counties with known GIS portals
        self.county_gis_endpoints = {
            'Dane': {
                'name': 'Dane County',
                'gis_portal': 'https://map.countyofdane.com/arcgis/rest/services',
                'zoning_service': 'https://map.countyofdane.com/arcgis/rest/services/PlanDev/Zoning/MapServer/0',
                'parcel_service': 'https://map.countyofdane.com/arcgis/rest/services/PlanDev/Parcels/MapServer/0',
                'type': 'arcgis_rest'
            },
            'Milwaukee': {
                'name': 'Milwaukee County',
                'gis_portal': 'https://gis.milwaukee.gov/arcgis/rest/services',
                'zoning_service': 'https://gis.milwaukee.gov/arcgis/rest/services/PlanningZoning/MapServer',
                'type': 'arcgis_rest'
            },
            'Brown': {
                'name': 'Brown County',
                'gis_portal': 'https://gis.co.brown.wi.us/arcgis/rest/services',
                'zoning_service': 'https://gis.co.brown.wi.us/arcgis/rest/services/Planning/MapServer',
                'type': 'arcgis_rest'
            },
            'Outagamie': {
                'name': 'Outagamie County',
                'gis_portal': 'https://gis.outagamie.org/rest/services',
                'zoning_service': 'https://gis.outagamie.org/rest/services/Planning/MapServer',
                'type': 'arcgis_rest'
            },
            'Winnebago': {
                'name': 'Winnebago County',
                'gis_portal': 'https://gis.co.winnebago.wi.us/arcgis/rest/services',
                'zoning_service': 'https://gis.co.winnebago.wi.us/arcgis/rest/services/Zoning/MapServer',
                'type': 'arcgis_rest'
            },
            'Wood': {
                'name': 'Wood County',
                'web_viewer': 'https://beacon.schneidercorp.com',
                'type': 'web_scraping'
            },
            'Washburn': {
                'name': 'Washburn County',
                'web_viewer': 'https://beacon.schneidercorp.com',
                'type': 'web_scraping'
            }
        }
        
        # Zoning code standardization mapping
        self.zoning_standardization = {
            'commercial_general': ['C', 'CG', 'C-1', 'C-2', 'C-3', 'GC', 'COMMERCIAL'],
            'commercial_retail': ['CR', 'C-R', 'RETAIL', 'NEIGHBORHOOD COMMERCIAL'],
            'commercial_office': ['CO', 'C-O', 'OFFICE', 'PROFESSIONAL'],
            'mixed_use': ['MU', 'M-U', 'MIXED', 'PUD', 'PLANNED UNIT'],
            'industrial': ['I', 'IND', 'M', 'MANUFACTURING', 'INDUSTRIAL'],
            'residential': ['R', 'RES', 'RESIDENTIAL', 'R-1', 'R-2', 'R-3']
        }
        
        self.logger.info("Wisconsin Zoning Data Collector initialized")
    
    def collect_county_zoning_data(self, county: str, max_records: int = 5000) -> List[ZoningDataRecord]:
        """
        Collect zoning data for a specific county
        
        Args:
            county: County name
            max_records: Maximum number of records to collect
            
        Returns:
            List of ZoningDataRecord objects
        """
        zoning_records = []
        
        if county not in self.county_gis_endpoints:
            self.logger.warning(f"No GIS endpoint configured for {county} County")
            return zoning_records
        
        county_config = self.county_gis_endpoints[county]
        
        try:
            self.logger.info(f"Collecting zoning data for {county} County")
            
            if county_config['type'] == 'arcgis_rest':
                records = self._collect_arcgis_zoning_data(county, county_config, max_records)
            elif county_config['type'] == 'web_scraping':
                records = self._collect_web_zoning_data(county, county_config, max_records)
            else:
                self.logger.warning(f"Unknown collection type for {county} County")
                return zoning_records
            
            zoning_records.extend(records)
            self.logger.info(f"Collected {len(records)} zoning records for {county} County")
            
        except Exception as e:
            self.logger.error(f"Error collecting zoning data for {county} County: {e}")
        
        return zoning_records
    
    def collect_all_counties_zoning_data(self, max_records_per_county: int = 2000) -> List[ZoningDataRecord]:
        """
        Collect zoning data for all configured counties
        
        Args:
            max_records_per_county: Maximum records per county
            
        Returns:
            List of ZoningDataRecord objects
        """
        all_records = []
        
        for county in self.county_gis_endpoints.keys():
            try:
                county_records = self.collect_county_zoning_data(county, max_records_per_county)
                all_records.extend(county_records)
                
                # Rate limiting
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Error collecting data for {county}: {e}")
                continue
        
        return all_records
    
    def _collect_arcgis_zoning_data(self, county: str, config: Dict, max_records: int) -> List[ZoningDataRecord]:
        """Collect data from ArcGIS REST services"""
        records = []
        
        try:
            # Build query URL
            base_url = config['zoning_service']
            query_params = {
                'where': '1=1',
                'outFields': '*',
                'returnGeometry': 'true',
                'f': 'json',
                'resultRecordCount': min(max_records, 1000)  # ArcGIS limit
            }
            
            query_url = f"{base_url}/query"
            
            response = self._make_request(query_url, params=query_params)
            data = response.json()
            
            if 'features' not in data:
                self.logger.warning(f"No features returned for {county} County")
                return records
            
            features = data['features']
            self.logger.info(f"Processing {len(features)} zoning features for {county} County")
            
            for feature in features:
                try:
                    record = self._parse_arcgis_zoning_feature(feature, county, config['zoning_service'])
                    if record:
                        records.append(record)
                        
                except Exception as e:
                    self.logger.debug(f"Error parsing zoning feature: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error collecting ArcGIS data for {county}: {e}")
        
        return records
    
    def _collect_web_zoning_data(self, county: str, config: Dict, max_records: int) -> List[ZoningDataRecord]:
        """Collect data from web interfaces (placeholder for future implementation)"""
        records = []
        
        # This would implement web scraping for counties that don't have REST APIs
        # For now, return empty list with a note
        self.logger.info(f"Web scraping for {county} County not yet implemented")
        
        return records
    
    def _parse_arcgis_zoning_feature(self, feature: Dict, county: str, source_url: str) -> Optional[ZoningDataRecord]:
        """Parse an ArcGIS feature into a ZoningDataRecord"""
        try:
            attributes = feature.get('attributes', {})
            geometry = feature.get('geometry', {})
            
            # Extract coordinates
            latitude, longitude = None, None
            if geometry and 'x' in geometry and 'y' in geometry:
                longitude = geometry['x']
                latitude = geometry['y']
            elif geometry and 'rings' in geometry:
                # Polygon - use centroid approximation
                rings = geometry['rings'][0] if geometry['rings'] else []
                if rings:
                    lons = [point[0] for point in rings]
                    lats = [point[1] for point in rings]
                    longitude = sum(lons) / len(lons)
                    latitude = sum(lats) / len(lats)
            
            # Extract basic information (field names vary by county)
            parcel_id = str(attributes.get('PARCEL_ID') or attributes.get('PIN') or attributes.get('OBJECTID', ''))
            zoning_code = str(attributes.get('ZONING') or attributes.get('ZONE') or attributes.get('ZONE_CODE', ''))
            zoning_description = str(attributes.get('ZONE_DESC') or attributes.get('ZONING_DESC') or 
                                   attributes.get('DESCRIPTION', ''))
            
            if not parcel_id or not zoning_code:
                return None
            
            # Extract additional fields
            address = attributes.get('ADDRESS') or attributes.get('SITE_ADDR') or attributes.get('PROP_ADDR')
            municipality = attributes.get('MUNICIPALITY') or attributes.get('CITY') or attributes.get('TOWN')
            current_land_use = attributes.get('LAND_USE') or attributes.get('USE_CODE')
            lot_size_acres = attributes.get('ACRES') or attributes.get('LOT_ACRES')
            lot_size_sqft = attributes.get('SQ_FEET') or attributes.get('LOT_SQFT')
            
            # Create record
            record = ZoningDataRecord(
                parcel_id=parcel_id,
                county=county,
                municipality=municipality,
                latitude=latitude,
                longitude=longitude,
                address=address,
                zoning_code=zoning_code,
                zoning_description=zoning_description,
                current_land_use=current_land_use,
                lot_size_acres=float(lot_size_acres) if lot_size_acres else None,
                lot_size_sqft=float(lot_size_sqft) if lot_size_sqft else None,
                data_source=f"{county} County GIS",
                source_url=source_url
            )
            
            # Calculate derived fields
            record.classify_business_permissions()
            record.calculate_data_quality_score()
            
            return record
            
        except Exception as e:
            self.logger.debug(f"Error parsing zoning feature: {e}")
            return None
    
    def save_zoning_data_to_bigquery(self, zoning_records: List[ZoningDataRecord]) -> bool:
        """
        Save zoning data to BigQuery
        
        Args:
            zoning_records: List of ZoningDataRecord objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert to DataFrame
            data = [record.dict() for record in zoning_records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No zoning data to save")
                return True
            
            # Ensure proper data types
            df['data_collection_date'] = pd.to_datetime(df['data_collection_date'])
            if 'last_updated' in df.columns:
                df['last_updated'] = pd.to_datetime(df['last_updated']).dt.date
            
            # Load to BigQuery
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'zoning_data'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_collection_date"
                ),
                clustering_fields=["county", "zoning_code", "commercial_allowed"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} zoning records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving zoning data to BigQuery: {e}")
            return False
    
    def run_zoning_collection(self, counties: List[str] = None, max_records_per_county: int = 2000) -> Dict[str, Any]:
        """
        Run complete zoning data collection
        
        Args:
            counties: List of counties to collect (None for all configured)
            max_records_per_county: Maximum records per county
            
        Returns:
            Collection summary dictionary
        """
        start_time = time.time()
        self.logger.info("Starting Wisconsin zoning data collection")
        
        summary = {
            'collection_date': datetime.now(),
            'state': 'WI',
            'data_source': 'County GIS',
            'counties_processed': 0,
            'total_records': 0,
            'success': False,
            'processing_time_seconds': 0,
            'county_results': {},
            'errors': []
        }
        
        try:
            if counties is None:
                counties = list(self.county_gis_endpoints.keys())
            
            all_records = []
            
            for county in counties:
                try:
                    county_records = self.collect_county_zoning_data(county, max_records_per_county)
                    all_records.extend(county_records)
                    
                    summary['county_results'][county] = {
                        'records_collected': len(county_records),
                        'success': True
                    }
                    summary['counties_processed'] += 1
                    
                except Exception as e:
                    error_msg = f"Error collecting {county} County: {e}"
                    self.logger.error(error_msg)
                    summary['errors'].append(error_msg)
                    summary['county_results'][county] = {
                        'records_collected': 0,
                        'success': False,
                        'error': str(e)
                    }
            
            summary['total_records'] = len(all_records)
            
            # Save to BigQuery
            if all_records:
                success = self.save_zoning_data_to_bigquery(all_records)
                summary['success'] = success
            else:
                self.logger.warning("No zoning records collected")
                summary['success'] = False
            
        except Exception as e:
            error_msg = f"Error in zoning collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"Zoning collection complete: {summary}")
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        """Not applicable for zoning collector"""
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        """Not applicable for zoning collector"""
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        """Not applicable for zoning collector"""
        return []


def main():
    """Test the zoning data collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = WisconsinZoningDataCollector()
        
        # Test with just Dane County
        summary = collector.run_zoning_collection(counties=['Dane'], max_records_per_county=100)
        print(f"Collection Summary: {summary}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()