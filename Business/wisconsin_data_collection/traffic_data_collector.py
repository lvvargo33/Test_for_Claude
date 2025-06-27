"""
Wisconsin DOT Traffic Data Collector
====================================

Collects highway and street traffic count data from Wisconsin DOT sources.
Integrates with existing BigQuery infrastructure and follows base collector pattern.
"""

import requests
import pandas as pd
import logging
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from pydantic import BaseModel, Field, validator

from base_collector import BaseDataCollector, DataCollectionError


@dataclass
class TrafficCount:
    """Traffic count data point"""
    location_id: str
    station_id: str
    route_name: str
    latitude: float
    longitude: float
    county: str
    aadt: int  # Annual Average Daily Traffic
    measurement_year: int
    highway_type: str  # Interstate, US Highway, State Highway, etc.
    functional_class: str
    urban_rural: str
    data_source: str = "WisDOT"
    collection_date: datetime = None
    
    def __post_init__(self):
        if self.collection_date is None:
            self.collection_date = datetime.now()


class TrafficDataRecord(BaseModel):
    """Pydantic model for traffic data records"""
    
    # Location identifiers
    location_id: str = Field(..., description="Unique location identifier")
    station_id: str = Field(..., description="Traffic counting station ID")
    route_name: str = Field(..., description="Highway/route name (e.g., 'I-94', 'US-41')")
    
    # Geographic information
    latitude: float = Field(..., description="Latitude coordinate")
    longitude: float = Field(..., description="Longitude coordinate")
    county: str = Field(..., description="County name")
    city: Optional[str] = Field(None, description="Nearest city")
    
    # Traffic data
    aadt: int = Field(..., description="Annual Average Daily Traffic")
    measurement_year: int = Field(..., description="Year of measurement")
    traffic_volume_category: Optional[str] = Field(None, description="Traffic volume category (Low/Medium/High)")
    
    # Classification
    highway_type: str = Field(..., description="Highway type (Interstate, US Highway, State Highway)")
    functional_class: str = Field(..., description="Functional classification")
    urban_rural: str = Field(..., description="Urban or rural designation")
    
    # Geometric characteristics
    lane_count: Optional[int] = Field(None, description="Number of lanes")
    median_type: Optional[str] = Field(None, description="Median type")
    access_control: Optional[str] = Field(None, description="Access control type")
    
    # Additional attributes
    truck_percentage: Optional[float] = Field(None, description="Percentage of truck traffic")
    peak_hour_factor: Optional[float] = Field(None, description="Peak hour factor")
    directional_split: Optional[str] = Field(None, description="Directional traffic split")
    
    # Metadata
    data_source: str = Field(default="WisDOT", description="Data source")
    data_collection_date: datetime = Field(default_factory=datetime.now, description="Data collection timestamp")
    data_quality_score: Optional[float] = Field(None, description="Data completeness score (0-100)")
    
    @validator('county')
    def title_case_county(cls, v):
        """Ensure county name is properly formatted"""
        if v:
            return v.title()
        return v
    
    @validator('aadt')
    def validate_aadt(cls, v):
        """Ensure AADT is reasonable"""
        if v < 0 or v > 500000:  # Reasonable bounds for traffic counts
            raise ValueError(f"AADT value {v} is outside reasonable bounds")
        return v
    
    def calculate_traffic_category(self):
        """Categorize traffic volume"""
        if self.aadt < 5000:
            self.traffic_volume_category = "Low"
        elif self.aadt < 20000:
            self.traffic_volume_category = "Medium"
        elif self.aadt < 75000:
            self.traffic_volume_category = "High"
        else:
            self.traffic_volume_category = "Very High"
    
    def calculate_data_quality_score(self):
        """Calculate data completeness score"""
        score = 0.0
        
        # Core required fields (60 points)
        if self.location_id and self.station_id:
            score += 15
        if self.route_name:
            score += 15
        if self.latitude and self.longitude:
            score += 15
        if self.aadt and self.measurement_year:
            score += 15
        
        # Location details (25 points)
        if self.county:
            score += 10
        if self.city:
            score += 5
        if self.highway_type:
            score += 10
        
        # Additional attributes (15 points)
        if self.functional_class:
            score += 5
        if self.lane_count:
            score += 5
        if self.truck_percentage:
            score += 5
        
        self.data_quality_score = round(score, 1)
        return self.data_quality_score


class WisconsinTrafficDataCollector(BaseDataCollector):
    """
    Wisconsin DOT Traffic Data Collector
    
    Collects traffic count data from Wisconsin DOT ArcGIS services
    and open data portals
    """
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # Wisconsin DOT data endpoints
        self.wisdot_endpoints = {
            'traffic_counts': 'https://data-wisdot.opendata.arcgis.com/datasets/WisDOT::traffic-counts.geojson',
            'traffic_counts_api': 'https://services1.arcgis.com/WzFsmainVTuD5KML/arcgis/rest/services/Traffic_Counts/FeatureServer/0',
            'highway_system': 'https://data-wisdot.opendata.arcgis.com/datasets/WisDOT::state-trunk-network.geojson'
        }
        
        # County name mapping (some data sources use different formats)
        self.county_mapping = {
            'ST CROIX': 'Saint Croix',
            'ST. CROIX': 'Saint Croix',
            'FOND DU LAC': 'Fond du Lac',
            'GREEN LAKE': 'Green Lake',
            'LA CROSSE': 'La Crosse'
        }
        
        self.logger.info("Wisconsin Traffic Data Collector initialized")
    
    def collect_highway_traffic_data(self, max_records: int = 10000) -> List[TrafficDataRecord]:
        """
        Collect highway traffic count data from Wisconsin DOT
        
        Args:
            max_records: Maximum number of records to collect
            
        Returns:
            List of TrafficDataRecord objects
        """
        traffic_records = []
        
        try:
            self.logger.info("Collecting Wisconsin DOT traffic count data")
            
            # Fetch data from ArcGIS service
            response = self._make_request(self.wisdot_endpoints['traffic_counts'])
            data = response.json()
            
            if 'features' not in data:
                raise DataCollectionError("Invalid response format from WisDOT API")
            
            features = data['features'][:max_records]
            self.logger.info(f"Processing {len(features)} traffic count features")
            
            for feature in features:
                try:
                    record = self._parse_traffic_feature(feature)
                    if record and self._is_valid_wisconsin_location(record):
                        traffic_records.append(record)
                        
                except Exception as e:
                    self.logger.warning(f"Error parsing traffic feature: {e}")
                    continue
            
            self.logger.info(f"Successfully collected {len(traffic_records)} traffic records")
            
        except Exception as e:
            self.logger.error(f"Error collecting traffic data: {e}")
            raise DataCollectionError(f"Failed to collect traffic data: {e}")
        
        return traffic_records
    
    def collect_city_traffic_data(self, cities: List[str] = None) -> List[TrafficDataRecord]:
        """
        Collect city-specific traffic data
        
        Args:
            cities: List of cities to collect data for
            
        Returns:
            List of TrafficDataRecord objects
        """
        if cities is None:
            cities = ['Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine']
        
        city_traffic_records = []
        
        for city in cities:
            try:
                self.logger.info(f"Collecting traffic data for {city}")
                records = self._collect_city_specific_traffic(city)
                city_traffic_records.extend(records)
                
            except Exception as e:
                self.logger.warning(f"Error collecting traffic data for {city}: {e}")
                continue
        
        return city_traffic_records
    
    def _parse_traffic_feature(self, feature: Dict) -> Optional[TrafficDataRecord]:
        """Parse a GeoJSON feature into a TrafficDataRecord"""
        try:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            coordinates = geometry.get('coordinates', [])
            
            if len(coordinates) < 2:
                return None
            
            # Extract basic information
            location_id = str(properties.get('OBJECTID', ''))
            station_id = str(properties.get('TRADS_ID', location_id))
            route_name = str(properties.get('TRFC_PT_LOC_DESC', ''))
            
            # Geographic coordinates
            longitude, latitude = coordinates[0], coordinates[1]
            
            # County information
            county = properties.get('COUNTY_NAME', '').title()
            county = self.county_mapping.get(county.upper(), county)
            
            # Traffic data
            aadt = int(properties.get('RDWY_AADT', 0)) if properties.get('RDWY_AADT') else 0
            measurement_year = int(properties.get('AADT_RPTG_YR', datetime.now().year))
            
            # Classification
            highway_type = self._classify_highway_type(route_name)
            functional_class = str(properties.get('FUNCTIONAL_CLASS', ''))
            urban_rural = str(properties.get('URBAN_RURAL', ''))
            
            # Additional attributes
            lane_count = properties.get('LANES')
            truck_percentage = properties.get('TRUCK_PCT')
            
            # Create record
            record = TrafficDataRecord(
                location_id=location_id,
                station_id=station_id,
                route_name=route_name,
                latitude=latitude,
                longitude=longitude,
                county=county,
                aadt=aadt,
                measurement_year=measurement_year,
                highway_type=highway_type,
                functional_class=functional_class,
                urban_rural=urban_rural,
                lane_count=int(lane_count) if lane_count else None,
                truck_percentage=float(truck_percentage) if truck_percentage else None
            )
            
            # Calculate derived fields
            record.calculate_traffic_category()
            record.calculate_data_quality_score()
            
            return record
            
        except Exception as e:
            self.logger.warning(f"Error parsing traffic feature: {e}")
            return None
    
    def _classify_highway_type(self, route_description: str) -> str:
        """Classify highway type based on route description"""
        if not route_description:
            return "Local"
        
        desc_upper = route_description.upper()
        
        # Interstate highways
        if any(marker in desc_upper for marker in ['I-', 'IS ', 'INTERSTATE']):
            return "Interstate"
        
        # US Highways
        elif any(marker in desc_upper for marker in ['US ', 'USH ', 'US-']):
            return "US Highway"
        
        # State Highways
        elif any(marker in desc_upper for marker in ['STH ', 'WIS ', 'STATE']):
            return "State Highway"
        
        # County Highways
        elif any(marker in desc_upper for marker in ['CTH ', 'COUNTY']):
            return "County Highway"
        
        # Ramps and connectors
        elif any(marker in desc_upper for marker in ['RAMP', 'ON RAMP', 'OFF RAMP']):
            return "Ramp/Connector"
        
        # Local roads
        else:
            return "Local"
    
    def _is_valid_wisconsin_location(self, record: TrafficDataRecord) -> bool:
        """Validate that the record is in Wisconsin"""
        # Wisconsin bounding box
        wi_bounds = {
            'min_lat': 42.4,
            'max_lat': 46.8,
            'min_lon': -92.8,
            'max_lon': -86.2
        }
        
        return (wi_bounds['min_lat'] <= record.latitude <= wi_bounds['max_lat'] and
                wi_bounds['min_lon'] <= record.longitude <= wi_bounds['max_lon'])
    
    def _collect_city_specific_traffic(self, city: str) -> List[TrafficDataRecord]:
        """Collect traffic data specific to a city"""
        # For now, filter highway data by proximity to city
        # In the future, this could integrate with city-specific APIs
        city_records = []
        
        # City center coordinates (approximate)
        city_centers = {
            'Milwaukee': (43.0389, -87.9065),
            'Madison': (43.0731, -89.4012),
            'Green Bay': (44.5133, -88.0133),
            'Kenosha': (42.5847, -87.8212),
            'Racine': (42.7261, -87.7829)
        }
        
        if city not in city_centers:
            return city_records
        
        city_lat, city_lon = city_centers[city]
        
        # This is a placeholder - in a full implementation, you would:
        # 1. Query city-specific traffic APIs
        # 2. Apply geographic filters to existing highway data
        # 3. Integrate with municipal traffic data sources
        
        self.logger.info(f"City-specific traffic collection for {city} would be implemented here")
        
        return city_records
    
    def save_traffic_data_to_bigquery(self, traffic_records: List[TrafficDataRecord]) -> bool:
        """
        Save traffic data to BigQuery
        
        Args:
            traffic_records: List of TrafficDataRecord objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert to DataFrame
            data = [record.dict() for record in traffic_records]
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("No traffic data to save")
                return True
            
            # Add required fields to match BigQuery schema
            df['traffic_count_id'] = df.apply(lambda row: f"{row['location_id']}_{row['measurement_year']}", axis=1)
            df['state'] = 'WI'
            
            # Ensure proper data types
            df['data_collection_date'] = pd.to_datetime(df['data_collection_date'])
            df['collection_date'] = df['data_collection_date']  # Alias for schema compatibility
            df['measurement_year'] = df['measurement_year'].astype(int)
            df['aadt_year'] = df['measurement_year']  # Alias for schema
            df['aadt'] = df['aadt'].astype(int)
            
            # Rename columns to match schema
            column_mapping = {
                'route_name': 'highway_name',
                'urban_rural': 'location_description',
                'data_collection_date': 'data_extraction_date'
            }
            df = df.rename(columns=column_mapping)
            
            # Add required timestamp field for partitioning
            df['data_extraction_date'] = datetime.utcnow()
            df['collection_method'] = 'API'
            
            # Load to BigQuery
            dataset_id = 'raw_traffic'  # Use the correct dataset
            table_id = 'traffic_counts'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Replace the table to update schema
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extraction_date"
                ),
                clustering_fields=["county", "highway_type"],
                autodetect=True  # Let BigQuery infer the schema
            )
            
            job = self.bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
            job.result()  # Wait for completion
            
            self.logger.info(f"Loaded {len(df)} traffic records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving traffic data to BigQuery: {e}")
            return False
    
    def run_traffic_collection(self, max_records: int = 10000) -> Dict[str, Any]:
        """
        Run complete traffic data collection
        
        Args:
            max_records: Maximum number of records to collect
            
        Returns:
            Collection summary dictionary
        """
        start_time = time.time()
        self.logger.info("Starting Wisconsin traffic data collection")
        
        summary = {
            'collection_date': datetime.now(),
            'state': 'WI',
            'data_source': 'WisDOT',
            'highway_records': 0,
            'city_records': 0,
            'total_records': 0,
            'success': False,
            'processing_time_seconds': 0,
            'errors': []
        }
        
        try:
            # Collect highway traffic data
            highway_records = self.collect_highway_traffic_data(max_records)
            summary['highway_records'] = len(highway_records)
            
            # Collect city traffic data
            city_records = self.collect_city_traffic_data()
            summary['city_records'] = len(city_records)
            
            # Combine all records
            all_records = highway_records + city_records
            summary['total_records'] = len(all_records)
            
            # Save to BigQuery
            if all_records:
                success = self.save_traffic_data_to_bigquery(all_records)
                summary['success'] = success
            else:
                self.logger.warning("No traffic records collected")
                summary['success'] = False
            
        except Exception as e:
            error_msg = f"Error in traffic collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        self.logger.info(f"Traffic collection complete: {summary}")
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        """Not applicable for traffic collector"""
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        """Not applicable for traffic collector"""
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        """Not applicable for traffic collector"""
        return []


def main():
    """Test the traffic data collector"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        collector = WisconsinTrafficDataCollector()
        summary = collector.run_traffic_collection(max_records=1000)
        print(f"Collection Summary: {summary}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()