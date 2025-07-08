#!/usr/bin/env python3
"""
Trade Area Analyzer - Drive-time based customer accessibility analysis
Uses OpenRouteService for isochrone generation and Census data for population counts
"""

import os
import json
import logging
import requests
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
import geopandas as gpd
from shapely.geometry import Point, Polygon, mapping
from shapely import wkt
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "location-optimizer-1"
DATASET_ID = "location_optimizer"
CREDENTIALS_PATH = "location-optimizer-1-96b6102d3548.json"

# OpenRouteService configuration (using free tier)
ORS_BASE_URL = "https://api.openrouteservice.org/v2/isochrones/driving-car"
ORS_API_KEY = os.environ.get("ORS_API_KEY", "5b3ce3597851110001cf6248d33b6d517e6840ddaebe04692fec12ec")

# Census API configuration
CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY", "")  # Get from https://api.census.gov/data/key_signup.html
CENSUS_BASE_URL = "https://api.census.gov/data/2022/acs/acs5"

class TradeAreaAnalyzer:
    def __init__(self):
        """Initialize the trade area analyzer"""
        # BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        self.bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Check for API keys
        if not ORS_API_KEY:
            logger.warning("OpenRouteService API key not set. Using offline mode.")
            self.offline_mode = True
        else:
            self.offline_mode = False
    
    def generate_isochrones(self, lat: float, lon: float, intervals: List[int] = [5, 10, 15]) -> Dict:
        """
        Generate drive-time isochrones using OpenRouteService
        
        Args:
            lat: Latitude of the location
            lon: Longitude of the location
            intervals: List of drive-time intervals in minutes
        
        Returns:
            Dictionary with isochrone polygons for each interval
        """
        if self.offline_mode:
            # Generate approximate circular buffers for offline mode
            return self._generate_offline_isochrones(lat, lon, intervals)
        
        headers = {
            'Authorization': ORS_API_KEY,
            'Content-Type': 'application/json'
        }
        
        body = {
            "locations": [[lon, lat]],  # Note: ORS uses lon/lat order
            "range": [i * 60 for i in intervals],  # Convert minutes to seconds
            "range_type": "time",
            "profile": "driving-car",
            "units": "mi"
        }
        
        try:
            response = requests.post(ORS_BASE_URL, json=body, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse isochrones
            isochrones = {}
            for i, feature in enumerate(data['features']):
                interval = intervals[i]
                geometry = feature['geometry']
                # Convert to shapely polygon
                if geometry['type'] == 'Polygon':
                    polygon = Polygon(geometry['coordinates'][0])
                elif geometry['type'] == 'MultiPolygon':
                    # Take the largest polygon
                    polygons = [Polygon(coords[0]) for coords in geometry['coordinates']]
                    polygon = max(polygons, key=lambda p: p.area)
                
                isochrones[f"{interval}_min"] = polygon
            
            return isochrones
            
        except Exception as e:
            logger.error(f"Error generating isochrones: {e}")
            # Fall back to circular approximation
            return self._generate_offline_isochrones(lat, lon, intervals)
    
    def _generate_offline_isochrones(self, lat: float, lon: float, intervals: List[int]) -> Dict:
        """Generate approximate isochrones using circular buffers"""
        # Approximate distances based on average speeds
        # 5 min = ~3 miles, 10 min = ~6 miles, 15 min = ~10 miles
        distance_map = {5: 0.043, 10: 0.087, 15: 0.145}  # Degrees (rough approximation)
        
        point = Point(lon, lat)
        isochrones = {}
        
        for interval in intervals:
            buffer_distance = distance_map.get(interval, interval * 0.0087)
            isochrones[f"{interval}_min"] = point.buffer(buffer_distance)
        
        return isochrones
    
    def get_census_blocks_in_polygon(self, polygon: Polygon, state_fips: str = "55") -> pd.DataFrame:
        """
        Get census block groups within a polygon with population data
        
        Args:
            polygon: Shapely polygon representing the area
            state_fips: State FIPS code (55 for Wisconsin)
        
        Returns:
            DataFrame with census block data
        """
        # Convert polygon to WKT for BigQuery
        polygon_wkt = polygon.wkt
        
        query = f"""
        WITH polygon_area AS (
            SELECT ST_GEOGFROMTEXT('{polygon_wkt}') as area
        )
        SELECT 
            geo_id,
            total_population,
            total_households,
            median_household_income,
            ST_ASTEXT(block_geom) as block_geom_wkt,
            ST_AREA(ST_INTERSECTION(block_geom, area)) / ST_AREA(block_geom) as overlap_ratio
        FROM `{PROJECT_ID}.raw_business_data.census_demographics` cd,
             polygon_area
        WHERE ST_INTERSECTS(block_geom, area)
            AND state_fips = '{state_fips}'
            AND total_population > 0
        """
        
        try:
            df = self.bq_client.query(query).to_dataframe()
            # Calculate population within polygon based on overlap
            df['population_in_polygon'] = (df['total_population'] * df['overlap_ratio']).astype(int)
            df['households_in_polygon'] = (df['total_households'] * df['overlap_ratio']).astype(int)
            return df
        except Exception as e:
            logger.error(f"Error querying census data: {e}")
            # Return empty dataframe if query fails
            return pd.DataFrame()
    
    def analyze_trade_area(self, business_id: str, business_name: str, 
                         business_type: str, lat: float, lon: float,
                         address: str = "", city: str = "", state: str = "WI") -> Dict:
        """
        Perform complete trade area analysis for a location
        
        Returns dictionary with all trade area metrics
        """
        logger.info(f"Analyzing trade area for {business_name} at ({lat}, {lon})")
        
        # Generate isochrones
        isochrones = self.generate_isochrones(lat, lon)
        
        # Initialize results
        results = {
            "business_id": business_id,
            "business_name": business_name,
            "business_type": business_type,
            "location": f"POINT({lon} {lat})",
            "address": address,
            "city": city,
            "state": state,
            "customers_0_5_min": 0,
            "customers_5_10_min": 0,
            "customers_10_15_min": 0,
            "calculation_date": date.today(),
            "last_updated": datetime.now()
        }
        
        # Get population for each ring
        if "5_min" in isochrones:
            df_5min = self.get_census_blocks_in_polygon(isochrones["5_min"])
            results["customers_0_5_min"] = df_5min['population_in_polygon'].sum() if not df_5min.empty else 0
        
        if "10_min" in isochrones:
            df_10min = self.get_census_blocks_in_polygon(isochrones["10_min"])
            total_10min = df_10min['population_in_polygon'].sum() if not df_10min.empty else 0
            # Subtract 5-minute population to get ring population
            results["customers_5_10_min"] = max(0, total_10min - results["customers_0_5_min"])
        
        if "15_min" in isochrones:
            df_15min = self.get_census_blocks_in_polygon(isochrones["15_min"])
            total_15min = df_15min['population_in_polygon'].sum() if not df_15min.empty else 0
            # Subtract 10-minute population to get ring population
            results["customers_10_15_min"] = max(0, total_15min - total_10min)
        
        # Calculate accessibility scores (simplified for now)
        results["car_accessibility_score"] = 0.9  # High car accessibility in Wisconsin
        results["walk_accessibility_score"] = 0.3 if city in ["Madison", "Milwaukee"] else 0.1
        results["transit_accessibility_score"] = 0.5 if city in ["Madison", "Milwaukee"] else 0.1
        
        # Count competitors (simplified - would query business data)
        results["competitors_same_type"] = 0
        results["market_saturation_index"] = 0.0
        
        # Data source
        results["data_source"] = "OpenRouteService" if not self.offline_mode else "Circular_Approximation"
        
        # Placeholder values for time-based populations (would use LEHD data)
        total_pop = results["customers_0_5_min"] + results["customers_5_10_min"] + results["customers_10_15_min"]
        results["morning_population"] = int(total_pop * 0.7)
        results["lunch_population"] = int(total_pop * 0.8)
        results["evening_population"] = int(total_pop * 0.6)
        results["weekend_population"] = int(total_pop * 0.5)
        
        return results
    
    def save_to_bigquery(self, trade_area_data: Dict):
        """Save trade area analysis to BigQuery"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.trade_area_profiles"
        
        # Convert to list for BigQuery
        rows_to_insert = [trade_area_data]
        
        errors = self.bq_client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            logger.error(f"Error inserting to BigQuery: {errors}")
        else:
            logger.info(f"Successfully saved trade area data for {trade_area_data['business_name']}")
    
    def analyze_existing_businesses(self, limit: int = 10):
        """
        Analyze trade areas for existing successful businesses in the database
        """
        # Query successful businesses from SBA data
        query = f"""
        SELECT DISTINCT
            CONCAT('SBA_', CAST(ROW_NUMBER() OVER () AS STRING)) as business_id,
            BorrowerName as business_name,
            CASE 
                WHEN LOWER(BorrowerName) LIKE '%restaurant%' OR LOWER(BorrowerName) LIKE '%pizza%' 
                     OR LOWER(BorrowerName) LIKE '%cafe%' THEN 'restaurant'
                WHEN LOWER(BorrowerName) LIKE '%salon%' OR LOWER(BorrowerName) LIKE '%spa%' THEN 'personal_services'
                WHEN LOWER(BorrowerName) LIKE '%fitness%' OR LOWER(BorrowerName) LIKE '%gym%' THEN 'fitness'
                ELSE 'retail'
            END as business_type,
            BorrowerAddress as address,
            BorrowerCity as city,
            BorrowerState as state,
            -- Wisconsin coordinates (would need geocoding in production)
            -89.4012 + (RAND() * 2) as longitude,  -- Random within Wisconsin bounds
            43.0731 + (RAND() * 3) as latitude
        FROM `{PROJECT_ID}.raw_business_data.sba_loan_approvals`
        WHERE BorrowerState = 'WI'
            AND GrossApproval > 100000  -- Successful larger loans
            AND BorrowerCity IS NOT NULL
        LIMIT {limit}
        """
        
        businesses = self.bq_client.query(query).to_dataframe()
        
        logger.info(f"Analyzing {len(businesses)} businesses")
        
        for _, business in businesses.iterrows():
            try:
                trade_area = self.analyze_trade_area(
                    business_id=business['business_id'],
                    business_name=business['business_name'],
                    business_type=business['business_type'],
                    lat=business['latitude'],
                    lon=business['longitude'],
                    address=business['address'],
                    city=business['city'],
                    state=business['state']
                )
                
                self.save_to_bigquery(trade_area)
                
                # Rate limiting for free API tier
                if not self.offline_mode:
                    time.sleep(2)  # OpenRouteService free tier limit
                    
            except Exception as e:
                logger.error(f"Error analyzing {business['business_name']}: {e}")
                continue

def main():
    """Run trade area analysis"""
    analyzer = TradeAreaAnalyzer()
    
    # Example: Analyze a specific location
    sample_trade_area = analyzer.analyze_trade_area(
        business_id="SAMPLE_001",
        business_name="Sample Pizza Restaurant",
        business_type="restaurant",
        lat=43.0731,  # Madison, WI
        lon=-89.4012,
        address="123 State St",
        city="Madison",
        state="WI"
    )
    
    print("\nSample Trade Area Analysis:")
    print(f"5-minute population: {sample_trade_area['customers_0_5_min']:,}")
    print(f"10-minute population: {sample_trade_area['customers_5_10_min']:,}")
    print(f"15-minute population: {sample_trade_area['customers_10_15_min']:,}")
    
    # Analyze existing businesses
    print("\nAnalyzing existing businesses from SBA data...")
    analyzer.analyze_existing_businesses(limit=5)
    
    print("\nTrade area analysis complete!")

if __name__ == "__main__":
    main()