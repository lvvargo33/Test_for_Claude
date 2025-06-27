"""
Google Places API Collector for Wisconsin Business Intelligence
==============================================================

Phase 1 Implementation: Milwaukee, Dane, and Brown Counties
Comprehensive business data collection for competitive analysis and location intelligence.

Features:
- Efficient area-based searching to minimize API calls
- Automatic business categorization and competitive analysis
- Integration with existing Wisconsin demographic and economic data
- Real-time data quality assessment and validation
"""

import googlemaps
import requests
import pandas as pd
import logging
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
import math
from geopy.distance import geodesic
import numpy as np

from base_collector import BaseDataCollector


@dataclass
class SearchArea:
    """Defines a geographic search area for Places API queries"""
    name: str
    center_lat: float
    center_lng: float
    radius_meters: int
    county: str
    metro_area: str
    priority: int = 1  # 1=highest, 3=lowest


class GooglePlacesCollector(BaseDataCollector):
    """
    Collects business data from Google Places API for Wisconsin analysis
    """
    
    def __init__(self, api_key: str, config_path: str = "data_sources.yaml"):
        super().__init__("WI", config_path)
        
        # Initialize Google Maps client
        self.gmaps = googlemaps.Client(key=api_key)
        self.api_key = api_key
        
        # API configuration
        self.places_config = {
            'base_url': 'https://maps.googleapis.com/maps/api/place',
            'default_radius': 2000,  # 2km radius
            'max_radius': 5000,      # 5km max radius
            'rate_limit_delay': 0.1,  # 100ms between requests
            'max_retries': 3,
            'timeout': 30
        }
        
        # Target business types for comprehensive collection
        self.target_business_types = [
            # Food Service
            'restaurant', 'meal_takeaway', 'meal_delivery', 'bakery', 'cafe', 'bar',
            # Retail
            'clothing_store', 'shoe_store', 'jewelry_store', 'electronics_store',
            'furniture_store', 'home_goods_store', 'grocery_or_supermarket',
            'convenience_store', 'gas_station',
            # Personal Services
            'hair_care', 'beauty_salon', 'spa', 'gym', 'car_repair', 'car_wash', 'laundry',
            # Professional Services
            'lawyer', 'accounting', 'real_estate_agency', 'insurance_agency',
            # Healthcare
            'doctor', 'dentist', 'pharmacy', 'veterinary_care',
            # Automotive
            'car_dealer', 'car_rental',
            # Entertainment
            'movie_theater', 'bowling_alley'
        ]
        
        # Phase 1 search areas
        self.phase1_search_areas = self._define_phase1_search_areas()
        
        # Phase 2 search areas
        self.phase2_search_areas = self._define_phase2_search_areas()
        
        # Business categorization mapping
        self.business_categories = self._load_business_categories()
        
        # Collection statistics
        self.api_calls_made = 0
        self.businesses_collected = 0
        self.errors_encountered = []
        
        self.logger.info("Google Places Collector initialized for Phase 1")
    
    def _define_phase1_search_areas(self) -> List[SearchArea]:
        """Define comprehensive search areas for Phase 1 counties"""
        search_areas = []
        
        # Milwaukee County - Major commercial areas
        milwaukee_areas = [
            # Downtown Milwaukee
            SearchArea("Downtown Milwaukee", 43.0389, -87.9065, 2000, "Milwaukee", "Milwaukee MSA", 1),
            # Major shopping areas
            SearchArea("Mayfair Mall Area", 43.0490, -88.0343, 2500, "Milwaukee", "Milwaukee MSA", 1),
            SearchArea("Bayshore Town Center", 43.1562, -87.8978, 2000, "Milwaukee", "Milwaukee MSA", 1),
            SearchArea("Southridge Mall", 43.0067, -88.0295, 2500, "Milwaukee", "Milwaukee MSA", 1),
            # Major corridors
            SearchArea("North Avenue Corridor", 43.0580, -87.9445, 3000, "Milwaukee", "Milwaukee MSA", 2),
            SearchArea("Brady Street", 43.0513, -87.8945, 1500, "Milwaukee", "Milwaukee MSA", 2),
            SearchArea("South 16th Street", 43.0194, -87.9380, 2500, "Milwaukee", "Milwaukee MSA", 2),
            # Suburbs
            SearchArea("Wauwatosa", 43.0642, -88.0076, 3000, "Milwaukee", "Milwaukee MSA", 2),
            SearchArea("West Allis", 43.0167, -88.0070, 3000, "Milwaukee", "Milwaukee MSA", 2),
            SearchArea("Brookfield", 43.0606, -88.1065, 3000, "Milwaukee", "Milwaukee MSA", 2),
            SearchArea("Franklin", 42.8789, -88.0384, 3000, "Milwaukee", "Milwaukee MSA", 3),
            SearchArea("Oak Creek", 42.8636, -87.9020, 3000, "Milwaukee", "Milwaukee MSA", 3),
            SearchArea("Greenfield", 42.9614, -88.0126, 3000, "Milwaukee", "Milwaukee MSA", 3),
        ]
        
        # Dane County (Madison) - Major commercial areas
        dane_areas = [
            # Downtown Madison
            SearchArea("Downtown Madison", 43.0731, -89.4012, 2000, "Dane", "Madison MSA", 1),
            # Major shopping areas
            SearchArea("West Towne Mall", 43.0642, -89.5123, 2500, "Dane", "Madison MSA", 1),
            SearchArea("East Towne Mall", 43.1235, -89.3654, 2500, "Dane", "Madison MSA", 1),
            # University area
            SearchArea("UW Campus Area", 43.0766, -89.4125, 2000, "Dane", "Madison MSA", 1),
            SearchArea("State Street", 43.0731, -89.3998, 1000, "Dane", "Madison MSA", 1),
            # Major corridors
            SearchArea("University Avenue", 43.0766, -89.4269, 3000, "Dane", "Madison MSA", 2),
            SearchArea("East Washington", 43.0808, -89.3668, 3000, "Dane", "Madison MSA", 2),
            SearchArea("Park Street", 43.0542, -89.4009, 2500, "Dane", "Madison MSA", 2),
            # Suburbs
            SearchArea("Middleton", 43.0922, -89.5043, 3000, "Dane", "Madison MSA", 2),
            SearchArea("Fitchburg", 43.0236, -89.4695, 3000, "Dane", "Madison MSA", 2),
            SearchArea("Sun Prairie", 43.1836, -89.2137, 3000, "Dane", "Madison MSA", 3),
            SearchArea("Verona", 42.9908, -89.5290, 3000, "Dane", "Madison MSA", 3),
        ]
        
        # Brown County (Green Bay) - Major commercial areas
        brown_areas = [
            # Downtown Green Bay
            SearchArea("Downtown Green Bay", 44.5133, -88.0133, 2000, "Brown", "Green Bay MSA", 1),
            # Major shopping areas
            SearchArea("Bay Park Square", 44.4725, -88.0576, 2500, "Brown", "Green Bay MSA", 1),
            SearchArea("West Side Green Bay", 44.4895, -88.0965, 3000, "Brown", "Green Bay MSA", 2),
            # Major corridors
            SearchArea("Military Avenue", 44.4895, -88.0434, 3000, "Brown", "Green Bay MSA", 2),
            SearchArea("University Avenue GB", 44.4664, -88.0173, 2500, "Brown", "Green Bay MSA", 2),
            # Suburbs
            SearchArea("De Pere", 44.4489, -88.0323, 3000, "Brown", "Green Bay MSA", 2),
            SearchArea("Ashwaubenon", 44.4647, -88.0707, 2500, "Brown", "Green Bay MSA", 2),
            SearchArea("Allouez", 44.4795, -87.9878, 2000, "Brown", "Green Bay MSA", 3),
        ]
        
        search_areas.extend(milwaukee_areas)
        search_areas.extend(dane_areas)
        search_areas.extend(brown_areas)
        
        self.logger.info(f"Defined {len(search_areas)} search areas for Phase 1")
        return search_areas
    
    def _define_phase2_search_areas(self) -> List[SearchArea]:
        """Define comprehensive search areas for Phase 2 counties"""
        search_areas = []
        
        # Winnebago County (Oshkosh, Neenah, Menasha) - Fox Cities region
        winnebago_areas = [
            # Oshkosh - Major commercial areas
            SearchArea("Downtown Oshkosh", 44.0247, -88.5426, 2000, "Winnebago", "Oshkosh-Neenah MSA", 1),
            SearchArea("Valley Fair Mall", 44.0058, -88.5173, 2500, "Winnebago", "Oshkosh-Neenah MSA", 1),
            SearchArea("South Park Mall", 44.0058, -88.5173, 2000, "Winnebago", "Oshkosh-Neenah MSA", 1),
            SearchArea("UW Oshkosh Area", 44.0247, -88.5503, 2000, "Winnebago", "Oshkosh-Neenah MSA", 2),
            SearchArea("West Oshkosh", 44.0247, -88.5703, 3000, "Winnebago", "Oshkosh-Neenah MSA", 2),
            # Neenah
            SearchArea("Downtown Neenah", 44.1858, -88.4626, 2000, "Winnebago", "Oshkosh-Neenah MSA", 1),
            SearchArea("Neenah Commercial", 44.1758, -88.4726, 2500, "Winnebago", "Oshkosh-Neenah MSA", 2),
            # Menasha
            SearchArea("Downtown Menasha", 44.2022, -88.4326, 2000, "Winnebago", "Oshkosh-Neenah MSA", 1),
            SearchArea("Menasha Commercial", 44.1922, -88.4426, 2500, "Winnebago", "Oshkosh-Neenah MSA", 2),
        ]
        
        # Eau Claire County - Western Wisconsin hub
        eau_claire_areas = [
            # Downtown Eau Claire
            SearchArea("Downtown Eau Claire", 44.8113, -91.4985, 2000, "Eau Claire", "Eau Claire MSA", 1),
            # Major shopping areas
            SearchArea("Oakwood Mall", 44.7713, -91.4685, 2500, "Eau Claire", "Eau Claire MSA", 1),
            SearchArea("Volume One Plaza", 44.8013, -91.4885, 2000, "Eau Claire", "Eau Claire MSA", 1),
            # University area
            SearchArea("UW-Eau Claire", 44.8063, -91.5035, 2000, "Eau Claire", "Eau Claire MSA", 1),
            SearchArea("Water Street District", 44.8113, -91.4935, 1500, "Eau Claire", "Eau Claire MSA", 1),
            # Major corridors
            SearchArea("Clairemont Avenue", 44.7913, -91.4585, 3000, "Eau Claire", "Eau Claire MSA", 2),
            SearchArea("Golf Road Corridor", 44.7613, -91.4485, 3000, "Eau Claire", "Eau Claire MSA", 2),
            SearchArea("North Eau Claire", 44.8313, -91.4985, 3000, "Eau Claire", "Eau Claire MSA", 2),
            SearchArea("South Eau Claire", 44.7813, -91.4985, 3000, "Eau Claire", "Eau Claire MSA", 2),
        ]
        
        # Marathon County (Wausau) - Central Wisconsin hub
        marathon_areas = [
            # Downtown Wausau
            SearchArea("Downtown Wausau", 44.9591, -89.6301, 2000, "Marathon", "Wausau MSA", 1),
            # Major shopping areas
            SearchArea("Wausau Center Mall", 44.9491, -89.6201, 2500, "Marathon", "Wausau MSA", 1),
            SearchArea("Cedar Creek Mall", 44.9291, -89.6001, 2500, "Marathon", "Wausau MSA", 1),
            # Major corridors
            SearchArea("Grand Avenue", 44.9591, -89.6101, 2500, "Marathon", "Wausau MSA", 2),
            SearchArea("Stewart Avenue", 44.9391, -89.6301, 2500, "Marathon", "Wausau MSA", 2),
            SearchArea("Rib Mountain Drive", 44.9191, -89.6501, 3000, "Marathon", "Wausau MSA", 2),
            # Suburban areas
            SearchArea("North Wausau", 44.9791, -89.6301, 3000, "Marathon", "Wausau MSA", 2),
            SearchArea("South Wausau", 44.9391, -89.6301, 3000, "Marathon", "Wausau MSA", 2),
            SearchArea("West Wausau", 44.9591, -89.6601, 3000, "Marathon", "Wausau MSA", 3),
        ]
        
        # Kenosha County - Southeast Wisconsin, Chicago metro influence
        kenosha_areas = [
            # Downtown Kenosha
            SearchArea("Downtown Kenosha", 42.5847, -87.8212, 2000, "Kenosha", "Chicago-Naperville-Elgin MSA", 1),
            # Major shopping areas
            SearchArea("Southport Shopping Center", 42.5647, -87.8112, 2500, "Kenosha", "Chicago-Naperville-Elgin MSA", 1),
            SearchArea("Pleasant Prairie Premium Outlets", 42.5547, -87.8312, 2500, "Kenosha", "Chicago-Naperville-Elgin MSA", 1),
            # Major corridors
            SearchArea("52nd Street Corridor", 42.5747, -87.8312, 3000, "Kenosha", "Chicago-Naperville-Elgin MSA", 2),
            SearchArea("Highway 50 Corridor", 42.5847, -87.8512, 3000, "Kenosha", "Chicago-Naperville-Elgin MSA", 2),
            SearchArea("75th Street", 42.5447, -87.8212, 2500, "Kenosha", "Chicago-Naperville-Elgin MSA", 2),
            # Suburban areas
            SearchArea("Pleasant Prairie", 42.5547, -87.8612, 3000, "Kenosha", "Chicago-Naperville-Elgin MSA", 2),
            SearchArea("Somers", 42.6047, -87.8212, 3000, "Kenosha", "Chicago-Naperville-Elgin MSA", 3),
        ]
        
        # Racine County - Southeast Wisconsin, Milwaukee metro influence
        racine_areas = [
            # Downtown Racine
            SearchArea("Downtown Racine", 42.7261, -87.7828, 2000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 1),
            # Major shopping areas
            SearchArea("Regency Mall", 42.7061, -87.7928, 2500, "Racine", "Milwaukee-Waukesha-West Allis MSA", 1),
            SearchArea("Festival Foods Plaza", 42.7161, -87.8128, 2000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 1),
            # Major corridors
            SearchArea("Washington Avenue", 42.7261, -87.7928, 2500, "Racine", "Milwaukee-Waukesha-West Allis MSA", 2),
            SearchArea("Durand Avenue", 42.7061, -87.7628, 3000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 2),
            SearchArea("Highway 20 Corridor", 42.7361, -87.8028, 3000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 2),
            # Suburban areas
            SearchArea("Mount Pleasant", 42.7161, -87.8328, 3000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 2),
            SearchArea("Caledonia", 42.7461, -87.8728, 3000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 3),
            SearchArea("Sturtevant", 42.6961, -87.8928, 3000, "Racine", "Milwaukee-Waukesha-West Allis MSA", 3),
        ]
        
        search_areas.extend(winnebago_areas)
        search_areas.extend(eau_claire_areas)
        search_areas.extend(marathon_areas)
        search_areas.extend(kenosha_areas)
        search_areas.extend(racine_areas)
        
        self.logger.info(f"Defined {len(search_areas)} search areas for Phase 2")
        return search_areas
    
    def _load_business_categories(self) -> Dict[str, str]:
        """Load business type to category mapping"""
        return {
            # Food Service
            'restaurant': 'Restaurant',
            'meal_takeaway': 'Takeout/Delivery',
            'meal_delivery': 'Delivery',
            'bakery': 'Bakery',
            'cafe': 'Cafe/Coffee Shop',
            'bar': 'Bar/Tavern',
            'food': 'Food Service',
            
            # Retail
            'clothing_store': 'Clothing Store',
            'shoe_store': 'Shoe Store',
            'jewelry_store': 'Jewelry Store',
            'electronics_store': 'Electronics Store',
            'furniture_store': 'Furniture Store',
            'home_goods_store': 'Home Goods',
            'grocery_or_supermarket': 'Grocery Store',
            'convenience_store': 'Convenience Store',
            'gas_station': 'Gas Station',
            
            # Personal Services
            'hair_care': 'Hair Salon',
            'beauty_salon': 'Beauty Salon',
            'spa': 'Spa/Wellness',
            'gym': 'Fitness Center',
            'car_repair': 'Auto Repair',
            'car_wash': 'Car Wash',
            'laundry': 'Laundry/Dry Cleaning',
            
            # Professional Services
            'lawyer': 'Legal Services',
            'accounting': 'Accounting',
            'real_estate_agency': 'Real Estate',
            'insurance_agency': 'Insurance',
            
            # Healthcare
            'hospital': 'Hospital',
            'doctor': 'Medical Practice',
            'dentist': 'Dental Practice',
            'pharmacy': 'Pharmacy',
            'veterinary_care': 'Veterinary',
            
            # Automotive
            'car_dealer': 'Auto Dealer',
            'car_rental': 'Car Rental',
            
            # Entertainment
            'movie_theater': 'Movie Theater',
            'bowling_alley': 'Bowling Alley'
        }
    
    def search_places_in_area(self, search_area: SearchArea, business_type: str = None) -> List[Dict]:
        """
        Search for places in a specific geographic area
        
        Args:
            search_area: SearchArea object defining the search region
            business_type: Specific business type to search for (optional)
            
        Returns:
            List of place dictionaries
        """
        places = []
        
        try:
            # Build search parameters
            location = (search_area.center_lat, search_area.center_lng)
            
            if business_type:
                # Search for specific business type
                query_params = {
                    'location': location,
                    'radius': search_area.radius_meters,
                    'type': business_type,
                    'language': 'en'
                }
                
                self.logger.info(f"Searching {search_area.name} for {business_type}")
                
                # Use nearby search
                response = self.gmaps.places_nearby(**query_params)
                
            else:
                # General business search
                query_params = {
                    'location': location,
                    'radius': search_area.radius_meters,
                    'language': 'en'
                }
                
                self.logger.info(f"General search in {search_area.name}")
                response = self.gmaps.places_nearby(**query_params)
            
            self.api_calls_made += 1
            
            # Process results
            if response.get('status') == 'OK':
                results = response.get('results', [])
                
                for place in results:
                    # Enhance place data with search context
                    enhanced_place = self._enhance_place_data(place, search_area, business_type)
                    places.append(enhanced_place)
                
                # Handle pagination if needed
                next_page_token = response.get('next_page_token')
                if next_page_token and len(results) == 20:  # Google returns max 20 per page
                    # Wait for token to become valid
                    time.sleep(2)
                    
                    # Get next page
                    next_response = self.gmaps.places_nearby(page_token=next_page_token)
                    self.api_calls_made += 1
                    
                    if next_response.get('status') == 'OK':
                        next_results = next_response.get('results', [])
                        for place in next_results:
                            enhanced_place = self._enhance_place_data(place, search_area, business_type)
                            places.append(enhanced_place)
                
                self.logger.info(f"Found {len(places)} places in {search_area.name}")
                
            else:
                error_msg = f"Search failed for {search_area.name}: {response.get('status')}"
                self.logger.warning(error_msg)
                self.errors_encountered.append(error_msg)
            
            # Rate limiting
            time.sleep(self.places_config['rate_limit_delay'])
            
        except Exception as e:
            error_msg = f"Error searching {search_area.name}: {e}"
            self.logger.error(error_msg)
            self.errors_encountered.append(error_msg)
        
        return places
    
    def _enhance_place_data(self, place: Dict, search_area: SearchArea, business_type: str = None) -> Dict:
        """
        Enhance place data with additional analysis and standardization
        
        Args:
            place: Raw place data from Google Places API
            search_area: SearchArea used for the search
            business_type: Business type searched for
            
        Returns:
            Enhanced place dictionary
        """
        enhanced = place.copy()
        
        # Add search context
        enhanced['search_area_name'] = search_area.name
        enhanced['search_center_lat'] = search_area.center_lat
        enhanced['search_center_lng'] = search_area.center_lng
        enhanced['search_radius_meters'] = search_area.radius_meters
        enhanced['county_name'] = search_area.county
        enhanced['metro_area'] = search_area.metro_area
        enhanced['search_business_type'] = business_type
        
        # Standardize business categorization
        place_types = place.get('types', [])
        enhanced['primary_type'] = self._determine_primary_type(place_types)
        enhanced['business_category'] = self.business_categories.get(enhanced['primary_type'], 'Other')
        enhanced['industry_sector'] = self._determine_industry_sector(enhanced['business_category'])
        
        # Add location standardization
        geometry = place.get('geometry', {})
        location = geometry.get('location', {})
        enhanced['geometry_location_lat'] = location.get('lat')
        enhanced['geometry_location_lng'] = location.get('lng')
        
        # Parse address components
        address_components = self._parse_address_components(place.get('formatted_address', ''))
        enhanced.update(address_components)
        
        # Add data quality metrics
        enhanced['data_confidence_score'] = self._calculate_confidence_score(place)
        enhanced['collection_date'] = datetime.now()
        enhanced['last_updated'] = datetime.now()
        enhanced['api_response_status'] = 'OK'
        enhanced['collection_method'] = 'nearby_search'
        
        # Store raw response for debugging
        enhanced['raw_api_response'] = json.dumps(place)
        
        return enhanced
    
    def _determine_primary_type(self, place_types: List[str]) -> str:
        """Determine the primary business type from Google's type list"""
        # Priority order for business types
        priority_types = [
            'restaurant', 'meal_takeaway', 'meal_delivery', 'bakery', 'cafe', 'bar',
            'clothing_store', 'shoe_store', 'jewelry_store', 'electronics_store',
            'hair_care', 'beauty_salon', 'spa', 'gym', 'car_repair',
            'doctor', 'dentist', 'pharmacy', 'lawyer', 'accounting'
        ]
        
        # Return first matching priority type
        for ptype in priority_types:
            if ptype in place_types:
                return ptype
        
        # Return first type if no priority match
        return place_types[0] if place_types else 'establishment'
    
    def _determine_industry_sector(self, business_category: str) -> str:
        """Determine high-level industry sector"""
        food_service = ['Restaurant', 'Takeout/Delivery', 'Delivery', 'Bakery', 'Cafe/Coffee Shop', 'Bar/Tavern']
        retail = ['Clothing Store', 'Shoe Store', 'Jewelry Store', 'Electronics Store', 'Furniture Store', 'Home Goods', 'Grocery Store', 'Convenience Store', 'Gas Station']
        personal_services = ['Hair Salon', 'Beauty Salon', 'Spa/Wellness', 'Fitness Center', 'Auto Repair', 'Car Wash', 'Laundry/Dry Cleaning']
        professional = ['Legal Services', 'Accounting', 'Real Estate', 'Insurance']
        healthcare = ['Hospital', 'Medical Practice', 'Dental Practice', 'Pharmacy', 'Veterinary']
        automotive = ['Auto Dealer', 'Car Rental']
        entertainment = ['Movie Theater', 'Bowling Alley']
        
        if business_category in food_service:
            return 'Food Service'
        elif business_category in retail:
            return 'Retail'
        elif business_category in personal_services:
            return 'Personal Services'
        elif business_category in professional:
            return 'Professional Services'
        elif business_category in healthcare:
            return 'Healthcare'
        elif business_category in automotive:
            return 'Automotive'
        elif business_category in entertainment:
            return 'Entertainment'
        else:
            return 'Other'
    
    def _parse_address_components(self, formatted_address: str) -> Dict[str, str]:
        """Parse address into components"""
        components = {
            'street_number': '',
            'route': '',
            'locality': '',
            'administrative_area_level_2': '',
            'administrative_area_level_1': '',
            'postal_code': '',
            'country': '',
            'city_name': '',
            'zip_code': ''
        }
        
        if not formatted_address:
            return components
        
        # Basic parsing - could be enhanced with geocoding API
        parts = formatted_address.split(', ')
        
        if len(parts) >= 1:
            # Extract city from typical format
            for part in parts:
                if any(city in part for city in ['Milwaukee', 'Madison', 'Green Bay', 'Wauwatosa', 'West Allis']):
                    components['locality'] = part.strip()
                    components['city_name'] = part.strip()
                elif part.strip().startswith('WI ') or part.strip() == 'WI':
                    components['administrative_area_level_1'] = 'WI'
                elif part.strip().isdigit() and len(part.strip()) == 5:
                    components['postal_code'] = part.strip()
                    components['zip_code'] = part.strip()
        
        return components
    
    def _calculate_confidence_score(self, place: Dict) -> float:
        """Calculate data confidence score (0-100)"""
        score = 50.0  # Base score
        
        # Boost for complete data
        if place.get('name'):
            score += 10
        if place.get('formatted_address'):
            score += 10
        if place.get('formatted_phone_number'):
            score += 5
        if place.get('website'):
            score += 5
        if place.get('rating'):
            score += 10
        if place.get('user_ratings_total', 0) > 0:
            score += 5
        if place.get('business_status') == 'OPERATIONAL':
            score += 5
        
        return min(100.0, score)
    
    def collect_phase1_data(self) -> pd.DataFrame:
        """
        Collect comprehensive business data for Phase 1 counties
        
        Returns:
            DataFrame with collected business data
        """
        all_places = []
        start_time = time.time()
        
        self.logger.info("Starting Phase 1 data collection")
        self.logger.info(f"Target areas: {len(self.phase1_search_areas)}")
        
        try:
            # Collect from each search area
            for i, search_area in enumerate(self.phase1_search_areas, 1):
                self.logger.info(f"Processing area {i}/{len(self.phase1_search_areas)}: {search_area.name}")
                
                # General search for all businesses
                area_places = self.search_places_in_area(search_area)
                all_places.extend(area_places)
                
                # Progress reporting
                if i % 5 == 0:
                    self.logger.info(f"Progress: {i}/{len(self.phase1_search_areas)} areas completed")
                    self.logger.info(f"API calls made: {self.api_calls_made}")
                    self.logger.info(f"Businesses collected: {len(all_places)}")
            
            # Remove duplicates based on place_id
            unique_places = {}
            for place in all_places:
                place_id = place.get('place_id')
                if place_id and place_id not in unique_places:
                    unique_places[place_id] = place
            
            self.businesses_collected = len(unique_places)
            
            # Convert to DataFrame
            df = pd.DataFrame(list(unique_places.values()))
            
            # Add competitive analysis
            if not df.empty:
                df = self._add_competitive_analysis(df)
            
            processing_time = time.time() - start_time
            
            self.logger.info("Phase 1 collection complete")
            self.logger.info(f"Total API calls: {self.api_calls_made}")
            self.logger.info(f"Unique businesses: {self.businesses_collected}")
            self.logger.info(f"Processing time: {processing_time:.1f} seconds")
            self.logger.info(f"Errors encountered: {len(self.errors_encountered)}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in Phase 1 collection: {e}")
            return pd.DataFrame()
    
    def _add_competitive_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add competitive density analysis to the dataset"""
        self.logger.info("Adding competitive analysis")
        
        df_enhanced = df.copy()
        
        # Initialize competitive analysis columns
        df_enhanced['competitor_density_0_5_mile'] = 0
        df_enhanced['competitor_density_1_mile'] = 0
        df_enhanced['competitor_density_3_mile'] = 0
        df_enhanced['nearest_competitor_distance_miles'] = None
        
        try:
            # Calculate competitive density for each business
            for idx, business in df_enhanced.iterrows():
                if pd.isna(business['geometry_location_lat']) or pd.isna(business['geometry_location_lng']):
                    continue
                
                business_location = (business['geometry_location_lat'], business['geometry_location_lng'])
                business_category = business['business_category']
                
                # Find competitors in same category
                same_category = df_enhanced[df_enhanced['business_category'] == business_category]
                
                distances = []
                count_0_5 = 0
                count_1 = 0
                count_3 = 0
                
                for _, competitor in same_category.iterrows():
                    if competitor['place_id'] == business['place_id']:
                        continue
                    
                    if pd.isna(competitor['geometry_location_lat']) or pd.isna(competitor['geometry_location_lng']):
                        continue
                    
                    competitor_location = (competitor['geometry_location_lat'], competitor['geometry_location_lng'])
                    
                    try:
                        distance = geodesic(business_location, competitor_location).miles
                        distances.append(distance)
                        
                        if distance <= 0.5:
                            count_0_5 += 1
                        if distance <= 1.0:
                            count_1 += 1
                        if distance <= 3.0:
                            count_3 += 1
                    except:
                        continue
                
                # Update competitive metrics
                df_enhanced.at[idx, 'competitor_density_0_5_mile'] = count_0_5
                df_enhanced.at[idx, 'competitor_density_1_mile'] = count_1
                df_enhanced.at[idx, 'competitor_density_3_mile'] = count_3
                
                if distances:
                    df_enhanced.at[idx, 'nearest_competitor_distance_miles'] = min(distances)
        
        except Exception as e:
            self.logger.error(f"Error in competitive analysis: {e}")
        
        return df_enhanced
    
    def save_to_bigquery(self, df: pd.DataFrame) -> bool:
        """
        Save collected data to BigQuery
        
        Args:
            df: DataFrame with business data
            
        Returns:
            True if successful
        """
        try:
            if not self.bq_client:
                self.logger.warning("BigQuery client not available")
                return False
            
            if df.empty:
                self.logger.warning("No data to save")
                return False
            
            # Prepare data for BigQuery
            df_clean = df.copy()
            
            # Handle datetime columns
            datetime_columns = ['collection_date', 'last_updated']
            for col in datetime_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_datetime(df_clean[col])
            
            # Handle array columns (convert to JSON strings for now)
            array_columns = ['types', 'opening_hours_weekday_text']
            for col in array_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
            
            # BigQuery configuration
            dataset_id = self.bq_config.get('datasets', {}).get('raw_data', 'raw_business_data')
            table_id = 'google_places_businesses'
            full_table_id = f"{self.bq_config['project_id']}.{dataset_id}.{table_id}"
            
            from google.cloud import bigquery
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="collection_date"
                ),
                clustering_fields=["county_name", "business_category", "city_name"]
            )
            
            job = self.bq_client.load_table_from_dataframe(df_clean, full_table_id, job_config=job_config)
            job.result()
            
            self.logger.info(f"Successfully saved {len(df_clean)} business records to BigQuery")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to BigQuery: {e}")
            return False
    
    def run_phase1_collection(self) -> Dict[str, Any]:
        """
        Run complete Phase 1 collection process
        
        Returns:
            Collection summary
        """
        start_time = time.time()
        
        summary = {
            'collection_date': datetime.now(),
            'phase': 'Phase 1',
            'counties': ['Milwaukee', 'Dane', 'Brown'],
            'search_areas': len(self.phase1_search_areas),
            'api_calls_made': 0,
            'businesses_collected': 0,
            'success': False,
            'processing_time_seconds': 0,
            'errors': [],
            'data_quality': {}
        }
        
        try:
            self.logger.info("Starting Phase 1 Google Places collection")
            
            # Collect data
            df = self.collect_phase1_data()
            
            if not df.empty:
                # Save to BigQuery
                save_success = self.save_to_bigquery(df)
                
                # Save local copy
                output_file = f"google_places_phase1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(output_file, index=False)
                
                # Calculate data quality metrics
                quality_metrics = {
                    'total_records': len(df),
                    'records_with_ratings': len(df[df['rating'].notna()]) if 'rating' in df.columns else 0,
                    'records_with_phone': len(df[df['formatted_phone_number'].notna()]) if 'formatted_phone_number' in df.columns else 0,
                    'records_with_website': len(df[df['website'].notna()]) if 'website' in df.columns else 0,
                    'avg_confidence_score': df['data_confidence_score'].mean() if 'data_confidence_score' in df.columns else 0,
                    'business_categories': df['business_category'].value_counts().to_dict() if 'business_category' in df.columns else {},
                    'county_distribution': df['county_name'].value_counts().to_dict() if 'county_name' in df.columns else {}
                }
                
                summary.update({
                    'api_calls_made': self.api_calls_made,
                    'businesses_collected': self.businesses_collected,
                    'success': save_success,
                    'data_quality': quality_metrics,
                    'errors': self.errors_encountered,
                    'output_file': output_file
                })
                
                self.logger.info(f"Phase 1 collection completed successfully")
                self.logger.info(f"Saved to: {output_file}")
                
            else:
                summary['success'] = False
                summary['errors'].append("No data collected")
                
        except Exception as e:
            error_msg = f"Error in Phase 1 collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        return summary
    
    def collect_phase2_data(self) -> pd.DataFrame:
        """
        Collect comprehensive business data for Phase 2 counties
        
        Returns:
            DataFrame with collected business data
        """
        all_places = []
        start_time = time.time()
        
        self.logger.info("Starting Phase 2 data collection")
        self.logger.info(f"Target areas: {len(self.phase2_search_areas)}")
        
        try:
            # Collect from each search area
            for i, search_area in enumerate(self.phase2_search_areas, 1):
                self.logger.info(f"Processing area {i}/{len(self.phase2_search_areas)}: {search_area.name}")
                
                # General search for all businesses
                area_places = self.search_places_in_area(search_area)
                all_places.extend(area_places)
                
                # Progress reporting
                if i % 5 == 0:
                    self.logger.info(f"Progress: {i}/{len(self.phase2_search_areas)} areas completed")
                    self.logger.info(f"API calls made: {self.api_calls_made}")
                    self.logger.info(f"Businesses collected: {len(all_places)}")
            
            # Remove duplicates based on place_id
            unique_places = {}
            for place in all_places:
                place_id = place.get('place_id')
                if place_id and place_id not in unique_places:
                    unique_places[place_id] = place
            
            self.businesses_collected = len(unique_places)
            
            # Convert to DataFrame
            df = pd.DataFrame(list(unique_places.values()))
            
            # Add competitive analysis
            if not df.empty:
                df = self._add_competitive_analysis(df)
            
            processing_time = time.time() - start_time
            
            self.logger.info("Phase 2 collection complete")
            self.logger.info(f"Total API calls: {self.api_calls_made}")
            self.logger.info(f"Unique businesses: {self.businesses_collected}")
            self.logger.info(f"Processing time: {processing_time:.1f} seconds")
            self.logger.info(f"Errors encountered: {len(self.errors_encountered)}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in Phase 2 collection: {e}")
            return pd.DataFrame()
    
    def run_phase2_collection(self) -> Dict[str, Any]:
        """
        Run complete Phase 2 collection process
        
        Returns:
            Collection summary
        """
        start_time = time.time()
        
        summary = {
            'collection_date': datetime.now(),
            'phase': 'Phase 2',
            'counties': ['Winnebago', 'Eau Claire', 'Marathon', 'Kenosha', 'Racine'],
            'search_areas': len(self.phase2_search_areas),
            'api_calls_made': 0,
            'businesses_collected': 0,
            'success': False,
            'processing_time_seconds': 0,
            'errors': [],
            'data_quality': {}
        }
        
        try:
            self.logger.info("Starting Phase 2 Google Places collection")
            
            # Reset counters for Phase 2
            self.api_calls_made = 0
            self.businesses_collected = 0
            self.errors_encountered = []
            
            # Collect data
            df = self.collect_phase2_data()
            
            if not df.empty:
                # Save to BigQuery
                save_success = self.save_to_bigquery(df)
                
                # Save local copy
                output_file = f"google_places_phase2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(output_file, index=False)
                
                # Calculate data quality metrics
                quality_metrics = {
                    'total_records': len(df),
                    'records_with_ratings': len(df[df['rating'].notna()]) if 'rating' in df.columns else 0,
                    'records_with_phone': len(df[df['formatted_phone_number'].notna()]) if 'formatted_phone_number' in df.columns else 0,
                    'records_with_website': len(df[df['website'].notna()]) if 'website' in df.columns else 0,
                    'avg_confidence_score': df['data_confidence_score'].mean() if 'data_confidence_score' in df.columns else 0,
                    'business_categories': df['business_category'].value_counts().to_dict() if 'business_category' in df.columns else {},
                    'county_distribution': df['county_name'].value_counts().to_dict() if 'county_name' in df.columns else {}
                }
                
                summary.update({
                    'api_calls_made': self.api_calls_made,
                    'businesses_collected': self.businesses_collected,
                    'success': save_success,
                    'data_quality': quality_metrics,
                    'errors': self.errors_encountered,
                    'output_file': output_file
                })
                
                self.logger.info(f"Phase 2 collection completed successfully")
                self.logger.info(f"Saved to: {output_file}")
                
            else:
                summary['success'] = False
                summary['errors'].append("No data collected")
                
        except Exception as e:
            error_msg = f"Error in Phase 2 collection: {e}"
            self.logger.error(error_msg)
            summary['errors'].append(error_msg)
            summary['success'] = False
        
        summary['processing_time_seconds'] = time.time() - start_time
        
        return summary
    
    # Abstract method implementations (required by base class)
    def collect_business_registrations(self, days_back: int = 90) -> List:
        return []
    
    def collect_sba_loans(self, days_back: int = 180) -> List:
        return []
    
    def collect_business_licenses(self, days_back: int = 30) -> List:
        return []


def main():
    """Test Google Places collection with sample API key"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # This would use your actual API key
    api_key = "YOUR_GOOGLE_PLACES_API_KEY"
    
    try:
        collector = GooglePlacesCollector(api_key)
        summary = collector.run_phase1_collection()
        
        print("\n" + "="*60)
        print("GOOGLE PLACES PHASE 1 COLLECTION SUMMARY")
        print("="*60)
        print(f"Phase: {summary['phase']}")
        print(f"Counties: {', '.join(summary['counties'])}")
        print(f"Search Areas: {summary['search_areas']}")
        print(f"API Calls Made: {summary['api_calls_made']}")
        print(f"Businesses Collected: {summary['businesses_collected']}")
        print(f"Success: {summary['success']}")
        print(f"Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        
        if summary.get('data_quality'):
            quality = summary['data_quality']
            print(f"\nDATA QUALITY METRICS:")
            print(f"Total Records: {quality['total_records']}")
            print(f"Records with Ratings: {quality['records_with_ratings']}")
            print(f"Records with Phone: {quality['records_with_phone']}")
            print(f"Avg Confidence Score: {quality['avg_confidence_score']:.1f}")
            
            print(f"\nTOP BUSINESS CATEGORIES:")
            for category, count in list(quality['business_categories'].items())[:5]:
                print(f"- {category}: {count}")
            
            print(f"\nCOUNTY DISTRIBUTION:")
            for county, count in quality['county_distribution'].items():
                print(f"- {county}: {count}")
        
        if summary.get('errors'):
            print(f"\nErrors: {len(summary['errors'])}")
            for error in summary['errors'][:3]:
                print(f"- {error}")
        
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()