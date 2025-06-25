"""
OpenStreetMap Data Collector
===========================

Collects real business and amenity data from OpenStreetMap using the Overpass API.
Focuses on Wisconsin locations with business intelligence applications.
"""

import requests
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
import yaml


@dataclass
class OSMBusinessData:
    """OpenStreetMap business/amenity data structure"""
    osm_id: str
    osm_type: str  # node, way, relation
    name: Optional[str] = None
    amenity: Optional[str] = None
    shop: Optional[str] = None
    cuisine: Optional[str] = None
    brand: Optional[str] = None
    operator: Optional[str] = None
    
    # Location data
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address_housenumber: Optional[str] = None
    address_street: Optional[str] = None
    address_city: Optional[str] = None
    address_state: Optional[str] = None
    address_postcode: Optional[str] = None
    address_country: Optional[str] = None
    
    # Contact information
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    
    # Business details
    opening_hours: Optional[str] = None
    wheelchair: Optional[str] = None
    takeaway: Optional[str] = None
    delivery: Optional[str] = None
    outdoor_seating: Optional[str] = None
    
    # Classification
    business_type: Optional[str] = None
    franchise_indicator: Optional[bool] = None
    
    # Metadata
    osm_version: Optional[int] = None
    osm_timestamp: Optional[str] = None
    data_collection_date: str = None
    
    def __post_init__(self):
        if self.data_collection_date is None:
            self.data_collection_date = datetime.now().isoformat()


class OSMDataCollector:
    """Collector for OpenStreetMap business data via Overpass API"""
    
    def __init__(self):
        self.overpass_url = "https://overpass-api.de/api/interpreter"
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Wisconsin-Business-Location-Optimizer/1.0 (https://github.com/your-org/location-optimizer)'
        })
        self.rate_limit_delay = 1.0  # 1 second between requests
        
        # Load Wisconsin county data
        with open('data_sources.yaml', 'r') as f:
            config = yaml.safe_load(f)
            self.wisconsin_counties = config['states']['wisconsin']['business_registrations']['primary']['target_counties']
            self.target_business_types = config['states']['wisconsin']['business_registrations']['primary']['target_business_types']
    
    def build_overpass_query(self, bbox: str, amenity_types: List[str] = None, 
                            shop_types: List[str] = None) -> str:
        """
        Build Overpass QL query for Wisconsin business data
        
        Args:
            bbox: Bounding box as "south,west,north,east"
            amenity_types: List of amenity types to query
            shop_types: List of shop types to query
            
        Returns:
            Overpass QL query string
        """
        if amenity_types is None:
            amenity_types = [
                'restaurant', 'fast_food', 'cafe', 'bar', 'pub', 'food_court',
                'bank', 'atm', 'pharmacy', 'hospital', 'clinic', 'dentist',
                'fuel', 'car_rental', 'car_repair', 'car_wash',
                'post_office', 'library', 'school', 'university',
                'hotel', 'motel', 'guest_house', 'hostel',
                'gym', 'fitness_centre', 'swimming_pool', 'spa',
                'beauty_salon', 'hairdresser', 'veterinary'
            ]
        
        if shop_types is None:
            shop_types = [
                'supermarket', 'convenience', 'department_store', 'mall',
                'clothes', 'shoes', 'jewelry', 'electronics', 'mobile_phone',
                'computer', 'bicycle', 'car', 'motorcycle', 'car_parts',
                'hardware', 'doityourself', 'garden_centre', 'furniture',
                'bakery', 'butcher', 'fishmonger', 'greengrocer', 'alcohol',
                'beauty', 'hairdresser', 'optician', 'medical_supply',
                'pet', 'toys', 'books', 'stationery', 'gift', 'florist'
            ]
        
        # Build query parts
        amenity_queries = []
        for amenity in amenity_types:
            amenity_queries.append(f'  node["amenity"="{amenity}"]({bbox});')
            amenity_queries.append(f'  way["amenity"="{amenity}"]({bbox});')
        
        shop_queries = []
        for shop in shop_types:
            shop_queries.append(f'  node["shop"="{shop}"]({bbox});')
            shop_queries.append(f'  way["shop"="{shop}"]({bbox});')
        
        # Combine into full query
        query = f"""[out:json][timeout:60];
(
{chr(10).join(amenity_queries)}
{chr(10).join(shop_queries)}
);
out center meta;
"""
        
        return query
    
    def get_wisconsin_bbox(self) -> str:
        """Get bounding box for Wisconsin"""
        # Wisconsin approximate bounding box: south,west,north,east
        return "42.4,-92.8,46.8,-86.2"
    
    def get_county_bbox(self, county: str) -> Optional[str]:
        """
        Get approximate bounding box for a Wisconsin county
        
        Args:
            county: County name
            
        Returns:
            Bounding box string or None if not found
        """
        # Approximate bounding boxes for major Wisconsin counties
        county_bboxes = {
            'Milwaukee': "42.8,-88.1,43.2,-87.8",
            'Dane': "42.9,-89.8,43.4,-89.1", 
            'Brown': "44.3,-88.3,44.7,-87.9",
            'Waukesha': "42.9,-88.5,43.2,-88.0",
            'Racine': "42.6,-88.1,42.9,-87.7",
            'Kenosha': "42.5,-88.1,42.7,-87.8",
            'Rock': "42.5,-89.3,42.8,-88.8",
            'Winnebago': "44.0,-88.8,44.3,-88.3",
            'La Crosse': "43.7,-91.5,44.0,-91.1",
            'Eau Claire': "44.6,-91.6,44.9,-91.3"
        }
        
        return county_bboxes.get(county)
    
    def execute_overpass_query(self, query: str) -> Dict[str, Any]:
        """
        Execute Overpass API query
        
        Args:
            query: Overpass QL query
            
        Returns:
            Query result as dictionary
        """
        try:
            self.logger.debug(f"Executing Overpass query: {query[:100]}...")
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            response = self.session.post(
                self.overpass_url,
                data=query,
                timeout=120  # 2 minute timeout for large queries
            )
            
            if response.status_code != 200:
                self.logger.error(f"Overpass API error: {response.status_code} - {response.text}")
                return {}
            
            return response.json()
            
        except requests.exceptions.Timeout:
            self.logger.error("Overpass API query timed out")
            return {}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Overpass API request failed: {e}")
            return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse Overpass API response: {e}")
            return {}
    
    def parse_osm_element(self, element: Dict[str, Any]) -> OSMBusinessData:
        """
        Parse OSM element into structured business data
        
        Args:
            element: OSM element from Overpass API
            
        Returns:
            OSMBusinessData object
        """
        tags = element.get('tags', {})
        
        # Extract coordinates
        lat, lon = None, None
        if element['type'] == 'node':
            lat = element.get('lat')
            lon = element.get('lon')
        elif element['type'] == 'way' and 'center' in element:
            lat = element['center'].get('lat')
            lon = element['center'].get('lon')
        
        # Determine business type
        business_type = self.classify_business_type(tags)
        
        # Check for franchise indicators
        franchise_indicator = self.detect_franchise(tags)
        
        return OSMBusinessData(
            osm_id=str(element['id']),
            osm_type=element['type'],
            name=tags.get('name'),
            amenity=tags.get('amenity'),
            shop=tags.get('shop'),
            cuisine=tags.get('cuisine'),
            brand=tags.get('brand'),
            operator=tags.get('operator'),
            
            # Location
            latitude=lat,
            longitude=lon,
            address_housenumber=tags.get('addr:housenumber'),
            address_street=tags.get('addr:street'),
            address_city=tags.get('addr:city'),
            address_state=tags.get('addr:state'),
            address_postcode=tags.get('addr:postcode'),
            address_country=tags.get('addr:country'),
            
            # Contact
            phone=tags.get('phone'),
            website=tags.get('website'),
            email=tags.get('email'),
            
            # Business details
            opening_hours=tags.get('opening_hours'),
            wheelchair=tags.get('wheelchair'),
            takeaway=tags.get('takeaway'),
            delivery=tags.get('delivery'),
            outdoor_seating=tags.get('outdoor_seating'),
            
            # Classification
            business_type=business_type,
            franchise_indicator=franchise_indicator,
            
            # Metadata
            osm_version=element.get('version'),
            osm_timestamp=element.get('timestamp')
        )
    
    def classify_business_type(self, tags: Dict[str, str]) -> Optional[str]:
        """
        Classify business type based on OSM tags
        
        Args:
            tags: OSM tags dictionary
            
        Returns:
            Business type classification
        """
        amenity = tags.get('amenity', '').lower()
        shop = tags.get('shop', '').lower()
        cuisine = tags.get('cuisine', '').lower()
        
        # Food & Beverage
        if amenity in ['restaurant', 'fast_food', 'cafe', 'bar', 'pub', 'food_court']:
            return 'food_beverage'
        if shop in ['bakery', 'butcher', 'fishmonger', 'greengrocer', 'alcohol']:
            return 'food_beverage'
        
        # Retail
        if shop in ['supermarket', 'convenience', 'department_store', 'mall', 'clothes', 
                   'shoes', 'jewelry', 'electronics', 'mobile_phone', 'computer']:
            return 'retail'
        
        # Personal Services
        if amenity in ['beauty_salon', 'hairdresser']:
            return 'personal_services'
        if shop in ['beauty', 'hairdresser', 'optician']:
            return 'personal_services'
        
        # Automotive
        if amenity in ['fuel', 'car_rental', 'car_repair', 'car_wash']:
            return 'automotive'
        if shop in ['car', 'motorcycle', 'car_parts']:
            return 'automotive'
        
        # Fitness & Recreation
        if amenity in ['gym', 'fitness_centre', 'swimming_pool', 'spa']:
            return 'fitness'
        
        # Healthcare
        if amenity in ['pharmacy', 'hospital', 'clinic', 'dentist', 'veterinary']:
            return 'healthcare'
        
        # Professional Services
        if amenity in ['bank', 'post_office', 'library']:
            return 'professional_services'
        
        # Hospitality
        if amenity in ['hotel', 'motel', 'guest_house', 'hostel']:
            return 'hospitality'
        
        return 'other'
    
    def detect_franchise(self, tags: Dict[str, str]) -> bool:
        """
        Detect if business is likely a franchise based on brand/operator tags
        
        Args:
            tags: OSM tags dictionary
            
        Returns:
            True if likely franchise, False otherwise
        """
        brand = tags.get('brand', '').lower()
        operator = tags.get('operator', '').lower()
        name = tags.get('name', '').lower()
        
        # Known franchise brands
        franchise_brands = {
            'mcdonalds', 'subway', 'starbucks', 'burger king', 'kfc', 'pizza hut',
            'dominos', 'taco bell', 'wendys', 'dunkin', 'tim hortons',
            'walmart', 'target', 'best buy', 'home depot', 'lowes',
            'cvs', 'walgreens', 'rite aid', 'shell', 'bp', 'mobil',
            'marriott', 'hilton', 'holiday inn', 'hampton inn',
            'anytime fitness', 'planet fitness', 'curves'
        }
        
        # Check brand, operator, and name for franchise indicators
        for franchise in franchise_brands:
            if (franchise in brand or franchise in operator or 
                franchise in name.replace(' ', '').replace("'", '')):
                return True
        
        return False
    
    def collect_wisconsin_businesses(self, limit_counties: List[str] = None) -> List[OSMBusinessData]:
        """
        Collect business data for Wisconsin or specific counties
        
        Args:
            limit_counties: List of counties to limit collection to
            
        Returns:
            List of OSM business data
        """
        all_businesses = []
        
        if limit_counties:
            # Collect by county
            for county in limit_counties:
                bbox = self.get_county_bbox(county)
                if bbox:
                    self.logger.info(f"Collecting OSM data for {county} County")
                    businesses = self._collect_for_bbox(bbox, f"{county} County")
                    all_businesses.extend(businesses)
                else:
                    self.logger.warning(f"No bounding box available for {county} County")
        else:
            # Collect for entire Wisconsin
            self.logger.info("Collecting OSM data for entire Wisconsin")
            bbox = self.get_wisconsin_bbox()
            all_businesses = self._collect_for_bbox(bbox, "Wisconsin")
        
        return all_businesses
    
    def _collect_for_bbox(self, bbox: str, area_name: str) -> List[OSMBusinessData]:
        """
        Collect OSM data for a specific bounding box
        
        Args:
            bbox: Bounding box string
            area_name: Name of area for logging
            
        Returns:
            List of OSM business data
        """
        businesses = []
        
        try:
            # Build and execute query
            query = self.build_overpass_query(bbox)
            result = self.execute_overpass_query(query)
            
            if not result or 'elements' not in result:
                self.logger.warning(f"No data returned for {area_name}")
                return businesses
            
            elements = result['elements']
            self.logger.info(f"Processing {len(elements)} OSM elements from {area_name}")
            
            # Parse each element
            for element in elements:
                try:
                    business = self.parse_osm_element(element)
                    
                    # Filter for businesses with names and coordinates
                    if business.name and business.latitude and business.longitude:
                        businesses.append(business)
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing OSM element {element.get('id', 'unknown')}: {e}")
                    continue
            
            self.logger.info(f"Collected {len(businesses)} businesses from {area_name}")
            
        except Exception as e:
            self.logger.error(f"Error collecting data for {area_name}: {e}")
        
        return businesses
    
    def save_to_json(self, businesses: List[OSMBusinessData], filename: str):
        """Save businesses to JSON file"""
        try:
            data = [asdict(business) for business in businesses]
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(businesses)} businesses to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")
    
    def generate_collection_report(self, businesses: List[OSMBusinessData]) -> str:
        """Generate summary report of collection results"""
        if not businesses:
            return "No businesses collected."
        
        # Count by business type
        type_counts = {}
        franchise_count = 0
        cities = set()
        
        for business in businesses:
            btype = business.business_type or 'unknown'
            type_counts[btype] = type_counts.get(btype, 0) + 1
            
            if business.franchise_indicator:
                franchise_count += 1
            
            if business.address_city:
                cities.add(business.address_city)
        
        report = []
        report.append(f"📊 OSM DATA COLLECTION REPORT")
        report.append("=" * 50)
        report.append(f"Total Businesses: {len(businesses)}")
        report.append(f"Franchise Businesses: {franchise_count} ({(franchise_count/len(businesses)*100):.1f}%)")
        report.append(f"Cities Covered: {len(cities)}")
        
        report.append(f"\n📈 BUSINESS TYPE BREAKDOWN:")
        for btype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(businesses)) * 100
            report.append(f"   {btype}: {count} ({percentage:.1f}%)")
        
        return "\n".join(report)


def test_osm_collector():
    """Test the OSM data collector"""
    logging.basicConfig(level=logging.INFO)
    
    collector = OSMDataCollector()
    
    print("🗺️ Testing OpenStreetMap Data Collection:")
    print("=" * 50)
    
    # Test with Madison area (Dane County)
    print("\n📍 Collecting OSM data for Madison area...")
    businesses = collector.collect_wisconsin_businesses(['Dane'])
    
    if businesses:
        print(f"\n✅ Collected {len(businesses)} businesses")
        
        # Show some examples
        print(f"\n📋 Sample businesses:")
        for i, business in enumerate(businesses[:5]):
            print(f"   {i+1}. {business.name} ({business.business_type})")
            print(f"      {business.amenity or business.shop} - {business.address_city}")
            if business.franchise_indicator:
                print(f"      🏢 Franchise: {business.brand}")
        
        # Generate report
        report = collector.generate_collection_report(businesses)
        print(f"\n{report}")
        
        # Save to file
        collector.save_to_json(businesses, 'sample_osm_businesses.json')
        
    else:
        print("❌ No businesses collected")


if __name__ == "__main__":
    test_osm_collector()