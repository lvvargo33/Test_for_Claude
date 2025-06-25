"""
OpenStreetMap Geocoding Module
=============================

Provides geocoding functionality using OpenStreetMap's Nominatim service
for converting addresses to latitude/longitude coordinates.
"""

import requests
import time
import logging
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
import re


@dataclass
class GeocodingResult:
    """Result of a geocoding operation"""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    formatted_address: Optional[str] = None
    display_name: Optional[str] = None
    place_id: Optional[str] = None
    confidence: Optional[float] = None
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        """Check if geocoding was successful"""
        return self.latitude is not None and self.longitude is not None


class OpenStreetMapGeocoder:
    """OpenStreetMap/Nominatim geocoding service"""
    
    def __init__(self, user_agent: str = "Wisconsin-Business-Location-Optimizer/1.0"):
        """
        Initialize geocoder
        
        Args:
            user_agent: User agent string for API requests
        """
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.user_agent = user_agent
        self.rate_limit_delay = 1.0  # 1 second between requests per Nominatim usage policy
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent
        })
        
    def standardize_address(self, address: str, city: str, state: str, zip_code: str = None) -> str:
        """
        Standardize address format for better geocoding results
        
        Args:
            address: Street address
            city: City name
            state: State code (e.g., 'WI')
            zip_code: ZIP code (optional)
            
        Returns:
            Standardized address string
        """
        components = []
        
        # Clean and add address
        if address:
            # Remove common problematic patterns
            clean_address = re.sub(r'\s+', ' ', address.strip())
            clean_address = re.sub(r'[^\w\s\-\.\#]', '', clean_address)
            components.append(clean_address)
        
        # Add city
        if city:
            components.append(city.strip())
        
        # Add state
        if state:
            components.append(state.upper().strip())
        
        # Add ZIP code if available
        if zip_code:
            clean_zip = re.sub(r'[^\d\-]', '', zip_code)
            if len(clean_zip) >= 5:
                components.append(clean_zip[:5])
        
        return ', '.join(components)
    
    def geocode_address(self, address: str, city: str, state: str, zip_code: str = None, 
                       timeout: int = 10) -> GeocodingResult:
        """
        Geocode a single address using OpenStreetMap Nominatim
        
        Args:
            address: Street address
            city: City name
            state: State code
            zip_code: ZIP code (optional)
            timeout: Request timeout in seconds
            
        Returns:
            GeocodingResult with coordinates and metadata
        """
        result = GeocodingResult()
        
        try:
            # Standardize address
            full_address = self.standardize_address(address, city, state, zip_code)
            
            # Prepare request parameters
            params = {
                'q': full_address,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1,
                'countrycodes': 'us',  # Limit to United States
                'bounded': 1,  # Prefer results within viewbox
                # Bias results towards Wisconsin
                'viewbox': '-92.8,46.8,-86.2,42.4'  # Wisconsin bounding box
            }
            
            self.logger.debug(f"Geocoding address: {full_address}")
            
            # Make request with rate limiting
            time.sleep(self.rate_limit_delay)
            response = self.session.get(self.base_url, params=params, timeout=timeout)
            
            if response.status_code != 200:
                result.error = f"HTTP {response.status_code}: {response.text}"
                self.logger.warning(f"Geocoding failed for '{full_address}': {result.error}")
                return result
            
            data = response.json()
            
            if not data:
                result.error = "No results found"
                self.logger.debug(f"No geocoding results for: {full_address}")
                return result
            
            # Extract data from first result
            first_result = data[0]
            
            result.latitude = float(first_result.get('lat', 0))
            result.longitude = float(first_result.get('lon', 0))
            result.display_name = first_result.get('display_name', '')
            result.place_id = first_result.get('place_id', '')
            
            # Calculate confidence based on result quality
            result.confidence = self._calculate_confidence(first_result, full_address)
            
            # Format standardized address
            address_parts = first_result.get('address', {})
            result.formatted_address = self._format_address(address_parts)
            
            self.logger.debug(f"Geocoded '{full_address}' → ({result.latitude}, {result.longitude})")
            
        except requests.RequestException as e:
            result.error = f"Request error: {str(e)}"
            self.logger.error(f"Request error geocoding '{full_address}': {e}")
        except Exception as e:
            result.error = f"Geocoding error: {str(e)}"
            self.logger.error(f"Unexpected error geocoding '{full_address}': {e}")
        
        return result
    
    def _calculate_confidence(self, nominatim_result: Dict, original_address: str) -> float:
        """
        Calculate confidence score based on result quality
        
        Args:
            nominatim_result: Raw result from Nominatim
            original_address: Original address string
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        score = 0.0
        
        # Base score from importance (if available)
        importance = float(nominatim_result.get('importance', 0.5))
        score += importance * 40  # Up to 40 points
        
        # Address detail completeness
        address = nominatim_result.get('address', {})
        detail_score = 0
        
        key_fields = ['house_number', 'road', 'city', 'state', 'postcode']
        for field in key_fields:
            if address.get(field):
                detail_score += 12  # 12 points per field (60 max)
        
        score += detail_score
        
        # Ensure score is between 0 and 100, then convert to 0.0-1.0
        score = max(0, min(100, score))
        return round(score / 100, 2)
    
    def _format_address(self, address_parts: Dict) -> str:
        """
        Format address from Nominatim address components
        
        Args:
            address_parts: Address components from Nominatim
            
        Returns:
            Formatted address string
        """
        components = []
        
        # House number and road
        house_number = address_parts.get('house_number', '')
        road = address_parts.get('road', '')
        if house_number and road:
            components.append(f"{house_number} {road}")
        elif road:
            components.append(road)
        
        # City
        city = (address_parts.get('city') or 
                address_parts.get('town') or 
                address_parts.get('village') or
                address_parts.get('hamlet'))
        if city:
            components.append(city)
        
        # State
        state = address_parts.get('state')
        if state:
            components.append(state)
        
        # ZIP code
        postcode = address_parts.get('postcode')
        if postcode:
            components.append(postcode)
        
        return ', '.join(components)
    
    def geocode_business_batch(self, businesses: list, address_field: str = 'address_full',
                              city_field: str = 'city', state_field: str = 'state',
                              zip_field: str = 'zip_code') -> Dict[str, GeocodingResult]:
        """
        Geocode a batch of business records
        
        Args:
            businesses: List of business objects/dicts
            address_field: Field name for street address
            city_field: Field name for city
            state_field: Field name for state
            zip_field: Field name for ZIP code
            
        Returns:
            Dictionary mapping business IDs to geocoding results
        """
        results = {}
        total = len(businesses)
        
        self.logger.info(f"Starting batch geocoding of {total} businesses")
        
        for i, business in enumerate(businesses, 1):
            try:
                # Extract address components (handle both dict and object access)
                if hasattr(business, address_field):
                    address = getattr(business, address_field, '')
                    city = getattr(business, city_field, '')
                    state = getattr(business, state_field, '')
                    zip_code = getattr(business, zip_field, '')
                    business_id = getattr(business, 'business_id', f'business_{i}')
                else:
                    address = business.get(address_field, '')
                    city = business.get(city_field, '')
                    state = business.get(state_field, '')
                    zip_code = business.get(zip_field, '')
                    business_id = business.get('business_id', f'business_{i}')
                
                # Skip if no city information
                if not city:
                    self.logger.warning(f"Skipping business {business_id}: no city information")
                    continue
                
                # Geocode the address
                result = self.geocode_address(address, city, state, zip_code)
                results[business_id] = result
                
                # Progress logging
                if i % 10 == 0 or i == total:
                    successful = sum(1 for r in results.values() if r.success)
                    self.logger.info(f"Geocoded {i}/{total} businesses ({successful} successful)")
                
            except Exception as e:
                self.logger.error(f"Error geocoding business {i}: {e}")
                continue
        
        successful = sum(1 for r in results.values() if r.success)
        success_rate = (successful / len(results)) * 100 if results else 0
        
        self.logger.info(f"Batch geocoding complete: {successful}/{len(results)} successful ({success_rate:.1f}%)")
        
        return results


def test_geocoder():
    """Test the geocoding functionality"""
    logging.basicConfig(level=logging.INFO)
    
    geocoder = OpenStreetMapGeocoder()
    
    # Test addresses in Wisconsin
    test_addresses = [
        {
            'address': '123 State Street',
            'city': 'Madison',
            'state': 'WI',
            'zip_code': '53703'
        },
        {
            'address': '1234 N Water St',
            'city': 'Milwaukee',
            'state': 'WI',
            'zip_code': '53202'
        },
        {
            'address': '',  # Test with missing address
            'city': 'Green Bay',
            'state': 'WI',
            'zip_code': '54301'
        }
    ]
    
    print("🗺️ Testing OpenStreetMap Geocoding:")
    print("=" * 50)
    
    for i, addr in enumerate(test_addresses, 1):
        print(f"\nTest {i}: {addr['address']}, {addr['city']}, {addr['state']} {addr['zip_code']}")
        
        result = geocoder.geocode_address(
            addr['address'], addr['city'], addr['state'], addr['zip_code']
        )
        
        if result.success:
            print(f"✅ Success: ({result.latitude}, {result.longitude})")
            print(f"   Confidence: {result.confidence}")
            print(f"   Formatted: {result.formatted_address}")
        else:
            print(f"❌ Failed: {result.error}")


if __name__ == "__main__":
    test_geocoder()