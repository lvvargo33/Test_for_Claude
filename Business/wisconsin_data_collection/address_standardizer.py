"""
Address Standardization Module
=============================

Standardizes addresses across different data sources for consistent geocoding
and data quality improvement.
"""

import re
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
import logging


@dataclass
class StandardizedAddress:
    """Standardized address result"""
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    street_type: Optional[str] = None
    unit: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    full_address: Optional[str] = None
    original_address: Optional[str] = None
    confidence: float = 0.0


class AddressStandardizer:
    """Address standardization and cleaning utility"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Common street type mappings
        self.street_type_mappings = {
            'st': 'street', 'str': 'street', 'strt': 'street',
            'ave': 'avenue', 'av': 'avenue', 'aven': 'avenue',
            'rd': 'road', 'dr': 'drive', 'drv': 'drive',
            'ln': 'lane', 'ct': 'court', 'cir': 'circle',
            'blvd': 'boulevard', 'pkwy': 'parkway', 'pl': 'place',
            'way': 'way', 'ter': 'terrace', 'trl': 'trail',
            'hwy': 'highway', 'fwy': 'freeway'
        }
        
        # Direction mappings
        self.direction_mappings = {
            'n': 'north', 'no': 'north', 'nth': 'north',
            's': 'south', 'so': 'south', 'sth': 'south',
            'e': 'east', 'ea': 'east', 'est': 'east',
            'w': 'west', 'we': 'west', 'wst': 'west',
            'ne': 'northeast', 'nw': 'northwest',
            'se': 'southeast', 'sw': 'southwest'
        }
        
        # Unit type patterns
        self.unit_patterns = [
            r'\b(?:apt|apartment|unit|ste|suite|#|lot|room|rm)\s*\.?\s*([a-z0-9\-]+)\b',
            r'\b([a-z0-9]+)\s*(?:apt|apartment|unit|ste|suite)\b',
            r'#\s*([a-z0-9\-]+)',
        ]
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Convert to lowercase and strip
        cleaned = text.lower().strip()
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove common problematic characters but keep essential punctuation
        cleaned = re.sub(r'[^\w\s\-\.\#]', ' ', cleaned)
        
        # Clean up extra spaces again
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def extract_unit(self, address: str) -> Tuple[str, Optional[str]]:
        """
        Extract unit/apartment information from address
        
        Returns:
            Tuple of (address_without_unit, unit)
        """
        if not address:
            return address, None
        
        address_clean = address.lower()
        
        for pattern in self.unit_patterns:
            match = re.search(pattern, address_clean, re.IGNORECASE)
            if match:
                unit = match.group(1).strip()
                # Remove the unit part from address
                address_without_unit = re.sub(pattern, '', address_clean, flags=re.IGNORECASE).strip()
                address_without_unit = re.sub(r'\s+', ' ', address_without_unit)
                return address_without_unit, unit
        
        return address, None
    
    def parse_street_address(self, address: str) -> Dict[str, Optional[str]]:
        """
        Parse street address into components
        
        Returns:
            Dictionary with street_number, street_name, street_type
        """
        if not address:
            return {'street_number': None, 'street_name': None, 'street_type': None}
        
        # Clean address and extract unit first
        address_clean, unit = self.extract_unit(self.clean_text(address))
        
        # Pattern for basic address parsing
        # Matches: [number] [direction] [name] [type]
        pattern = r'^(\d+[a-z]?)\s+(?:(n|s|e|w|ne|nw|se|sw|north|south|east|west|northeast|northwest|southeast|southwest)\s+)?(.+?)(?:\s+(st|street|ave|avenue|rd|road|dr|drive|ln|lane|ct|court|cir|circle|blvd|boulevard|pkwy|parkway|pl|place|way|ter|terrace|trl|trail|hwy|highway|fwy|freeway))?$'
        
        match = re.match(pattern, address_clean, re.IGNORECASE)
        
        if match:
            street_number = match.group(1)
            direction = match.group(2)
            street_name = match.group(3)
            street_type = match.group(4)
            
            # Standardize direction
            if direction:
                direction = self.direction_mappings.get(direction.lower(), direction)
                street_name = f"{direction} {street_name}"
            
            # Standardize street type
            if street_type:
                street_type = self.street_type_mappings.get(street_type.lower(), street_type)
            
            return {
                'street_number': street_number,
                'street_name': street_name.strip() if street_name else None,
                'street_type': street_type,
                'unit': unit
            }
        
        # Fallback: try to extract just the number
        number_match = re.match(r'^(\d+[a-z]?)\s+(.+)', address_clean)
        if number_match:
            return {
                'street_number': number_match.group(1),
                'street_name': number_match.group(2).strip(),
                'street_type': None,
                'unit': unit
            }
        
        return {
            'street_number': None,
            'street_name': address_clean if address_clean else None,
            'street_type': None,
            'unit': unit
        }
    
    def standardize_zip_code(self, zip_code: str) -> Optional[str]:
        """Standardize ZIP code format"""
        if not zip_code:
            return None
        
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', zip_code)
        
        # Return 5-digit ZIP if we have at least 5 digits
        if len(digits) >= 5:
            return digits[:5]
        
        return None
    
    def standardize_state(self, state: str) -> Optional[str]:
        """Standardize state to 2-letter code"""
        if not state:
            return None
        
        state_clean = state.strip().upper()
        
        # State name to abbreviation mapping (partial - can be extended)
        state_mappings = {
            'WISCONSIN': 'WI', 'ILLINOIS': 'IL', 'IOWA': 'IA',
            'MINNESOTA': 'MN', 'MICHIGAN': 'MI', 'INDIANA': 'IN',
            'OHIO': 'OH', 'MISSOURI': 'MO', 'KANSAS': 'KS',
            'NEBRASKA': 'NE', 'NORTH DAKOTA': 'ND', 'SOUTH DAKOTA': 'SD'
        }
        
        # If it's already a 2-letter code, return it
        if len(state_clean) == 2:
            return state_clean
        
        # Try to map full name to abbreviation
        return state_mappings.get(state_clean, state_clean[:2] if len(state_clean) >= 2 else None)
    
    def standardize_city(self, city: str) -> Optional[str]:
        """Standardize city name"""
        if not city:
            return None
        
        # Clean and title case
        city_clean = self.clean_text(city)
        
        # Title case each word
        return ' '.join(word.capitalize() for word in city_clean.split())
    
    def standardize_address(self, address: str = None, city: str = None, 
                          state: str = None, zip_code: str = None) -> StandardizedAddress:
        """
        Standardize complete address
        
        Args:
            address: Street address
            city: City name
            state: State name or code
            zip_code: ZIP code
            
        Returns:
            StandardizedAddress object
        """
        result = StandardizedAddress()
        result.original_address = f"{address or ''}, {city or ''}, {state or ''} {zip_code or ''}".strip(', ')
        
        # Parse street address
        if address:
            street_parts = self.parse_street_address(address)
            result.street_number = street_parts['street_number']
            result.street_name = street_parts['street_name']
            result.street_type = street_parts['street_type']
            result.unit = street_parts['unit']
        
        # Standardize other components
        result.city = self.standardize_city(city)
        result.state = self.standardize_state(state)
        result.zip_code = self.standardize_zip_code(zip_code)
        
        # Build full standardized address
        address_parts = []
        
        if result.street_number:
            street_part = result.street_number
            if result.street_name:
                street_part += f" {result.street_name}"
            if result.street_type:
                street_part += f" {result.street_type.title()}"
            if result.unit:
                street_part += f" #{result.unit}"
            address_parts.append(street_part)
        
        if result.city:
            address_parts.append(result.city)
        
        if result.state:
            state_zip = result.state
            if result.zip_code:
                state_zip += f" {result.zip_code}"
            address_parts.append(state_zip)
        
        result.full_address = ', '.join(address_parts) if address_parts else None
        
        # Calculate confidence score
        result.confidence = self._calculate_confidence(result)
        
        return result
    
    def _calculate_confidence(self, result: StandardizedAddress) -> float:
        """Calculate confidence score for standardized address"""
        score = 0.0
        
        # Street components
        if result.street_number:
            score += 25
        if result.street_name:
            score += 25
        if result.street_type:
            score += 15
        
        # Location components
        if result.city:
            score += 20
        if result.state:
            score += 10
        if result.zip_code and len(result.zip_code) == 5:
            score += 5
        
        return min(score / 100, 1.0)
    
    def standardize_business_addresses(self, businesses: list, 
                                     address_field: str = 'business_address',
                                     city_field: str = 'city',
                                     state_field: str = 'state',
                                     zip_field: str = 'zip_code') -> list:
        """
        Standardize addresses for a list of business records
        
        Args:
            businesses: List of business objects/dicts
            address_field: Field name for street address
            city_field: Field name for city
            state_field: Field name for state
            zip_field: Field name for ZIP code
            
        Returns:
            List of businesses with standardized addresses
        """
        standardized_businesses = []
        
        for business in businesses:
            try:
                # Extract address components (handle both dict and object access)
                if hasattr(business, address_field):
                    address = getattr(business, address_field, '')
                    city = getattr(business, city_field, '')
                    state = getattr(business, state_field, '')
                    zip_code = getattr(business, zip_field, '')
                else:
                    address = business.get(address_field, '')
                    city = business.get(city_field, '')
                    state = business.get(state_field, '')
                    zip_code = business.get(zip_field, '')
                
                # Standardize address
                std_address = self.standardize_address(address, city, state, zip_code)
                
                # Update business record
                if hasattr(business, address_field):
                    # Object with attributes
                    setattr(business, 'formatted_address', std_address.full_address)
                    if std_address.city:
                        setattr(business, city_field, std_address.city)
                    if std_address.state:
                        setattr(business, state_field, std_address.state)
                    if std_address.zip_code:
                        setattr(business, zip_field, std_address.zip_code)
                else:
                    # Dictionary
                    business['formatted_address'] = std_address.full_address
                    if std_address.city:
                        business[city_field] = std_address.city
                    if std_address.state:
                        business[state_field] = std_address.state
                    if std_address.zip_code:
                        business[zip_field] = std_address.zip_code
                
                standardized_businesses.append(business)
                
            except Exception as e:
                self.logger.warning(f"Error standardizing address for business: {e}")
                standardized_businesses.append(business)  # Keep original if standardization fails
        
        return standardized_businesses


def test_address_standardizer():
    """Test the address standardization functionality"""
    logging.basicConfig(level=logging.INFO)
    
    standardizer = AddressStandardizer()
    
    # Test addresses
    test_addresses = [
        {
            'address': '123 n water st apt 2b',
            'city': 'milwaukee',
            'state': 'wisconsin',
            'zip_code': '53202-1234'
        },
        {
            'address': '456 E WASHINGTON AVE',
            'city': 'MADISON',
            'state': 'WI',
            'zip_code': '53703'
        },
        {
            'address': '789 oak street',
            'city': 'green bay',
            'state': 'wi',
            'zip_code': '54301'
        },
        {
            'address': '1000 S 1st St Unit B',
            'city': 'La Crosse',
            'state': 'Wisconsin',
            'zip_code': '54601'
        }
    ]
    
    print("🏠 Testing Address Standardization:")
    print("=" * 60)
    
    for i, addr in enumerate(test_addresses, 1):
        print(f"\nTest {i}:")
        print(f"Original: {addr['address']}, {addr['city']}, {addr['state']} {addr['zip_code']}")
        
        result = standardizer.standardize_address(
            addr['address'], addr['city'], addr['state'], addr['zip_code']
        )
        
        print(f"Standardized: {result.full_address}")
        print(f"Components: {result.street_number} | {result.street_name} | {result.street_type}")
        if result.unit:
            print(f"Unit: {result.unit}")
        print(f"Confidence: {result.confidence:.2f}")


if __name__ == "__main__":
    test_address_standardizer()