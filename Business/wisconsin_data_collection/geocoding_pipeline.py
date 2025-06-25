"""
Geocoding Pipeline for Business Data
===================================

Integrates address standardization and OpenStreetMap geocoding
for Wisconsin business registration data.
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass

from geocoding import OpenStreetMapGeocoder, GeocodingResult
from address_standardizer import AddressStandardizer
from dfi_collector import DFIBusinessRecord


@dataclass
class GeocodingStats:
    """Statistics from geocoding operation"""
    total_records: int = 0
    successful_geocodes: int = 0
    failed_geocodes: int = 0
    standardized_addresses: int = 0
    avg_confidence: float = 0.0
    processing_time_seconds: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.successful_geocodes / self.total_records) * 100


class BusinessGeocodingPipeline:
    """Complete geocoding pipeline for business data"""
    
    def __init__(self, user_agent: str = "Wisconsin-Business-Location-Optimizer/1.0"):
        """
        Initialize geocoding pipeline
        
        Args:
            user_agent: User agent for geocoding requests
        """
        self.geocoder = OpenStreetMapGeocoder(user_agent)
        self.standardizer = AddressStandardizer()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def process_dfi_businesses(self, businesses: List[DFIBusinessRecord]) -> tuple[List[DFIBusinessRecord], GeocodingStats]:
        """
        Process DFI business records through complete geocoding pipeline
        
        Args:
            businesses: List of DFI business records
            
        Returns:
            Tuple of (processed_businesses, geocoding_stats)
        """
        start_time = datetime.now()
        stats = GeocodingStats(total_records=len(businesses))
        
        self.logger.info(f"Starting geocoding pipeline for {len(businesses)} DFI businesses")
        
        processed_businesses = []
        confidence_scores = []
        
        for i, business in enumerate(businesses, 1):
            try:
                # Step 1: Address standardization
                std_address = self.standardizer.standardize_address(
                    business.business_address,
                    business.city,
                    business.state,
                    business.zip_code
                )
                
                if std_address.full_address:
                    business.formatted_address = std_address.full_address
                    stats.standardized_addresses += 1
                
                # Update business with standardized components
                if std_address.city:
                    business.city = std_address.city
                if std_address.state:
                    business.state = std_address.state
                if std_address.zip_code:
                    business.zip_code = std_address.zip_code
                
                # Step 2: Geocoding with standardized address
                if business.city:  # Must have at least city to geocode
                    geocoding_result = self.geocoder.geocode_address(
                        business.business_address or '',
                        business.city,
                        business.state,
                        business.zip_code
                    )
                    
                    if geocoding_result.success:
                        # Update business with geocoding results
                        business.latitude = geocoding_result.latitude
                        business.longitude = geocoding_result.longitude
                        business.geocoding_confidence = geocoding_result.confidence
                        business.geocoding_date = datetime.now().isoformat()
                        business.geocoding_source = 'OpenStreetMap'
                        
                        # Use geocoded formatted address if available
                        if geocoding_result.formatted_address:
                            business.formatted_address = geocoding_result.formatted_address
                        
                        stats.successful_geocodes += 1
                        confidence_scores.append(geocoding_result.confidence)
                        
                        self.logger.debug(f"Geocoded: {business.business_name} → ({business.latitude}, {business.longitude})")
                    else:
                        stats.failed_geocodes += 1
                        self.logger.debug(f"Geocoding failed for: {business.business_name} - {geocoding_result.error}")
                else:
                    stats.failed_geocodes += 1
                    self.logger.debug(f"Skipping geocoding for {business.business_name}: no city information")
                
                processed_businesses.append(business)
                
                # Progress logging
                if i % 10 == 0 or i == len(businesses):
                    self.logger.info(f"Processed {i}/{len(businesses)} businesses "
                                   f"({stats.successful_geocodes} geocoded)")
                
            except Exception as e:
                self.logger.error(f"Error processing business {business.business_name}: {e}")
                stats.failed_geocodes += 1
                processed_businesses.append(business)  # Keep original record
        
        # Calculate final statistics
        end_time = datetime.now()
        stats.processing_time_seconds = (end_time - start_time).total_seconds()
        
        if confidence_scores:
            stats.avg_confidence = sum(confidence_scores) / len(confidence_scores)
        
        self.logger.info(f"Geocoding pipeline complete: {stats.successful_geocodes}/{stats.total_records} "
                        f"successful ({stats.success_rate:.1f}%) in {stats.processing_time_seconds:.1f}s")
        
        return processed_businesses, stats
    
    def process_business_dict_list(self, businesses: List[Dict]) -> tuple[List[Dict], GeocodingStats]:
        """
        Process list of business dictionaries through geocoding pipeline
        
        Args:
            businesses: List of business dictionaries
            
        Returns:
            Tuple of (processed_businesses, geocoding_stats)
        """
        start_time = datetime.now()
        stats = GeocodingStats(total_records=len(businesses))
        
        self.logger.info(f"Starting geocoding pipeline for {len(businesses)} business records")
        
        processed_businesses = []
        confidence_scores = []
        
        for i, business in enumerate(businesses, 1):
            try:
                # Extract address fields (with fallback names)
                address = business.get('address', business.get('business_address', ''))
                city = business.get('city', '')
                state = business.get('state', 'WI')
                zip_code = business.get('zip_code', business.get('zip', ''))
                
                # Step 1: Address standardization
                std_address = self.standardizer.standardize_address(address, city, state, zip_code)
                
                if std_address.full_address:
                    business['formatted_address'] = std_address.full_address
                    stats.standardized_addresses += 1
                
                # Update business with standardized components
                if std_address.city:
                    business['city'] = std_address.city
                if std_address.state:
                    business['state'] = std_address.state
                if std_address.zip_code:
                    business['zip_code'] = std_address.zip_code
                
                # Step 2: Geocoding
                if business.get('city'):
                    geocoding_result = self.geocoder.geocode_address(
                        address,
                        business['city'],
                        business['state'],
                        business.get('zip_code')
                    )
                    
                    if geocoding_result.success:
                        # Update business with geocoding results
                        business['latitude'] = geocoding_result.latitude
                        business['longitude'] = geocoding_result.longitude
                        business['geocoding_confidence'] = geocoding_result.confidence
                        business['geocoding_date'] = datetime.now().isoformat()
                        business['geocoding_source'] = 'OpenStreetMap'
                        
                        if geocoding_result.formatted_address:
                            business['formatted_address'] = geocoding_result.formatted_address
                        
                        stats.successful_geocodes += 1
                        confidence_scores.append(geocoding_result.confidence)
                    else:
                        stats.failed_geocodes += 1
                else:
                    stats.failed_geocodes += 1
                
                processed_businesses.append(business)
                
                # Progress logging
                if i % 10 == 0 or i == len(businesses):
                    self.logger.info(f"Processed {i}/{len(businesses)} businesses "
                                   f"({stats.successful_geocodes} geocoded)")
                
            except Exception as e:
                self.logger.error(f"Error processing business record {i}: {e}")
                stats.failed_geocodes += 1
                processed_businesses.append(business)
        
        # Calculate final statistics
        end_time = datetime.now()
        stats.processing_time_seconds = (end_time - start_time).total_seconds()
        
        if confidence_scores:
            stats.avg_confidence = sum(confidence_scores) / len(confidence_scores)
        
        self.logger.info(f"Geocoding pipeline complete: {stats.successful_geocodes}/{stats.total_records} "
                        f"successful ({stats.success_rate:.1f}%) in {stats.processing_time_seconds:.1f}s")
        
        return processed_businesses, stats
    
    def create_sample_dfi_businesses(self) -> List[DFIBusinessRecord]:
        """Create sample DFI business records for testing"""
        sample_businesses = [
            DFIBusinessRecord(
                business_name="Madison Pizza Company LLC",
                entity_type="Limited Liability Company",
                registration_date="03/15/2024",
                status="Active",
                business_id="M123456789",
                business_address="123 State Street",
                city="Madison",
                state="WI",
                zip_code="53703",
                business_type="restaurant"
            ),
            DFIBusinessRecord(
                business_name="Milwaukee Hair Studio Inc",
                entity_type="Corporation",
                registration_date="02/28/2024",
                status="Active",
                business_id="M987654321",
                business_address="456 N Water St",
                city="Milwaukee", 
                state="WI",
                zip_code="53202",
                business_type="personal_services"
            ),
            DFIBusinessRecord(
                business_name="Green Bay Auto Repair LLC",
                entity_type="Limited Liability Company", 
                registration_date="01/10/2024",
                status="Active",
                business_id="G555666777",
                business_address="789 Lombardi Ave",
                city="Green Bay",
                state="WI",
                zip_code="54304",
                business_type="automotive"
            ),
            DFIBusinessRecord(
                business_name="La Crosse Fitness Center",
                entity_type="Limited Liability Company",
                registration_date="04/01/2024", 
                status="Active",
                business_id="L111222333",
                business_address="321 Main Street",
                city="La Crosse",
                state="WI",
                zip_code="54601",
                business_type="fitness"
            ),
            DFIBusinessRecord(
                business_name="Test Business No Address",
                entity_type="Corporation",
                registration_date="03/01/2024",
                status="Active", 
                business_id="T999888777",
                business_address=None,
                city="Madison",
                state="WI",
                zip_code=None,
                business_type="other"
            )
        ]
        
        return sample_businesses


def test_geocoding_pipeline():
    """Test the complete geocoding pipeline"""
    logging.basicConfig(level=logging.INFO)
    
    pipeline = BusinessGeocodingPipeline()
    
    print("🗺️ Testing Complete Geocoding Pipeline:")
    print("=" * 50)
    
    # Create sample businesses
    sample_businesses = pipeline.create_sample_dfi_businesses()
    
    print(f"\n📋 Processing {len(sample_businesses)} sample businesses:")
    for business in sample_businesses:
        print(f"   • {business.business_name} - {business.business_address}, {business.city}")
    
    # Process through pipeline
    processed_businesses, stats = pipeline.process_dfi_businesses(sample_businesses)
    
    print(f"\n📊 Geocoding Results:")
    print(f"   Total Records: {stats.total_records}")
    print(f"   Successful: {stats.successful_geocodes}")
    print(f"   Failed: {stats.failed_geocodes}")
    print(f"   Success Rate: {stats.success_rate:.1f}%")
    print(f"   Avg Confidence: {stats.avg_confidence:.2f}")
    print(f"   Processing Time: {stats.processing_time_seconds:.1f}s")
    
    print(f"\n🎯 Geocoded Businesses:")
    for business in processed_businesses:
        if business.latitude and business.longitude:
            print(f"   ✅ {business.business_name}")
            print(f"      Location: ({business.latitude:.6f}, {business.longitude:.6f})")
            print(f"      Confidence: {business.geocoding_confidence:.2f}")
            print(f"      Address: {business.formatted_address}")
        else:
            print(f"   ❌ {business.business_name} - No coordinates")


if __name__ == "__main__":
    test_geocoding_pipeline()