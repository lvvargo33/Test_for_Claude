"""
Enhanced DFI Business Collector with Geocoding
==============================================

Extends the original DFI collector to include OpenStreetMap geocoding
and address standardization capabilities.
"""

import logging
from typing import List
from datetime import datetime

from dfi_collector import DFIBusinessCollector, DFIBusinessRecord
from geocoding_pipeline import BusinessGeocodingPipeline, GeocodingStats


class EnhancedDFICollector(DFIBusinessCollector):
    """DFI collector with integrated geocoding capabilities"""
    
    def __init__(self):
        """Initialize enhanced collector with geocoding pipeline"""
        super().__init__()
        self.geocoding_pipeline = BusinessGeocodingPipeline()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def collect_and_geocode_recent_registrations(self, days_back: int = 90, 
                                               geocode: bool = True) -> tuple[List[DFIBusinessRecord], GeocodingStats]:
        """
        Collect recent DFI registrations and geocode them
        
        Args:
            days_back: Number of days to look back for registrations
            geocode: Whether to perform geocoding (default: True)
            
        Returns:
            Tuple of (business_records, geocoding_stats)
        """
        self.logger.info(f"Collecting DFI registrations from last {days_back} days with geocoding: {geocode}")
        
        # Step 1: Collect raw business registrations
        businesses = self.collect_recent_registrations(days_back)
        
        if not businesses:
            self.logger.warning("No businesses collected from DFI")
            return businesses, GeocodingStats()
        
        self.logger.info(f"Collected {len(businesses)} raw business registrations")
        
        # Step 2: Geocode if requested
        if geocode:
            self.logger.info("Starting geocoding process...")
            businesses, stats = self.geocoding_pipeline.process_dfi_businesses(businesses)
            
            self.logger.info(f"Geocoding complete: {stats.successful_geocodes} successful, "
                           f"{stats.failed_geocodes} failed ({stats.success_rate:.1f}% success rate)")
            
            return businesses, stats
        else:
            # Return empty stats if no geocoding performed
            return businesses, GeocodingStats(total_records=len(businesses))
    
    def geocode_existing_businesses(self, businesses: List[DFIBusinessRecord]) -> tuple[List[DFIBusinessRecord], GeocodingStats]:
        """
        Geocode existing business records
        
        Args:
            businesses: List of DFI business records to geocode
            
        Returns:
            Tuple of (geocoded_businesses, geocoding_stats)
        """
        self.logger.info(f"Geocoding {len(businesses)} existing business records")
        
        return self.geocoding_pipeline.process_dfi_businesses(businesses)
    
    def save_geocoded_businesses_to_bigquery(self, businesses: List[DFIBusinessRecord], 
                                           table_name: str = "dfi_business_registrations_geocoded"):
        """
        Save geocoded business records to BigQuery
        
        Args:
            businesses: List of geocoded business records
            table_name: BigQuery table name
        """
        try:
            from google.cloud import bigquery
            from google.cloud.bigquery import LoadJobConfig, WriteDisposition
            
            client = bigquery.Client()
            
            # Prepare data for BigQuery
            rows_to_insert = []
            
            for business in businesses:
                row = {
                    'business_id': business.business_id,
                    'business_name': business.business_name,
                    'entity_type': business.entity_type,
                    'registration_date': business.registration_date,
                    'status': business.status,
                    'business_address': business.business_address,
                    'city': business.city,
                    'state': business.state,
                    'zip_code': business.zip_code,
                    'county': business.county,
                    'business_type': business.business_type,
                    'naics_code': business.naics_code,
                    'agent_name': business.agent_name,
                    'source': business.source,
                    
                    # Geocoding fields
                    'latitude': business.latitude,
                    'longitude': business.longitude,
                    'geocoding_confidence': business.geocoding_confidence,
                    'formatted_address': business.formatted_address,
                    'geocoding_date': business.geocoding_date,
                    'geocoding_source': business.geocoding_source,
                    
                    # Metadata
                    'data_extraction_date': datetime.now().isoformat()
                }
                
                rows_to_insert.append(row)
            
            # Define table reference
            table_ref = client.dataset('raw_business_data').table(table_name)
            
            # Configure load job
            job_config = LoadJobConfig(
                write_disposition=WriteDisposition.WRITE_APPEND,
                autodetect=True
            )
            
            # Load data
            job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
            job.result()  # Wait for job to complete
            
            self.logger.info(f"Successfully saved {len(rows_to_insert)} geocoded businesses to BigQuery table: {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error saving to BigQuery: {e}")
            raise
    
    def generate_geocoding_report(self, businesses: List[DFIBusinessRecord], 
                                stats: GeocodingStats) -> str:
        """
        Generate a summary report of geocoding results
        
        Args:
            businesses: List of processed businesses
            stats: Geocoding statistics
            
        Returns:
            Report string
        """
        report = []
        report.append("📍 GEOCODING REPORT")
        report.append("=" * 50)
        report.append(f"Total Businesses Processed: {stats.total_records}")
        report.append(f"Successfully Geocoded: {stats.successful_geocodes}")
        report.append(f"Failed Geocoding: {stats.failed_geocodes}")
        report.append(f"Success Rate: {stats.success_rate:.1f}%")
        report.append(f"Average Confidence: {stats.avg_confidence:.2f}")
        report.append(f"Addresses Standardized: {stats.standardized_addresses}")
        report.append(f"Processing Time: {stats.processing_time_seconds:.1f} seconds")
        
        # Business type breakdown
        type_counts = {}
        geocoded_type_counts = {}
        
        for business in businesses:
            btype = business.business_type or 'unknown'
            type_counts[btype] = type_counts.get(btype, 0) + 1
            
            if business.latitude and business.longitude:
                geocoded_type_counts[btype] = geocoded_type_counts.get(btype, 0) + 1
        
        report.append("\n📊 BUSINESS TYPE BREAKDOWN:")
        for btype, count in sorted(type_counts.items()):
            geocoded = geocoded_type_counts.get(btype, 0)
            rate = (geocoded / count) * 100 if count > 0 else 0
            report.append(f"   {btype}: {geocoded}/{count} ({rate:.1f}%)")
        
        # Location breakdown
        city_counts = {}
        geocoded_city_counts = {}
        
        for business in businesses:
            city = business.city or 'unknown'
            city_counts[city] = city_counts.get(city, 0) + 1
            
            if business.latitude and business.longitude:
                geocoded_city_counts[city] = geocoded_city_counts.get(city, 0) + 1
        
        report.append("\n🏙️ CITY BREAKDOWN (Top 10):")
        sorted_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for city, count in sorted_cities:
            geocoded = geocoded_city_counts.get(city, 0)
            rate = (geocoded / count) * 100 if count > 0 else 0
            report.append(f"   {city}: {geocoded}/{count} ({rate:.1f}%)")
        
        return "\n".join(report)


def demo_enhanced_collector():
    """Demonstrate the enhanced DFI collector with geocoding"""
    logging.basicConfig(level=logging.INFO)
    
    collector = EnhancedDFICollector()
    
    print("🚀 Enhanced DFI Collector Demo")
    print("=" * 50)
    
    # Note: This would normally collect real data from DFI
    # For demo purposes, we'll create sample data
    print("\n📋 Creating sample business data...")
    
    sample_businesses = [
        DFIBusinessRecord(
            business_name="Demo Restaurant LLC",
            entity_type="Limited Liability Company",
            registration_date="03/15/2024",
            status="Active",
            business_id="DEMO001",
            business_address="100 State Street",
            city="Madison",
            state="WI",
            zip_code="53703",
            business_type="restaurant"
        ),
        DFIBusinessRecord(
            business_name="Demo Salon Inc",
            entity_type="Corporation", 
            registration_date="02/28/2024",
            status="Active",
            business_id="DEMO002",
            business_address="200 E Washington Ave",
            city="Madison",
            state="WI",
            zip_code="53703",
            business_type="personal_services"
        )
    ]
    
    print(f"Created {len(sample_businesses)} sample businesses")
    
    # Geocode the sample businesses
    print("\n🗺️ Geocoding businesses...")
    geocoded_businesses, stats = collector.geocode_existing_businesses(sample_businesses)
    
    # Generate and display report
    report = collector.generate_geocoding_report(geocoded_businesses, stats)
    print(f"\n{report}")
    
    print("\n✅ Demo complete!")


if __name__ == "__main__":
    demo_enhanced_collector()