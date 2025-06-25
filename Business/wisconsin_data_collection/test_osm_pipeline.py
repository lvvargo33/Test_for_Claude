"""
Test OSM Collection Pipeline (Without BigQuery)
==============================================

Test version of the OSM pipeline that works without BigQuery authentication.
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import asdict

from osm_data_collector import OSMDataCollector, OSMBusinessData
from models import OSMBusinessEntity, OSMDataSummary, BusinessType, DataSource


class TestOSMPipeline:
    """Test OSM collection pipeline without BigQuery dependency"""
    
    def __init__(self):
        """Initialize test pipeline"""
        self.collector = OSMDataCollector()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def convert_osm_data_to_entities(self, osm_businesses: List[OSMBusinessData]) -> List[OSMBusinessEntity]:
        """Convert OSM business data to Pydantic entities"""
        entities = []
        
        for business in osm_businesses:
            try:
                # Convert dataclass to dict
                business_dict = asdict(business)
                
                # Handle business type conversion
                business_type_str = business_dict.get('business_type', 'other')
                try:
                    business_dict['business_type'] = BusinessType(business_type_str)
                except ValueError:
                    business_dict['business_type'] = BusinessType.OTHER
                
                # Ensure required fields have defaults
                business_dict['franchise_indicator'] = business_dict.get('franchise_indicator', False)
                
                # Parse timestamp if it's a string
                if isinstance(business_dict.get('osm_timestamp'), str):
                    try:
                        business_dict['osm_timestamp'] = datetime.fromisoformat(
                            business_dict['osm_timestamp'].replace('Z', '+00:00')
                        )
                    except:
                        business_dict['osm_timestamp'] = None
                
                # Create Pydantic entity
                entity = OSMBusinessEntity(**business_dict)
                
                # Calculate data quality score
                entity.calculate_data_quality_score()
                
                entities.append(entity)
                
            except Exception as e:
                self.logger.warning(f"Error converting OSM business {business.osm_id}: {e}")
                continue
        
        self.logger.info(f"Converted {len(entities)} OSM businesses to entities")
        return entities
    
    def collect_and_analyze_wisconsin_data(self, counties: List[str] = None) -> Dict:
        """
        Collect and analyze OSM data for Wisconsin
        
        Args:
            counties: List of counties to collect (None for all Wisconsin)
            
        Returns:
            Analysis results dictionary
        """
        start_time = datetime.now()
        area_name = f"{', '.join(counties)} Counties" if counties else "Wisconsin"
        
        self.logger.info(f"Starting OSM data collection for {area_name}")
        
        # Collect OSM data
        osm_businesses = self.collector.collect_wisconsin_businesses(counties)
        
        if not osm_businesses:
            self.logger.warning("No businesses collected")
            return {"success": False, "message": "No businesses collected"}
        
        # Convert to entities
        entities = self.convert_osm_data_to_entities(osm_businesses)
        
        # Calculate analysis
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        analysis = self._analyze_entities(entities, area_name, processing_time)
        
        # Save to JSON files
        self._save_results_to_json(osm_businesses, entities, analysis, area_name, start_time)
        
        return analysis
    
    def _analyze_entities(self, entities: List[OSMBusinessEntity], 
                         area_name: str, processing_time: float) -> Dict:
        """Analyze collected entities"""
        if not entities:
            return {"success": False, "message": "No valid entities"}
        
        # Count business types
        business_type_counts = {}
        franchise_count = 0
        cities = set()
        contact_count = 0
        address_count = 0
        quality_scores = []
        
        # Brand analysis
        brands = {}
        amenity_counts = {}
        shop_counts = {}
        
        for entity in entities:
            # Business type counts
            btype = entity.business_type.value
            business_type_counts[btype] = business_type_counts.get(btype, 0) + 1
            
            # Franchise count
            if entity.franchise_indicator:
                franchise_count += 1
                
                # Track franchise brands
                if entity.brand:
                    brands[entity.brand] = brands.get(entity.brand, 0) + 1
            
            # Cities
            if entity.address_city:
                cities.add(entity.address_city)
            
            # Contact info
            if entity.phone or entity.website or entity.email:
                contact_count += 1
            
            # Address info
            if entity.address_street and entity.address_city:
                address_count += 1
            
            # Quality scores
            if entity.data_quality_score:
                quality_scores.append(entity.data_quality_score)
            
            # OSM tag analysis
            if entity.amenity:
                amenity_counts[entity.amenity] = amenity_counts.get(entity.amenity, 0) + 1
            if entity.shop:
                shop_counts[entity.shop] = shop_counts.get(entity.shop, 0) + 1
        
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        return {
            "success": True,
            "collection_info": {
                "area_name": area_name,
                "collection_date": datetime.now().isoformat(),
                "processing_time_seconds": processing_time,
                "total_businesses": len(entities)
            },
            "business_analysis": {
                "business_type_counts": business_type_counts,
                "franchise_count": franchise_count,
                "franchise_percentage": (franchise_count / len(entities)) * 100,
                "cities_covered": len(cities),
                "top_cities": list(cities)[:10] if cities else []
            },
            "data_quality": {
                "avg_quality_score": avg_quality,
                "businesses_with_contact": contact_count,
                "businesses_with_address": address_count,
                "contact_percentage": (contact_count / len(entities)) * 100,
                "address_percentage": (address_count / len(entities)) * 100
            },
            "franchise_analysis": {
                "top_brands": dict(sorted(brands.items(), key=lambda x: x[1], reverse=True)[:10])
            },
            "osm_tag_analysis": {
                "top_amenities": dict(sorted(amenity_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
                "top_shops": dict(sorted(shop_counts.items(), key=lambda x: x[1], reverse=True)[:10])
            }
        }
    
    def _save_results_to_json(self, osm_businesses: List[OSMBusinessData], 
                             entities: List[OSMBusinessEntity], 
                             analysis: Dict, area_name: str, timestamp: datetime):
        """Save results to JSON files"""
        try:
            # Clean area name for filename
            clean_area = area_name.replace(' ', '_').replace(',', '').replace('Counties', 'Co')
            timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
            
            # Save raw OSM data
            raw_filename = f"osm_raw_{clean_area}_{timestamp_str}.json"
            self.collector.save_to_json(osm_businesses, raw_filename)
            
            # Save processed entities
            entities_filename = f"osm_entities_{clean_area}_{timestamp_str}.json"
            entities_data = [entity.dict() for entity in entities]
            # Convert datetime objects to strings for JSON serialization
            for entity_data in entities_data:
                for key, value in entity_data.items():
                    if isinstance(value, datetime):
                        entity_data[key] = value.isoformat()
                    elif hasattr(value, 'value'):  # Handle enums
                        entity_data[key] = value.value
            
            with open(entities_filename, 'w', encoding='utf-8') as f:
                json.dump(entities_data, f, indent=2, ensure_ascii=False)
            
            # Save analysis
            analysis_filename = f"osm_analysis_{clean_area}_{timestamp_str}.json"
            with open(analysis_filename, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved results to: {raw_filename}, {entities_filename}, {analysis_filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
    
    def generate_report(self, analysis: Dict) -> str:
        """Generate detailed analysis report"""
        if not analysis.get("success"):
            return f"❌ Collection failed: {analysis.get('message', 'Unknown error')}"
        
        info = analysis["collection_info"]
        business = analysis["business_analysis"]
        quality = analysis["data_quality"]
        franchises = analysis["franchise_analysis"]
        tags = analysis["osm_tag_analysis"]
        
        report = []
        report.append("🗺️ OPENSTREETMAP DATA ANALYSIS REPORT")
        report.append("=" * 60)
        report.append(f"Area: {info['area_name']}")
        report.append(f"Collection Date: {info['collection_date']}")
        report.append(f"Processing Time: {info['processing_time_seconds']:.1f} seconds")
        report.append(f"Total Businesses: {info['total_businesses']:,}")
        
        report.append(f"\n🏪 BUSINESS ANALYSIS:")
        report.append(f"   Cities Covered: {business['cities_covered']}")
        report.append(f"   Franchise Businesses: {business['franchise_count']} ({business['franchise_percentage']:.1f}%)")
        
        report.append(f"\n📊 BUSINESS TYPE BREAKDOWN:")
        for btype, count in sorted(business['business_type_counts'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / info['total_businesses']) * 100
            report.append(f"   {btype}: {count:,} ({percentage:.1f}%)")
        
        report.append(f"\n📈 DATA QUALITY:")
        report.append(f"   Average Quality Score: {quality['avg_quality_score']:.1f}/100")
        report.append(f"   With Contact Info: {quality['businesses_with_contact']} ({quality['contact_percentage']:.1f}%)")
        report.append(f"   With Full Address: {quality['businesses_with_address']} ({quality['address_percentage']:.1f}%)")
        
        if franchises['top_brands']:
            report.append(f"\n🏢 TOP FRANCHISE BRANDS:")
            for brand, count in list(franchises['top_brands'].items())[:5]:
                report.append(f"   {brand}: {count} locations")
        
        if tags['top_amenities']:
            report.append(f"\n🏷️ TOP AMENITY TYPES:")
            for amenity, count in list(tags['top_amenities'].items())[:5]:
                report.append(f"   {amenity}: {count} businesses")
        
        if tags['top_shops']:
            report.append(f"\n🛍️ TOP SHOP TYPES:")
            for shop, count in list(tags['top_shops'].items())[:5]:
                report.append(f"   {shop}: {count} businesses")
        
        if business['top_cities']:
            report.append(f"\n🏙️ CITIES COVERED:")
            cities_str = ", ".join(sorted(business['top_cities'][:10]))
            report.append(f"   {cities_str}")
        
        return "\n".join(report)


def main():
    """Main function to test OSM collection pipeline"""
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 OSM Data Collection Pipeline Test")
    print("=" * 45)
    
    pipeline = TestOSMPipeline()
    
    # Test with a smaller area first (Brown County - Green Bay)
    print("\n📍 Collecting OSM data for Brown County (Green Bay area)...")
    
    analysis = pipeline.collect_and_analyze_wisconsin_data(['Brown'])
    
    if analysis.get("success"):
        # Generate and display report
        report = pipeline.generate_report(analysis)
        print(f"\n{report}")
        print("\n✅ Pipeline test complete!")
    else:
        print(f"❌ Pipeline failed: {analysis.get('message')}")


if __name__ == "__main__":
    main()