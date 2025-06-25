"""
OpenStreetMap Data Collection Pipeline
====================================

Complete pipeline for collecting, processing, and storing OSM business data.
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import asdict

from osm_data_collector import OSMDataCollector, OSMBusinessData
from models import OSMBusinessEntity, OSMDataSummary, BusinessType, DataSource
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition


class OSMCollectionPipeline:
    """Complete OSM data collection and storage pipeline"""
    
    def __init__(self, project_id: str = "location-optimizer-1"):
        """
        Initialize OSM collection pipeline
        
        Args:
            project_id: Google Cloud project ID
        """
        self.collector = OSMDataCollector()
        self.bigquery_client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = "raw_business_data"
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def convert_osm_data_to_entities(self, osm_businesses: List[OSMBusinessData]) -> List[OSMBusinessEntity]:
        """
        Convert OSM business data to Pydantic entities
        
        Args:
            osm_businesses: List of raw OSM business data
            
        Returns:
            List of validated OSM business entities
        """
        entities = []
        
        for business in osm_businesses:
            try:
                # Convert dataclass to dict
                business_dict = asdict(business)
                
                # Handle business type conversion
                if business_dict.get('business_type'):
                    business_type_str = business_dict['business_type']
                    # Try to map to BusinessType enum
                    try:
                        business_dict['business_type'] = BusinessType(business_type_str)
                    except ValueError:
                        business_dict['business_type'] = BusinessType.OTHER
                else:
                    business_dict['business_type'] = BusinessType.OTHER
                
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
    
    def save_to_bigquery(self, entities: List[OSMBusinessEntity], 
                        summary: OSMDataSummary) -> bool:
        """
        Save OSM entities and summary to BigQuery
        
        Args:
            entities: List of OSM business entities
            summary: Collection summary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Save business entities
            if entities:
                self._save_entities_to_bigquery(entities)
            
            # Save collection summary
            self._save_summary_to_bigquery(summary)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to BigQuery: {e}")
            return False
    
    def _save_entities_to_bigquery(self, entities: List[OSMBusinessEntity]):
        """Save OSM business entities to BigQuery"""
        table_id = f"{self.project_id}.{self.dataset_id}.osm_businesses"
        
        # Convert entities to dictionaries for BigQuery
        rows = []
        for entity in entities:
            row = entity.dict()
            
            # Convert datetime objects to strings
            if row.get('osm_timestamp'):
                row['osm_timestamp'] = row['osm_timestamp'].isoformat()
            if row.get('data_collection_date'):
                row['data_collection_date'] = row['data_collection_date'].isoformat()
            
            # Convert enum to string
            if isinstance(row.get('business_type'), BusinessType):
                row['business_type'] = row['business_type'].value
            if isinstance(row.get('data_source'), DataSource):
                row['data_source'] = row['data_source'].value
            
            rows.append(row)
        
        # Configure load job
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND,
            autodetect=False,  # Use predefined schema
        )
        
        # Load data
        job = self.bigquery_client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        self.logger.info(f"Saved {len(entities)} OSM businesses to BigQuery")
    
    def _save_summary_to_bigquery(self, summary: OSMDataSummary):
        """Save collection summary to BigQuery"""
        table_id = f"{self.project_id}.{self.dataset_id}.osm_collection_summary"
        
        # Convert summary to dictionary
        row = summary.dict()
        
        # Convert datetime to string
        if row.get('collection_date'):
            row['collection_date'] = row['collection_date'].isoformat()
        
        # Convert business type counts dict to JSON string
        if row.get('business_type_counts'):
            row['business_type_counts'] = json.dumps(row['business_type_counts'])
        
        # Configure load job
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )
        
        # Load data
        job = self.bigquery_client.load_table_from_json([row], table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        self.logger.info("Saved OSM collection summary to BigQuery")
    
    def collect_and_store_wisconsin_data(self, counties: List[str] = None, 
                                       save_to_bigquery: bool = True,
                                       save_to_json: bool = True) -> OSMDataSummary:
        """
        Complete pipeline: collect OSM data for Wisconsin and store it
        
        Args:
            counties: List of counties to collect (None for all Wisconsin)
            save_to_bigquery: Whether to save to BigQuery
            save_to_json: Whether to save to JSON file
            
        Returns:
            Collection summary
        """
        start_time = datetime.now()
        
        area_name = f"{', '.join(counties)} Counties" if counties else "Wisconsin"
        bbox = self.collector.get_wisconsin_bbox()
        
        self.logger.info(f"Starting OSM data collection for {area_name}")
        
        # Collect OSM data
        osm_businesses = self.collector.collect_wisconsin_businesses(counties)
        
        # Convert to entities
        entities = self.convert_osm_data_to_entities(osm_businesses)
        
        # Calculate summary statistics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        # Count business types
        business_type_counts = {}
        franchises_count = 0
        cities = set()
        contact_count = 0
        address_count = 0
        quality_scores = []
        
        for entity in entities:
            # Business type counts
            btype = entity.business_type.value
            business_type_counts[btype] = business_type_counts.get(btype, 0) + 1
            
            # Franchise count
            if entity.franchise_indicator:
                franchises_count += 1
            
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
        
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Create summary
        summary = OSMDataSummary(
            collection_date=start_time,
            area_name=area_name,
            bbox=bbox,
            total_elements=len(osm_businesses),  # Raw OSM elements
            businesses_collected=len(entities),
            franchises_identified=franchises_count,
            business_type_counts=business_type_counts,
            avg_data_quality_score=avg_quality,
            businesses_with_contact=contact_count,
            businesses_with_address=address_count,
            success=len(entities) > 0,
            processing_time_seconds=processing_time,
            api_requests_made=1,  # Simplified for now
            cities_covered=len(cities)
        )
        
        # Save data
        if save_to_bigquery and entities:
            try:
                self.save_to_bigquery(entities, summary)
                self.logger.info("Data saved to BigQuery successfully")
            except Exception as e:
                self.logger.error(f"Failed to save to BigQuery: {e}")
        
        if save_to_json and entities:
            try:
                # Save entities
                filename = f"osm_businesses_{area_name.replace(' ', '_').replace(',', '')}_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
                self.collector.save_to_json(osm_businesses, filename)
                
                # Save summary
                summary_filename = f"osm_summary_{area_name.replace(' ', '_').replace(',', '')}_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
                with open(summary_filename, 'w') as f:
                    summary_dict = summary.dict()
                    # Convert datetime to string for JSON serialization
                    summary_dict['collection_date'] = summary_dict['collection_date'].isoformat()
                    json.dump(summary_dict, f, indent=2)
                
                self.logger.info(f"Data saved to JSON files: {filename}, {summary_filename}")
            except Exception as e:
                self.logger.error(f"Failed to save to JSON: {e}")
        
        self.logger.info(f"OSM collection pipeline complete: {len(entities)} businesses collected")
        
        return summary
    
    def generate_pipeline_report(self, summary: OSMDataSummary) -> str:
        """Generate detailed pipeline report"""
        report = []
        report.append("🗺️ OSM DATA COLLECTION PIPELINE REPORT")
        report.append("=" * 60)
        report.append(f"Collection Date: {summary.collection_date}")
        report.append(f"Area: {summary.area_name}")
        report.append(f"Status: {'✅ SUCCESS' if summary.success else '❌ FAILED'}")
        report.append(f"Processing Time: {summary.processing_time_seconds:.1f} seconds")
        
        report.append(f"\n📊 COLLECTION RESULTS:")
        report.append(f"   Total OSM Elements: {summary.total_elements}")
        report.append(f"   Valid Businesses: {summary.businesses_collected}")
        report.append(f"   Franchise Businesses: {summary.franchises_identified}")
        report.append(f"   Cities Covered: {summary.cities_covered}")
        
        report.append(f"\n📈 DATA QUALITY:")
        report.append(f"   Average Quality Score: {summary.avg_data_quality_score:.1f}/100")
        report.append(f"   Businesses with Contact Info: {summary.businesses_with_contact}")
        report.append(f"   Businesses with Full Address: {summary.businesses_with_address}")
        
        if summary.business_type_counts:
            report.append(f"\n🏪 BUSINESS TYPE BREAKDOWN:")
            sorted_types = sorted(summary.business_type_counts.items(), key=lambda x: x[1], reverse=True)
            for btype, count in sorted_types:
                percentage = (count / summary.businesses_collected) * 100 if summary.businesses_collected > 0 else 0
                report.append(f"   {btype}: {count} ({percentage:.1f}%)")
        
        return "\n".join(report)


def main():
    """Main function to run OSM collection pipeline"""
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 OSM Data Collection Pipeline")
    print("=" * 40)
    
    pipeline = OSMCollectionPipeline()
    
    # Test with Dane County (Madison area) first
    print("\n📍 Collecting OSM data for Dane County...")
    
    summary = pipeline.collect_and_store_wisconsin_data(
        counties=['Dane'],
        save_to_bigquery=False,  # Set to True when BigQuery is set up
        save_to_json=True
    )
    
    # Generate and display report
    report = pipeline.generate_pipeline_report(summary)
    print(f"\n{report}")
    
    print("\n✅ Pipeline complete!")


if __name__ == "__main__":
    main()