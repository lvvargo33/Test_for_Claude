"""
BigQuery Table Setup for OpenStreetMap Data
==========================================

Creates and configures BigQuery tables for storing OSM business data.
"""

import logging
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Table, TimePartitioning
from typing import List, Dict


class OSMBigQuerySetup:
    """Setup BigQuery tables for OSM data storage"""
    
    def __init__(self, project_id: str = "location-optimizer-1"):
        """
        Initialize BigQuery setup
        
        Args:
            project_id: Google Cloud project ID
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = "raw_business_data"
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_osm_businesses_table(self) -> Table:
        """
        Create table for OSM business data
        
        Returns:
            Created BigQuery table
        """
        table_id = f"{self.project_id}.{self.dataset_id}.osm_businesses"
        
        # Define schema
        schema = [
            # OSM identifiers
            SchemaField("osm_id", "STRING", mode="REQUIRED", description="OpenStreetMap element ID"),
            SchemaField("osm_type", "STRING", mode="REQUIRED", description="OSM element type (node, way, relation)"),
            SchemaField("osm_version", "INTEGER", mode="NULLABLE", description="OSM element version"),
            SchemaField("osm_timestamp", "TIMESTAMP", mode="NULLABLE", description="OSM last modification timestamp"),
            
            # Business information
            SchemaField("name", "STRING", mode="NULLABLE", description="Business name"),
            SchemaField("amenity", "STRING", mode="NULLABLE", description="OSM amenity tag"),
            SchemaField("shop", "STRING", mode="NULLABLE", description="OSM shop tag"),
            SchemaField("cuisine", "STRING", mode="NULLABLE", description="Cuisine type for restaurants"),
            SchemaField("brand", "STRING", mode="NULLABLE", description="Brand name"),
            SchemaField("operator", "STRING", mode="NULLABLE", description="Operator/chain name"),
            
            # Location information
            SchemaField("latitude", "FLOAT", mode="REQUIRED", description="Latitude coordinate"),
            SchemaField("longitude", "FLOAT", mode="REQUIRED", description="Longitude coordinate"),
            SchemaField("address_housenumber", "STRING", mode="NULLABLE", description="House number"),
            SchemaField("address_street", "STRING", mode="NULLABLE", description="Street name"),
            SchemaField("address_city", "STRING", mode="NULLABLE", description="City name"),
            SchemaField("address_state", "STRING", mode="NULLABLE", description="State"),
            SchemaField("address_postcode", "STRING", mode="NULLABLE", description="Postal code"),
            SchemaField("address_country", "STRING", mode="NULLABLE", description="Country"),
            
            # Contact information
            SchemaField("phone", "STRING", mode="NULLABLE", description="Phone number"),
            SchemaField("website", "STRING", mode="NULLABLE", description="Website URL"),
            SchemaField("email", "STRING", mode="NULLABLE", description="Email address"),
            
            # Business details
            SchemaField("opening_hours", "STRING", mode="NULLABLE", description="Opening hours"),
            SchemaField("wheelchair", "STRING", mode="NULLABLE", description="Wheelchair accessibility"),
            SchemaField("takeaway", "STRING", mode="NULLABLE", description="Takeaway available"),
            SchemaField("delivery", "STRING", mode="NULLABLE", description="Delivery available"),
            SchemaField("outdoor_seating", "STRING", mode="NULLABLE", description="Outdoor seating available"),
            
            # Classification
            SchemaField("business_type", "STRING", mode="REQUIRED", description="Categorized business type"),
            SchemaField("franchise_indicator", "BOOLEAN", mode="REQUIRED", description="Likely franchise business"),
            
            # Metadata
            SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source"),
            SchemaField("data_collection_date", "TIMESTAMP", mode="REQUIRED", description="Data collection timestamp"),
            SchemaField("data_quality_score", "FLOAT", mode="NULLABLE", description="Data completeness score (0-100)"),
        ]
        
        # Create table with partitioning and clustering
        table = Table(table_id, schema=schema)
        
        # Partition by data collection date
        table.time_partitioning = TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_collection_date"
        )
        
        # Cluster by business type, state, and city for better query performance
        table.clustering_fields = ["business_type", "address_state", "address_city"]
        
        # Create table
        table = self.client.create_table(table, exists_ok=True)
        self.logger.info(f"Created OSM businesses table: {table_id}")
        
        return table
    
    def create_osm_collection_summary_table(self) -> Table:
        """
        Create table for OSM data collection summaries
        
        Returns:
            Created BigQuery table
        """
        table_id = f"{self.project_id}.{self.dataset_id}.osm_collection_summary"
        
        # Define schema
        schema = [
            SchemaField("collection_date", "TIMESTAMP", mode="REQUIRED", description="Collection timestamp"),
            SchemaField("area_name", "STRING", mode="REQUIRED", description="Area name (e.g., 'Dane County', 'Wisconsin')"),
            SchemaField("bbox", "STRING", mode="REQUIRED", description="Bounding box used for collection"),
            
            # Collection counts
            SchemaField("total_elements", "INTEGER", mode="REQUIRED", description="Total OSM elements returned"),
            SchemaField("businesses_collected", "INTEGER", mode="REQUIRED", description="Valid businesses collected"),
            SchemaField("franchises_identified", "INTEGER", mode="REQUIRED", description="Franchise businesses identified"),
            
            # Business type breakdown (stored as JSON string)
            SchemaField("business_type_counts", "STRING", mode="NULLABLE", description="Count by business type (JSON)"),
            
            # Data quality metrics
            SchemaField("avg_data_quality_score", "FLOAT", mode="NULLABLE", description="Average data quality score"),
            SchemaField("businesses_with_contact", "INTEGER", mode="REQUIRED", description="Businesses with contact info"),
            SchemaField("businesses_with_address", "INTEGER", mode="REQUIRED", description="Businesses with full address"),
            
            # Success metrics
            SchemaField("success", "BOOLEAN", mode="REQUIRED", description="Collection success status"),
            SchemaField("processing_time_seconds", "FLOAT", mode="NULLABLE", description="Processing time in seconds"),
            SchemaField("api_requests_made", "INTEGER", mode="REQUIRED", description="Number of API requests made"),
            
            # Coverage metrics
            SchemaField("cities_covered", "INTEGER", mode="REQUIRED", description="Number of unique cities"),
            SchemaField("coverage_area_sqkm", "FLOAT", mode="NULLABLE", description="Approximate coverage area"),
        ]
        
        # Create table
        table = Table(table_id, schema=schema)
        
        # Partition by collection date
        table.time_partitioning = TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="collection_date"
        )
        
        # Create table
        table = self.client.create_table(table, exists_ok=True)
        self.logger.info(f"Created OSM collection summary table: {table_id}")
        
        return table
    
    def create_osm_geocoding_comparison_view(self) -> None:
        """
        Create view comparing OSM data with geocoded DFI data
        """
        view_id = f"{self.project_id}.{self.dataset_id}.osm_dfi_comparison"
        
        query = f"""
        CREATE OR REPLACE VIEW `{view_id}` AS
        WITH osm_businesses AS (
          SELECT 
            name as business_name,
            business_type,
            latitude,
            longitude,
            address_city as city,
            address_state as state,
            brand,
            franchise_indicator,
            'OSM' as data_source,
            data_collection_date
          FROM `{self.project_id}.{self.dataset_id}.osm_businesses`
          WHERE address_state = 'WI' OR address_state = 'Wisconsin'
        ),
        dfi_businesses AS (
          SELECT 
            business_name,
            business_type,
            latitude,
            longitude,
            city,
            state,
            CAST(NULL AS STRING) as brand,
            FALSE as franchise_indicator,
            'DFI' as data_source,
            CAST(data_extraction_date AS TIMESTAMP) as data_collection_date
          FROM `{self.project_id}.{self.dataset_id}.dfi_business_registrations`
          WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        )
        SELECT * FROM osm_businesses
        UNION ALL
        SELECT * FROM dfi_businesses
        ORDER BY data_collection_date DESC, business_name
        """
        
        job = self.client.query(query)
        job.result()  # Wait for completion
        
        self.logger.info(f"Created OSM-DFI comparison view: {view_id}")
    
    def setup_all_tables(self) -> List[Table]:
        """
        Set up all OSM-related tables and views
        
        Returns:
            List of created tables
        """
        self.logger.info("Setting up OSM BigQuery tables...")
        
        tables = []
        
        # Create main tables
        tables.append(self.create_osm_businesses_table())
        tables.append(self.create_osm_collection_summary_table())
        
        # Create views
        self.create_osm_geocoding_comparison_view()
        
        self.logger.info(f"Setup complete: {len(tables)} tables created")
        
        return tables
    
    def verify_tables(self) -> bool:
        """
        Verify that all tables were created successfully
        
        Returns:
            True if all tables exist, False otherwise
        """
        required_tables = [
            "osm_businesses",
            "osm_collection_summary"
        ]
        
        dataset_ref = self.client.dataset(self.dataset_id)
        
        for table_name in required_tables:
            table_ref = dataset_ref.table(table_name)
            try:
                self.client.get_table(table_ref)
                self.logger.info(f"✅ Table {table_name} exists")
            except Exception as e:
                self.logger.error(f"❌ Table {table_name} not found: {e}")
                return False
        
        return True
    
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get information about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        table = self.client.get_table(table_ref)
        
        return {
            "table_id": table.table_id,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created,
            "modified": table.modified,
            "schema_fields": len(table.schema),
            "partitioning": table.time_partitioning.field if table.time_partitioning else None,
            "clustering": table.clustering_fields if table.clustering_fields else None
        }


def main():
    """Main function to set up OSM BigQuery tables"""
    logging.basicConfig(level=logging.INFO)
    
    print("🏗️ Setting up OSM BigQuery Tables")
    print("=" * 40)
    
    try:
        setup = OSMBigQuerySetup()
        
        # Setup all tables
        tables = setup.setup_all_tables()
        
        print(f"\n✅ Successfully created {len(tables)} tables")
        
        # Verify setup
        if setup.verify_tables():
            print("✅ All tables verified successfully")
            
            # Show table info
            print("\n📊 Table Information:")
            for table_name in ["osm_businesses", "osm_collection_summary"]:
                try:
                    info = setup.get_table_info(table_name)
                    print(f"\n{table_name}:")
                    print(f"   Rows: {info['num_rows']}")
                    print(f"   Size: {info['num_bytes']:,} bytes")
                    print(f"   Schema fields: {info['schema_fields']}")
                    print(f"   Partitioned by: {info['partitioning']}")
                    print(f"   Clustered by: {info['clustering']}")
                except Exception as e:
                    print(f"   Error getting info: {e}")
        else:
            print("❌ Table verification failed")
            
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        logging.error(f"OSM BigQuery setup failed: {e}")


if __name__ == "__main__":
    main()