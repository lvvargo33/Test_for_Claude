"""
Setup Phase 1 BigQuery Tables
==============================

Creates BigQuery tables for Phase 1 data sources:
- traffic_counts
- zoning_data
- consumer_spending
"""

import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning, TimePartitioningType
import yaml


def load_config():
    """Load configuration from data_sources.yaml"""
    with open('data_sources.yaml', 'r') as f:
        return yaml.safe_load(f)


def create_traffic_counts_table(client, project_id, dataset_id):
    """Create traffic_counts table"""
    table_id = f"{project_id}.{dataset_id}.traffic_counts"
    
    schema = [
        SchemaField("location_id", "STRING", mode="REQUIRED", description="Unique location identifier"),
        SchemaField("station_id", "STRING", mode="REQUIRED", description="Traffic counting station ID"),
        SchemaField("route_name", "STRING", mode="REQUIRED", description="Highway/route name"),
        SchemaField("latitude", "FLOAT", mode="REQUIRED", description="Latitude coordinate"),
        SchemaField("longitude", "FLOAT", mode="REQUIRED", description="Longitude coordinate"),
        SchemaField("county", "STRING", mode="REQUIRED", description="County name"),
        SchemaField("city", "STRING", mode="NULLABLE", description="Nearest city"),
        SchemaField("aadt", "INTEGER", mode="REQUIRED", description="Annual Average Daily Traffic"),
        SchemaField("measurement_year", "INTEGER", mode="REQUIRED", description="Year of measurement"),
        SchemaField("traffic_volume_category", "STRING", mode="NULLABLE", description="Traffic volume category"),
        SchemaField("highway_type", "STRING", mode="REQUIRED", description="Highway type classification"),
        SchemaField("functional_class", "STRING", mode="NULLABLE", description="Functional classification"),
        SchemaField("urban_rural", "STRING", mode="NULLABLE", description="Urban or rural designation"),
        SchemaField("lane_count", "INTEGER", mode="NULLABLE", description="Number of lanes"),
        SchemaField("median_type", "STRING", mode="NULLABLE", description="Median type"),
        SchemaField("access_control", "STRING", mode="NULLABLE", description="Access control type"),
        SchemaField("truck_percentage", "FLOAT", mode="NULLABLE", description="Percentage of truck traffic"),
        SchemaField("peak_hour_factor", "FLOAT", mode="NULLABLE", description="Peak hour factor"),
        SchemaField("directional_split", "STRING", mode="NULLABLE", description="Directional traffic split"),
        SchemaField("data_source", "STRING", mode="REQUIRED", description="Data source"),
        SchemaField("data_collection_date", "TIMESTAMP", mode="REQUIRED", description="Data collection timestamp"),
        SchemaField("data_quality_score", "FLOAT", mode="NULLABLE", description="Data completeness score")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Wisconsin DOT traffic count data with AADT and highway characteristics"
    
    # Set up partitioning and clustering
    table.time_partitioning = TimePartitioning(
        type_=TimePartitioningType.DAY,
        field="data_collection_date"
    )
    table.clustering_fields = ["county", "highway_type", "traffic_volume_category"]
    
    # Create table
    table = client.create_table(table)
    print(f"✓ Created table {table_id}")
    
    return table


def create_zoning_data_table(client, project_id, dataset_id):
    """Create zoning_data table"""
    table_id = f"{project_id}.{dataset_id}.zoning_data"
    
    schema = [
        SchemaField("parcel_id", "STRING", mode="REQUIRED", description="Unique parcel identifier"),
        SchemaField("county", "STRING", mode="REQUIRED", description="County name"),
        SchemaField("municipality", "STRING", mode="NULLABLE", description="City/town/village name"),
        SchemaField("latitude", "FLOAT", mode="NULLABLE", description="Latitude coordinate"),
        SchemaField("longitude", "FLOAT", mode="NULLABLE", description="Longitude coordinate"),
        SchemaField("address", "STRING", mode="NULLABLE", description="Property address"),
        SchemaField("zoning_code", "STRING", mode="REQUIRED", description="Zoning classification code"),
        SchemaField("zoning_description", "STRING", mode="REQUIRED", description="Zoning classification description"),
        SchemaField("zoning_district", "STRING", mode="NULLABLE", description="Zoning district name"),
        SchemaField("current_land_use", "STRING", mode="NULLABLE", description="Current land use classification"),
        SchemaField("future_land_use", "STRING", mode="NULLABLE", description="Future land use designation"),
        SchemaField("comprehensive_plan_designation", "STRING", mode="NULLABLE", description="Comprehensive plan designation"),
        SchemaField("lot_size_acres", "FLOAT", mode="NULLABLE", description="Lot size in acres"),
        SchemaField("lot_size_sqft", "FLOAT", mode="NULLABLE", description="Lot size in square feet"),
        SchemaField("frontage_feet", "FLOAT", mode="NULLABLE", description="Street frontage in feet"),
        SchemaField("commercial_allowed", "BOOLEAN", mode="REQUIRED", description="Commercial use allowed"),
        SchemaField("retail_allowed", "BOOLEAN", mode="REQUIRED", description="Retail use allowed"),
        SchemaField("restaurant_allowed", "BOOLEAN", mode="REQUIRED", description="Restaurant use allowed"),
        SchemaField("mixed_use_allowed", "BOOLEAN", mode="REQUIRED", description="Mixed use allowed"),
        SchemaField("overlay_districts", "STRING", mode="NULLABLE", description="Overlay districts"),
        SchemaField("special_districts", "STRING", mode="NULLABLE", description="Special districts"),
        SchemaField("flood_zone", "STRING", mode="NULLABLE", description="FEMA flood zone designation"),
        SchemaField("setback_front", "FLOAT", mode="NULLABLE", description="Front setback requirement (feet)"),
        SchemaField("setback_side", "FLOAT", mode="NULLABLE", description="Side setback requirement (feet)"),
        SchemaField("setback_rear", "FLOAT", mode="NULLABLE", description="Rear setback requirement (feet)"),
        SchemaField("max_building_height", "FLOAT", mode="NULLABLE", description="Maximum building height (feet)"),
        SchemaField("max_lot_coverage", "FLOAT", mode="NULLABLE", description="Maximum lot coverage (%)"),
        SchemaField("min_parking_spaces", "INTEGER", mode="NULLABLE", description="Minimum parking spaces required"),
        SchemaField("building_permit_required", "BOOLEAN", mode="REQUIRED", description="Building permit required"),
        SchemaField("conditional_use_permit_required", "BOOLEAN", mode="REQUIRED", description="Conditional use permit required"),
        SchemaField("variance_required", "BOOLEAN", mode="REQUIRED", description="Variance may be required"),
        SchemaField("data_source", "STRING", mode="REQUIRED", description="County GIS data source"),
        SchemaField("source_url", "STRING", mode="NULLABLE", description="Source URL"),
        SchemaField("data_collection_date", "TIMESTAMP", mode="REQUIRED", description="Data collection timestamp"),
        SchemaField("data_quality_score", "FLOAT", mode="NULLABLE", description="Data completeness score"),
        SchemaField("last_updated", "DATE", mode="NULLABLE", description="Last update date from source")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Wisconsin county zoning data with business use permissions and regulatory requirements"
    
    # Set up partitioning and clustering
    table.time_partitioning = TimePartitioning(
        type_=TimePartitioningType.DAY,
        field="data_collection_date"
    )
    table.clustering_fields = ["county", "zoning_code", "commercial_allowed"]
    
    # Create table
    table = client.create_table(table)
    print(f"✓ Created table {table_id}")
    
    return table


def create_consumer_spending_table(client, project_id, dataset_id):
    """Create consumer_spending table"""
    table_id = f"{project_id}.{dataset_id}.consumer_spending"
    
    schema = [
        SchemaField("geo_fips", "STRING", mode="REQUIRED", description="Geographic FIPS code"),
        SchemaField("geo_name", "STRING", mode="REQUIRED", description="Geographic area name"),
        SchemaField("state_fips", "STRING", mode="REQUIRED", description="State FIPS code"),
        SchemaField("state_name", "STRING", mode="REQUIRED", description="State name"),
        SchemaField("data_year", "INTEGER", mode="REQUIRED", description="Data year"),
        SchemaField("data_period", "STRING", mode="REQUIRED", description="Data period (annual/quarterly)"),
        SchemaField("total_pce", "FLOAT", mode="NULLABLE", description="Total Personal Consumption Expenditures (millions)"),
        SchemaField("goods_total", "FLOAT", mode="NULLABLE", description="Total spending on goods (millions)"),
        SchemaField("goods_durable", "FLOAT", mode="NULLABLE", description="Durable goods spending (millions)"),
        SchemaField("goods_nondurable", "FLOAT", mode="NULLABLE", description="Nondurable goods spending (millions)"),
        SchemaField("services_total", "FLOAT", mode="NULLABLE", description="Total spending on services (millions)"),
        SchemaField("food_beverages", "FLOAT", mode="NULLABLE", description="Food and beverages spending (millions)"),
        SchemaField("housing_utilities", "FLOAT", mode="NULLABLE", description="Housing and utilities spending (millions)"),
        SchemaField("transportation", "FLOAT", mode="NULLABLE", description="Transportation spending (millions)"),
        SchemaField("healthcare", "FLOAT", mode="NULLABLE", description="Healthcare spending (millions)"),
        SchemaField("recreation", "FLOAT", mode="NULLABLE", description="Recreation spending (millions)"),
        SchemaField("education", "FLOAT", mode="NULLABLE", description="Education spending (millions)"),
        SchemaField("restaurants_hotels", "FLOAT", mode="NULLABLE", description="Restaurants and hotels spending (millions)"),
        SchemaField("other_services", "FLOAT", mode="NULLABLE", description="Other services spending (millions)"),
        SchemaField("total_pce_per_capita", "FLOAT", mode="NULLABLE", description="PCE per capita (dollars)"),
        SchemaField("goods_per_capita", "FLOAT", mode="NULLABLE", description="Goods spending per capita (dollars)"),
        SchemaField("services_per_capita", "FLOAT", mode="NULLABLE", description="Services spending per capita (dollars)"),
        SchemaField("total_pce_growth_rate", "FLOAT", mode="NULLABLE", description="Total PCE growth rate (%)"),
        SchemaField("goods_growth_rate", "FLOAT", mode="NULLABLE", description="Goods spending growth rate (%)"),
        SchemaField("services_growth_rate", "FLOAT", mode="NULLABLE", description="Services spending growth rate (%)"),
        SchemaField("population", "INTEGER", mode="NULLABLE", description="Population count"),
        SchemaField("seasonally_adjusted", "BOOLEAN", mode="REQUIRED", description="Data is seasonally adjusted"),
        SchemaField("retail_relevant_spending", "FLOAT", mode="NULLABLE", description="Retail-relevant spending categories combined"),
        SchemaField("restaurant_relevant_spending", "FLOAT", mode="NULLABLE", description="Restaurant-relevant spending categories"),
        SchemaField("services_business_relevant", "FLOAT", mode="NULLABLE", description="Services relevant to business location"),
        SchemaField("data_source", "STRING", mode="REQUIRED", description="Bureau of Economic Analysis"),
        SchemaField("api_dataset", "STRING", mode="REQUIRED", description="BEA API dataset used"),
        SchemaField("data_collection_date", "TIMESTAMP", mode="REQUIRED", description="Data collection timestamp"),
        SchemaField("data_quality_score", "FLOAT", mode="NULLABLE", description="Data completeness score")
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Bureau of Economic Analysis consumer spending data by geographic area and category"
    
    # Set up partitioning and clustering
    table.time_partitioning = TimePartitioning(
        type_=TimePartitioningType.DAY,
        field="data_collection_date"
    )
    table.clustering_fields = ["state_fips", "data_year", "data_period"]
    
    # Create table
    table = client.create_table(table)
    print(f"✓ Created table {table_id}")
    
    return table


def main():
    """Set up Phase 1 BigQuery tables"""
    print("PHASE 1 BIGQUERY SETUP")
    print("=" * 50)
    print(f"Setup started at: {datetime.now()}")
    
    try:
        # Load configuration
        config = load_config()
        bq_config = config['bigquery']
        project_id = bq_config['project_id']
        dataset_id = bq_config['datasets']['raw_data']
        
        print(f"Project: {project_id}")
        print(f"Dataset: {dataset_id}")
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Verify dataset exists
        try:
            dataset = client.get_dataset(f"{project_id}.{dataset_id}")
            print(f"✓ Dataset {dataset_id} exists")
        except Exception as e:
            print(f"✗ Dataset {dataset_id} not found: {e}")
            print("Please create the dataset first using setup_bigquery.py")
            return 1
        
        # Create tables
        tables_created = []
        
        try:
            table = create_traffic_counts_table(client, project_id, dataset_id)
            tables_created.append(table.table_id)
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• Table traffic_counts already exists")
            else:
                print(f"✗ Error creating traffic_counts table: {e}")
        
        try:
            table = create_zoning_data_table(client, project_id, dataset_id)
            tables_created.append(table.table_id)
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• Table zoning_data already exists")
            else:
                print(f"✗ Error creating zoning_data table: {e}")
        
        try:
            table = create_consumer_spending_table(client, project_id, dataset_id)
            tables_created.append(table.table_id)
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• Table consumer_spending already exists")
            else:
                print(f"✗ Error creating consumer_spending table: {e}")
        
        print("\\n" + "=" * 50)
        print("PHASE 1 BIGQUERY SETUP COMPLETE")
        print("=" * 50)
        print(f"Tables created: {len(tables_created)}")
        
        if tables_created:
            print("\\nNew tables:")
            for table_id in tables_created:
                print(f"  • {table_id}")
        
        print(f"\\nSetup completed at: {datetime.now()}")
        
        return 0
        
    except Exception as e:
        print(f"\\n✗ FATAL ERROR: {e}")
        return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    exit(main())