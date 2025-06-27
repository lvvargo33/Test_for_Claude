#!/usr/bin/env python3
"""
Upload Google Places Data to BigQuery
=====================================

Uploads the collected Google Places data to BigQuery with proper schema and partitioning.
"""

import os
import pandas as pd
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Project configuration
PROJECT_ID = "location-optimizer-1"
DATASET_ID = "wisconsin_business_data"
TABLE_ID = "google_places_businesses"

def setup_bigquery_client():
    """Initialize BigQuery client"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        logger.info(f"✓ BigQuery client initialized for project: {PROJECT_ID}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        return None

def create_dataset_if_not_exists(client):
    """Create dataset if it doesn't exist"""
    try:
        dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "Wisconsin business intelligence data including Google Places API data"
        
        try:
            client.get_dataset(dataset_id)
            logger.info(f"✓ Dataset {DATASET_ID} already exists")
        except NotFound:
            dataset = client.create_dataset(dataset, timeout=30)
            logger.info(f"✓ Created dataset {DATASET_ID}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        return False

def create_table_schema():
    """Define BigQuery table schema for Google Places data"""
    schema = [
        # Core Google Places fields
        bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("formatted_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("formatted_phone_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("international_phone_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rating", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("user_ratings_total", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("price_level", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("business_status", "STRING", mode="NULLABLE"),
        
        # Location data
        bigquery.SchemaField("geometry_location_lat", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("geometry_location_lng", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("vicinity", "STRING", mode="NULLABLE"),
        
        # Address components
        bigquery.SchemaField("street_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("route", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("locality", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("administrative_area_level_1", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("administrative_area_level_2", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("postal_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("zip_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        
        # Business categorization
        bigquery.SchemaField("types", "STRING", mode="REPEATED"),
        bigquery.SchemaField("primary_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("business_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("industry_sector", "STRING", mode="NULLABLE"),
        
        # Search context
        bigquery.SchemaField("search_area_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("search_center_lat", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("search_center_lng", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("search_radius_meters", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("county_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("metro_area", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("search_business_type", "STRING", mode="NULLABLE"),
        
        # Competitive analysis
        bigquery.SchemaField("competitor_density_0_5_mile", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("competitor_density_1_mile", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("competitor_density_3_mile", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("nearest_competitor_distance_miles", "FLOAT", mode="NULLABLE"),
        
        # Data quality and metadata
        bigquery.SchemaField("data_confidence_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("collection_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("api_response_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collection_method", "STRING", mode="NULLABLE"),
        
        # Raw data for debugging
        bigquery.SchemaField("raw_api_response", "STRING", mode="NULLABLE"),
        
        # Additional fields that might be in the CSV
        bigquery.SchemaField("icon", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("icon_background_color", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("icon_mask_base_uri", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("plus_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reference", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("scope", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("permanently_closed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("opening_hours", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("photos", "STRING", mode="NULLABLE"),
    ]
    
    return schema

def create_table_if_not_exists(client):
    """Create table if it doesn't exist"""
    try:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        try:
            client.get_table(table_id)
            logger.info(f"✓ Table {TABLE_ID} already exists")
            return True
        except NotFound:
            # Create table with schema
            schema = create_table_schema()
            table = bigquery.Table(table_id, schema=schema)
            
            # Set up partitioning by collection_date
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="collection_date"
            )
            
            # Set up clustering
            table.clustering_fields = ["county_name", "business_category", "city_name"]
            
            table = client.create_table(table, timeout=30)
            logger.info(f"✓ Created table {TABLE_ID}")
            return True
            
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return False

def prepare_data_for_bigquery(df):
    """Prepare DataFrame for BigQuery upload"""
    logger.info("Preparing data for BigQuery...")
    
    # Make a copy to avoid modifying original
    df_clean = df.copy()
    
    # Handle datetime columns
    datetime_columns = ['collection_date', 'last_updated']
    for col in datetime_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Handle JSON/array columns
    json_columns = ['geometry', 'types', 'opening_hours', 'photos']
    for col in json_columns:
        if col in df_clean.columns:
            # Convert to string representation
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if pd.notna(x) else None)
    
    # Handle 'types' specifically - convert to repeated field format
    if 'types' in df_clean.columns:
        def parse_types(types_str):
            if pd.isna(types_str) or types_str == 'None':
                return []
            try:
                # Try to parse as JSON array
                if types_str.startswith('[') and types_str.endswith(']'):
                    return eval(types_str)  # Safe for known data format
                else:
                    return [types_str]
            except:
                return [str(types_str)]
        
        df_clean['types'] = df_clean['types'].apply(parse_types)
    
    # Ensure required fields exist
    if 'place_id' not in df_clean.columns:
        logger.error("Missing required 'place_id' column")
        return None
    
    # Handle missing location data
    location_cols = ['geometry_location_lat', 'geometry_location_lng']
    for col in location_cols:
        if col not in df_clean.columns:
            df_clean[col] = None
    
    # Convert boolean columns
    bool_columns = ['permanently_closed']
    for col in bool_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].map({'True': True, 'False': False, True: True, False: False})
    
    # Replace NaN with None for BigQuery compatibility
    df_clean = df_clean.where(pd.notna(df_clean), None)
    
    logger.info(f"✓ Data prepared: {len(df_clean)} rows, {len(df_clean.columns)} columns")
    return df_clean

def upload_to_bigquery(client, df):
    """Upload DataFrame to BigQuery"""
    try:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Prepare data
        df_clean = prepare_data_for_bigquery(df)
        if df_clean is None:
            return False
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Append to existing data
            autodetect=True,  # Auto-detect schema from data
            allow_field_addition=True,  # Allow new fields
            allow_field_relaxation=True,  # Allow type relaxation
        )
        
        # Upload data
        logger.info(f"Starting upload to {table_id}...")
        job = client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        logger.info(f"✓ Successfully uploaded {len(df_clean)} rows to BigQuery")
        
        # Verify the upload
        table = client.get_table(table_id)
        logger.info(f"✓ Table now contains {table.num_rows} total rows")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload to BigQuery: {e}")
        return False

def main():
    """Main upload process"""
    logger.info("=" * 60)
    logger.info("GOOGLE PLACES DATA TO BIGQUERY UPLOAD")
    logger.info("=" * 60)
    
    # Find the most recent Google Places CSV file
    csv_files = [f for f in os.listdir('.') if f.startswith('google_places_phase1_') and f.endswith('.csv')]
    if not csv_files:
        logger.error("No Google Places CSV files found")
        return False
    
    # Use the most recent file
    csv_file = sorted(csv_files)[-1]
    logger.info(f"Using data file: {csv_file}")
    
    try:
        # Load data
        logger.info("Loading CSV data...")
        df = pd.read_csv(csv_file)
        logger.info(f"✓ Loaded {len(df)} rows from CSV")
        
        # Set up BigQuery
        client = setup_bigquery_client()
        if not client:
            return False
        
        # Create dataset and table
        if not create_dataset_if_not_exists(client):
            return False
        
        if not create_table_if_not_exists(client):
            return False
        
        # Upload data
        success = upload_to_bigquery(client, df)
        
        if success:
            logger.info("=" * 60)
            logger.info("UPLOAD COMPLETE!")
            logger.info(f"✓ Dataset: {DATASET_ID}")
            logger.info(f"✓ Table: {TABLE_ID}")
            logger.info(f"✓ Records uploaded: {len(df)}")
            logger.info("=" * 60)
        
        return success
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)