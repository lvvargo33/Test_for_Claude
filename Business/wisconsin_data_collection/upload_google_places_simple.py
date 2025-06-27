#!/usr/bin/env python3
"""
Simple Google Places Data Upload to BigQuery
===========================================

Simple upload with auto-schema detection
"""

import pandas as pd
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "location-optimizer-1"
DATASET_ID = "wisconsin_business_data"
TABLE_ID = "google_places_businesses"

def main():
    logger.info("Starting Google Places data upload to BigQuery...")
    
    # Load data
    df = pd.read_csv('google_places_phase1_20250627_212804.csv')
    logger.info(f"Loaded {len(df)} rows")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    # Create dataset if needed
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset {DATASET_ID} exists")
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {DATASET_ID}")
    
    # Prepare data
    df_clean = df.copy()
    
    # Handle datetime columns
    datetime_cols = ['collection_date', 'last_updated']
    for col in datetime_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Handle complex columns by converting to strings
    complex_cols = ['geometry', 'photos', 'raw_api_response']
    for col in complex_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
    
    # Handle types column (convert to JSON string)
    if 'types' in df_clean.columns:
        df_clean['types'] = df_clean['types'].astype(str)
    
    # Fix string columns that might have NaN as float
    string_cols = ['city_name', 'county_name', 'name', 'formatted_address', 'business_category']
    for col in string_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).replace('nan', None)
    
    # Replace NaN with None
    df_clean = df_clean.where(pd.notna(df_clean), None)
    
    # Upload with auto-schema detection
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace existing data
        autodetect=True,
    )
    
    logger.info(f"Uploading to {table_id}...")
    job = client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
    job.result()
    
    # Verify
    table = client.get_table(table_id)
    logger.info(f"✅ Success! Uploaded {table.num_rows} rows to BigQuery")
    logger.info(f"Table: {table_id}")
    
    return True

if __name__ == "__main__":
    main()