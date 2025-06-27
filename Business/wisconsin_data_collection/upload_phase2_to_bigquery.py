#!/usr/bin/env python3
"""
Upload Google Places Phase 2 Data to BigQuery
"""

import pandas as pd
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load Phase 2 data
    df = pd.read_csv('google_places_phase2_20250627_214354.csv')
    logger.info(f"Loaded {len(df)} Phase 2 records")
    
    # Simple data cleaning
    df_clean = df.copy()
    
    # Convert everything to strings to avoid type issues
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object' or df_clean[col].dtype == 'float64':
            df_clean[col] = df_clean[col].astype(str).replace('nan', '')
    
    # Upload to BigQuery - append to existing table
    client = bigquery.Client(project="location-optimizer-1")
    
    # Use same table as Phase 1 to combine data
    table_id = "location-optimizer-1.wisconsin_business_data.google_places_raw"
    
    # Append to existing data
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Append to existing data
        autodetect=True,
    )
    
    logger.info("Uploading Phase 2 data to BigQuery...")
    job = client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
    job.result()
    
    # Verify total count
    table = client.get_table(table_id)
    logger.info(f"✅ SUCCESS! Phase 2 uploaded")
    logger.info(f"Phase 2 records: {len(df_clean)}")
    logger.info(f"Total records in table: {table.num_rows}")
    logger.info(f"Table: {table_id}")

if __name__ == "__main__":
    main()