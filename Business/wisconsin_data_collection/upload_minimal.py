#!/usr/bin/env python3
"""
Minimal Google Places Data Upload to BigQuery
"""

import pandas as pd
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load and prepare data
    df = pd.read_csv('google_places_phase1_20250627_212804.csv')
    logger.info(f"Loaded {len(df)} rows")
    
    # Simple data cleaning
    df_clean = df.copy()
    
    # Convert everything to strings to avoid type issues
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object' or df_clean[col].dtype == 'float64':
            df_clean[col] = df_clean[col].astype(str).replace('nan', '')
    
    # Upload to BigQuery
    client = bigquery.Client(project="location-optimizer-1")
    
    # Simple table reference
    table_id = "location-optimizer-1.wisconsin_business_data.google_places_raw"
    
    # Simple job config
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace data
        autodetect=True,  # Auto-detect schema
    )
    
    logger.info("Uploading to BigQuery...")
    job = client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
    job.result()
    
    # Verify
    table = client.get_table(table_id)
    logger.info(f"✅ SUCCESS! Uploaded {table.num_rows} rows")
    logger.info(f"Table: {table_id}")

if __name__ == "__main__":
    main()