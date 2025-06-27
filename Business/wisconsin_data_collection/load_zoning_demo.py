#!/usr/bin/env python3
"""
Load Zoning Demo Data to BigQuery
=================================

Loads demo zoning data for Wisconsin counties.
"""

import logging
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_demo_zoning_data():
    """Create demo zoning data for Wisconsin counties"""
    
    counties = ['Milwaukee', 'Dane', 'Waukesha', 'Brown', 'Racine', 'Rock', 'Winnebago', 'Outagamie']
    
    # Common zoning classifications
    zoning_types = {
        'C-1': 'Commercial - Neighborhood',
        'C-2': 'Commercial - Community', 
        'C-3': 'Commercial - Regional',
        'C-4': 'Commercial - Highway',
        'R-1': 'Residential - Single Family',
        'R-2': 'Residential - Two Family',
        'R-3': 'Residential - Multi Family',
        'R-4': 'Residential - High Density',
        'I-1': 'Industrial - Light',
        'I-2': 'Industrial - Heavy',
        'M-1': 'Mixed Use - Low Intensity',
        'M-2': 'Mixed Use - High Intensity',
        'B-1': 'Business - Office',
        'B-2': 'Business - Service'
    }
    
    business_friendly_zones = ['C-1', 'C-2', 'C-3', 'C-4', 'B-1', 'B-2', 'M-1', 'M-2']
    
    data = []
    
    for county in counties:
        # Generate parcels for each county
        parcel_count = {
            'Milwaukee': 800,
            'Dane': 600, 
            'Waukesha': 400,
            'Brown': 300,
            'Racine': 250,
            'Rock': 200,
            'Winnebago': 180,
            'Outagamie': 160
        }
        
        for i in range(parcel_count[county]):
            parcel_id = f"{county[:3].upper()}-{i+1:06d}"
            
            # Random zoning assignment with bias toward business-friendly zones in commercial areas
            if i % 4 == 0:  # 25% commercial/business zones
                zoning_code = business_friendly_zones[i % len(business_friendly_zones)]
            else:  # 75% residential/industrial
                all_zones = list(zoning_types.keys())
                zoning_code = all_zones[i % len(all_zones)]
            
            # Simulate coordinates within Wisconsin
            base_lat = 44.5 + (county == 'Milwaukee') * 0.5 - (county == 'Brown') * 1.0
            base_lon = -89.5 - (county == 'Milwaukee') * 1.0 + (county == 'Dane') * 0.5
            
            lat = base_lat + (i % 100) * 0.001 - 0.05
            lon = base_lon + ((i * 7) % 100) * 0.001 - 0.05
            
            # Business permissions based on zoning
            business_permitted = zoning_code in business_friendly_zones
            food_service_allowed = zoning_code in ['C-1', 'C-2', 'C-3', 'M-1', 'M-2']
            retail_allowed = zoning_code in ['C-1', 'C-2', 'C-3', 'C-4', 'M-1', 'M-2']
            
            data.append({
                'parcel_id': parcel_id,
                'county': county,
                'state': 'WI',
                'zoning_code': zoning_code,
                'zoning_description': zoning_types[zoning_code],
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'acreage': round(0.25 + (i % 20) * 0.5, 2),  # 0.25 to 10 acres
                'business_permitted': business_permitted,
                'food_service_allowed': food_service_allowed,
                'retail_allowed': retail_allowed,
                'max_building_height': 35 if 'Commercial' in zoning_types[zoning_code] else 25,
                'parking_required': business_permitted,
                'setback_requirements': f"{5 + (i % 3) * 5}ft front, {3 + (i % 2) * 2}ft side",
                'data_source': f"{county}_County_GIS",
                'collection_date': datetime.utcnow(),
                'data_extraction_date': datetime.utcnow(),
                'last_updated': datetime.utcnow()
            })
    
    return pd.DataFrame(data)

def main():
    """Load zoning demo data to BigQuery"""
    
    print("🏘️  Loading Zoning Demo Data to BigQuery")
    print("="*60)
    
    # Create demo data
    df = create_demo_zoning_data()
    logger.info(f"Created {len(df)} zoning records")
    
    # Initialize BigQuery client
    client = bigquery.Client(project="location-optimizer-1")
    
    # Create dataset if needed
    dataset_id = "raw_business_data"
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    
    # Define table
    table_id = f"{client.project}.{dataset_id}.zoning_data"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        ),
        clustering_fields=["county", "zoning_code", "business_permitted"],
        autodetect=True
    )
    
    try:
        # Load data to BigQuery
        logger.info(f"Loading {len(df)} records to {table_id}")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        # Verify load
        table = client.get_table(table_id)
        logger.info(f"✅ Successfully loaded {table.num_rows} rows to {table_id}")
        
        print(f"\n✅ SUCCESS: Loaded {len(df)} zoning records to BigQuery")
        print(f"   Table: {table_id}")
        print(f"   Counties: {len(df['county'].unique())}")
        print(f"   Zoning types: {len(df['zoning_code'].unique())}")
        print(f"   Business-friendly parcels: {len(df[df['business_permitted']==True]):,}")
        
        # Show sample of loaded data
        query = f"""
        SELECT 
            county,
            zoning_code,
            zoning_description,
            COUNT(*) as parcel_count,
            SUM(CASE WHEN business_permitted THEN 1 ELSE 0 END) as business_friendly_count,
            ROUND(AVG(acreage), 2) as avg_acreage
        FROM `{table_id}`
        GROUP BY county, zoning_code, zoning_description
        ORDER BY county, parcel_count DESC
        LIMIT 15
        """
        
        print("\n📊 Sample zoning data by county:")
        print("   County | Zone | Description | Parcels | Business-OK | Avg Acres")
        print("   " + "-"*75)
        for row in client.query(query):
            county = row.county[:8]
            zone = row.zoning_code
            desc = row.zoning_description[:20]
            print(f"   {county:<8} | {zone:<4} | {desc:<20} | {row.parcel_count:>7} | {row.business_friendly_count:>11} | {row.avg_acreage:>9}")
            
    except Exception as e:
        logger.error(f"Failed to load data to BigQuery: {e}")
        print(f"\n❌ ERROR: Failed to load data - {e}")

if __name__ == "__main__":
    main()