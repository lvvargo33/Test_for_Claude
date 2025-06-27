#!/usr/bin/env python3
"""
Load Consumer Spending Demo Data to BigQuery
============================================

Loads demo consumer spending data for Wisconsin.
"""

import logging
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_demo_consumer_spending_data():
    """Create demo consumer spending data for Wisconsin"""
    
    # Wisconsin state-level consumer spending data (simulated)
    # Based on typical BEA Personal Consumption Expenditure categories
    data = []
    
    years = [2021, 2022, 2023]
    
    # State-level data
    for year in years:
        base_spending = 300_000_000_000  # $300B base
        growth_rate = 1.03 ** (year - 2021)  # 3% annual growth
        
        categories = {
            'total_pce': base_spending * growth_rate,
            'goods_total': base_spending * 0.32 * growth_rate,
            'goods_durable': base_spending * 0.12 * growth_rate,
            'services_total': base_spending * 0.68 * growth_rate,
            'housing_utilities': base_spending * 0.18 * growth_rate,
            'healthcare': base_spending * 0.17 * growth_rate,
            'transportation': base_spending * 0.09 * growth_rate,
            'recreation': base_spending * 0.08 * growth_rate,
            'food_beverages': base_spending * 0.14 * growth_rate,
            'restaurants_hotels': base_spending * 0.07 * growth_rate,
        }
        
        for category, amount in categories.items():
            data.append({
                'state': 'WI',
                'year': year,
                'spending_category': category,
                'spending_amount': amount,
                'per_capita_spending': amount / 5_900_000,  # WI population ~5.9M
                'data_source': 'BEA_DEMO',
                'collection_date': datetime.utcnow(),
                'data_extraction_date': datetime.utcnow()
            })
    
    # Add some county-level data for major counties
    counties = ['Milwaukee', 'Dane', 'Waukesha', 'Brown', 'Racine']
    county_populations = {
        'Milwaukee': 950_000,
        'Dane': 550_000,
        'Waukesha': 400_000,
        'Brown': 265_000,
        'Racine': 195_000
    }
    
    for county in counties:
        for year in years:
            # County spending proportional to population
            county_factor = county_populations[county] / 5_900_000
            county_spending = base_spending * county_factor * (1.03 ** (year - 2021))
            
            data.append({
                'state': 'WI',
                'county': county,
                'year': year,
                'spending_category': 'total_pce',
                'spending_amount': county_spending,
                'per_capita_spending': county_spending / county_populations[county],
                'data_source': 'BEA_DEMO',
                'collection_date': datetime.utcnow(),
                'data_extraction_date': datetime.utcnow()
            })
    
    return pd.DataFrame(data)

def main():
    """Load consumer spending demo data to BigQuery"""
    
    print("💰 Loading Consumer Spending Demo Data to BigQuery")
    print("="*60)
    
    # Create demo data
    df = create_demo_consumer_spending_data()
    logger.info(f"Created {len(df)} consumer spending records")
    
    # Initialize BigQuery client
    client = bigquery.Client(project="location-optimizer-1")
    
    # Create dataset if needed
    dataset_id = "raw_business_data"
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    
    # Define table
    table_id = f"{client.project}.{dataset_id}.consumer_spending"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        ),
        clustering_fields=["state", "year", "spending_category"],
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
        
        print(f"\n✅ SUCCESS: Loaded {len(df)} consumer spending records to BigQuery")
        print(f"   Table: {table_id}")
        print(f"   Years: 2021-2023")
        print(f"   Categories: 10 spending categories")
        print(f"   Geographic: State + 5 major counties")
        
        # Show sample of loaded data
        query = f"""
        SELECT 
            state,
            county,
            year,
            spending_category,
            ROUND(spending_amount / 1000000, 2) as spending_millions,
            ROUND(per_capita_spending, 2) as per_capita
        FROM `{table_id}`
        WHERE spending_category = 'total_pce'
        ORDER BY year DESC, spending_amount DESC
        LIMIT 10
        """
        
        print("\n📊 Sample data (Total Personal Consumption Expenditures):")
        for row in client.query(query):
            location = row.county if row.county else row.state
            print(f"   {location} ({row.year}): ${row.spending_millions:,.0f}M total, ${row.per_capita:,.0f} per capita")
            
    except Exception as e:
        logger.error(f"Failed to load data to BigQuery: {e}")
        print(f"\n❌ ERROR: Failed to load data - {e}")

if __name__ == "__main__":
    main()