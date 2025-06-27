#!/usr/bin/env python3
"""
Load Phase 2 Demo Data to BigQuery
==================================

Loads demo data for all Phase 2 sources since the collectors are using placeholder data.
"""

import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import uuid
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_demo_real_estate_data():
    """Create demo commercial real estate data"""
    
    cities = ['Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine', 'Appleton', 'Waukesha']
    property_types = ['office', 'retail', 'restaurant', 'industrial', 'mixed_use', 'warehouse']
    
    data = []
    
    for i in range(150):  # 150 properties
        city = random.choice(cities)
        prop_type = random.choice(property_types)
        
        # Generate realistic property data
        sqft = random.randint(1000, 50000)
        price_per_sqft = random.uniform(80, 300)
        asking_price = sqft * price_per_sqft
        
        data.append({
            'property_id': f"WI-{city[:3].upper()}-{i+1:04d}",
            'address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'First', 'State'])} St",
            'city': city,
            'county': 'Milwaukee' if city == 'Milwaukee' else 'Dane' if city == 'Madison' else 'Brown',
            'state': 'WI',
            'zip_code': f"5{random.randint(3000, 4999)}",
            'property_type': prop_type,
            'square_footage': sqft,
            'asking_price': round(asking_price, -3),  # Round to nearest thousand
            'price_per_sqft': round(price_per_sqft, 2),
            'year_built': random.randint(1970, 2020),
            'parking_spaces': random.randint(5, 100),
            'lease_rate_monthly': round(sqft * random.uniform(1.5, 4.0), 2),
            'cap_rate': round(random.uniform(4.5, 8.5), 2),
            'days_on_market': random.randint(1, 365),
            'zoning': random.choice(['C-1', 'C-2', 'C-3', 'I-1', 'M-1']),
            'utilities': 'Electric, Gas, Water, Sewer',
            'listing_status': random.choice(['Active', 'Under Contract', 'Sold']),
            'data_source': 'Demo_LoopNet',
            'collection_date': datetime.utcnow(),
            'data_extraction_date': datetime.utcnow()
        })
    
    return pd.DataFrame(data)

def create_demo_industry_benchmarks():
    """Create demo industry benchmark data"""
    
    industries = [
        'Quick Service Restaurant',
        'Casual Dining Restaurant', 
        'Retail Clothing',
        'Grocery Store',
        'Fitness Center',
        'Hair Salon',
        'Auto Repair',
        'Coffee Shop'
    ]
    
    data = []
    
    for industry in industries:
        for year in [2022, 2023]:
            # Financial benchmarks
            revenue = random.randint(250000, 2000000)
            
            data.append({
                'industry_name': industry,
                'naics_code': f"{random.randint(441, 722)}{random.randint(10, 99)}",
                'benchmark_year': year,
                'average_revenue': revenue,
                'revenue_per_employee': random.randint(45000, 120000),
                'operating_margin_pct': round(random.uniform(8, 25), 1),
                'rent_as_pct_revenue': round(random.uniform(6, 15), 1),
                'labor_cost_pct': round(random.uniform(25, 45), 1),
                'marketing_cost_pct': round(random.uniform(2, 8), 1),
                'inventory_turnover': round(random.uniform(4, 12), 1),
                'average_transaction': round(random.uniform(15, 85), 2),
                'customers_per_day': random.randint(50, 500),
                'failure_rate_pct': round(random.uniform(15, 35), 1),
                'startup_cost_low': random.randint(50000, 150000),
                'startup_cost_high': random.randint(200000, 500000),
                'payback_period_years': round(random.uniform(2, 5), 1),
                'data_source': 'SBA_Demo',
                'report_date': datetime.utcnow(),
                'data_extraction_date': datetime.utcnow()
            })
    
    return pd.DataFrame(data)

def create_demo_employment_projections():
    """Create demo employment projections"""
    
    industries = ['Accommodation and Food Services', 'Retail Trade', 'Healthcare', 'Professional Services', 'Manufacturing', 'Construction']
    areas = ['Wisconsin', 'Milwaukee MSA', 'Madison MSA', 'Green Bay MSA', 'Appleton MSA']
    
    data = []
    
    for industry in industries:
        for area in areas:
            base_employment = random.randint(10000, 200000)
            growth_rate = random.uniform(-2, 8)  # -2% to 8% annual growth
            
            data.append({
                'area_name': area,
                'area_type': 'MSA' if 'MSA' in area else 'State',
                'industry_title': industry,
                'naics_code': f"{random.randint(11, 92)}",
                'projection_period': '2022-2032',
                'base_year': 2022,
                'projected_year': 2032,
                'base_employment': base_employment,
                'projected_employment': round(base_employment * (1 + growth_rate/100)**10),
                'change_numeric': round(base_employment * (growth_rate/100) * 10),
                'change_percent': round(growth_rate, 1),
                'annual_growth_rate': round(growth_rate, 2),
                'openings_due_to_growth': random.randint(500, 5000),
                'openings_due_to_replacement': random.randint(1000, 10000),
                'total_openings': random.randint(1500, 15000),
                'data_source': 'BLS_Demo',
                'projection_date': datetime.utcnow(),
                'data_extraction_date': datetime.utcnow()
            })
    
    return pd.DataFrame(data)

def create_demo_oes_wages():
    """Create demo OES wage data"""
    
    occupations = [
        'Food Service Managers',
        'Retail Salespersons', 
        'Cashiers',
        'Customer Service Representatives',
        'General Managers',
        'Marketing Specialists',
        'Accountants',
        'Administrative Assistants'
    ]
    
    areas = ['Wisconsin', 'Milwaukee MSA', 'Madison MSA', 'Green Bay MSA']
    
    data = []
    
    for occupation in occupations:
        for area in areas:
            median_wage = random.uniform(25000, 85000)
            
            data.append({
                'area_name': area,
                'area_type': 'MSA' if 'MSA' in area else 'State',
                'occupation_title': occupation,
                'soc_code': f"{random.randint(11, 53)}-{random.randint(1000, 9999)}",
                'employment_level': random.randint(500, 25000),
                'employment_per_1000': round(random.uniform(1, 50), 2),
                'median_hourly_wage': round(median_wage / 2080, 2),  # Convert to hourly
                'median_annual_wage': round(median_wage, -2),  # Round to hundreds
                'wage_10th_percentile': round(median_wage * 0.6, 2),
                'wage_25th_percentile': round(median_wage * 0.8, 2),
                'wage_75th_percentile': round(median_wage * 1.3, 2),
                'wage_90th_percentile': round(median_wage * 1.6, 2),
                'location_quotient': round(random.uniform(0.5, 2.0), 2),
                'data_year': 2023,
                'data_source': 'BLS_OES_Demo',
                'survey_date': datetime.utcnow(),
                'data_extraction_date': datetime.utcnow()
            })
    
    return pd.DataFrame(data)

def load_to_bigquery(df, table_id, description):
    """Load DataFrame to BigQuery with proper schema"""
    
    client = bigquery.Client(project="location-optimizer-1")
    
    # Create dataset if needed
    dataset_id = table_id.split('.')[1]
    dataset = bigquery.Dataset(f"location-optimizer-1.{dataset_id}")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_extraction_date"
        ),
        autodetect=True
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        table = client.get_table(table_id)
        logger.info(f"✅ Loaded {table.num_rows} rows to {table_id}")
        return table.num_rows
        
    except Exception as e:
        logger.error(f"❌ Failed to load {table_id}: {e}")
        return 0

def main():
    """Load all Phase 2 demo data"""
    
    print("🚀 Loading Phase 2 Demo Data to BigQuery")
    print("="*60)
    
    total_records = 0
    
    # 1. Commercial Real Estate
    print("🏢 Loading Commercial Real Estate Demo Data...")
    df_real_estate = create_demo_real_estate_data()
    records = load_to_bigquery(
        df_real_estate, 
        "location-optimizer-1.raw_real_estate.commercial_real_estate",
        "Commercial real estate listings"
    )
    total_records += records
    print(f"   ✅ {records} commercial properties loaded")
    
    # 2. Industry Benchmarks
    print("📊 Loading Industry Benchmarks Demo Data...")
    df_benchmarks = create_demo_industry_benchmarks()
    records = load_to_bigquery(
        df_benchmarks,
        "location-optimizer-1.processed_business_data.industry_benchmarks", 
        "Industry financial benchmarks"
    )
    total_records += records
    print(f"   ✅ {records} industry benchmarks loaded")
    
    # 3. Employment Projections
    print("📈 Loading Employment Projections Demo Data...")
    df_projections = create_demo_employment_projections()
    records = load_to_bigquery(
        df_projections,
        "location-optimizer-1.processed_business_data.employment_projections",
        "BLS employment projections"
    )
    total_records += records
    print(f"   ✅ {records} employment projections loaded")
    
    # 4. OES Wages
    print("💰 Loading OES Wages Demo Data...")
    df_wages = create_demo_oes_wages()
    records = load_to_bigquery(
        df_wages,
        "location-optimizer-1.processed_business_data.oes_wages",
        "Occupational wage data"
    )
    total_records += records
    print(f"   ✅ {records} wage records loaded")
    
    print("\n" + "="*60)
    print("📈 PHASE 2 DEMO DATA LOADING COMPLETE")
    print("="*60)
    print(f"Total Records Loaded: {total_records:,}")
    
    if total_records > 0:
        print("✅ All Phase 2 data sources successfully loaded!")
        
        # Show summary
        print("\n📊 Phase 2 Data Summary:")
        print(f"   • Commercial Real Estate: {len(df_real_estate)} properties")
        print(f"   • Industry Benchmarks: {len(df_benchmarks)} benchmark records")
        print(f"   • Employment Projections: {len(df_projections)} industry projections")
        print(f"   • OES Wages: {len(df_wages)} occupation wage records")
        
    else:
        print("❌ No data was loaded successfully")

if __name__ == "__main__":
    main()