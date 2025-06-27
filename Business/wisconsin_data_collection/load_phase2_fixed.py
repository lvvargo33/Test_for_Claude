#!/usr/bin/env python3
"""
Load Phase 2 Data with Fixed Schemas
====================================

Loads Phase 2 data with proper BigQuery schema handling.
"""

import logging
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import time

# Import collectors
from real_estate_collector import WisconsinRealEstateCollector
from industry_benchmarks_collector import IndustryBenchmarksCollector  
from enhanced_employment_collector import EnhancedEmploymentCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_real_estate_data():
    """Load real estate data with proper schema"""
    print("🏢 Loading Commercial Real Estate Data")
    print("-" * 40)
    
    collector = WisconsinRealEstateCollector()
    result = collector.run_real_estate_collection()
    
    if result.get('success'):
        print(f"   ✅ SUCCESS: {result['total_records']} records")
        return result['total_records']
    else:
        # Try manual loading with schema fixes
        logger.info("Attempting manual real estate data loading...")
        
        # Get sample data
        county_records = []
        for county in ['Milwaukee']:
            county_records.extend(collector.collect_county_property_records(county))
        
        loopnet_records = collector.collect_loopnet_data(['Milwaukee'], ['office'])
        
        all_records = county_records + loopnet_records
        if not all_records:
            print("   ❌ No data collected")
            return 0
            
        # Convert to DataFrame
        df = pd.DataFrame([record.dict() for record in all_records])
        
        # Add required timestamp field
        df['data_extraction_date'] = datetime.utcnow()
        
        # Load to BigQuery
        client = bigquery.Client(project="location-optimizer-1")
        table_id = "location-optimizer-1.raw_real_estate.commercial_real_estate"
        
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
            print(f"   ✅ SUCCESS: {len(df)} records loaded manually")
            return len(df)
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            return 0

def load_industry_benchmarks():
    """Load industry benchmarks with proper schema"""
    print("📊 Loading Industry Benchmarks Data")
    print("-" * 40)
    
    collector = IndustryBenchmarksCollector()
    result = collector.run_benchmarks_collection()
    
    if result.get('success'):
        print(f"   ✅ SUCCESS: {result['total_records']} records")
        return result['total_records']
    else:
        # Try manual loading
        logger.info("Attempting manual benchmarks data loading...")
        
        # Get sample data
        sba_data = collector.collect_sba_benchmarks()
        franchise_data = collector.collect_franchise_benchmarks()
        industry_data = collector.collect_industry_report_benchmarks()
        
        all_records = sba_data + franchise_data + industry_data
        if not all_records:
            print("   ❌ No data collected")
            return 0
            
        # Convert to DataFrame
        df = pd.DataFrame([record.dict() for record in all_records])
        
        # Add required timestamp field
        df['data_extraction_date'] = datetime.utcnow()
        
        # Load to BigQuery
        client = bigquery.Client(project="location-optimizer-1")
        table_id = "location-optimizer-1.processed_business_data.industry_benchmarks"
        
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
            print(f"   ✅ SUCCESS: {len(df)} records loaded manually")
            return len(df)
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            return 0

def load_employment_data():
    """Load employment projections and wages with proper schema"""
    print("👥 Loading Employment Data (Projections + Wages)")
    print("-" * 40)
    
    collector = EnhancedEmploymentCollector()
    result = collector.run_enhanced_employment_collection()
    
    if result.get('success'):
        print(f"   ✅ SUCCESS: {result['employment_projections']} projections, {result['wage_records']} wages")
        return result['employment_projections'] + result['wage_records']
    else:
        # Try manual loading
        logger.info("Attempting manual employment data loading...")
        
        # Get sample data
        projections = collector.collect_employment_projections()
        wages = collector.collect_oes_wage_data()
        
        total_loaded = 0
        
        # Load projections
        if projections:
            df_proj = pd.DataFrame([record.dict() for record in projections])
            df_proj['data_extraction_date'] = datetime.utcnow()
            
            client = bigquery.Client(project="location-optimizer-1")
            table_id = "location-optimizer-1.processed_business_data.employment_projections"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extraction_date"
                ),
                autodetect=True
            )
            
            try:
                job = client.load_table_from_dataframe(df_proj, table_id, job_config=job_config)
                job.result()
                print(f"   ✅ Projections: {len(df_proj)} records")
                total_loaded += len(df_proj)
            except Exception as e:
                print(f"   ❌ Projections failed: {e}")
        
        # Load wages
        if wages:
            df_wages = pd.DataFrame([record.dict() for record in wages])
            df_wages['data_extraction_date'] = datetime.utcnow()
            
            client = bigquery.Client(project="location-optimizer-1")
            table_id = "location-optimizer-1.processed_business_data.oes_wages"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="data_extraction_date"
                ),
                autodetect=True
            )
            
            try:
                job = client.load_table_from_dataframe(df_wages, table_id, job_config=job_config)
                job.result()
                print(f"   ✅ Wages: {len(df_wages)} records")
                total_loaded += len(df_wages)
            except Exception as e:
                print(f"   ❌ Wages failed: {e}")
        
        return total_loaded

def main():
    """Load all Phase 2 data sources"""
    
    print("🚀 Phase 2 Data Loading with Fixed Schemas")
    print("="*60)
    
    total_records = 0
    start_time = time.time()
    
    # Load each data source
    total_records += load_real_estate_data()
    time.sleep(2)  # Brief pause between loads
    
    total_records += load_industry_benchmarks()
    time.sleep(2)
    
    total_records += load_employment_data()
    
    processing_time = time.time() - start_time
    
    print("\n" + "="*60)
    print("📈 PHASE 2 LOADING SUMMARY")
    print("="*60)
    print(f"Total Records Loaded: {total_records:,}")
    print(f"Processing Time: {processing_time:.1f} seconds")
    
    if total_records > 0:
        print("✅ Phase 2 data loading completed successfully!")
        
        # Verify in BigQuery
        client = bigquery.Client(project="location-optimizer-1")
        tables_to_check = [
            "location-optimizer-1.raw_real_estate.commercial_real_estate",
            "location-optimizer-1.processed_business_data.industry_benchmarks", 
            "location-optimizer-1.processed_business_data.employment_projections",
            "location-optimizer-1.processed_business_data.oes_wages"
        ]
        
        print("\n📊 BigQuery Table Status:")
        for table_id in tables_to_check:
            try:
                table = client.get_table(table_id)
                table_name = table_id.split('.')[-1]
                print(f"   {table_name}: {table.num_rows:,} rows")
            except Exception:
                table_name = table_id.split('.')[-1]
                print(f"   {table_name}: Not found")
    else:
        print("❌ No data was loaded successfully")

if __name__ == "__main__":
    main()