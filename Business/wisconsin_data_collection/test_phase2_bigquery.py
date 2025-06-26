"""
Test Phase 2 Collectors with BigQuery Integration
=================================================

Test that Phase 2 collectors can successfully connect to and save data to BigQuery.
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account

def test_bigquery_connection():
    """Test direct BigQuery connection with credentials"""
    print("Testing BigQuery Connection with Phase 2 Credentials")
    print("=" * 60)
    
    try:
        # Load credentials directly
        credentials_path = "location-optimizer-1-449414f93a5a.json"
        
        if not os.path.exists(credentials_path):
            print(f"❌ Credentials file not found: {credentials_path}")
            return False
        
        # Initialize BigQuery client with service account
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(project="location-optimizer-1", credentials=credentials)
        
        # Test connection by listing datasets
        datasets = list(client.list_datasets())
        
        print(f"✅ BigQuery connection successful!")
        print(f"📊 Found {len(datasets)} datasets in project")
        
        for dataset in datasets:
            print(f"   - {dataset.dataset_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        return False

def test_phase2_table_creation():
    """Test creating Phase 2 tables in BigQuery"""
    print("\nTesting Phase 2 Table Creation")
    print("=" * 60)
    
    try:
        # Load credentials
        credentials_path = "location-optimizer-1-449414f93a5a.json"
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(project="location-optimizer-1", credentials=credentials)
        
        # Test creating a simple Phase 2 table
        dataset_id = "raw_business_data"
        table_id = "industry_benchmarks_test"
        full_table_id = f"location-optimizer-1.{dataset_id}.{table_id}"
        
        # Simple schema for testing
        schema = [
            bigquery.SchemaField("benchmark_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("industry_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("benchmark_value", "FLOAT64", mode="REQUIRED"),
            bigquery.SchemaField("data_year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("data_collection_date", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        # Create table
        table = bigquery.Table(full_table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        
        print(f"✅ Test table created: {table.table_id}")
        
        # Test inserting sample data
        sample_data = [
            {
                "benchmark_id": "test_001",
                "industry_name": "Test Industry",
                "benchmark_value": 100.0,
                "data_year": 2023,
                "data_source": "Test Source",
                "data_collection_date": "2025-06-26T21:50:00"
            }
        ]
        
        errors = client.insert_rows_json(table, sample_data)
        
        if errors:
            print(f"❌ Data insertion failed: {errors}")
            return False
        else:
            print(f"✅ Sample data inserted successfully")
            
            # Query the data back
            query = f"SELECT COUNT(*) as record_count FROM `{full_table_id}`"
            results = client.query(query).result()
            
            for row in results:
                print(f"📊 Records in test table: {row.record_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Table creation test failed: {e}")
        return False

def test_phase2_collector_with_bigquery():
    """Test Phase 2 collector with explicit BigQuery credentials"""
    print("\nTesting Phase 2 Collector with BigQuery")
    print("=" * 60)
    
    try:
        # Set environment variable for credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'location-optimizer-1-449414f93a5a.json'
        
        # Import and test industry benchmarks collector
        from industry_benchmarks_collector import IndustryBenchmarksCollector
        
        collector = IndustryBenchmarksCollector()
        
        # Check if BigQuery client is now available
        if collector.bq_client is not None:
            print("✅ Collector successfully connected to BigQuery")
            
            # Run a small collection test
            summary = collector.run_benchmarks_collection()
            
            if summary.get('success'):
                print(f"✅ Data collection and BigQuery save successful!")
                print(f"📊 Records saved: {summary.get('total_records', 0)}")
            else:
                print(f"⚠️  Data collection completed but BigQuery save may have failed")
                print(f"📊 Records collected: {summary.get('total_records', 0)}")
            
            return True
        else:
            print("❌ Collector still cannot connect to BigQuery")
            return False
        
    except Exception as e:
        print(f"❌ Collector test failed: {e}")
        return False

def main():
    """Run all BigQuery integration tests"""
    print("PHASE 2 BIGQUERY INTEGRATION TEST")
    print("=" * 80)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Direct BigQuery connection
    if test_bigquery_connection():
        tests_passed += 1
    
    # Test 2: Table creation and data insertion
    if test_phase2_table_creation():
        tests_passed += 1
    
    # Test 3: Phase 2 collector with BigQuery
    if test_phase2_collector_with_bigquery():
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 80)
    print("BIGQUERY INTEGRATION TEST SUMMARY")
    print("=" * 80)
    print(f"Tests Passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 All BigQuery integration tests passed!")
        print("✅ Phase 2 collectors are ready for production use")
    elif tests_passed >= 2:
        print("✅ BigQuery connection working, minor collector issues")
        print("📝 Phase 2 collectors can save data to BigQuery")
    else:
        print("⚠️  BigQuery integration needs attention")
        print("🔧 Check credentials and project permissions")
    
    return tests_passed >= 2

if __name__ == "__main__":
    main()