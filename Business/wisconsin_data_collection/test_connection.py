"""
Test Google Cloud BigQuery Connection
===================================

Quick test to verify BigQuery connectivity and project access.
"""

import os
import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Forbidden

def test_bigquery_connection():
    """Test BigQuery connection and project access"""
    
    project_id = "location-optimizer-1"
    
    print("🔍 Testing BigQuery Connection")
    print("=" * 40)
    print(f"Project ID: {project_id}")
    
    try:
        # Initialize client
        client = bigquery.Client(project=project_id)
        print("✅ BigQuery client initialized successfully")
        
        # Test project access
        datasets = list(client.list_datasets())
        print(f"✅ Project access confirmed. Found {len(datasets)} existing datasets:")
        
        for dataset in datasets:
            print(f"   📁 {dataset.dataset_id}")
        
        # Test query permissions
        test_query = "SELECT 1 as test_value"
        query_job = client.query(test_query)
        results = query_job.result()
        print("✅ Query permissions confirmed")
        
        return True
        
    except Forbidden as e:
        print(f"❌ Permission denied: {e}")
        print("🔧 You need to authenticate with Google Cloud.")
        print("   Run: gcloud auth application-default login")
        return False
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("🔧 Check your Google Cloud credentials and project access.")
        return False

def check_environment():
    """Check environment setup"""
    print("\n🔧 Environment Check")
    print("=" * 40)
    
    # Check for credentials
    creds_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_env:
        print(f"✅ GOOGLE_APPLICATION_CREDENTIALS: {creds_env}")
    else:
        print("⚠️  GOOGLE_APPLICATION_CREDENTIALS not set")
        print("   This is OK if using gcloud auth application-default login")
    
    # Check Python packages
    try:
        import google.cloud.bigquery
        print("✅ google-cloud-bigquery installed")
    except ImportError:
        print("❌ google-cloud-bigquery not installed")
        return False
    
    try:
        import pydantic
        print("✅ pydantic installed")
    except ImportError:
        print("❌ pydantic not installed")
        return False
        
    try:
        import yaml
        print("✅ PyYAML installed")
    except ImportError:
        print("❌ PyYAML not installed")
        return False
    
    return True

if __name__ == "__main__":
    print("🎯 Location Optimizer - Connection Test")
    print("=" * 50)
    
    # Check environment
    env_ok = check_environment()
    
    if env_ok:
        # Test BigQuery connection
        connection_ok = test_bigquery_connection()
        
        if connection_ok:
            print("\n🎉 All tests passed! Ready to proceed with setup.")
        else:
            print("\n❌ Connection test failed. Please fix authentication issues.")
            sys.exit(1)
    else:
        print("\n❌ Environment check failed. Please install missing packages.")
        sys.exit(1)