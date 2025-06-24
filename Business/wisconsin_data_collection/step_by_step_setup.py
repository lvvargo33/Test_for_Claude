"""
Step-by-Step Setup Guide
=======================

Interactive guide to set up the complete Location Optimizer system.
"""

import os
import sys
import json
from pathlib import Path

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("🔍 Checking Prerequisites")
    print("=" * 30)
    
    # Check Python packages
    packages = [
        'google.cloud.bigquery',
        'pydantic', 
        'yaml',
        'pandas',
        'requests'
    ]
    
    missing_packages = []
    for package in packages:
        try:
            __import__(package.replace('.', '/').replace('/', '.'))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("   Run: pip install -r requirements.txt")
        return False
    
    # Check files
    required_files = [
        'models.py',
        'base_collector.py',
        'wisconsin_collector.py',
        'data_sources.yaml',
        'setup_bigquery.py',
        'main.py'
    ]
    
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
            return False
    
    print("✅ All prerequisites met!")
    return True

def setup_authentication():
    """Guide user through authentication setup"""
    print("\n🔐 Authentication Setup")
    print("=" * 25)
    
    # Check if already authenticated
    creds_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_env and os.path.exists(creds_env):
        print(f"✅ Found existing credentials: {creds_env}")
        return test_authentication()
    
    print("Choose authentication method:")
    print("1. Service Account JSON file (recommended)")
    print("2. Use mock authentication for testing")
    print("3. Skip authentication (for testing only)")
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == "1":
        return setup_service_account()
    elif choice == "2":
        return setup_mock_auth()
    elif choice == "3":
        print("⚠️  Skipping authentication - BigQuery operations will fail")
        return True
    else:
        print("❌ Invalid choice")
        return False

def setup_service_account():
    """Set up service account authentication"""
    print("\n📋 Service Account Setup:")
    print("1. Go to: https://console.cloud.google.com/iam-admin/serviceaccounts?project=location-optimizer-1")
    print("2. Create service account with BigQuery Admin role")
    print("3. Download JSON key file")
    print("4. Enter the path to your JSON file below")
    
    json_path = input("\nEnter path to service account JSON file: ").strip()
    
    if not json_path:
        print("❌ No path provided")
        return False
    
    if not os.path.exists(json_path):
        print(f"❌ File not found: {json_path}")
        return False
    
    # Validate and set up
    try:
        with open(json_path, 'r') as f:
            creds = json.load(f)
        
        if creds.get('type') != 'service_account':
            print("❌ Not a valid service account file")
            return False
        
        # Set environment variable
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_path
        
        # Create .env file
        with open('.env', 'w') as f:
            f.write(f"GOOGLE_APPLICATION_CREDENTIALS={json_path}\n")
            f.write("GOOGLE_CLOUD_PROJECT=location-optimizer-1\n")
        
        print("✅ Service account credentials configured")
        return test_authentication()
        
    except Exception as e:
        print(f"❌ Error setting up credentials: {e}")
        return False

def setup_mock_auth():
    """Set up mock authentication for testing"""
    print("\n🧪 Setting up mock authentication...")
    
    try:
        from setup_mock_auth import setup_mock_environment
        setup_mock_environment()
        print("✅ Mock authentication ready")
        print("⚠️  Note: BigQuery operations will fail, but you can test the architecture")
        return True
    except Exception as e:
        print(f"❌ Error setting up mock auth: {e}")
        return False

def test_authentication():
    """Test authentication"""
    print("\n🧪 Testing authentication...")
    
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project="location-optimizer-1")
        
        # Test query
        query = "SELECT 1 as test"
        results = client.query(query)
        list(results)
        
        print("✅ Authentication successful!")
        return True
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("   You can still test with mock data")
        return False

def setup_bigquery():
    """Set up BigQuery infrastructure"""
    print("\n🏗️ Setting up BigQuery Infrastructure")
    print("=" * 35)
    
    try:
        print("Creating datasets and tables...")
        
        # Import and run setup
        from setup_bigquery import BigQuerySetup
        setup = BigQuerySetup()
        setup.setup_all_tables()
        
        print("✅ BigQuery infrastructure created!")
        return True
        
    except Exception as e:
        print(f"❌ BigQuery setup failed: {e}")
        print("   You can still test with sample data")
        return False

def test_data_collection():
    """Test data collection with sample data"""
    print("\n📊 Testing Data Collection")
    print("=" * 25)
    
    try:
        print("Running sample data collection...")
        
        # Import and test collector
        from wisconsin_collector import WisconsinDataCollector
        collector = WisconsinDataCollector()
        
        # Collect sample data
        businesses = collector.collect_business_registrations(days_back=7)
        loans = collector.collect_sba_loans(days_back=30)
        licenses = collector.collect_business_licenses(days_back=7)
        
        print(f"✅ Collected {len(businesses)} businesses")
        print(f"✅ Collected {len(loans)} SBA loans")
        print(f"✅ Collected {len(licenses)} licenses")
        print(f"✅ Total: {len(businesses) + len(loans) + len(licenses)} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Data collection test failed: {e}")
        return False

def generate_sample_prospects():
    """Generate sample prospect list"""
    print("\n🎯 Generating Sample Prospects")
    print("=" * 30)
    
    try:
        # Create sample prospect data
        prospects = [
            {
                'source': 'SBA_LOAN',
                'business_name': 'Milwaukee Restaurant Group LLC',
                'city': 'Milwaukee',
                'contact_approach': 'Phone: Congratulations on SBA approval!',
                'value_indicator': 250000,
                'lead_quality': 'HIGH'
            },
            {
                'source': 'NEW_BUSINESS',
                'business_name': 'Madison Fitness Center',
                'city': 'Madison',
                'contact_approach': 'Email: Welcome to Wisconsin! Free location analysis',
                'value_indicator': 85,
                'lead_quality': 'MEDIUM'
            },
            {
                'source': 'SBA_LOAN',
                'business_name': 'Green Bay Auto Services',
                'city': 'Green Bay',
                'contact_approach': 'Phone: Location optimization consultation',
                'value_indicator': 150000,
                'lead_quality': 'MEDIUM'
            }
        ]
        
        # Save to CSV
        import pandas as pd
        df = pd.DataFrame(prospects)
        df.to_csv('sample_prospects.csv', index=False)
        
        print("✅ Sample prospects generated: sample_prospects.csv")
        print("\n📋 Top Prospects:")
        for i, prospect in enumerate(prospects, 1):
            print(f"   {i}. {prospect['business_name']} ({prospect['city']})")
            print(f"      Quality: {prospect['lead_quality']}, Approach: {prospect['contact_approach']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Prospect generation failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🎯 Location Optimizer - Complete Setup Guide")
    print("=" * 50)
    print("This guide will walk you through setting up the complete system.")
    print("=" * 50)
    
    steps = [
        ("Prerequisites Check", check_prerequisites),
        ("Authentication Setup", setup_authentication),
        ("BigQuery Infrastructure", setup_bigquery),
        ("Data Collection Test", test_data_collection),
        ("Sample Prospects Generation", generate_sample_prospects)
    ]
    
    for step_name, step_func in steps:
        print(f"\n🔄 {step_name}")
        print("=" * 50)
        
        try:
            success = step_func()
            if success:
                print(f"✅ {step_name} completed successfully")
            else:
                print(f"⚠️  {step_name} completed with warnings")
                
                if "Authentication" in step_name or "BigQuery" in step_name:
                    continue_anyway = input("Continue anyway? (y/n): ").strip().lower()
                    if continue_anyway != 'y':
                        print("❌ Setup cancelled")
                        return False
        except Exception as e:
            print(f"❌ {step_name} failed: {e}")
            continue_anyway = input("Continue anyway? (y/n): ").strip().lower()
            if continue_anyway != 'y':
                print("❌ Setup cancelled")
                return False
    
    # Final summary
    print("\n" + "=" * 50)
    print("🎉 SETUP COMPLETE!")
    print("=" * 50)
    
    print("\n📋 What you can do now:")
    print("   1. Test architecture: python test_architecture_offline.py")
    print("   2. Setup BigQuery: python main.py --setup")
    print("   3. Collect data: python main.py --collect")
    print("   4. Run analysis: python main.py --analyze")
    print("   5. Export prospects: python main.py --export-prospects")
    
    print("\n📁 Key files created:")
    print("   • sample_prospects.csv - Sample prospect list")
    print("   • .env - Environment configuration (if using service account)")
    print("   • location_optimizer.log - Application logs")
    
    print("\n📊 Next steps for production:")
    print("   1. Replace sample data sources with real APIs")
    print("   2. Set up automated daily collection")
    print("   3. Implement client dashboard")
    print("   4. Add Illinois as second state")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🚀 Ready to build your $300K-500K location optimization business!")
    sys.exit(0 if success else 1)