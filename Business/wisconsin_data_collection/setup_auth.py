"""
Authentication Setup Helper
==========================

Helps set up Google Cloud authentication for the Location Optimizer system.
"""

import os
import json
from pathlib import Path

def setup_service_account_auth(json_file_path: str):
    """
    Set up service account authentication
    
    Args:
        json_file_path: Path to the downloaded service account JSON file
    """
    
    print("🔧 Setting up Service Account Authentication")
    print("=" * 45)
    
    # Check if file exists
    if not os.path.exists(json_file_path):
        print(f"❌ File not found: {json_file_path}")
        print("   Please download your service account key from Google Cloud Console")
        return False
    
    # Validate JSON file
    try:
        with open(json_file_path, 'r') as f:
            creds = json.load(f)
        
        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        for field in required_fields:
            if field not in creds:
                print(f"❌ Invalid service account file: missing '{field}'")
                return False
        
        print(f"✅ Valid service account file for project: {creds['project_id']}")
        
    except json.JSONDecodeError:
        print("❌ Invalid JSON file")
        return False
    
    # Set environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path
    
    # Create .env file for persistence
    env_content = f"""# Google Cloud Authentication
GOOGLE_APPLICATION_CREDENTIALS={json_file_path}
GOOGLE_CLOUD_PROJECT=location-optimizer-1
BIGQUERY_PROJECT_ID=location-optimizer-1
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print(f"✅ Environment variable set: GOOGLE_APPLICATION_CREDENTIALS")
    print(f"✅ Created .env file for persistence")
    
    # Test authentication
    print("\n🧪 Testing authentication...")
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=creds['project_id'])
        
        # Test with a simple query
        query = "SELECT 1 as test"
        results = client.query(query)
        list(results)  # Execute the query
        
        print("✅ Authentication successful!")
        return True
        
    except Exception as e:
        print(f"❌ Authentication test failed: {e}")
        return False

def setup_alternative_auth():
    """
    Set up alternative authentication methods
    """
    print("🔧 Alternative Authentication Methods")
    print("=" * 40)
    
    print("Method 1: Application Default Credentials")
    print("  1. Install Google Cloud SDK")
    print("  2. Run: gcloud auth application-default login")
    print("  3. Follow the browser authentication flow")
    
    print("\nMethod 2: Environment Variables")
    print("  1. Set GOOGLE_APPLICATION_CREDENTIALS to your JSON file path")
    print("  2. Export the variable in your shell")
    
    print("\nMethod 3: Google Cloud Shell/Compute Engine")
    print("  Authentication is automatic when running on Google Cloud")

def main():
    """Main setup function"""
    print("🎯 Location Optimizer - Authentication Setup")
    print("=" * 50)
    
    print("Choose your authentication method:")
    print("1. Service Account JSON file (recommended)")
    print("2. Application Default Credentials")
    print("3. Show alternative methods")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == "1":
        json_path = input("Enter path to your service account JSON file: ").strip()
        if json_path:
            success = setup_service_account_auth(json_path)
            if success:
                print("\n🎉 Authentication setup complete!")
                print("\nNext steps:")
                print("  1. Run: python main.py --setup")
                print("  2. Run: python main.py --collect")
            else:
                print("\n❌ Authentication setup failed. Please try again.")
        else:
            print("❌ No file path provided.")
    
    elif choice == "2":
        print("\nPlease run the following command in your terminal:")
        print("  gcloud auth application-default login")
        print("\nThen test with: python test_connection.py")
    
    elif choice == "3":
        setup_alternative_auth()
    
    else:
        print("❌ Invalid choice. Please run the script again.")

if __name__ == "__main__":
    main()