"""
Mock Authentication Setup for Testing
====================================

Sets up mock authentication for testing the Location Optimizer system
when Google Cloud credentials are not available.
"""

import os
import json
import tempfile
from pathlib import Path

def create_mock_credentials():
    """Create mock Google Cloud credentials for testing"""
    
    mock_credentials = {
        "type": "service_account",
        "project_id": "location-optimizer-1",
        "private_key_id": "mock_key_id",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMOCK_PRIVATE_KEY\n-----END PRIVATE KEY-----\n",
        "client_email": "mock-service@location-optimizer-1.iam.gserviceaccount.com",
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mock-service%40location-optimizer-1.iam.gserviceaccount.com"
    }
    
    # Create temporary credentials file
    temp_dir = tempfile.mkdtemp()
    creds_file = os.path.join(temp_dir, "mock_credentials.json")
    
    with open(creds_file, 'w') as f:
        json.dump(mock_credentials, f, indent=2)
    
    return creds_file

def setup_mock_environment():
    """Setup environment for mock testing"""
    
    # Create mock credentials
    creds_file = create_mock_credentials()
    
    # Set environment variables
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_file
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'location-optimizer-1'
    
    print("🔧 Mock Authentication Setup")
    print("=" * 40)
    print(f"✅ Mock credentials created: {creds_file}")
    print(f"✅ GOOGLE_APPLICATION_CREDENTIALS set")
    print(f"✅ GOOGLE_CLOUD_PROJECT set to: location-optimizer-1")
    print("\n⚠️  NOTE: This is for testing only. BigQuery operations will fail.")
    print("   Use this to test the application structure and data models.")
    
    return creds_file

if __name__ == "__main__":
    creds_file = setup_mock_environment()
    print(f"\n🎯 Mock environment ready!")
    print(f"   Credentials file: {creds_file}")
    print(f"   Run your tests now with mock authentication.")