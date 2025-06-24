#!/usr/bin/env python3
"""
Setup Google Cloud Scheduler for weekly DFI collection
More reliable than cron for production use
"""

def setup_cloud_scheduler():
    print('☁️  Google Cloud Scheduler Setup')
    print('=' * 40)
    
    print('''
🚀 Google Cloud Scheduler (Recommended for Production)

This is the most reliable option for production use:

1. **Create a Cloud Function:**
   - Upload weekly_dfi_collection.py as a Cloud Function
   - Set memory: 1GB, timeout: 540 seconds
   - Add BigQuery permissions

2. **Create Cloud Scheduler Job:**
   - Schedule: "0 7 * * 1" (every Monday 7 AM)
   - Target: HTTP trigger to your Cloud Function
   - Timezone: America/Chicago (Central Time)

3. **Setup Commands:**
   ```bash
   # Deploy Cloud Function
   gcloud functions deploy weekly-dfi-collection \\
     --runtime python312 \\
     --trigger-http \\
     --entry-point weekly_dfi_collection \\
     --memory 1GB \\
     --timeout 540s \\
     --set-env-vars GOOGLE_APPLICATION_CREDENTIALS=credentials.json

   # Create Scheduler Job  
   gcloud scheduler jobs create http dfi-weekly-collection \\
     --schedule="0 7 * * 1" \\
     --uri=https://us-central1-location-optimizer-1.cloudfunctions.net/weekly-dfi-collection \\
     --http-method=GET \\
     --time-zone=America/Chicago
   ```

✅ **Benefits:**
   • Runs reliably in the cloud
   • Automatic error handling and retries
   • Email notifications on failures
   • Scales automatically
   • No server maintenance needed

💰 **Cost:** ~$0.10/month (very cheap)
    ''')

def simple_manual_option():
    print('📱 Simple Manual Option')
    print('=' * 30)
    
    print('''
If you prefer to keep it simple, you can:

🗓️  **Run manually each Monday:**
   ```bash
   cd /workspaces/Test_for_Claude/Business/wisconsin_data_collection
   export GOOGLE_APPLICATION_CREDENTIALS="location-optimizer-1-449414f93a5a.json"
   python3 weekly_dfi_collection.py
   ```

⏰ **Set a phone reminder:** 
   • Every Monday at 7:00 AM
   • "Run Wisconsin business data collection"
   • Takes 2 minutes to run

📊 **Results:**
   • Fresh Wisconsin business data every week
   • New prospects added to BigQuery
   • Ready for your consulting outreach
    ''')

if __name__ == "__main__":
    setup_cloud_scheduler()
    print()
    simple_manual_option()