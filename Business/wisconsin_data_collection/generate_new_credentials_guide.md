# Generate New Service Account Credentials

## Steps to Generate New Credentials:

1. **Go to Google Cloud Console**
   - Visit: https://console.cloud.google.com
   - Select project: `location-optimizer-1`

2. **Navigate to Service Accounts**
   - In the left menu, go to "IAM & Admin" > "Service Accounts"
   - Or directly: https://console.cloud.google.com/iam-admin/serviceaccounts?project=location-optimizer-1

3. **Find Your Service Account**
   - Look for: `lv-account@location-optimizer-1.iam.gserviceaccount.com`
   - Click on the service account email

4. **Generate New Key**
   - Go to the "Keys" tab
   - Click "Add Key" > "Create new key"
   - Select "JSON" format
   - Click "Create"
   - A JSON file will download automatically

5. **Replace Old Credentials**
   - Save the downloaded JSON file as `location-optimizer-1-new.json`
   - Move it to your project directory
   - Update your code to use the new file

## Command Line Alternative (if you have gcloud CLI):

```bash
# List service accounts
gcloud iam service-accounts list --project=location-optimizer-1

# Create new key
gcloud iam service-accounts keys create location-optimizer-1-new.json \
  --iam-account=lv-account@location-optimizer-1.iam.gserviceaccount.com \
  --project=location-optimizer-1
```

## After Getting New Credentials:

1. **Encrypt the new file** (for security):
   ```bash
   gpg --symmetric --cipher-algo AES256 location-optimizer-1-new.json
   ```

2. **Test the new credentials**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="location-optimizer-1-new.json"
   python test_connection.py
   ```

3. **Update your environment** to use the new credentials file