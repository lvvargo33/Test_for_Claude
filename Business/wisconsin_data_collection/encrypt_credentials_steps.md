# Steps to Encrypt Your New Credentials

## 1. First, upload the file to this workspace
- In VS Code/GitHub Codespaces, drag and drop `location-optimizer-1-96b6102d3548.json` from your Windows Downloads folder into the `/workspaces/Test_for_Claude/Business/wisconsin_data_collection/` directory
- Or use the upload button in the file explorer

## 2. Once uploaded, run these commands:

```bash
# Navigate to the directory
cd /workspaces/Test_for_Claude/Business/wisconsin_data_collection/

# Create a backup (just in case)
cp location-optimizer-1-96b6102d3548.json location-optimizer-1-96b6102d3548.json.backup

# Encrypt with GPG using the same passphrase
gpg --symmetric --cipher-algo AES256 --output location-optimizer-1-96b6102d3548.json.gpg location-optimizer-1-96b6102d3548.json

# When prompted, enter your passphrase: 49UyfV*#qjfiTR%!

# Verify the encrypted file was created
ls -la location-optimizer-1-96b6102d3548.json.gpg

# Test decryption (to make sure it works)
gpg --decrypt location-optimizer-1-96b6102d3548.json.gpg > test-decrypt.json

# Check if decryption worked
head test-decrypt.json

# If everything looks good, remove the unencrypted files
rm location-optimizer-1-96b6102d3548.json
rm test-decrypt.json
rm location-optimizer-1-96b6102d3548.json.backup
```

## 3. Update .gitignore to prevent accidental commits

Add these lines to your .gitignore:
```
*.json
!package.json
!tsconfig.json
!data_sources.yaml
*credentials*
*service-account*
```

## 4. Test the new credentials
```bash
# Decrypt for testing
gpg --decrypt location-optimizer-1-96b6102d3548.json.gpg > location-optimizer-1-96b6102d3548.json

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json"

# Run connection test
python test_connection.py
```