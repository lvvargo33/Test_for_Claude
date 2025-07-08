#!/bin/bash
# Setup BigQuery credentials for the Universal Business Analysis Engine

# Set the path to the decrypted credentials file
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-96b6102d3548.json"

echo "✓ BigQuery credentials configured"
echo "  GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS"

# Verify the file exists
if [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "✓ Credentials file exists"
else
    echo "✗ Credentials file not found at: $GOOGLE_APPLICATION_CREDENTIALS"
    echo "  Please decrypt the credentials file first using:"
    echo "  gpg --decrypt location-optimizer-1-96b6102d3548.json.gpg > location-optimizer-1-96b6102d3548.json"
fi