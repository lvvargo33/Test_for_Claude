#!/usr/bin/env python3
"""Delete and recreate traffic table"""

from google.cloud import bigquery

client = bigquery.Client(project="location-optimizer-1")

# Delete existing table
table_id = "location-optimizer-1.raw_traffic.traffic_counts"
client.delete_table(table_id, not_found_ok=True)
print(f"Deleted table {table_id}")

# The traffic collector will create it with proper schema
print("Table deleted. The traffic collector will recreate it with proper schema.")