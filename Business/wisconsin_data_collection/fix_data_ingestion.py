# Quick fix for date format issues in wisconsin_data_ingestion.py
import sys
import re

# Read the current file
with open('wisconsin_data_ingestion.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add proper date conversion in the load_to_bigquery method
# Find the line where we create business_df and add date conversion
old_pattern = r"business_df = pd\.DataFrame\(\["
new_pattern = """# Convert date strings to proper datetime objects
                for b in businesses:
                    if isinstance(b.registration_date, str):
                        try:
                            b.registration_date = pd.to_datetime(b.registration_date).date()
                        except:
                            b.registration_date = None
                
                business_df = pd.DataFrame(["""

content = re.sub(old_pattern, new_pattern, content)

# Write the fixed file
with open('wisconsin_data_ingestion_fixed.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Created fixed version: wisconsin_data_ingestion_fixed.py")