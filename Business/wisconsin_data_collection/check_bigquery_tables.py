#!/usr/bin/env python3
"""Check BigQuery tables and compare with data sources"""

from google.cloud import bigquery
import yaml
import json
from datetime import datetime

# Load credentials
client = bigquery.Client(project="location-optimizer-1")

# Load data sources config
with open("data_sources.yaml", "r") as f:
    data_sources = yaml.safe_load(f)

print("=" * 80)
print("BIGQUERY DATA INVENTORY REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# Get all datasets
datasets = list(client.list_datasets())
print(f"\nFound {len(datasets)} datasets:")

all_tables = {}
for dataset in datasets:
    dataset_id = dataset.dataset_id
    tables = list(client.list_tables(dataset.reference))
    all_tables[dataset_id] = [table.table_id for table in tables]
    print(f"\n📁 {dataset_id} ({len(tables)} tables)")
    for table in tables:
        # Get table info
        table_ref = dataset.reference.table(table.table_id)
        table_obj = client.get_table(table_ref)
        row_count = table_obj.num_rows
        size_mb = table_obj.num_bytes / 1024 / 1024 if table_obj.num_bytes else 0
        print(f"   - {table.table_id}: {row_count:,} rows, {size_mb:.1f} MB")

# Expected tables from data_sources.yaml
expected_tables = data_sources['bigquery']['tables']

print("\n" + "=" * 80)
print("DATA SOURCE ANALYSIS")
print("=" * 80)

# Phase 1 Data Sources
print("\n📊 PHASE 1 DATA SOURCES (Core Success Factors)")
print("-" * 40)

phase1_sources = {
    "Traffic Data": {
        "source": "WisDOT Traffic Counts",
        "expected_table": "traffic_counts",
        "dataset": "raw_traffic"
    },
    "Zoning Data": {
        "source": "County GIS Sources",
        "expected_table": "zoning_data",
        "dataset": "raw_business_data"
    },
    "Consumer Spending": {
        "source": "BEA Regional Data",
        "expected_table": "consumer_spending",
        "dataset": "raw_business_data"
    },
    "Census Demographics": {
        "source": "Census ACS 5-Year",
        "expected_table": "census_demographics",
        "dataset": "raw_census"
    }
}

# Phase 2 Data Sources
print("\n🏢 PHASE 2 DATA SOURCES (Market Intelligence)")
print("-" * 40)

phase2_sources = {
    "Commercial Real Estate": {
        "source": "County Property Records + LoopNet",
        "expected_table": "commercial_real_estate",
        "dataset": "raw_real_estate"
    },
    "Industry Benchmarks": {
        "source": "SBA/Franchise/Industry Reports",
        "expected_table": "industry_benchmarks",
        "dataset": "processed_business_data"
    },
    "Employment Projections": {
        "source": "BLS Employment Projections",
        "expected_table": "employment_projections",
        "dataset": "processed_business_data"
    },
    "OES Wages": {
        "source": "BLS OES Wage Data",
        "expected_table": "oes_wages",
        "dataset": "processed_business_data"
    }
}

# Other Data Sources
print("\n🗂️ OTHER DATA SOURCES")
print("-" * 40)

other_sources = {
    "Business Registrations": {
        "source": "Wisconsin DFI",
        "expected_table": "business_entities",
        "dataset": "raw_business_data"
    },
    "SBA Loans": {
        "source": "SBA FOIA Data",
        "expected_table": "sba_loan_approvals",
        "dataset": "raw_sba_data"
    },
    "Business Licenses": {
        "source": "City/County Sources",
        "expected_table": "business_licenses",
        "dataset": "raw_business_licenses"
    }
}

# Check status
def check_table_status(expected_table, dataset):
    """Check if table exists in any dataset"""
    for ds, tables in all_tables.items():
        if expected_table in tables:
            return f"✅ Found in {ds}"
    return f"❌ Not found (expected in {dataset})"

print("\n" + "=" * 80)
print("STATUS SUMMARY")
print("=" * 80)

all_sources = {**phase1_sources, **phase2_sources, **other_sources}
missing_count = 0

for category, sources in [("PHASE 1", phase1_sources), ("PHASE 2", phase2_sources), ("OTHER", other_sources)]:
    print(f"\n{category}:")
    for name, info in sources.items():
        status = check_table_status(info['expected_table'], info['dataset'])
        print(f"  {name}: {status}")
        if "❌" in status:
            missing_count += 1

print(f"\n📈 SUMMARY: {len(all_sources) - missing_count}/{len(all_sources)} data sources loaded")

# Create missing sources report
missing_sources = []
for name, info in all_sources.items():
    status = check_table_status(info['expected_table'], info['dataset'])
    if "❌" in status:
        missing_sources.append({
            "name": name,
            "source": info['source'],
            "expected_table": info['expected_table'],
            "expected_dataset": info['dataset']
        })

if missing_sources:
    print("\n" + "=" * 80)
    print("MISSING DATA SOURCES TO LOAD")
    print("=" * 80)
    for ms in missing_sources:
        print(f"\n📍 {ms['name']}")
        print(f"   Source: {ms['source']}")
        print(f"   Expected: {ms['expected_dataset']}.{ms['expected_table']}")

# Save report
report = {
    "generated": datetime.now().isoformat(),
    "total_datasets": len(datasets),
    "total_tables": sum(len(tables) for tables in all_tables.values()),
    "data_sources_configured": len(all_sources),
    "data_sources_loaded": len(all_sources) - missing_count,
    "missing_sources": missing_sources,
    "all_tables": all_tables
}

with open("bigquery_inventory_report.json", "w") as f:
    json.dump(report, f, indent=2)

print(f"\n✅ Full report saved to: bigquery_inventory_report.json")