#!/usr/bin/env python3
"""
Analyze All Data Sources in BigQuery
====================================

Examines all data sources to determine:
1. Whether data is real or sample/demo data
2. Min/max years of data coverage
3. Data quality and completeness
"""

import logging
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_data_source(client, table_id, source_name):
    """Analyze a specific data source"""
    
    try:
        table = client.get_table(table_id)
        
        if table.num_rows == 0:
            return {
                'source': source_name,
                'table': table_id,
                'status': 'EMPTY',
                'rows': 0,
                'data_type': 'No Data',
                'years_min': None,
                'years_max': None,
                'sample_data': []
            }
        
        # Get basic info
        result = {
            'source': source_name,
            'table': table_id,
            'status': 'HAS DATA',
            'rows': table.num_rows,
            'size_mb': round(table.num_bytes / 1024 / 1024, 2)
        }
        
        # Determine year fields to check
        year_fields = []
        schema_fields = [field.name.lower() for field in table.schema]
        
        possible_year_fields = [
            'year', 'data_year', 'measurement_year', 'aadt_year', 'benchmark_year',
            'base_year', 'projected_year', 'collection_year'
        ]
        
        for field in possible_year_fields:
            if field in schema_fields:
                year_fields.append(field)
        
        # Build query to analyze data
        year_queries = []
        for field in year_fields[:2]:  # Limit to first 2 year fields
            year_queries.append(f"MIN({field}) as min_{field}, MAX({field}) as max_{field}")
        
        if year_queries:
            year_query_str = ", " + ", ".join(year_queries)
        else:
            year_query_str = ""
        
        # Query for analysis
        query = f"""
        SELECT 
            COUNT(*) as total_rows
            {year_query_str}
        FROM `{table_id}`
        """
        
        stats = list(client.query(query))[0]
        
        # Extract year ranges
        years_min = None
        years_max = None
        
        for field in year_fields[:2]:
            min_val = getattr(stats, f'min_{field}', None)
            max_val = getattr(stats, f'max_{field}', None)
            
            if min_val and max_val:
                if years_min is None or min_val < years_min:
                    years_min = min_val
                if years_max is None or max_val > years_max:
                    years_max = max_val
        
        result['years_min'] = years_min
        result['years_max'] = years_max
        
        # Get sample data to determine if real or demo
        sample_query = f"""
        SELECT *
        FROM `{table_id}`
        LIMIT 3
        """
        
        sample_rows = []
        for row in client.query(sample_query):
            sample_rows.append(dict(row))
        
        result['sample_data'] = sample_rows
        
        # Determine if data is real or demo based on patterns
        result['data_type'] = determine_data_type(source_name, sample_rows, table_id)
        
        return result
        
    except Exception as e:
        logger.error(f"Error analyzing {table_id}: {e}")
        return {
            'source': source_name,
            'table': table_id,
            'status': 'ERROR',
            'error': str(e)
        }

def determine_data_type(source_name, sample_rows, table_id):
    """Determine if data is real or demo based on content analysis"""
    
    if not sample_rows:
        return "No Data"
    
    # Check for obvious demo indicators
    demo_indicators = [
        'demo', 'sample', 'test', 'placeholder', 'example',
        'Demo_', 'SBA_Demo', 'BLS_Demo', 'Demo_LoopNet'
    ]
    
    for row in sample_rows:
        for key, value in row.items():
            if isinstance(value, str):
                for indicator in demo_indicators:
                    if indicator in value:
                        return "Demo Data"
    
    # Source-specific analysis
    if 'traffic' in source_name.lower():
        # Check if traffic data looks real (WisDOT data)
        if any('I-' in str(row.get('highway_name', '')) or 'USH' in str(row.get('highway_name', '')) for row in sample_rows):
            return "Real Data (WisDOT)"
        return "Demo Data"
    
    elif 'consumer_spending' in source_name.lower():
        # Consumer spending - check for realistic amounts
        for row in sample_rows:
            data_source = row.get('data_source', '')
            if 'BEA_DEMO' in str(data_source):
                return "Demo Data"
        return "Possibly Real Data"
    
    elif 'zoning' in source_name.lower():
        # Zoning data - check for realistic parcel IDs
        for row in sample_rows:
            parcel_id = row.get('parcel_id', '')
            if isinstance(parcel_id, str) and '-' in parcel_id and len(parcel_id.split('-')) == 2:
                return "Demo Data"
        return "Real Data"
    
    elif 'commercial_real_estate' in source_name.lower():
        # Real estate - check data source
        for row in sample_rows:
            data_source = row.get('data_source', '')
            if 'Demo_LoopNet' in str(data_source):
                return "Demo Data"
        return "Possibly Real Data"
    
    elif 'industry_benchmarks' in source_name.lower():
        # Industry benchmarks
        for row in sample_rows:
            data_source = row.get('data_source', '')
            if 'SBA_Demo' in str(data_source):
                return "Demo Data"
        return "Possibly Real Data"
    
    elif 'employment' in source_name.lower() or 'wages' in source_name.lower():
        # Employment/wage data
        for row in sample_rows:
            data_source = row.get('data_source', '')
            if any(demo in str(data_source) for demo in ['Demo', 'BLS_Demo', 'BLS_OES_Demo']):
                return "Demo Data"
        return "Possibly Real Data"
    
    elif 'census' in source_name.lower() or 'demographic' in source_name.lower():
        # Census data is typically real from API
        return "Real Data (Census API)"
    
    elif 'sba' in source_name.lower():
        # SBA loan data
        return "Real Data (SBA API)"
    
    elif 'business' in source_name.lower() and 'license' in source_name.lower():
        # Business licenses
        return "Mixed (Real + Sample)"
    
    elif 'dfi' in source_name.lower() or 'business_entities' in source_name.lower():
        # Business registrations
        return "Real Data (DFI)"
    
    elif 'osm' in source_name.lower():
        # OpenStreetMap data
        return "Real Data (OpenStreetMap)"
    
    elif 'bls' in source_name.lower():
        # BLS employment data
        return "Real Data (BLS API)"
    
    else:
        return "Unknown"

def main():
    """Analyze all data sources"""
    
    print("🔍 COMPREHENSIVE DATA SOURCE ANALYSIS")
    print("="*80)
    
    client = bigquery.Client(project="location-optimizer-1")
    
    # Define all data sources to analyze
    data_sources = [
        # Phase 1 Sources
        {
            'name': 'Traffic Data',
            'table': 'location-optimizer-1.raw_traffic.traffic_counts',
            'category': 'Phase 1'
        },
        {
            'name': 'Zoning Data', 
            'table': 'location-optimizer-1.raw_business_data.zoning_data',
            'category': 'Phase 1'
        },
        {
            'name': 'Consumer Spending',
            'table': 'location-optimizer-1.raw_business_data.consumer_spending',
            'category': 'Phase 1'
        },
        {
            'name': 'Census Demographics',
            'table': 'location-optimizer-1.raw_business_data.census_demographics',
            'category': 'Phase 1'
        },
        
        # Phase 2 Sources
        {
            'name': 'Commercial Real Estate',
            'table': 'location-optimizer-1.raw_real_estate.commercial_real_estate',
            'category': 'Phase 2'
        },
        {
            'name': 'Industry Benchmarks',
            'table': 'location-optimizer-1.processed_business_data.industry_benchmarks',
            'category': 'Phase 2'
        },
        {
            'name': 'Employment Projections',
            'table': 'location-optimizer-1.processed_business_data.employment_projections',
            'category': 'Phase 2'
        },
        {
            'name': 'OES Wages',
            'table': 'location-optimizer-1.processed_business_data.oes_wages',
            'category': 'Phase 2'
        },
        
        # Supporting Sources
        {
            'name': 'Business Registrations (DFI)',
            'table': 'location-optimizer-1.raw_business_data.dfi_business_registrations',
            'category': 'Supporting'
        },
        {
            'name': 'SBA Loans',
            'table': 'location-optimizer-1.raw_business_data.sba_loan_approvals',
            'category': 'Supporting'
        },
        {
            'name': 'Business Licenses',
            'table': 'location-optimizer-1.raw_business_data.business_licenses',
            'category': 'Supporting'
        },
        {
            'name': 'OpenStreetMap Businesses',
            'table': 'location-optimizer-1.raw_business_data.osm_businesses',
            'category': 'Supporting'
        },
        {
            'name': 'BLS Employment Data',
            'table': 'location-optimizer-1.raw_business_data.bls_laus_data',
            'category': 'Supporting'
        }
    ]
    
    # Analyze each source
    results = []
    for source in data_sources:
        print(f"\n📊 Analyzing {source['name']}...")
        result = analyze_data_source(client, source['table'], source['name'])
        result['category'] = source['category']
        results.append(result)
    
    # Generate comprehensive report
    print("\n" + "="*80)
    print("📈 DATA SOURCE ANALYSIS REPORT")
    print("="*80)
    
    # Summary by category
    categories = ['Phase 1', 'Phase 2', 'Supporting']
    
    for category in categories:
        category_results = [r for r in results if r.get('category') == category]
        
        print(f"\n🏷️  {category.upper()} DATA SOURCES")
        print("-" * 50)
        
        for result in category_results:
            if result['status'] == 'HAS DATA':
                years_str = ""
                if result.get('years_min') and result.get('years_max'):
                    if result['years_min'] == result['years_max']:
                        years_str = f" | {result['years_min']}"
                    else:
                        years_str = f" | {result['years_min']}-{result['years_max']}"
                
                data_type = result.get('data_type', 'Unknown')
                
                # Color coding for data type
                if 'Real Data' in data_type:
                    type_indicator = "✅ REAL"
                elif 'Demo Data' in data_type:
                    type_indicator = "⚠️  DEMO"
                elif 'Mixed' in data_type:
                    type_indicator = "🔄 MIXED"
                else:
                    type_indicator = "❓ UNKNOWN"
                
                print(f"   {result['source']:<30} | {result['rows']:>6,} rows | {type_indicator:<12} {years_str}")
                if 'Demo Data' in data_type:
                    print(f"      └─ Contains sample/placeholder data")
                
            elif result['status'] == 'EMPTY':
                print(f"   {result['source']:<30} | {result['rows']:>6} rows | ❌ EMPTY")
            else:
                print(f"   {result['source']:<30} | ERROR: {result.get('error', 'Unknown')}")
    
    # Overall summary
    print(f"\n📊 OVERALL SUMMARY")
    print("-" * 50)
    
    total_sources = len(results)
    real_data_sources = len([r for r in results if 'Real Data' in r.get('data_type', '')])
    demo_data_sources = len([r for r in results if 'Demo Data' in r.get('data_type', '')])
    mixed_sources = len([r for r in results if 'Mixed' in r.get('data_type', '')])
    empty_sources = len([r for r in results if r['status'] == 'EMPTY'])
    
    print(f"Total Data Sources: {total_sources}")
    print(f"✅ Real Data Sources: {real_data_sources}")
    print(f"⚠️  Demo Data Sources: {demo_data_sources}")
    print(f"🔄 Mixed Sources: {mixed_sources}")
    print(f"❌ Empty Sources: {empty_sources}")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS")
    print("-" * 50)
    
    if demo_data_sources > 0:
        print("• Replace demo data with real API connections:")
        for result in results:
            if 'Demo Data' in result.get('data_type', ''):
                print(f"  - {result['source']}")
    
    if empty_sources > 0:
        print("• Populate empty data sources:")
        for result in results:
            if result['status'] == 'EMPTY':
                print(f"  - {result['source']}")
    
    if real_data_sources > 0:
        print("• Real data sources ready for analysis:")
        for result in results:
            if 'Real Data' in result.get('data_type', ''):
                print(f"  - {result['source']}")

if __name__ == "__main__":
    main()