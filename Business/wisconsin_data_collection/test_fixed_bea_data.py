#!/usr/bin/env python3
"""
Test Fixed BEA Data Collection
==============================

Test the fixed BEA collector and display the results.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fixed_bea_collector import FixedBEACollector
import pandas as pd

def test_fixed_collector():
    """Test the fixed collector and display results"""
    
    print("🔍 Testing Fixed BEA Consumer Spending Data Collection")
    print("="*60)
    
    # Initialize collector
    collector = FixedBEACollector()
    
    # Test with just 2023 data first
    print("\n1. Testing single year (2023):")
    records_2023 = collector.collect_pce_data_by_year(2023)
    
    print(f"   Collected {len(records_2023)} records for 2023")
    
    # Show first few records
    print("\n   Sample records:")
    for i, record in enumerate(records_2023[:5]):
        print(f"   {i+1}. {record['spending_category']:25} ${record['spending_amount_millions']:>10.2f}M")
    
    # Create structured DataFrame
    df_2023 = collector.create_structured_dataframe(records_2023)
    
    print(f"\n   Structured data columns: {list(df_2023.columns)[:10]}...")
    
    # Show key spending categories
    print("\n2. Key Spending Categories for 2023:")
    key_columns = ['total_pce', 'goods_durable', 'goods_nondurable', 'services', 
                   'food_services_accommodations', 'health_care', 'housing_utilities']
    
    available_columns = [col for col in key_columns if col in df_2023.columns]
    
    for col in available_columns:
        value = df_2023[col].iloc[0] if not df_2023.empty else 0
        print(f"   {col.replace('_', ' ').title():30} ${value:>10.2f}M")
    
    # Test multiple years
    print("\n3. Testing multiple years (2021-2023):")
    all_records = collector.collect_all_years(2021, 2023)
    
    print(f"   Total records collected: {len(all_records)}")
    
    # Create full DataFrame
    df_all = collector.create_structured_dataframe(all_records)
    
    print(f"   Years in dataset: {sorted(df_all['data_year'].unique())}")
    
    # Show year-over-year changes
    print("\n4. Year-over-Year Total PCE:")
    if 'total_pce' in df_all.columns:
        for _, row in df_all.iterrows():
            print(f"   {row['data_year']}: ${row['total_pce']:>10.2f}M "
                  f"(${row['total_pce_per_capita']:>8,.0f} per capita)")
    
    # Calculate growth rates
    if len(df_all) > 1 and 'total_pce' in df_all.columns:
        df_all = df_all.sort_values('data_year')
        df_all['pce_growth_rate'] = df_all['total_pce'].pct_change() * 100
        
        print("\n5. Growth Rates:")
        for _, row in df_all.iterrows():
            if pd.notna(row.get('pce_growth_rate')):
                print(f"   {row['data_year']}: {row['pce_growth_rate']:>6.2f}% growth")
    
    # Show data quality
    print("\n6. Data Quality Check:")
    print(f"   Total spending categories found: {len([col for col in df_all.columns if col.endswith('_pce') or col.endswith('_goods') or col.endswith('_services')])}")
    print(f"   Data quality score: {df_all['data_quality_score'].iloc[0] if 'data_quality_score' in df_all.columns else 'N/A'}")
    
    return df_all

def display_full_data_structure(df):
    """Display the full data structure"""
    
    print("\n7. Full Data Structure (all columns):")
    print("   Spending categories:")
    
    spending_cols = [col for col in df.columns if not col.startswith(('data_', 'state', 'geo_', 'population', 'api_', 'seasonally'))]
    
    for col in sorted(spending_cols):
        if df[col].notna().any():
            sample_value = df[col].iloc[0] if not df.empty else 0
            print(f"   - {col:40} {sample_value:>12.2f}")

if __name__ == "__main__":
    df = test_fixed_collector()
    display_full_data_structure(df)