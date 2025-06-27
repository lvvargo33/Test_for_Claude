#!/usr/bin/env python3
"""
Wisconsin County Population Analysis
===================================

Analyze Wisconsin counties by population to prioritize GIS data collection.
"""

import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_wisconsin_counties():
    """Analyze Wisconsin counties by population and economic importance"""
    
    print("📊 Wisconsin County Population & Economic Analysis")
    print("="*70)
    
    # Wisconsin county data (2020 Census + economic data)
    wisconsin_counties = [
        # Top 10 by population (covers ~70% of state population)
        {'county': 'Milwaukee', 'population': 939489, 'rank': 1, 'metro_area': 'Milwaukee MSA', 'gis_available': True, 'priority': 'Critical'},
        {'county': 'Dane', 'population': 561504, 'rank': 2, 'metro_area': 'Madison MSA', 'gis_available': True, 'priority': 'Critical'},
        {'county': 'Waukesha', 'population': 406978, 'rank': 3, 'metro_area': 'Milwaukee MSA', 'gis_available': True, 'priority': 'High'},
        {'county': 'Brown', 'population': 268740, 'rank': 4, 'metro_area': 'Green Bay MSA', 'gis_available': True, 'priority': 'High'},
        {'county': 'Racine', 'population': 195408, 'rank': 5, 'metro_area': 'Milwaukee MSA', 'gis_available': False, 'priority': 'High'},
        {'county': 'Outagamie', 'population': 188557, 'rank': 6, 'metro_area': 'Appleton MSA', 'gis_available': True, 'priority': 'Medium'},
        {'county': 'Winnebago', 'population': 171730, 'rank': 7, 'metro_area': 'Appleton MSA', 'gis_available': True, 'priority': 'Medium'},
        {'county': 'Rock', 'population': 163687, 'rank': 8, 'metro_area': 'Janesville MSA', 'gis_available': False, 'priority': 'Medium'},
        {'county': 'Kenosha', 'population': 169561, 'rank': 9, 'metro_area': 'Chicago MSA', 'gis_available': False, 'priority': 'Medium'},
        {'county': 'Washington', 'population': 135404, 'rank': 10, 'metro_area': 'Milwaukee MSA', 'gis_available': False, 'priority': 'Medium'},
        
        # Next 10 by population  
        {'county': 'La Crosse', 'population': 120784, 'rank': 11, 'metro_area': 'La Crosse MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Eau Claire', 'population': 105710, 'rank': 12, 'metro_area': 'Eau Claire MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Sheboygan', 'population': 118034, 'rank': 13, 'metro_area': 'Sheboygan MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Marathon', 'population': 138013, 'rank': 14, 'metro_area': 'Wausau MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Fond du Lac', 'population': 104154, 'rank': 15, 'metro_area': None, 'gis_available': False, 'priority': 'Low'},
        {'county': 'Jefferson', 'population': 84900, 'rank': 16, 'metro_area': 'Milwaukee MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'St. Croix', 'population': 93536, 'rank': 17, 'metro_area': 'Minneapolis MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Walworth', 'population': 106478, 'rank': 18, 'metro_area': 'Chicago MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Calumet', 'population': 52442, 'rank': 19, 'metro_area': 'Appleton MSA', 'gis_available': False, 'priority': 'Low'},
        {'county': 'Ozaukee', 'population': 91503, 'rank': 20, 'metro_area': 'Milwaukee MSA', 'gis_available': False, 'priority': 'Low'}
    ]
    
    df = pd.DataFrame(wisconsin_counties)
    
    # Calculate cumulative population coverage
    total_wi_population = 5893718  # 2020 Census
    df['pop_percentage'] = (df['population'] / total_wi_population * 100).round(1)
    df['cumulative_percentage'] = df['pop_percentage'].cumsum().round(1)
    
    # Priority tiers analysis
    print("\n🎯 COUNTY PRIORITIZATION FOR GIS DATA COLLECTION")
    print("-" * 60)
    
    priority_analysis = {
        'Critical': df[df['priority'] == 'Critical'],
        'High': df[df['priority'] == 'High'], 
        'Medium': df[df['priority'] == 'Medium'],
        'Low': df[df['priority'] == 'Low']
    }
    
    for priority, counties in priority_analysis.items():
        if len(counties) > 0:
            total_pop = counties['population'].sum()
            total_pct = counties['pop_percentage'].sum()
            gis_available = counties['gis_available'].sum()
            
            print(f"\n📍 {priority.upper()} PRIORITY ({len(counties)} counties)")
            print(f"   Population: {total_pop:,} ({total_pct:.1f}% of state)")
            print(f"   GIS Available: {gis_available}/{len(counties)} counties")
            
            for _, county in counties.iterrows():
                gis_status = "✅ GIS" if county['gis_available'] else "❌ No GIS"
                metro = county['metro_area'] if county['metro_area'] else "Non-metro"
                print(f"   • {county['county']}: {county['population']:,} ({county['pop_percentage']:.1f}%) | {gis_status} | {metro}")
    
    # GIS implementation recommendation
    print(f"\n🗺️  GIS IMPLEMENTATION STRATEGY")
    print("-" * 60)
    
    gis_available = df[df['gis_available'] == True]
    gis_priority_order = gis_available.sort_values('population', ascending=False)
    
    print("Recommended GIS Implementation Order:")
    
    phase = 1
    cumulative_coverage = 0
    
    for _, county in gis_priority_order.iterrows():
        cumulative_coverage += county['pop_percentage']
        print(f"   Phase {phase}: {county['county']} County")
        print(f"      Population: {county['population']:,} ({county['pop_percentage']:.1f}%)")
        print(f"      Cumulative Coverage: {cumulative_coverage:.1f}% of Wisconsin")
        print(f"      Metro Area: {county['metro_area']}")
        print(f"      Business Impact: {county['priority']} priority")
        print()
        phase += 1
    
    print(f"📊 GIS-Enabled Counties Cover: {cumulative_coverage:.1f}% of Wisconsin population")
    
    return df, gis_priority_order

def create_data_source_inventory():
    """Create comprehensive data source inventory with loading frequency"""
    
    print("\n📋 DATA SOURCE INVENTORY & LOADING FREQUENCY")
    print("="*80)
    
    inventory = {
        'real_data_sources': [
            {
                'source': 'Traffic Data (WisDOT)',
                'table': 'raw_traffic.traffic_counts',
                'data_type': 'Real',
                'coverage': '1996-2023 (27 years)',
                'current_records': 9981,
                'update_frequency': 'Annual',
                'loading_strategy': 'Automated Annual',
                'api_available': True,
                'priority': 'High',
                'notes': 'Highway traffic counts, reliable WisDOT data'
            },
            {
                'source': 'Census Demographics',
                'table': 'raw_business_data.census_demographics', 
                'data_type': 'Real',
                'coverage': 'ACS 5-Year data',
                'current_records': 288,
                'update_frequency': 'Annual (September)',
                'loading_strategy': 'Automated Annual',
                'api_available': True,
                'priority': 'High',
                'notes': 'Census ACS data, very reliable'
            },
            {
                'source': 'Business Registrations (DFI)',
                'table': 'raw_business_data.dfi_business_registrations',
                'data_type': 'Real',
                'coverage': 'Current registrations',
                'current_records': 43,
                'update_frequency': 'Weekly',
                'loading_strategy': 'Automated Weekly',
                'api_available': False,
                'priority': 'Medium',
                'notes': 'Wisconsin DFI data, web scraping required'
            },
            {
                'source': 'SBA Loans',
                'table': 'raw_business_data.sba_loan_approvals',
                'data_type': 'Real',
                'coverage': 'Historical loan data',
                'current_records': 2904,
                'update_frequency': 'Quarterly',
                'loading_strategy': 'Automated Quarterly',
                'api_available': True,
                'priority': 'Medium',
                'notes': 'SBA FOIA data, reliable API'
            },
            {
                'source': 'BLS Employment Data',
                'table': 'raw_business_data.bls_laus_data',
                'data_type': 'Real', 
                'coverage': '2015-2023 (8 years)',
                'current_records': 10368,
                'update_frequency': 'Monthly',
                'loading_strategy': 'Automated Monthly',
                'api_available': True,
                'priority': 'High',
                'notes': 'Bureau of Labor Statistics, very reliable'
            },
            {
                'source': 'BEA Consumer Spending',
                'table': 'raw_business_data.consumer_spending',
                'data_type': 'Real (Partial)',
                'coverage': '2019-2023 (5 years)',
                'current_records': 5,
                'update_frequency': 'Annual',
                'loading_strategy': 'Automated Annual',
                'api_available': True,
                'priority': 'Medium',
                'notes': 'BEA API working but limited data returned'
            }
        ],
        
        'demo_data_sources': [
            {
                'source': 'Zoning Data',
                'table': 'raw_business_data.zoning_data',
                'data_type': 'Demo',
                'coverage': '8 counties demo data',
                'current_records': 2890,
                'update_frequency': 'Quarterly (when real)',
                'loading_strategy': 'Ad Hoc by County',
                'api_available': 'Varies by County',
                'priority': 'High',
                'notes': 'Need county-by-county GIS implementation'
            },
            {
                'source': 'Commercial Real Estate',
                'table': 'raw_real_estate.commercial_real_estate',
                'data_type': 'Demo',
                'coverage': '150 demo properties',
                'current_records': 150,
                'update_frequency': 'Weekly (when real)',
                'loading_strategy': 'Automated Weekly',
                'api_available': 'API Key Issues',
                'priority': 'High',
                'notes': 'LoopNet API subscription needs fixing'
            },
            {
                'source': 'Industry Benchmarks',
                'table': 'processed_business_data.industry_benchmarks',
                'data_type': 'Demo',
                'coverage': '16 demo benchmarks',
                'current_records': 16,
                'update_frequency': 'Semi-Annual',
                'loading_strategy': 'Ad Hoc Manual',
                'api_available': False,
                'priority': 'Medium',
                'notes': 'SBA reports require web scraping'
            },
            {
                'source': 'Employment Projections',
                'table': 'processed_business_data.employment_projections',
                'data_type': 'Demo',
                'coverage': '30 demo projections',
                'current_records': 30,
                'update_frequency': 'Biennial',
                'loading_strategy': 'Manual Download',
                'api_available': False,
                'priority': 'Medium',
                'notes': 'BLS bulk downloads available'
            },
            {
                'source': 'OES Wages',
                'table': 'processed_business_data.oes_wages',
                'data_type': 'Demo',
                'coverage': '32 demo wage records',
                'current_records': 32,
                'update_frequency': 'Annual (May)',
                'loading_strategy': 'Manual Download', 
                'api_available': False,
                'priority': 'Medium',
                'notes': 'BLS bulk downloads available'
            }
        ],
        
        'mixed_sources': [
            {
                'source': 'Business Licenses',
                'table': 'raw_business_data.business_licenses',
                'data_type': 'Mixed',
                'coverage': '90 mixed records',
                'current_records': 90,
                'update_frequency': 'Monthly',
                'loading_strategy': 'Ad Hoc by City',
                'api_available': 'Varies',
                'priority': 'Medium',
                'notes': 'City-specific APIs vary widely'
            },
            {
                'source': 'OpenStreetMap Businesses',
                'table': 'raw_business_data.osm_businesses',
                'data_type': 'Real (Unknown Quality)',
                'coverage': '9,542 business records',
                'current_records': 9542,
                'update_frequency': 'Monthly',
                'loading_strategy': 'Automated Monthly',
                'api_available': True,
                'priority': 'Low',
                'notes': 'OSM data quality varies by region'
            }
        ]
    }
    
    # Display inventory by category
    for category, sources in inventory.items():
        category_name = category.replace('_', ' ').title()
        print(f"\n🗂️  {category_name.upper()}")
        print("-" * 50)
        
        for source in sources:
            status_emoji = {
                'Real': '✅',
                'Demo': '⚠️ ',
                'Mixed': '🔄',
                'Real (Partial)': '🟡',
                'Real (Unknown Quality)': '❓'
            }.get(source['data_type'], '❓')
            
            print(f"{status_emoji} {source['source']}")
            print(f"   Records: {source['current_records']:,} | {source['data_type']} | {source['update_frequency']}")
            print(f"   Strategy: {source['loading_strategy']} | Priority: {source['priority']}")
            print(f"   Notes: {source['notes']}")
            print()
    
    # Loading frequency summary
    print(f"\n⏰ RECOMMENDED LOADING FREQUENCIES")
    print("-" * 50)
    
    frequency_groups = {}
    for category, sources in inventory.items():
        for source in sources:
            freq = source['loading_strategy']
            if freq not in frequency_groups:
                frequency_groups[freq] = []
            frequency_groups[freq].append(source['source'])
    
    for frequency, sources in frequency_groups.items():
        print(f"\n{frequency}:")
        for source in sources:
            print(f"   • {source}")
    
    return inventory

def main():
    """Main analysis function"""
    
    print("🔍 WISCONSIN DATA COLLECTION STRATEGIC ANALYSIS")
    print("="*80)
    
    # County analysis
    counties_df, gis_order = analyze_wisconsin_counties()
    
    # Data source inventory
    inventory = create_data_source_inventory()
    
    # Summary recommendations
    print(f"\n🎯 STRATEGIC RECOMMENDATIONS")
    print("="*60)
    print("1. IMMEDIATE: Fix LoopNet API subscription for real estate data")
    print("2. SHORT-TERM: Implement Milwaukee & Dane County GIS (covers 25% of state)")
    print("3. MEDIUM-TERM: Add Brown & Waukesha counties (covers 40% of state)")
    print("4. ONGOING: Automate high-frequency sources (traffic, employment, demographics)")
    print("5. QUARTERLY: Manual collection of industry benchmarks and projections")

if __name__ == "__main__":
    main()