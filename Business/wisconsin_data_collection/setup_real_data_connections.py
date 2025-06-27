#!/usr/bin/env python3
"""
Setup Real Data Connections
===========================

Configure API keys and data source connections for real data collection.
"""

import os
import logging
from datetime import datetime
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_api_keys():
    """Setup all API keys"""
    
    print("🔑 Setting Up API Keys")
    print("="*50)
    
    # Set API keys
    api_keys = {
        'BEA_API_KEY': '1988DB31-BD6F-4482-A53F-F82AA2BE2E23',
        'LOOPNET_API_KEY': '076f3014a8msh724b957d1c5e85ep125678jsn4335aaddbc1a',
        # Note: BLS doesn't require API key for most public data
        'BLS_API_KEY': None  # BLS public API is generally open
    }
    
    for key, value in api_keys.items():
        if value:
            os.environ[key] = value
            print(f"✅ {key} configured")
        else:
            print(f"ℹ️  {key} not required for public BLS data")
    
    return api_keys

def analyze_wisconsin_gis_sources():
    """Analyze Wisconsin GIS data availability"""
    
    print("\n🗺️  Wisconsin GIS Data Sources Analysis")
    print("="*50)
    
    # Wisconsin has both state-wide and county-specific sources
    gis_sources = {
        'state_wide': {
            'name': 'Wisconsin State Cartographer Office',
            'url': 'https://www.sco.wisc.edu/data-and-services/',
            'coverage': 'Entire state',
            'data_types': ['Parcels', 'Zoning (limited)', 'Land use'],
            'api_available': False,
            'notes': 'Provides links to county data, not centralized'
        },
        'wisconsin_open_data': {
            'name': 'Wisconsin Open Data Portal',
            'url': 'https://data.wisconsin.gov/',
            'coverage': 'State-wide datasets',
            'data_types': ['Some zoning', 'Administrative boundaries'],
            'api_available': True,
            'notes': 'Limited zoning detail, more for boundaries'
        },
        'county_specific': {
            'name': 'Individual County GIS Systems',
            'coverage': 'County by county',
            'major_counties': {
                'Milwaukee': 'https://gis.milwaukee.gov/',
                'Dane': 'https://map.countyofdane.com/',
                'Brown': 'https://gis.co.brown.wi.us/',
                'Waukesha': 'https://www.waukeshacounty.gov/departments/administration/gis/',
                'Winnebago': 'https://gis.co.winnebago.wi.us/'
            },
            'api_available': 'Varies by county',
            'notes': 'Most detailed zoning data, requires individual connections'
        }
    }
    
    print("📍 GIS Data Availability:")
    print("• State-wide centralized zoning: ❌ Not available")
    print("• County-by-county approach: ✅ Required for detailed zoning")
    print("• Major counties with APIs: 5+ counties")
    print("• Smaller counties: May require web scraping or manual data")
    
    return gis_sources

def test_loopnet_api():
    """Test LoopNet API connection"""
    
    print("\n🏢 Testing LoopNet API Connection")
    print("="*50)
    
    # LoopNet API is typically through RapidAPI
    headers = {
        'X-RapidAPI-Key': '076f3014a8msh724b957d1c5e85ep125678jsn4335aaddbc1a',
        'X-RapidAPI-Host': 'loopnet-com.p.rapidapi.com'  # Common RapidAPI host
    }
    
    # Test endpoint (this is a common pattern for RapidAPI LoopNet)
    test_url = "https://loopnet-com.p.rapidapi.com/properties/search"
    
    try:
        # Test with Milwaukee search
        params = {
            'location': 'Milwaukee, WI',
            'propertyType': 'office',
            'limit': 5
        }
        
        response = requests.get(test_url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            print("✅ LoopNet API connection successful!")
            data = response.json()
            print(f"   Sample response keys: {list(data.keys())[:5]}")
            return True
        else:
            print(f"⚠️  LoopNet API returned status {response.status_code}")
            print(f"   Response: {response.text[:200]}...")
            return False
            
    except Exception as e:
        print(f"❌ LoopNet API test failed: {e}")
        print("   Note: API endpoint may need adjustment based on actual LoopNet API documentation")
        return False

def analyze_bls_data_access():
    """Analyze BLS data access options"""
    
    print("\n👥 BLS Data Access Analysis")
    print("="*50)
    
    bls_sources = {
        'employment_projections': {
            'url': 'https://www.bls.gov/emp/data/',
            'api_required': False,
            'data_format': 'CSV/Excel downloads',
            'coverage': 'National and state-level',
            'update_frequency': 'Every 2 years',
            'notes': 'Available as bulk downloads, no API key needed'
        },
        'oes_wages': {
            'url': 'https://www.bls.gov/oes/tables.htm',
            'api_required': False,
            'data_format': 'CSV/Excel downloads',
            'coverage': 'National, state, and MSA level',
            'update_frequency': 'Annual (May data)',
            'notes': 'Available as bulk downloads'
        },
        'bls_api': {
            'url': 'https://api.bls.gov/publicAPI/v2/',
            'api_required': True,
            'registration_url': 'https://data.bls.gov/registrationEngine/',
            'rate_limits': '25 queries/day (unregistered), 500/day (registered)',
            'notes': 'For real-time queries, but bulk downloads often better'
        }
    }
    
    print("📊 BLS Data Access Options:")
    print("• Employment Projections: ✅ Bulk downloads available (no API key)")
    print("• OES Wage Data: ✅ Bulk downloads available (no API key)")  
    print("• Real-time API: Optional (registration recommended for higher limits)")
    
    return bls_sources

def analyze_sba_advocacy_access():
    """Analyze SBA Office of Advocacy data access"""
    
    print("\n📈 SBA Office of Advocacy Data Access")
    print("="*50)
    
    sba_sources = {
        'advocacy_reports': {
            'base_url': 'https://advocacy.sba.gov/resources/research-and-data/',
            'api_available': False,
            'access_method': 'Web scraping + PDF parsing',
            'key_reports': [
                'Small Business GDP Report',
                'Small Business Profiles by State', 
                'Small Business Economic Impact Study',
                'Frequently Asked Questions about Small Business'
            ],
            'data_format': 'PDF reports, some Excel',
            'update_frequency': 'Annual/Semi-annual'
        },
        'sba_data_portal': {
            'url': 'https://data.sba.gov/',
            'api_available': True,
            'access_method': 'CKAN API',
            'data_types': ['Loan data', 'Disaster assistance', 'Contracting'],
            'notes': 'More transactional data, less industry benchmarks'
        },
        'alternative_sources': {
            'ibisworld': 'Premium industry reports (paid)',
            'franchise_associations': 'Industry-specific benchmark data',
            'trade_associations': 'Sector-specific financial benchmarks'
        }
    }
    
    print("📋 SBA Advocacy Data Access:")
    print("• Official Reports: ✅ Available via web scraping")
    print("• Structured API: ❌ Not available for benchmarks")
    print("• Alternative: Web scraping + PDF text extraction")
    print("• Data Quality: High (official government source)")
    
    return sba_sources

def create_implementation_plan():
    """Create implementation plan for real data connections"""
    
    print("\n🎯 Implementation Plan for Real Data Connections")
    print("="*80)
    
    plan = {
        'immediate': [
            '1. Test BEA API with existing key for consumer spending',
            '2. Test LoopNet API with provided key',
            '3. Download BLS bulk data files for employment/wages'
        ],
        'short_term': [
            '4. Implement county GIS collectors for top 5 counties',
            '5. Build SBA Advocacy report scraper',
            '6. Update Phase 2 collectors with real API connections'
        ],
        'ongoing': [
            '7. Set up automated data refresh schedules',
            '8. Expand to additional counties as needed',
            '9. Monitor API rate limits and optimize requests'
        ]
    }
    
    for phase, tasks in plan.items():
        print(f"\n🔹 {phase.upper()} TASKS:")
        for task in tasks:
            print(f"   {task}")
    
    return plan

def main():
    """Main function to analyze and setup data connections"""
    
    print("🚀 REAL DATA CONNECTIONS SETUP & ANALYSIS")
    print("="*80)
    
    # Setup API keys
    api_keys = setup_api_keys()
    
    # Analyze each data source
    gis_sources = analyze_wisconsin_gis_sources()
    loopnet_success = test_loopnet_api()
    bls_sources = analyze_bls_data_access()
    sba_sources = analyze_sba_advocacy_access()
    
    # Create implementation plan
    plan = create_implementation_plan()
    
    print(f"\n✅ SUMMARY")
    print("="*50)
    print("• BEA API: ✅ Key configured")
    print(f"• LoopNet API: {'✅ Working' if loopnet_success else '⚠️ Needs verification'}")
    print("• BLS Data: ✅ Bulk downloads available")
    print("• Wisconsin GIS: ⚠️ County-by-county approach needed")
    print("• SBA Advocacy: ⚠️ Web scraping required")

if __name__ == "__main__":
    main()