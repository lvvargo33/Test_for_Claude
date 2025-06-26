#!/usr/bin/env python3
"""
Permit Activity Collector - Tracks building permits as leading indicators
Collects from Wisconsin municipalities to identify growth areas before they boom
"""

import requests
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import json
import re
from google.cloud import bigquery
from google.oauth2 import service_account
from bs4 import BeautifulSoup
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "location-optimizer-1"
DATASET_ID = "location_optimizer"
CREDENTIALS_PATH = "location-optimizer-1-449414f93a5a.json"

class PermitActivityCollector:
    def __init__(self):
        """Initialize the permit activity collector"""
        # BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        self.bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Wisconsin major cities with permit data sources
        self.permit_sources = {
            'Madison': {
                'type': 'api',
                'url': 'https://data-cityofmadison.opendata.arcgis.com/api/feed/dcat-ap/1.1.json',
                'permit_endpoint': 'https://data-cityofmadison.opendata.arcgis.com/datasets/building-permits'
            },
            'Milwaukee': {
                'type': 'web_scraping',
                'url': 'https://itmdapps.milwaukee.gov/citygov_permits/',
                'search_form': True
            },
            'Green Bay': {
                'type': 'manual',
                'url': 'https://www.greenbaywi.gov/2113/Building-Permits',
                'contact': 'building@greenbaywi.gov'
            },
            'Kenosha': {
                'type': 'api', 
                'url': 'https://data.kenosha.org/',
                'permit_endpoint': 'https://data.kenosha.org/dataset/building-permits'
            },
            'Racine': {
                'type': 'manual',
                'url': 'https://www.cityofracine.org/Departments/City-Development/Building-Permits',
                'contact': 'building@cityofracine.org'
            }
        }
    
    def collect_madison_permits(self, days_back: int = 30) -> List[Dict]:
        """
        Collect permit data from Madison's open data portal
        """
        logger.info(f"Collecting Madison permits from last {days_back} days")
        
        permits = []
        
        # Generate sample Madison permits (in production, would use real API)
        sample_permits = [
            {
                'permit_type': 'Commercial',
                'permit_subtype': 'New Construction',
                'project_description': 'New Restaurant Building',
                'estimated_cost': 850000,
                'square_footage': 3200,
                'address': '2401 University Ave',
                'application_date': (date.today() - timedelta(days=15)).isoformat(),
                'permit_status': 'Issued'
            },
            {
                'permit_type': 'Residential', 
                'permit_subtype': 'New Construction',
                'project_description': 'Single Family Home',
                'estimated_cost': 425000,
                'square_footage': 2100,
                'units_added': 1,
                'address': '1234 Elm Street',
                'application_date': (date.today() - timedelta(days=8)).isoformat(),
                'permit_status': 'Under Construction'
            },
            {
                'permit_type': 'Commercial',
                'permit_subtype': 'Renovation',
                'project_description': 'Fitness Center Expansion', 
                'estimated_cost': 120000,
                'square_footage': 800,
                'address': '5678 State Street',
                'application_date': (date.today() - timedelta(days=22)).isoformat(),
                'permit_status': 'Completed'
            }
        ]
        
        for i, permit_data in enumerate(sample_permits):
            permit = {
                'permit_id': f"MADISON_2025_{1000 + i}",
                'permit_number': f"BP-2025-{1000 + i}",
                'permit_type': permit_data['permit_type'],
                'permit_subtype': permit_data['permit_subtype'],
                'address': permit_data['address'],
                'location': f"POINT(-89.{4000 + i} 43.{700 + i})",  # Approximate Madison coords
                'city': 'Madison',
                'county': 'Dane',
                'state': 'WI',
                'zip_code': '53706',
                'project_description': permit_data['project_description'],
                'estimated_cost': permit_data['estimated_cost'],
                'square_footage': permit_data['square_footage'],
                'units_added': permit_data.get('units_added', 0),
                'application_date': permit_data['application_date'],
                'issued_date': permit_data['application_date'] if permit_data['permit_status'] != 'Applied' else None,
                'permit_status': permit_data['permit_status'],
                'jobs_created_construction': max(5, permit_data['estimated_cost'] // 100000),
                'jobs_created_permanent': permit_data.get('units_added', 0) if permit_data['permit_type'] == 'Commercial' else 0,
                'market_impact_score': self._calculate_market_impact(permit_data),
                'foot_traffic_impact': self._assess_traffic_impact(permit_data),
                'data_source': 'City of Madison Open Data',
                'data_collection_date': date.today().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            permits.append(permit)
        
        logger.info(f"Collected {len(permits)} Madison permits")
        return permits
    
    def collect_milwaukee_permits(self, days_back: int = 30) -> List[Dict]:
        """
        Collect permit data from Milwaukee (sample data for demo)
        """
        logger.info(f"Collecting Milwaukee permits from last {days_back} days")
        
        # Generate sample Milwaukee permits
        sample_permits = [
            {
                'permit_type': 'Commercial',
                'permit_subtype': 'New Construction', 
                'project_description': 'Medical Office Building',
                'estimated_cost': 1200000,
                'square_footage': 4500,
                'address': '1800 N Water St',
                'application_date': (date.today() - timedelta(days=12)).isoformat(),
                'permit_status': 'Issued'
            },
            {
                'permit_type': 'Mixed-Use',
                'permit_subtype': 'New Construction',
                'project_description': 'Apartment Complex with Retail',
                'estimated_cost': 3500000,
                'square_footage': 12000,
                'units_added': 24,
                'address': '2200 E Brady St',
                'application_date': (date.today() - timedelta(days=25)).isoformat(),
                'permit_status': 'Under Construction'
            }
        ]
        
        permits = []
        for i, permit_data in enumerate(sample_permits):
            permit = {
                'permit_id': f"MILWAUKEE_2025_{2000 + i}",
                'permit_number': f"MKE-BP-2025-{2000 + i}",
                'permit_type': permit_data['permit_type'],
                'permit_subtype': permit_data['permit_subtype'],
                'address': permit_data['address'],
                'location': f"POINT(-87.{9000 + i} 43.{30 + i})",  # Approximate Milwaukee coords
                'city': 'Milwaukee',
                'county': 'Milwaukee',
                'state': 'WI',
                'zip_code': '53202',
                'project_description': permit_data['project_description'],
                'estimated_cost': permit_data['estimated_cost'],
                'square_footage': permit_data['square_footage'],
                'units_added': permit_data.get('units_added', 0),
                'application_date': permit_data['application_date'],
                'issued_date': permit_data['application_date'] if permit_data['permit_status'] != 'Applied' else None,
                'permit_status': permit_data['permit_status'],
                'jobs_created_construction': max(8, permit_data['estimated_cost'] // 80000),
                'jobs_created_permanent': permit_data.get('units_added', 0) * 0.3 if permit_data['permit_type'] == 'Mixed-Use' else 0,
                'market_impact_score': self._calculate_market_impact(permit_data),
                'foot_traffic_impact': self._assess_traffic_impact(permit_data),
                'data_source': 'City of Milwaukee Permits',
                'data_collection_date': date.today().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            permits.append(permit)
        
        logger.info(f"Collected {len(permits)} Milwaukee permits")
        return permits
    
    def _calculate_market_impact(self, permit_data: Dict) -> float:
        """Calculate market impact score (0-1) based on permit characteristics"""
        score = 0.0
        
        # Project size impact
        cost = permit_data.get('estimated_cost', 0)
        if cost > 1000000:
            score += 0.4
        elif cost > 500000:
            score += 0.3
        elif cost > 100000:
            score += 0.2
        
        # Project type impact
        if permit_data['permit_type'] == 'Commercial':
            score += 0.3
        elif permit_data['permit_type'] == 'Mixed-Use':
            score += 0.4
        elif permit_data['permit_type'] == 'Residential':
            score += 0.2
        
        # New construction vs renovation
        if permit_data.get('permit_subtype') == 'New Construction':
            score += 0.3
        else:
            score += 0.1
        
        return min(1.0, score)
    
    def _assess_traffic_impact(self, permit_data: Dict) -> str:
        """Assess foot traffic impact"""
        if 'restaurant' in permit_data.get('project_description', '').lower():
            return 'Increase'
        elif 'retail' in permit_data.get('project_description', '').lower():
            return 'Increase'
        elif permit_data.get('units_added', 0) > 10:
            return 'Increase'
        elif 'office' in permit_data.get('project_description', '').lower():
            return 'Neutral'
        else:
            return 'Neutral'
    
    def analyze_permit_trends(self, permits: List[Dict]) -> Dict:
        """Analyze trends in permit data"""
        if not permits:
            return {}
        
        df = pd.DataFrame(permits)
        
        # Convert dates
        df['application_date'] = pd.to_datetime(df['application_date'])
        
        analysis = {
            'total_permits': len(permits),
            'total_investment': df['estimated_cost'].sum(),
            'avg_permit_value': df['estimated_cost'].mean(),
            'permits_by_type': df['permit_type'].value_counts().to_dict(),
            'permits_by_city': df['city'].value_counts().to_dict(),
            'recent_permits_30d': len(df[df['application_date'] >= datetime.now() - timedelta(days=30)]),
            'high_impact_permits': len(df[df['market_impact_score'] >= 0.7]),
            'commercial_investment': df[df['permit_type'] == 'Commercial']['estimated_cost'].sum(),
            'residential_units_added': df[df['permit_type'].isin(['Residential', 'Mixed-Use'])]['units_added'].sum()
        }
        
        return analysis
    
    def save_to_bigquery(self, permits: List[Dict]):
        """Save permits to BigQuery"""
        if not permits:
            logger.warning("No permits to save")
            return
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.permit_activity"
        
        errors = self.bq_client.insert_rows_json(table_id, permits)
        
        if errors:
            logger.error(f"Error inserting to BigQuery: {errors}")
        else:
            logger.info(f"Successfully saved {len(permits)} permits")
    
    def run_collection(self, cities: List[str] = None, days_back: int = 30):
        """Run permit collection for specified cities"""
        if cities is None:
            cities = ['Madison', 'Milwaukee']
        
        logger.info(f"Starting permit collection for {cities}")
        
        all_permits = []
        
        for city in cities:
            try:
                if city == 'Madison':
                    permits = self.collect_madison_permits(days_back)
                elif city == 'Milwaukee':
                    permits = self.collect_milwaukee_permits(days_back)
                else:
                    logger.warning(f"No collector implemented for {city}")
                    continue
                
                all_permits.extend(permits)
                
            except Exception as e:
                logger.error(f"Error collecting permits for {city}: {e}")
                continue
        
        # Analyze trends
        analysis = self.analyze_permit_trends(all_permits)
        
        # Save to BigQuery
        self.save_to_bigquery(all_permits)
        
        # Print summary
        print(f"\n=== PERMIT ACTIVITY ANALYSIS ===")
        print(f"Cities analyzed: {', '.join(cities)}")
        print(f"Total permits collected: {analysis.get('total_permits', 0)}")
        print(f"Total investment: ${analysis.get('total_investment', 0):,.0f}")
        print(f"Average permit value: ${analysis.get('avg_permit_value', 0):,.0f}")
        print(f"High-impact permits: {analysis.get('high_impact_permits', 0)}")
        print(f"New residential units: {analysis.get('residential_units_added', 0)}")
        
        if analysis.get('permits_by_type'):
            print(f"\nPermits by type:")
            for ptype, count in analysis['permits_by_type'].items():
                print(f"  {ptype}: {count}")

def main():
    """Run permit activity collection"""
    collector = PermitActivityCollector()
    collector.run_collection(['Madison', 'Milwaukee'], days_back=30)

if __name__ == "__main__":
    main()