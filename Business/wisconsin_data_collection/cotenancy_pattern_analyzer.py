#!/usr/bin/env python3
"""
Co-tenancy Pattern Analyzer - Identifies successful business combinations
Analyzes SBA loan data and business registrations to find winning co-tenancy patterns
"""

import logging
from datetime import datetime, date
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict
import json

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

class CotenancyAnalyzer:
    def __init__(self):
        """Initialize the co-tenancy analyzer"""
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        self.client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        
        # Business type mappings
        self.business_categories = {
            'restaurant': ['restaurant', 'pizza', 'cafe', 'coffee', 'bakery', 'deli', 'grill', 'bistro'],
            'fitness': ['gym', 'fitness', 'yoga', 'martial arts', 'dance', 'pilates'],
            'personal_services': ['salon', 'spa', 'barber', 'nail', 'beauty', 'massage'],
            'retail_food': ['grocery', 'market', 'convenience', 'liquor', 'beer', 'wine'],
            'medical': ['dental', 'medical', 'clinic', 'chiro', 'therapy', 'veterinary'],
            'retail_general': ['retail', 'store', 'shop', 'boutique', 'apparel', 'clothing']
        }
    
    def categorize_business(self, business_name: str) -> str:
        """Categorize a business based on its name"""
        name_lower = business_name.lower()
        
        for category, keywords in self.business_categories.items():
            for keyword in keywords:
                if keyword in name_lower:
                    return category
        
        return 'other'
    
    def analyze_sba_cotenancy_patterns(self) -> pd.DataFrame:
        """
        Analyze co-tenancy patterns from SBA loan data
        Looking at businesses that got loans in similar timeframes and locations
        """
        logger.info("Analyzing SBA loan data for co-tenancy patterns...")
        
        # Query to find businesses that got loans around the same time in the same area
        query = f"""
        WITH business_locations AS (
            SELECT 
                borrower_name,
                borrower_address,
                borrower_city,
                borrower_zip,
                approval_date,
                loan_amount,
                jobs_supported,
                franchise_name,
                borrower_state,
                -- Extract year for grouping
                EXTRACT(YEAR FROM approval_date) as approval_year
            FROM `{PROJECT_ID}.raw_business_data.sba_loan_approvals`
            WHERE borrower_state = 'WI'
                AND approval_date IS NOT NULL
                AND loan_amount > 50000  -- Focus on substantial loans
        ),
        
        nearby_businesses AS (
            -- Find businesses in the same city that got loans within 2 years of each other
            SELECT 
                a.borrower_name as business_a,
                b.borrower_name as business_b,
                a.borrower_city as city,
                a.approval_year as year_a,
                b.approval_year as year_b,
                a.loan_amount as loan_a,
                b.loan_amount as loan_b,
                a.jobs_supported as jobs_a,
                b.jobs_supported as jobs_b,
                ABS(a.approval_year - b.approval_year) as year_diff
            FROM business_locations a
            JOIN business_locations b
                ON a.borrower_city = b.borrower_city
                AND a.borrower_name != b.borrower_name
                AND ABS(a.approval_year - b.approval_year) <= 2
        )
        
        SELECT *
        FROM nearby_businesses
        WHERE year_diff <= 2
        ORDER BY city, year_a
        """
        
        df = self.client.query(query).to_dataframe()
        
        # Categorize businesses
        df['category_a'] = df['business_a'].apply(self.categorize_business)
        df['category_b'] = df['business_b'].apply(self.categorize_business)
        
        return df
    
    def calculate_success_patterns(self, df: pd.DataFrame) -> Dict:
        """
        Calculate success rates for different business type combinations
        """
        patterns = defaultdict(lambda: {
            'count': 0,
            'total_loan_amount': 0,
            'avg_jobs_created': 0,
            'cities': set()
        })
        
        for _, row in df.iterrows():
            # Create sorted tuple to avoid duplicates (A,B) and (B,A)
            pair = tuple(sorted([row['category_a'], row['category_b']]))
            
            patterns[pair]['count'] += 1
            patterns[pair]['total_loan_amount'] += row['loan_a'] + row['loan_b']
            patterns[pair]['avg_jobs_created'] += (row['jobs_a'] or 0) + (row['jobs_b'] or 0)
            patterns[pair]['cities'].add(row['city'])
        
        # Calculate averages and success metrics
        results = []
        for pair, data in patterns.items():
            if data['count'] >= 3:  # Minimum sample size
                avg_jobs = data['avg_jobs_created'] / data['count']
                avg_loan = data['total_loan_amount'] / data['count']
                
                # Success score based on loan amount and jobs created
                success_score = (avg_loan / 100000) * 0.5 + (avg_jobs / 10) * 0.5
                
                results.append({
                    'business_type_1': pair[0],
                    'business_type_2': pair[1],
                    'occurrence_count': data['count'],
                    'avg_loan_amount': avg_loan,
                    'avg_jobs_created': avg_jobs,
                    'success_score': min(success_score, 1.0),  # Cap at 1.0
                    'cities': list(data['cities'])[:10]  # Top 10 cities
                })
        
        return sorted(results, key=lambda x: x['success_score'], reverse=True)
    
    def analyze_osm_cotenancy(self) -> List[Dict]:
        """
        Analyze co-tenancy patterns from OpenStreetMap data
        """
        logger.info("Analyzing OSM data for current co-tenancy patterns...")
        
        # Check if OSM data exists
        try:
            query = f"""
            SELECT COUNT(*) as count
            FROM `{PROJECT_ID}.raw_business_data.osm_businesses`
            """
            result = self.client.query(query).to_dataframe()
            if result['count'][0] == 0:
                logger.warning("No OSM data found. Skipping OSM analysis.")
                return []
        except Exception as e:
            logger.warning(f"OSM businesses table not found or error: {e}. Skipping OSM analysis.")
            return []
        
        # Query for businesses within 250 meters of each other
        try:
            cluster_query = f"""
        WITH business_clusters AS (
            SELECT 
                a.name as business_a,
                b.name as business_b,
                a.amenity as type_a,
                b.amenity as type_b,
                a.city,
                ST_DISTANCE(a.location, b.location) as distance_meters
            FROM `{PROJECT_ID}.raw_business_data.osm_businesses` a
            JOIN `{PROJECT_ID}.raw_business_data.osm_businesses` b
                ON a.city = b.city
                AND a.osm_id != b.osm_id
                AND ST_DISTANCE(a.location, b.location) <= 250  -- Within 250 meters
            WHERE a.state = 'Wisconsin'
                AND b.state = 'Wisconsin'
        )
        SELECT 
            type_a,
            type_b,
            COUNT(*) as pair_count,
            AVG(distance_meters) as avg_distance
        FROM business_clusters
        GROUP BY type_a, type_b
        HAVING pair_count >= 5
        ORDER BY pair_count DESC
        """
        
            df = self.client.query(cluster_query).to_dataframe()
            
            results = []
            for _, row in df.iterrows():
                results.append({
                    'type_1': row['type_a'],
                    'type_2': row['type_b'],
                    'frequency': row['pair_count'],
                    'avg_distance_meters': row['avg_distance']
                })
            
            return results
        except Exception as e:
            logger.warning(f"Error analyzing OSM cotenancy: {e}")
            return []
    
    def generate_cotenancy_recommendations(self) -> Dict:
        """
        Generate comprehensive co-tenancy recommendations
        """
        # Analyze SBA patterns
        sba_df = self.analyze_sba_cotenancy_patterns()
        sba_patterns = self.calculate_success_patterns(sba_df)
        
        # Analyze OSM patterns
        osm_patterns = self.analyze_osm_cotenancy()
        
        # Combine insights
        recommendations = {}
        
        # Process SBA patterns
        for pattern in sba_patterns[:20]:  # Top 20 patterns
            key = pattern['business_type_1']
            
            if key not in recommendations:
                recommendations[key] = {
                    'anchor_business_type': key,
                    'anchor_business_category': self._get_category_group(key),
                    'within_same_plaza': [],
                    'within_quarter_mile': [],
                    'negative_cotenancy': [],
                    'ideal_anchors': [],
                    'sample_size': 0,
                    'confidence_score': 0.0,
                    'analysis_period_years': 5,
                    'applicable_states': ['WI'],
                    'applicable_metro_areas': []
                }
            
            # Add successful pairing
            recommendations[key]['within_same_plaza'].append({
                'business_type': pattern['business_type_2'],
                'success_rate': pattern['success_score'],
                'avg_revenue_impact': pattern['avg_loan_amount'] / 100000,  # Normalized
                'sample_size': pattern['occurrence_count']
            })
            
            recommendations[key]['sample_size'] += pattern['occurrence_count']
            recommendations[key]['applicable_metro_areas'].extend(pattern['cities'])
        
        # Calculate confidence scores
        for key in recommendations:
            rec = recommendations[key]
            # Confidence based on sample size
            rec['confidence_score'] = min(rec['sample_size'] / 100, 1.0)
            # Unique metro areas
            rec['applicable_metro_areas'] = list(set(rec['applicable_metro_areas']))[:10]
            
            # Sort co-tenancy by success rate
            rec['within_same_plaza'].sort(key=lambda x: x['success_rate'], reverse=True)
            
            # Identify ideal anchors (grocery stores, big box retail)
            if key in ['restaurant', 'personal_services', 'fitness']:
                rec['ideal_anchors'] = ['grocery_store', 'walmart', 'target', 'home_depot']
        
        return recommendations
    
    def _get_category_group(self, business_type: str) -> str:
        """Get higher level category grouping"""
        category_groups = {
            'restaurant': 'food_service',
            'fitness': 'health_wellness',
            'personal_services': 'health_wellness',
            'retail_food': 'retail',
            'medical': 'health_wellness',
            'retail_general': 'retail'
        }
        return category_groups.get(business_type, 'other')
    
    def save_to_bigquery(self, recommendations: Dict):
        """Save co-tenancy patterns to BigQuery"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.cotenancy_success_patterns"
        
        rows_to_insert = []
        
        for key, data in recommendations.items():
            row = {
                'anchor_business_type': data['anchor_business_type'],
                'anchor_business_category': data['anchor_business_category'],
                'within_same_plaza': data['within_same_plaza'][:10],  # Top 10
                'within_quarter_mile': data.get('within_quarter_mile', []),
                'negative_cotenancy': data.get('negative_cotenancy', []),
                'ideal_anchors': data.get('ideal_anchors', []),
                'sample_size': data['sample_size'],
                'confidence_score': data['confidence_score'],
                'analysis_period_years': data['analysis_period_years'],
                'applicable_states': data['applicable_states'],
                'applicable_metro_areas': data['applicable_metro_areas'],
                'analysis_date': date.today().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            rows_to_insert.append(row)
        
        # Insert rows
        errors = self.client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            logger.error(f"Error inserting to BigQuery: {errors}")
        else:
            logger.info(f"Successfully saved {len(rows_to_insert)} co-tenancy patterns")
    
    def print_summary_report(self, recommendations: Dict):
        """Print a summary of co-tenancy findings"""
        print("\n=== CO-TENANCY ANALYSIS REPORT ===\n")
        
        for business_type, data in recommendations.items():
            print(f"\n{business_type.upper()}")
            print("-" * 40)
            print(f"Sample Size: {data['sample_size']} business pairs")
            print(f"Confidence: {data['confidence_score']:.2%}")
            print(f"Markets: {', '.join(data['applicable_metro_areas'][:5])}")
            
            print("\nBest Co-tenancy Partners:")
            for i, partner in enumerate(data['within_same_plaza'][:5]):
                print(f"  {i+1}. {partner['business_type']} "
                      f"(Success: {partner['success_rate']:.2f}, "
                      f"Count: {partner['sample_size']})")
            
            if data.get('ideal_anchors'):
                print(f"\nIdeal Anchors: {', '.join(data['ideal_anchors'])}")

def main():
    """Run co-tenancy analysis"""
    analyzer = CotenancyAnalyzer()
    
    print("Analyzing co-tenancy patterns from SBA loan data...")
    recommendations = analyzer.generate_cotenancy_recommendations()
    
    # Print summary
    analyzer.print_summary_report(recommendations)
    
    # Save to BigQuery
    print("\nSaving patterns to BigQuery...")
    analyzer.save_to_bigquery(recommendations)
    
    print("\nCo-tenancy analysis complete!")

if __name__ == "__main__":
    main()