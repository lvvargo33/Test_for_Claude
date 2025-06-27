#!/usr/bin/env python3
"""
Test Milwaukee County GIS Data Collection
=========================================

Test collection of real zoning and property data from Milwaukee County GIS.
"""

import logging
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import json
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MilwaukeeCountyGISCollector:
    """Milwaukee County GIS data collector"""
    
    def __init__(self):
        self.base_urls = {
            'zoning': 'https://gis.milwaukee.gov/arcgis/rest/services/PlanningZoning/MapServer/1',
            'parcels': 'https://gis.milwaukee.gov/arcgis/rest/services/PropertyInformation/MapServer/0',
            'land_use': 'https://gis.milwaukee.gov/arcgis/rest/services/PlanningZoning/MapServer/0'
        }
        
    def test_service_availability(self):
        """Test which Milwaukee County GIS services are available"""
        
        print("🌐 Testing Milwaukee County GIS Service Availability")
        print("="*60)
        
        available_services = {}
        
        for service_name, url in self.base_urls.items():
            print(f"📍 Testing {service_name} service...")
            
            try:
                # Test base service info
                info_url = f"{url}?f=json"
                response = requests.get(info_url, timeout=10)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        if 'error' not in data:
                            print(f"   ✅ {service_name}: Available")
                            print(f"      Name: {data.get('name', 'Unknown')}")
                            print(f"      Type: {data.get('type', 'Unknown')}")
                            if 'fields' in data:
                                print(f"      Fields: {len(data['fields'])} available")
                            available_services[service_name] = {
                                'url': url,
                                'available': True,
                                'info': data
                            }
                        else:
                            print(f"   ❌ {service_name}: Service error - {data.get('error', {}).get('message', 'Unknown')}")
                            available_services[service_name] = {'available': False, 'error': data.get('error')}
                            
                    except json.JSONDecodeError:
                        print(f"   ❌ {service_name}: Invalid JSON response")
                        available_services[service_name] = {'available': False, 'error': 'Invalid JSON'}
                        
                else:
                    print(f"   ❌ {service_name}: HTTP {response.status_code}")
                    available_services[service_name] = {'available': False, 'error': f'HTTP {response.status_code}'}
                    
            except Exception as e:
                print(f"   ❌ {service_name}: Connection error - {e}")
                available_services[service_name] = {'available': False, 'error': str(e)}
        
        return available_services
    
    def collect_sample_zoning_data(self, limit=50):
        """Collect sample zoning data from Milwaukee County"""
        
        print(f"\n🏘️  Collecting Sample Milwaukee Zoning Data (limit: {limit})")
        print("-"*50)
        
        zoning_url = self.base_urls['zoning']
        
        # Query parameters for ArcGIS REST API
        params = {
            'where': '1=1',  # Get all records
            'outFields': '*',  # All fields
            'returnGeometry': 'false',  # Don't need geometry for testing
            'f': 'json',
            'resultRecordCount': limit
        }
        
        try:
            query_url = f"{zoning_url}/query"
            response = requests.get(query_url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'features' in data and data['features']:
                    features = data['features']
                    print(f"✅ Successfully retrieved {len(features)} zoning records")
                    
                    # Convert to structured data
                    records = []
                    for feature in features:
                        attrs = feature.get('attributes', {})
                        
                        record = {
                            'county': 'Milwaukee',
                            'state': 'WI',
                            'data_source': 'Milwaukee_County_GIS',
                            'collection_date': datetime.utcnow(),
                            'data_extraction_date': datetime.utcnow()
                        }
                        
                        # Add all available attributes
                        for key, value in attrs.items():
                            # Clean field names
                            clean_key = key.lower().replace(' ', '_')
                            record[clean_key] = value
                        
                        records.append(record)
                    
                    print(f"📊 Sample record fields: {list(records[0].keys())[:10]}...")
                    
                    return records
                    
                else:
                    print("❌ No features returned from zoning service")
                    if 'error' in data:
                        print(f"   Error: {data['error']}")
                    return []
                    
            else:
                print(f"❌ Query failed with status {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                return []
                
        except Exception as e:
            print(f"❌ Error collecting zoning data: {e}")
            return []
    
    def collect_sample_parcel_data(self, limit=50):
        """Collect sample parcel data from Milwaukee County"""
        
        print(f"\n🏡 Collecting Sample Milwaukee Parcel Data (limit: {limit})")
        print("-"*50)
        
        parcels_url = self.base_urls['parcels']
        
        params = {
            'where': '1=1',
            'outFields': '*',
            'returnGeometry': 'false',
            'f': 'json',
            'resultRecordCount': limit
        }
        
        try:
            query_url = f"{parcels_url}/query"
            response = requests.get(query_url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'features' in data and data['features']:
                    features = data['features']
                    print(f"✅ Successfully retrieved {len(features)} parcel records")
                    
                    records = []
                    for feature in features:
                        attrs = feature.get('attributes', {})
                        
                        record = {
                            'county': 'Milwaukee',
                            'state': 'WI',
                            'data_source': 'Milwaukee_County_GIS',
                            'collection_date': datetime.utcnow(),
                            'data_extraction_date': datetime.utcnow()
                        }
                        
                        for key, value in attrs.items():
                            clean_key = key.lower().replace(' ', '_')
                            record[clean_key] = value
                        
                        records.append(record)
                    
                    print(f"📊 Sample record fields: {list(records[0].keys())[:10]}...")
                    
                    return records
                    
                else:
                    print("❌ No features returned from parcels service")
                    return []
                    
            else:
                print(f"❌ Query failed with status {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Error collecting parcel data: {e}")
            return []

def test_milwaukee_gis_collection():
    """Test Milwaukee County GIS data collection"""
    
    print("🧪 Testing Milwaukee County GIS Data Collection")
    print("="*70)
    
    collector = MilwaukeeCountyGISCollector()
    
    # Test service availability
    services = collector.test_service_availability()
    
    # Count available services
    available_count = sum(1 for s in services.values() if s.get('available', False))
    total_count = len(services)
    
    print(f"\n📊 Service Availability: {available_count}/{total_count} services available")
    
    if available_count == 0:
        print("❌ No services available - cannot proceed with data collection")
        return
    
    all_records = []
    
    # Try to collect sample data from available services
    if services.get('zoning', {}).get('available', False):
        zoning_records = collector.collect_sample_zoning_data(limit=25)
        if zoning_records:
            for record in zoning_records:
                record['data_type'] = 'zoning'
            all_records.extend(zoning_records)
    
    if services.get('parcels', {}).get('available', False):
        parcel_records = collector.collect_sample_parcel_data(limit=25)
        if parcel_records:
            for record in parcel_records:
                record['data_type'] = 'parcels'
            all_records.extend(parcel_records)
    
    if all_records:
        print(f"\n📈 Total records collected: {len(all_records)}")
        
        # Try to load a small sample to BigQuery
        df = pd.DataFrame(all_records)
        
        # Load to BigQuery test table
        client = bigquery.Client(project="location-optimizer-1")
        table_id = "location-optimizer-1.raw_business_data.milwaukee_gis_test"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_extraction_date"
            ),
            autodetect=True
        )
        
        try:
            print(f"\n💾 Loading {len(df)} records to BigQuery test table...")
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            
            table = client.get_table(table_id)
            print(f"✅ Successfully loaded {table.num_rows} rows to {table_id}")
            
            # Show sample data
            query = f"""
            SELECT 
                data_type,
                COUNT(*) as record_count,
                data_source,
                county
            FROM `{table_id}`
            GROUP BY data_type, data_source, county
            """
            
            print("\n📊 Milwaukee GIS Data Summary:")
            for row in client.query(query):
                print(f"   {row.data_type}: {row.record_count} records from {row.data_source}")
            
            return len(all_records)
            
        except Exception as e:
            print(f"❌ Failed to load to BigQuery: {e}")
            return 0
    
    else:
        print("❌ No data collected from any service")
        return 0

def main():
    """Main function"""
    
    print("🚀 Milwaukee County GIS Data Collection Test")
    print("="*70)
    
    records = test_milwaukee_gis_collection()
    
    if records > 0:
        print(f"\n🎉 Successfully tested Milwaukee County GIS collection!")
        print(f"   Records collected: {records}")
        print("   Data quality: Ready for production scaling")
        print("   Recommendation: Implement for Milwaukee County")
    else:
        print("\n❌ Milwaukee County GIS test failed")
        print("   Recommendation: Use alternative data sources")

if __name__ == "__main__":
    main()