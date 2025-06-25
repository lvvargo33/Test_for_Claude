#!/usr/bin/env python3
"""
Run OSM Data Collection with BigQuery Storage
============================================

Collect OSM data for Wisconsin and store in BigQuery tables.
"""

import logging
import os
from osm_collection_pipeline import OSMCollectionPipeline

def main():
    """Run OSM collection with BigQuery storage enabled"""
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 OSM Data Collection to BigQuery")
    print("=" * 50)
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    try:
        pipeline = OSMCollectionPipeline()
        
        # Collect for Dane County first (test run)
        print("\n📍 Collecting OSM data for Dane County and storing in BigQuery...")
        
        summary = pipeline.collect_and_store_wisconsin_data(
            counties=['Dane'],
            save_to_bigquery=True,  # Enable BigQuery storage
            save_to_json=True       # Also save JSON for backup
        )
        
        # Generate and display report
        report = pipeline.generate_pipeline_report(summary)
        print(f"\n{report}")
        
        print("\n✅ OSM data collection to BigQuery complete!")
        
        if summary.success:
            print(f"\n📊 Summary:")
            print(f"   - {summary.businesses_collected} businesses stored in BigQuery")
            print(f"   - {summary.franchises_identified} franchise businesses identified")
            print(f"   - {summary.cities_covered} cities covered")
            print(f"   - Average data quality: {summary.avg_data_quality_score:.1f}/100")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"OSM BigQuery collection failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()