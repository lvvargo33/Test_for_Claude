#!/usr/bin/env python3
"""
Collect Remaining Wisconsin Counties
===================================

Continue collecting OSM data for remaining major Wisconsin counties.
"""

import logging
import os
import time
from datetime import datetime
from osm_collection_pipeline import OSMCollectionPipeline

def main():
    """Collect remaining major Wisconsin counties"""
    logging.basicConfig(level=logging.INFO)
    
    # Counties not yet collected (based on current BigQuery data)
    remaining_counties = [
        'Racine',      # Southeast
        'Kenosha',     # Southeast border  
        'Rock',        # Janesville
        'Winnebago',   # Oshkosh
        'La Crosse',   # Western Wisconsin
        'Eau Claire'   # Northwestern Wisconsin
    ]
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    pipeline = OSMCollectionPipeline()
    
    print(f"🗺️ Collecting {len(remaining_counties)} Remaining Major Counties")
    print("=" * 60)
    
    total_collected = 0
    successful_counties = []
    
    for i, county in enumerate(remaining_counties, 1):
        print(f"\n[{i}/{len(remaining_counties)}] Processing {county} County...")
        
        try:
            start_time = datetime.now()
            
            summary = pipeline.collect_and_store_wisconsin_data(
                counties=[county],
                save_to_bigquery=True,
                save_to_json=False
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if summary.success:
                total_collected += summary.businesses_collected
                successful_counties.append(county)
                print(f"   ✅ {summary.businesses_collected:,} businesses collected ({duration:.1f}s)")
                print(f"   🏢 {summary.franchises_identified} franchises")
                print(f"   🏙️  {summary.cities_covered} cities")
                print(f"   📈 {summary.avg_data_quality_score:.1f}/100 quality")
            else:
                print(f"   ❌ Collection failed")
            
            # Brief pause between counties
            if i < len(remaining_counties):
                print("   ⏸️  Brief pause...")
                time.sleep(2)
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            continue
    
    print(f"\n🎉 REMAINING COUNTIES COLLECTION COMPLETE!")
    print("=" * 50)
    print(f"✅ {len(successful_counties)}/{len(remaining_counties)} counties successful")
    print(f"📊 {total_collected:,} additional businesses collected")
    print(f"🏙️  Counties added: {', '.join(successful_counties)}")

if __name__ == "__main__":
    main()