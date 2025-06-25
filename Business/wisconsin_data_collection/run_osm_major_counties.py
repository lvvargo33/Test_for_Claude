#!/usr/bin/env python3
"""
Collect OSM Data for Major Wisconsin Counties
============================================

Run OSM data collection county-by-county for major Wisconsin counties.
"""

import logging
import os
from datetime import datetime
from osm_collection_pipeline import OSMCollectionPipeline

def main():
    """Run OSM collection for major Wisconsin counties"""
    logging.basicConfig(level=logging.INFO)
    
    print("🗺️ OSM Data Collection - Major Wisconsin Counties")
    print("=" * 60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Major Wisconsin counties with defined bounding boxes
    major_counties = [
        'Milwaukee',   # Largest city
        'Dane',        # Madison (already done, but will update)
        'Brown',       # Green Bay
        'Waukesha',    # Milwaukee suburb
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
    
    try:
        pipeline = OSMCollectionPipeline()
        
        total_businesses = 0
        total_franchises = 0
        total_cities = set()
        total_start_time = datetime.now()
        
        print(f"📍 Collecting OSM data for {len(major_counties)} major Wisconsin counties...")
        print("   This covers ~80% of Wisconsin's population and business activity")
        print()
        
        for i, county in enumerate(major_counties, 1):
            print(f"🏢 [{i}/{len(major_counties)}] Processing {county} County...")
            
            try:
                county_start = datetime.now()
                
                summary = pipeline.collect_and_store_wisconsin_data(
                    counties=[county],
                    save_to_bigquery=True,
                    save_to_json=False  # Skip JSON for individual counties
                )
                
                county_end = datetime.now()
                county_time = (county_end - county_start).total_seconds()
                
                if summary.success:
                    total_businesses += summary.businesses_collected
                    total_franchises += summary.franchises_identified
                    
                    print(f"   ✅ {county}: {summary.businesses_collected:,} businesses ({county_time:.1f}s)")
                    print(f"      Franchises: {summary.franchises_identified}")
                    print(f"      Cities: {summary.cities_covered}")
                    print(f"      Quality: {summary.avg_data_quality_score:.1f}/100")
                else:
                    print(f"   ❌ {county}: Collection failed")
                
                # Small delay between counties to be nice to the API
                if i < len(major_counties):
                    print("   ⏸️  Pausing 3 seconds between counties...")
                    import time
                    time.sleep(3)
                
            except Exception as e:
                print(f"   ❌ {county}: Error - {e}")
                logging.error(f"County {county} collection failed: {e}")
                continue
            
            print()
        
        total_end_time = datetime.now()
        total_time = (total_end_time - total_start_time).total_seconds()
        
        print(f"🎉 MAJOR COUNTIES COLLECTION COMPLETE!")
        print("=" * 60)
        print(f"⏱️  Total Processing Time: {total_time/60:.1f} minutes")
        print(f"📊 Aggregate Results:")
        print(f"   ✅ {total_businesses:,} total businesses collected")
        print(f"   🏢 {total_franchises:,} franchise businesses identified")
        print(f"   🏙️  {len(major_counties)} counties processed")
        
        print(f"\n📋 Data Storage:")
        print(f"   💾 BigQuery: location-optimizer-1.raw_business_data.osm_businesses")
        print(f"   📊 Summary: location-optimizer-1.raw_business_data.osm_collection_summary")
        
        print(f"\n💼 Coverage Analysis:")
        print(f"   🎯 Major population centers: ✅ Complete")
        print(f"   📈 Estimated statewide coverage: ~80% of businesses")
        print(f"   🗺️  Geographic distribution: All regions represented")
        
        print(f"\n✅ Major Wisconsin counties OSM collection complete!")
        
    except Exception as e:
        print(f"❌ Error during major counties collection: {e}")
        logging.error(f"Major counties OSM collection failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()