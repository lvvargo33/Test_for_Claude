#!/usr/bin/env python3
"""
Collect OSM Data for All Wisconsin Counties
==========================================

Run comprehensive OSM data collection for entire state of Wisconsin.
"""

import logging
import os
from datetime import datetime
from osm_collection_pipeline import OSMCollectionPipeline

def main():
    """Run OSM collection for all Wisconsin counties"""
    logging.basicConfig(level=logging.INFO)
    
    print("🗺️ OSM Data Collection - All Wisconsin Counties")
    print("=" * 60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("⚠️  This may take 20-30 minutes for statewide collection")
    print()
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    try:
        pipeline = OSMCollectionPipeline()
        
        # Collect for ALL Wisconsin (counties=None means statewide)
        print("📍 Starting statewide Wisconsin OSM data collection...")
        print("   Expected: 50,000+ businesses across 72 counties")
        print("   This will collect businesses from all major categories:")
        print("   - Restaurants, retail stores, gas stations")
        print("   - Healthcare facilities, banks, hotels") 
        print("   - Automotive services, personal services")
        print("   - And many more business types")
        print()
        
        start_time = datetime.now()
        
        summary = pipeline.collect_and_store_wisconsin_data(
            counties=None,          # None = all Wisconsin
            save_to_bigquery=True,  # Store in BigQuery
            save_to_json=True       # Also save JSON backup
        )
        
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        # Generate and display report
        report = pipeline.generate_pipeline_report(summary)
        print(f"\n{report}")
        
        print(f"\n🎉 STATEWIDE COLLECTION COMPLETE!")
        print("=" * 50)
        print(f"⏱️  Total Processing Time: {total_time/60:.1f} minutes")
        print(f"📊 Collection Results:")
        
        if summary.success:
            print(f"   ✅ {summary.businesses_collected:,} businesses collected statewide")
            print(f"   🏢 {summary.franchises_identified:,} franchise businesses identified")
            print(f"   🏙️  {summary.cities_covered} unique cities covered")
            print(f"   📈 {summary.avg_data_quality_score:.1f}/100 average data quality")
            print(f"   📞 {summary.businesses_with_contact:,} businesses with contact info")
            print(f"   📍 {summary.businesses_with_address:,} businesses with full addresses")
            
            print(f"\n📋 Data Storage:")
            print(f"   💾 BigQuery: location-optimizer-1.raw_business_data.osm_businesses")
            print(f"   📄 JSON Backup: osm_businesses_Wisconsin_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
            
            # Business value summary
            print(f"\n💼 Business Intelligence Value:")
            print(f"   🎯 Complete Wisconsin business landscape mapping")
            print(f"   🔍 Franchise vs independent business analysis")
            print(f"   📊 Market density analysis by city/county")
            print(f"   🗺️  Geographic distribution insights")
            
        else:
            print(f"   ❌ Collection failed - check logs for details")
        
        print(f"\n✅ Wisconsin OSM data collection pipeline complete!")
        
    except Exception as e:
        print(f"❌ Error during statewide collection: {e}")
        logging.error(f"Wisconsin OSM collection failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()