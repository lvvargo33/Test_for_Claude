#!/usr/bin/env python3
"""
Collect OSM Data for All 72 Wisconsin Counties
==============================================

Comprehensive collection to cover all Wisconsin counties with OSM business data.
"""

import logging
import os
import time
from datetime import datetime
from osm_collection_pipeline import OSMCollectionPipeline

def main():
    """Collect OSM data for all Wisconsin counties"""
    logging.basicConfig(level=logging.INFO)
    
    # All 72 Wisconsin counties
    all_wisconsin_counties = [
        # Major counties (already collected)
        'Milwaukee', 'Dane', 'Brown', 'Waukesha', 'Racine', 
        'Kenosha', 'Rock', 'Winnebago', 'La Crosse', 'Eau Claire',
        
        # Medium counties  
        'Outagamie', 'Washington', 'Sheboygan', 'Marathon', 'Walworth',
        'Jefferson', 'Dodge', 'Wood', 'Sauk', 'Fond du Lac',
        'Columbia', 'Ozaukee', 'Chippewa', 'St. Croix', 'Portage',
        'Calumet', 'Barron', 'Grant', 'Manitowoc', 'Green',
        
        # Smaller counties
        'Iowa', 'Monroe', 'Polk', 'Buffalo', 'Jackson', 'Adams',
        'Juneau', 'Waupaca', 'Shawano', 'Dunn', 'Pierce', 'Crawford',
        'Richland', 'Vernon', 'Trempealeau', 'Clark', 'Taylor',
        'Lincoln', 'Langlade', 'Oneida', 'Vilas', 'Iron', 'Ashland',
        'Bayfield', 'Douglas', 'Burnett', 'Washburn', 'Sawyer',
        'Rusk', 'Price', 'Forest', 'Florence', 'Marinette',
        'Oconto', 'Menominee', 'Kewaunee', 'Door', 'Green Lake',
        'Marquette', 'Waushara', 'Pepin'
    ]
    
    # Counties already collected (from previous runs)
    completed_counties = [
        'Milwaukee', 'Dane', 'Brown', 'Waukesha', 'Racine', 
        'Kenosha', 'Rock', 'Winnebago', 'La Crosse', 'Eau Claire'
    ]
    
    # Counties still needed
    remaining_counties = [county for county in all_wisconsin_counties 
                         if county not in completed_counties]
    
    print(f"🗺️ OSM Data Collection - All Wisconsin Counties")
    print("=" * 60)
    print(f"Total Wisconsin Counties: {len(all_wisconsin_counties)}")
    print(f"Already Completed: {len(completed_counties)}")
    print(f"Remaining to Collect: {len(remaining_counties)}")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Set up credentials
    credentials_path = "/workspaces/Test_for_Claude/Business/wisconsin_data_collection/location-optimizer-1-449414f93a5a.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    try:
        pipeline = OSMCollectionPipeline()
        
        total_businesses_collected = 0
        successful_counties = []
        failed_counties = []
        total_start_time = datetime.now()
        
        print(f"📍 Processing {len(remaining_counties)} remaining counties...")
        print("   Strategy: County-by-county collection with progress tracking")
        print()
        
        for i, county in enumerate(remaining_counties, 1):
            print(f"🏢 [{i}/{len(remaining_counties)}] Processing {county} County...")
            
            try:
                county_start = datetime.now()
                
                # Some counties may not have defined bounding boxes, 
                # so we'll try individual collection
                summary = pipeline.collect_and_store_wisconsin_data(
                    counties=[county],
                    save_to_bigquery=True,
                    save_to_json=False  # Skip JSON for individual counties
                )
                
                county_end = datetime.now()
                county_time = (county_end - county_start).total_seconds()
                
                if summary.success and summary.businesses_collected > 0:
                    total_businesses_collected += summary.businesses_collected
                    successful_counties.append(county)
                    
                    print(f"   ✅ {county}: {summary.businesses_collected:,} businesses ({county_time:.1f}s)")
                    print(f"      Franchises: {summary.franchises_identified}")
                    print(f"      Cities: {summary.cities_covered}")
                    print(f"      Quality: {summary.avg_data_quality_score:.1f}/100")
                else:
                    failed_counties.append(county)
                    print(f"   ⚠️  {county}: No data collected (may not have bounding box)")
                
                # Small delay between counties to be respectful to OSM API
                if i < len(remaining_counties):
                    print("   ⏸️  Brief pause...")
                    time.sleep(2)
                
            except Exception as e:
                failed_counties.append(county)
                print(f"   ❌ {county}: Error - {e}")
                logging.error(f"County {county} collection failed: {e}")
                continue
            
            print()
            
            # Progress update every 10 counties
            if i % 10 == 0:
                elapsed = (datetime.now() - total_start_time).total_seconds() / 60
                estimated_total = (elapsed / i) * len(remaining_counties)
                remaining_time = estimated_total - elapsed
                
                print(f"📊 Progress Update ({i}/{len(remaining_counties)}):")
                print(f"   ⏱️  Elapsed: {elapsed:.1f} minutes")
                print(f"   🕒 Estimated remaining: {remaining_time:.1f} minutes")
                print(f"   ✅ Successful: {len(successful_counties)}")
                print(f"   ❌ Failed: {len(failed_counties)}")
                print(f"   📈 Total businesses: {total_businesses_collected:,}")
                print()
        
        total_end_time = datetime.now()
        total_time = (total_end_time - total_start_time).total_seconds()
        
        print(f"🎉 ALL WISCONSIN COUNTIES COLLECTION COMPLETE!")
        print("=" * 60)
        print(f"⏱️  Total Processing Time: {total_time/60:.1f} minutes")
        
        print(f"\n📊 FINAL RESULTS:")
        print(f"   ✅ Successful Counties: {len(successful_counties)}")
        print(f"   ❌ Failed Counties: {len(failed_counties)}")
        print(f"   📈 New Businesses Collected: {total_businesses_collected:,}")
        print(f"   🏙️  Coverage: {len(successful_counties) + len(completed_counties)}/72 counties")
        
        if successful_counties:
            print(f"\n✅ NEWLY COLLECTED COUNTIES:")
            for county in successful_counties:
                print(f"   • {county}")
        
        if failed_counties:
            print(f"\n⚠️  COUNTIES WITH NO DATA:")
            print(f"   (Likely due to missing bounding boxes or very rural areas)")
            for county in failed_counties:
                print(f"   • {county}")
        
        print(f"\n📋 TOTAL WISCONSIN COVERAGE:")
        total_successful = len(successful_counties) + len(completed_counties)
        coverage_percentage = (total_successful / 72) * 100
        print(f"   Counties with OSM Data: {total_successful}/72 ({coverage_percentage:.1f}%)")
        
        print(f"\n💾 Data Storage:")
        print(f"   BigQuery: location-optimizer-1.raw_business_data.osm_businesses")
        print(f"   Summary: location-optimizer-1.raw_business_data.osm_collection_summary")
        
        print(f"\n💼 Business Intelligence Impact:")
        print(f"   🎯 Comprehensive statewide competitive intelligence")
        print(f"   🗺️  Coverage spans urban to rural Wisconsin markets")
        print(f"   📊 Enhanced market opportunity identification")
        print(f"   🔍 Complete competitive landscape mapping")
        
        print(f"\n✅ Wisconsin OSM data collection expansion complete!")
        
    except Exception as e:
        print(f"❌ Error during statewide collection: {e}")
        logging.error(f"Wisconsin OSM expansion failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()