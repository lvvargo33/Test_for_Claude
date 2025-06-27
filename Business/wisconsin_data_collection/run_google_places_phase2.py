#!/usr/bin/env python3
"""
Google Places Phase 2 Data Collection Runner
===========================================

Collects business data for Phase 2 Wisconsin counties:
- Winnebago County (Oshkosh, Neenah, Menasha)
- Eau Claire County (Eau Claire)
- Marathon County (Wausau)
- Kenosha County (Kenosha)
- Racine County (Racine)
"""

import os
import logging
from google_places_collector import GooglePlacesCollector

def main():
    """Run Phase 2 Google Places collection"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("GOOGLE PLACES PHASE 2 COLLECTION")
    print("=" * 60)
    print()
    print("Phase 2 Counties:")
    print("- Winnebago County (Oshkosh, Neenah, Menasha) - 9 search areas")
    print("- Eau Claire County (Eau Claire) - 9 search areas")
    print("- Marathon County (Wausau) - 9 search areas")
    print("- Kenosha County (Kenosha) - 8 search areas")
    print("- Racine County (Racine) - 9 search areas")
    print("- Total: 44 search areas")
    print()
    
    try:
        # Get API key from environment
        api_key = os.environ.get('GOOGLE_PLACES_API_KEY')
        if not api_key:
            print("❌ Error: GOOGLE_PLACES_API_KEY environment variable not set")
            print("Set it with: export GOOGLE_PLACES_API_KEY='your_key_here'")
            return False
        
        print(f"✓ API key loaded (length: {len(api_key)} characters)")
        print()
        
        # Initialize collector
        print("Initializing Google Places collector...")
        collector = GooglePlacesCollector(api_key)
        
        # Clear the API key from memory immediately
        api_key = None
        
        print("✓ Collector initialized")
        print(f"✓ Phase 2 search areas defined: {len(collector.phase2_search_areas)}")
        print()
        
        # Run Phase 2 collection
        summary = collector.run_phase2_collection()
        
        # Display results
        print("\n" + "=" * 60)
        print("PHASE 2 COLLECTION SUMMARY")
        print("=" * 60)
        print(f"Phase: {summary['phase']}")
        print(f"Counties: {', '.join(summary['counties'])}")
        print(f"Search Areas: {summary['search_areas']}")
        print(f"API Calls Made: {summary['api_calls_made']}")
        print(f"Businesses Collected: {summary['businesses_collected']}")
        print(f"Success: {summary['success']}")
        print(f"Processing Time: {summary['processing_time_seconds']:.1f} seconds")
        
        if summary.get('data_quality'):
            quality = summary['data_quality']
            print(f"\nDATA QUALITY METRICS:")
            print(f"Total Records: {quality['total_records']}")
            print(f"Records with Ratings: {quality['records_with_ratings']}")
            print(f"Records with Phone: {quality['records_with_phone']}")
            print(f"Avg Confidence Score: {quality['avg_confidence_score']:.1f}")
            
            print(f"\nTOP BUSINESS CATEGORIES:")
            for category, count in list(quality['business_categories'].items())[:5]:
                print(f"- {category}: {count}")
            
            print(f"\nCOUNTY DISTRIBUTION:")
            for county, count in quality['county_distribution'].items():
                print(f"- {county}: {count}")
        
        if summary.get('errors'):
            print(f"\nERRORS ENCOUNTERED: {len(summary['errors'])}")
            for error in summary['errors'][:5]:
                print(f"- {error}")
        
        if summary.get('output_file'):
            print(f"\n✓ Data saved to: {summary['output_file']}")
        
        print("\n" + "=" * 60)
        print("PHASE 2 COLLECTION COMPLETE")
        print("=" * 60)
        
        return summary['success']
        
    except KeyboardInterrupt:
        print("\n❌ Collection interrupted by user")
        return False
    except Exception as e:
        logging.error(f"Error during Phase 2 collection: {e}")
        print(f"❌ Collection failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)