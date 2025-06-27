#!/usr/bin/env python3
"""
Secure Google Places API Collection Runner
==========================================

This script safely handles API keys for Google Places data collection.
The API key is never stored permanently and is cleared from memory after use.
"""

import os
import sys
import tempfile
import logging
from pathlib import Path

from google_places_collector import GooglePlacesCollector


def get_api_key_securely():
    """Get API key from environment variable or temporary file"""
    
    # Option 1: Check environment variable first
    api_key = os.environ.get('GOOGLE_PLACES_API_KEY')
    if api_key:
        print("✓ Using API key from environment variable")
        return api_key
    
    # Option 2: Check for temporary key file
    temp_key_file = '/tmp/google_places_key.txt'
    if os.path.exists(temp_key_file):
        try:
            with open(temp_key_file, 'r') as f:
                api_key = f.read().strip()
            
            # Immediately delete the file
            os.remove(temp_key_file)
            print("✓ Using API key from temporary file (file deleted)")
            return api_key
            
        except Exception as e:
            print(f"Error reading temporary key file: {e}")
    
    # Option 3: Interactive input (hidden)
    try:
        import getpass
        api_key = getpass.getpass("Enter Google Places API Key (hidden input): ")
        return api_key
    except (ImportError, KeyboardInterrupt):
        print("API key input cancelled")
        return None


def clean_up_credentials():
    """Clean up any remaining credential traces"""
    # Clear environment variable if it exists
    if 'GOOGLE_PLACES_API_KEY' in os.environ:
        del os.environ['GOOGLE_PLACES_API_KEY']
    
    # Remove any temporary files
    temp_files = ['/tmp/google_places_key.txt', '/tmp/gp_key.txt']
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass


def main():
    """Run secure Google Places collection"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 60)
    print("SECURE GOOGLE PLACES API COLLECTION")
    print("=" * 60)
    print()
    print("API Key Options:")
    print("1. Set environment variable: export GOOGLE_PLACES_API_KEY='your_key'")
    print("2. Create temporary file: echo 'your_key' > /tmp/google_places_key.txt")
    print("3. Interactive input (will prompt)")
    print()
    
    try:
        # Get API key securely
        api_key = get_api_key_securely()
        
        if not api_key:
            print("❌ No API key provided. Exiting.")
            return
        
        if len(api_key) < 30:
            print("⚠️  Warning: API key seems too short. Please verify.")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                return
        
        print(f"✓ API key loaded (length: {len(api_key)} characters)")
        print()
        
        # Initialize collector
        print("Initializing Google Places collector...")
        collector = GooglePlacesCollector(api_key)
        
        # Clear the API key from memory immediately
        api_key = None
        
        print("✓ Collector initialized")
        print()
        print("Starting Phase 1 data collection...")
        print("- Milwaukee County: 13 search areas")
        print("- Dane County (Madison): 12 search areas") 
        print("- Brown County (Green Bay): 8 search areas")
        print()
        
        # Run collection
        summary = collector.run_phase1_collection()
        
        # Display results
        print("\n" + "=" * 60)
        print("COLLECTION SUMMARY")
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
        print("COLLECTION COMPLETE")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n❌ Collection interrupted by user")
    except Exception as e:
        logging.error(f"Error during collection: {e}")
        print(f"❌ Collection failed: {e}")
    finally:
        # Always clean up credentials
        clean_up_credentials()
        print("🔒 Credentials cleaned up")


if __name__ == "__main__":
    main()