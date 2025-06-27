#!/usr/bin/env python3
"""
GPG-Encrypted Google Places Phase 3 Collection Runner
====================================================

Runs Phase 3 collection with GPG-encrypted API key security for:
- Sauk County (Wisconsin Dells)
- Door County (Sturgeon Bay, Fish Creek, Ephraim)  
- La Crosse County (La Crosse)
- Portage County (Stevens Point)
- Douglas County (Superior)
- Rock County (Beloit)
"""

import os
import sys
import subprocess
import getpass
from pathlib import Path

from google_places_collector import GooglePlacesCollector


class GPGKeyManager:
    """Manages GPG-encrypted API keys with passphrase protection"""
    
    def __init__(self):
        self.encrypted_key_file = '/tmp/google_places_key.gpg'
    
    def decrypt_api_key(self, passphrase):
        """Decrypt API key from GPG file"""
        if not os.path.exists(self.encrypted_key_file):
            print(f"❌ Encrypted key file not found: {self.encrypted_key_file}")
            return None
        
        try:
            # Decrypt using GPG
            cmd = ['gpg', '--decrypt', '--batch', '--yes', '--quiet',
                   '--passphrase', passphrase, self.encrypted_key_file]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                api_key = result.stdout.strip()
                print("✓ API key decrypted successfully")
                return api_key
            else:
                print(f"❌ GPG decryption failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print("❌ GPG decryption timed out")
            return None
        except Exception as e:
            print(f"❌ Error during decryption: {e}")
            return None


def run_phase3_collection():
    """Run Phase 3 collection using encrypted API key"""
    print("=" * 60)
    print("GPG-ENCRYPTED GOOGLE PLACES PHASE 3 COLLECTION")
    print("=" * 60)
    print()
    
    gpg_manager = GPGKeyManager()
    
    # Check if encrypted key exists
    if not os.path.exists(gpg_manager.encrypted_key_file):
        print("❌ No encrypted API key found.")
        print("Run setup first with: python run_google_places_gpg.py --setup")
        return False
    
    try:
        # Get passphrase from environment or temporary file
        passphrase = None
        temp_pass_file = '/tmp/passphrase_input.txt'
        
        if os.path.exists(temp_pass_file):
            try:
                with open(temp_pass_file, 'r') as f:
                    passphrase = f.read().strip()
                os.remove(temp_pass_file)  # Remove immediately
                print("✓ Passphrase loaded from temporary file")
            except Exception as e:
                print(f"Error reading passphrase file: {e}")
        
        if not passphrase:
            passphrase = os.environ.get('GPG_PASSPHRASE')
            if passphrase:
                print("✓ Passphrase loaded from environment variable")
        
        if not passphrase:
            print("Please provide the decryption passphrase using one of these methods:")
            print(f"1. Create file: echo 'YOUR_PASSPHRASE' > {temp_pass_file}")
            print("2. Set environment: export GPG_PASSPHRASE='YOUR_PASSPHRASE'")
            return False
        
        # Decrypt API key
        print("Decrypting API key...")
        api_key = gpg_manager.decrypt_api_key(passphrase)
        
        # Clear passphrase from memory
        passphrase = None
        
        if not api_key:
            print("❌ Failed to decrypt API key. Check your passphrase.")
            return False
        
        print("✓ API key decrypted successfully")
        print()
        
        # Initialize collector
        print("Initializing Google Places collector...")
        collector = GooglePlacesCollector(api_key)
        
        # Clear API key from memory immediately
        api_key = None
        
        print("✓ Collector initialized")
        print(f"✓ Phase 3 search areas: {len(collector.phase3_search_areas)}")
        print()
        print("Starting Phase 3 data collection...")
        print("Counties and Cities:")
        print("- Sauk County (Wisconsin Dells, Baraboo): 6 areas")
        print("- Door County (Sturgeon Bay, Fish Creek, Ephraim): 8 areas")
        print("- La Crosse County (La Crosse, Onalaska): 8 areas")
        print("- Portage County (Stevens Point, Plover): 8 areas")
        print("- Douglas County (Superior): 8 areas")
        print("- Rock County (Beloit, Janesville): 8 areas")
        print("- Total: 46 search areas")
        print()
        
        # Run Phase 3 collection
        summary = collector.run_phase3_collection()
        
        # Display results
        print("\n" + "=" * 60)
        print("PHASE 3 COLLECTION SUMMARY")
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
        print("PHASE 3 COLLECTION COMPLETE")
        print("=" * 60)
        
        return True
        
    except KeyboardInterrupt:
        print("\n❌ Collection interrupted by user")
        return False
    except Exception as e:
        print(f"❌ Collection failed: {e}")
        return False
    finally:
        # Clean up (but keep encrypted file for reuse)
        print("🔒 Clearing sensitive data from memory")


def main():
    """Main entry point"""
    success = run_phase3_collection()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()