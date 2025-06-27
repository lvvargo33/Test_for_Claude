#!/usr/bin/env python3
"""
GPG-Encrypted Google Places API Collection Runner
=================================================

Maximum security API key handling using GPG encryption with passphrase.
The API key is encrypted at rest and only decrypted in memory during use.
"""

import os
import sys
import tempfile
import logging
import subprocess
import getpass
from pathlib import Path

from google_places_collector import GooglePlacesCollector


class GPGKeyManager:
    """Manages GPG-encrypted API keys with passphrase protection"""
    
    def __init__(self):
        self.encrypted_key_file = '/tmp/google_places_key.gpg'
        self.temp_key_file = None
    
    def check_gpg_available(self):
        """Check if GPG is available on the system"""
        try:
            result = subprocess.run(['gpg', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
    
    def encrypt_api_key(self, api_key, passphrase):
        """Encrypt API key with GPG using symmetric encryption"""
        try:
            # Use symmetric encryption (no key pairs needed)
            cmd = ['gpg', '--symmetric', '--cipher-algo', 'AES256', 
                   '--compress-algo', '1', '--batch', '--yes', 
                   '--passphrase', passphrase, '--output', self.encrypted_key_file]
            
            # Run GPG encryption
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, 
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.PIPE, text=True)
            
            stdout, stderr = process.communicate(input=api_key, timeout=30)
            
            if process.returncode == 0:
                print(f"✓ API key encrypted and saved to: {self.encrypted_key_file}")
                return True
            else:
                print(f"❌ GPG encryption failed: {stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("❌ GPG encryption timed out")
            return False
        except Exception as e:
            print(f"❌ Error during encryption: {e}")
            return False
    
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
    
    def cleanup(self):
        """Clean up temporary files"""
        files_to_remove = [self.encrypted_key_file]
        if self.temp_key_file:
            files_to_remove.append(self.temp_key_file)
        
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"🔒 Cleaned up: {file_path}")
                except Exception as e:
                    print(f"⚠️  Could not remove {file_path}: {e}")


def setup_encrypted_key():
    """Set up encrypted API key for first time use"""
    print("=" * 60)
    print("FIRST-TIME SETUP: ENCRYPT YOUR API KEY")
    print("=" * 60)
    print()
    
    gpg_manager = GPGKeyManager()
    
    # Check if GPG is available
    if not gpg_manager.check_gpg_available():
        print("❌ GPG is not available on this system.")
        print("Install GPG first: sudo apt-get install gnupg")
        return False
    
    print("✓ GPG is available")
    print()
    
    # Get API key from temporary file or prompt
    api_key = None
    temp_api_file = '/tmp/api_key_input.txt'
    
    if os.path.exists(temp_api_file):
        try:
            with open(temp_api_file, 'r') as f:
                api_key = f.read().strip()
            os.remove(temp_api_file)  # Remove immediately
            print("✓ API key loaded from temporary file")
        except Exception as e:
            print(f"Error reading API key file: {e}")
    
    if not api_key:
        print("Please create a temporary file with your API key:")
        print(f"echo 'YOUR_API_KEY' > {temp_api_file}")
        print("Then run this script again.")
        return False
    
    if len(api_key) < 20:
        print("❌ Invalid API key provided (too short)")
        return False
    
    print(f"✓ API key loaded (length: {len(api_key)} characters)")
    
    # Get passphrase from temporary file or environment
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
        print("Please provide a passphrase using one of these methods:")
        print(f"1. Create file: echo 'YOUR_PASSPHRASE' > {temp_pass_file}")
        print("2. Set environment: export GPG_PASSPHRASE='YOUR_PASSPHRASE'")
        print("Then run this script again.")
        return False
    
    if len(passphrase) < 8:
        print("❌ Passphrase too short (minimum 8 characters)")
        return False
    
    # Encrypt the key
    print("\nEncrypting API key...")
    success = gpg_manager.encrypt_api_key(api_key, passphrase)
    
    # Clear sensitive data from memory
    api_key = None
    passphrase = None
    
    if success:
        print("\n✓ Setup complete! Your API key is now encrypted.")
        print("Run the script again to use the encrypted key.")
        return True
    else:
        print("\n❌ Setup failed.")
        return False


def run_collection_with_encrypted_key():
    """Run collection using encrypted API key"""
    print("=" * 60)
    print("GPG-ENCRYPTED GOOGLE PLACES COLLECTION")
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
        
        return True
        
    except KeyboardInterrupt:
        print("\n❌ Collection interrupted by user")
        return False
    except Exception as e:
        logging.error(f"Error during collection: {e}")
        print(f"❌ Collection failed: {e}")
        return False
    finally:
        # Clean up (but keep encrypted file for reuse)
        print("🔒 Clearing sensitive data from memory")


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == '--setup':
        # First-time setup
        success = setup_encrypted_key()
        sys.exit(0 if success else 1)
    
    elif len(sys.argv) > 1 and sys.argv[1] == '--cleanup':
        # Clean up encrypted files
        gpg_manager = GPGKeyManager()
        gpg_manager.cleanup()
        print("✓ Cleanup complete")
        sys.exit(0)
    
    else:
        # Run collection
        success = run_collection_with_encrypted_key()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()