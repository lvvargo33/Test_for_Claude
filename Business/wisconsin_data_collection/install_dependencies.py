#!/usr/bin/env python3
"""
Smart Dependency Installer
==========================

Installs dependencies in the right order with proper error handling.
"""

import subprocess
import sys
import time

def install_package(package, timeout=120):
    """Install a single package with timeout"""
    print(f"📦 Installing {package}...")
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", package, "--upgrade"
        ], capture_output=True, text=True, timeout=timeout)
        
        if result.returncode == 0:
            print(f"✅ {package} installed successfully")
            return True
        else:
            print(f"❌ Failed to install {package}: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {package} installation timed out")
        return False

def install_core_dependencies():
    """Install core dependencies"""
    print("🚀 Installing core dependencies...")
    
    # Core packages in order of importance
    core_packages = [
        "pydantic",
        "PyYAML", 
        "requests",
        "beautifulsoup4",
        "python-dotenv",
        "tenacity"
    ]
    
    failed_packages = []
    
    for package in core_packages:
        if not install_package(package, timeout=60):
            failed_packages.append(package)
        time.sleep(1)  # Brief pause between installs
    
    return failed_packages

def install_optional_dependencies():
    """Install optional heavy dependencies"""
    print("\n🔧 Installing optional dependencies...")
    
    optional_packages = [
        "pandas",
        "google-cloud-bigquery",
        "pyarrow",
        "lxml"
    ]
    
    failed_packages = []
    
    for package in optional_packages:
        print(f"\n⚠️  Installing {package} (this may take a while)...")
        if not install_package(package, timeout=300):  # Longer timeout for heavy packages
            failed_packages.append(package)
            print(f"   Skipping {package} - you can install it later if needed")
        time.sleep(2)
    
    return failed_packages

def main():
    """Main installer function"""
    print("🎯 Smart Dependency Installer")
    print("=" * 35)
    
    # Install core dependencies first
    core_failures = install_core_dependencies()
    
    if core_failures:
        print(f"\n❌ Failed to install core packages: {', '.join(core_failures)}")
        print("   Please install these manually:")
        for package in core_failures:
            print(f"     pip install {package}")
        return False
    
    print("\n✅ All core dependencies installed!")
    
    # Ask about optional dependencies
    install_optional = input("\n❓ Install optional dependencies (BigQuery, Pandas)? [y/N]: ").lower().startswith('y')
    
    if install_optional:
        optional_failures = install_optional_dependencies()
        if optional_failures:
            print(f"\n⚠️  Some optional packages failed: {', '.join(optional_failures)}")
            print("   The system will work in offline mode without these packages")
    
    print("\n🎉 Dependency installation complete!")
    print("   Run: python test_lightweight.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
