#!/usr/bin/env python3
"""
Test script for industry health collector
"""

from industry_health_collector import IndustryHealthCollector

def test_single_industry():
    """Test with a single industry"""
    collector = IndustryHealthCollector()
    
    # Test just food services
    msa_code = "31540"  # Madison, WI
    industry_code = "722"  # Food Services
    
    # Build series ID manually
    employment_series = f"ENU{msa_code}{industry_code}1"
    wage_series = f"ENU{msa_code}{industry_code}3"
    
    print(f"Testing series IDs:")
    print(f"Employment: {employment_series}")
    print(f"Wages: {wage_series}")
    
    # Make a single request
    result = collector._make_bls_request([employment_series], 2020, 2024)
    print(f"API Response: {result}")

if __name__ == "__main__":
    test_single_industry()