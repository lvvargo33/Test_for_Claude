#!/usr/bin/env python3
"""
Test error handling and edge cases
"""

import sys
import tempfile
import os
from datetime import date

def test_error_handling():
    print('🧪 Testing Error Handling and Edge Cases')
    print('=' * 50)
    
    errors_found = 0
    
    # Test 1: Invalid data model inputs
    print('\n1. Testing invalid data model inputs...')
    try:
        sys.path.append('.')
        from models import BusinessEntity, SBALoanRecord, BusinessLicense
        
        # Test missing required fields
        try:
            BusinessEntity(business_id='TEST')  # Missing required fields
            print('❌ Should have failed with missing required fields')
            errors_found += 1
        except Exception:
            print('✅ Correctly rejected missing required fields')
        
        # Test invalid date
        try:
            BusinessEntity(
                business_id='TEST', source_id='SRC', business_name='Test', 
                business_type='restaurant', city='Milwaukee', state='WI',
                registration_date='invalid-date', status='active', data_source='test'
            )
            print('❌ Should have failed with invalid date')
            errors_found += 1
        except Exception:
            print('✅ Correctly rejected invalid date format')
            
    except Exception as e:
        print(f'❌ Error in model validation tests: {e}')
        errors_found += 1
    
    # Test 2: Configuration file errors
    print('\n2. Testing configuration file handling...')
    try:
        import yaml
        
        # Test missing config file
        try:
            with open('nonexistent_config.yaml', 'r') as f:
                yaml.safe_load(f)
            print('❌ Should have failed with missing config file')
            errors_found += 1
        except FileNotFoundError:
            print('✅ Correctly handled missing config file')
        except Exception as e:
            print(f'❌ Unexpected error: {e}')
            errors_found += 1
            
        # Test malformed YAML
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('invalid: yaml: content: [unclosed')
            temp_file = f.name
        
        try:
            with open(temp_file, 'r') as f:
                yaml.safe_load(f)
            print('❌ Should have failed with malformed YAML')
            errors_found += 1
        except yaml.YAMLError:
            print('✅ Correctly handled malformed YAML')
        except Exception as e:
            print(f'❌ Unexpected error: {e}')
            errors_found += 1
        finally:
            os.unlink(temp_file)
            
    except Exception as e:
        print(f'❌ Error in config tests: {e}')
        errors_found += 1
    
    # Test 3: Network/API simulation errors
    print('\n3. Testing network error simulation...')
    try:
        import requests
        
        # Test timeout/connection error
        try:
            response = requests.get('http://nonexistent-domain-12345.com', timeout=1)
            print('❌ Should have failed with connection error')
            errors_found += 1
        except requests.exceptions.RequestException:
            print('✅ Correctly handled network connection error')
        except Exception as e:
            print(f'❌ Unexpected error: {e}')
            errors_found += 1
            
    except Exception as e:
        print(f'❌ Error in network tests: {e}')
        errors_found += 1
    
    # Test 4: File system errors
    print('\n4. Testing file system error handling...')
    try:
        import pandas as pd
        
        # Test reading non-existent CSV
        try:
            df = pd.read_csv('nonexistent_file.csv')
            print('❌ Should have failed with missing CSV file')
            errors_found += 1
        except FileNotFoundError:
            print('✅ Correctly handled missing CSV file')
        except Exception as e:
            print(f'❌ Unexpected error: {e}')
            errors_found += 1
            
        # Test malformed CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('header1,header2\nvalue1,value2,extra_value\nincomplete_row')
            temp_csv = f.name
        
        try:
            df = pd.read_csv(temp_csv)
            print('✅ Pandas handled malformed CSV gracefully')
        except Exception as e:
            print(f'✅ Correctly handled malformed CSV: {type(e).__name__}')
        finally:
            os.unlink(temp_csv)
            
    except Exception as e:
        print(f'❌ Error in file system tests: {e}')
        errors_found += 1
    
    # Test 5: Business logic edge cases
    print('\n5. Testing business logic edge cases...')
    try:
        # Test empty strings and None values
        from models import BusinessEntity
        
        # Test empty business name
        try:
            business = BusinessEntity(
                business_id='TEST', source_id='SRC', business_name='',
                business_type='restaurant', city='Milwaukee', state='WI',
                registration_date=date.today(), status='active', data_source='test'
            )
            print('✅ Accepted empty business name (may be valid)')
        except Exception as e:
            print(f'✅ Rejected empty business name: {type(e).__name__}')
        
        # Test very long strings
        try:
            long_name = 'A' * 1000
            business = BusinessEntity(
                business_id='TEST', source_id='SRC', business_name=long_name,
                business_type='restaurant', city='Milwaukee', state='WI',
                registration_date=date.today(), status='active', data_source='test'
            )
            print('✅ Accepted very long business name')
        except Exception as e:
            print(f'✅ Rejected very long business name: {type(e).__name__}')
            
    except Exception as e:
        print(f'❌ Error in business logic tests: {e}')
        errors_found += 1
    
    # Test 6: Memory and performance edge cases
    print('\n6. Testing performance edge cases...')
    try:
        # Test large data generation
        sys.path.append('.')
        from wisconsin_collector import WisconsinDataCollector
        
        # Create collector without BigQuery
        collector = WisconsinDataCollector.__new__(WisconsinDataCollector)
        collector.business_types = {'restaurants': ['restaurant']}
        
        # Test generating large dataset
        try:
            large_dataset = collector.generate_sample_businesses(1000)
            print(f'✅ Generated large dataset: {len(large_dataset)} records')
        except Exception as e:
            print(f'❌ Failed to generate large dataset: {e}')
            errors_found += 1
            
    except Exception as e:
        print(f'❌ Error in performance tests: {e}')
        errors_found += 1
    
    # Summary
    print('\n' + '=' * 50)
    print(f'🎯 Error Handling Test Summary')
    print(f'   Errors found: {errors_found}')
    
    if errors_found == 0:
        print('✅ All error handling tests passed!')
        return True
    else:
        print(f'⚠️  {errors_found} issues found in error handling')
        return False

if __name__ == "__main__":
    success = test_error_handling()
    sys.exit(0 if success else 1)