#!/usr/bin/env python3
"""
Test DFI date range search for recent business registrations
"""

import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta

def test_dfi_date_search():
    print('🗓️  Testing DFI Date Range Search')
    print('=' * 50)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    search_url = "https://apps.dfi.wi.gov/apps/corpsearch/Advanced.aspx"
    
    try:
        # Step 1: Get the form
        print('📥 Loading search form...')
        response = session.get(search_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Step 2: Extract form data
        form_data = {}
        for hidden in soup.find_all('input', type='hidden'):
            name = hidden.get('name')
            value = hidden.get('value', '')
            if name:
                form_data[name] = value
        
        # Step 3: Search for recent registrations (last 6 months)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        
        start_date_str = start_date.strftime('%m/%d/%Y')
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        form_data.update({
            'ctl00$cpContent$rblIncludeActiveEntities': 'Only',  # Active businesses only
            'ctl00$cpContent$rblIncludeOldNames': 'Exclude',     # Current names only
            'ctl00$cpContent$txtIncorporationDateStart': start_date_str,  # Start date
            'ctl00$cpContent$txtIncorporationDateEnd': end_date_str,      # End date
            'ctl00$cpContent$btnSearch2': 'Search Records'               # Search button
        })
        
        print(f'🔍 Searching for businesses registered between {start_date_str} and {end_date_str}...')
        time.sleep(2)  # Rate limiting
        
        search_response = session.post(search_url, data=form_data)
        
        if search_response.status_code == 200:
            print('✅ Search successful')
            
            # Save and analyze response
            with open('dfi_date_search_results.html', 'w', encoding='utf-8') as f:
                f.write(search_response.text)
            
            # Parse for results
            result_soup = BeautifulSoup(search_response.content, 'html.parser')
            
            # Look for results count
            count_span = result_soup.find('span', id='ctl00_cpContent_lblResultsCount')
            if count_span:
                count_text = count_span.get_text(strip=True)
                print(f'📊 {count_text}')
            
            # Look for results table
            results_table = result_soup.find('table', {'id': 'results'})
            if results_table:
                rows = results_table.find_all('tr')[1:]  # Skip header
                print(f'📋 Found {len(rows)} business records')
                
                # Show first few target businesses
                target_count = 0
                for i, row in enumerate(rows[:20]):  # Look at first 20
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        # Extract business name
                        name_cell = cells[1]
                        name_link = name_cell.find('span', class_='name')
                        if name_link and name_link.find('a'):
                            business_name = name_link.find('a').get_text(strip=True).lower()
                        else:
                            business_name = name_cell.get_text(strip=True).split('\n')[0].strip().lower()
                        
                        # Check if it's a target business type
                        target_keywords = [
                            'pizza', 'restaurant', 'cafe', 'bar', 'grill', 'food', 'salon', 
                            'fitness', 'gym', 'auto', 'repair', 'gas', 'hotel', 'retail',
                            'store', 'shop', 'spa', 'cleaning', 'child care', 'daycare'
                        ]
                        
                        is_target = any(keyword in business_name for keyword in target_keywords)
                        
                        if is_target:
                            target_count += 1
                            business_id = cells[0].get_text(strip=True)
                            reg_date = cells[2].get_text(strip=True)
                            status = cells[3].find('span', class_='statusDescription')
                            status_text = status.get_text(strip=True) if status else cells[3].get_text(strip=True).split('\n')[0].strip()
                            
                            print(f'   🎯 {business_name.title()} (ID: {business_id})')
                            print(f'      Date: {reg_date} | Status: {status_text}')
                
                print(f'\n🎯 Found {target_count} target businesses out of {len(rows)} total')
                
                if target_count > 0:
                    print('✅ DFI date search is working and finding target businesses!')
                    return True
                else:
                    print('ℹ️  No target businesses found in recent registrations')
            else:
                print('❌ No results table found')
                
                # Check for "no records" message
                if 'no records found' in search_response.text.lower():
                    print('ℹ️  DFI returned "no records found" - try longer date range')
                
        else:
            print(f'❌ Search failed: {search_response.status_code}')
            
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
        
    return False

if __name__ == "__main__":
    test_dfi_date_search()