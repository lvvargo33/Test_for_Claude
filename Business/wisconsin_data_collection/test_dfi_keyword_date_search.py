#!/usr/bin/env python3
"""
Test DFI keyword + date search for recent target business registrations
"""

import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta

def test_dfi_keyword_date_search():
    print('🎯 Testing DFI Keyword + Date Search')
    print('=' * 50)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    search_url = "https://apps.dfi.wi.gov/apps/corpsearch/Advanced.aspx"
    
    # Keywords to search for target businesses
    target_keywords = ['RESTAURANT', 'PIZZA', 'CAFE', 'BAR', 'SALON', 'FITNESS', 'AUTO', 'RETAIL']
    
    for keyword in target_keywords[:3]:  # Test first 3 keywords
        try:
            print(f'\n🔍 Searching for "{keyword}" businesses...')
            
            # Step 1: Get the form
            response = session.get(search_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Step 2: Extract form data
            form_data = {}
            for hidden in soup.find_all('input', type='hidden'):
                name = hidden.get('name')
                value = hidden.get('value', '')
                if name:
                    form_data[name] = value
            
            # Step 3: Search for recent registrations (last 6 months) with keyword
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            
            start_date_str = start_date.strftime('%m/%d/%Y')
            end_date_str = end_date.strftime('%m/%d/%Y')
            
            form_data.update({
                'ctl00$cpContent$txtSearchString': keyword,              # Search keyword
                'ctl00$cpContent$rblTextSearchType': 'AllWords',         # Using all words
                'ctl00$cpContent$rblNameSet': 'Entities',                # Search entity names
                'ctl00$cpContent$rblIncludeActiveEntities': 'Only',      # Active businesses only
                'ctl00$cpContent$rblIncludeOldNames': 'Exclude',         # Current names only
                'ctl00$cpContent$txtIncorporationDateStart': start_date_str,  # Start date
                'ctl00$cpContent$txtIncorporationDateEnd': end_date_str,      # End date
                'ctl00$cpContent$btnSearch2': 'Search Records'               # Search button
            })
            
            print(f'   Date range: {start_date_str} to {end_date_str}')
            time.sleep(3)  # Rate limiting
            
            search_response = session.post(search_url, data=form_data)
            
            if search_response.status_code == 200:
                # Parse for results
                result_soup = BeautifulSoup(search_response.content, 'html.parser')
                
                # Look for results count
                count_span = result_soup.find('span', id='ctl00_cpContent_lblResultsCount')
                if count_span:
                    count_text = count_span.get_text(strip=True)
                    print(f'   📊 {count_text}')
                    
                    # Look for results table
                    results_table = result_soup.find('table', {'id': 'results'})
                    if results_table:
                        rows = results_table.find_all('tr')[1:]  # Skip header
                        print(f'   📋 Found {len(rows)} business records')
                        
                        # Show first few results
                        for i, row in enumerate(rows[:3]):  # Show first 3
                            cells = row.find_all('td')
                            if len(cells) >= 4:
                                # Extract business name
                                name_cell = cells[1]
                                name_link = name_cell.find('span', class_='name')
                                if name_link and name_link.find('a'):
                                    business_name = name_link.find('a').get_text(strip=True)
                                else:
                                    business_name = name_cell.get_text(strip=True).split('\n')[0].strip()
                                
                                business_id = cells[0].get_text(strip=True)
                                reg_date = cells[2].get_text(strip=True)
                                status = cells[3].find('span', class_='statusDescription')
                                status_text = status.get_text(strip=True) if status else cells[3].get_text(strip=True).split('\n')[0].strip()
                                
                                print(f'     {i+1}. {business_name} (ID: {business_id})')
                                print(f'        Date: {reg_date} | Status: {status_text}')
                        
                        if len(rows) > 0:
                            print(f'   ✅ Found {len(rows)} {keyword} businesses!')
                            return True
                    else:
                        print(f'   ℹ️  No results table found')
                else:
                    # Check for error messages
                    error_div = result_soup.find('div', class_='formError')
                    if error_div:
                        print(f'   ❌ Error: {error_div.get_text(strip=True)}')
                    else:
                        print(f'   ℹ️  No results found for "{keyword}"')
                        
            else:
                print(f'   ❌ Search failed: {search_response.status_code}')
                
        except Exception as e:
            print(f'   ❌ Error searching "{keyword}": {e}')
            continue
    
    print(f'\n🎯 DFI keyword + date search testing completed')
    return False

if __name__ == "__main__":
    test_dfi_keyword_date_search()