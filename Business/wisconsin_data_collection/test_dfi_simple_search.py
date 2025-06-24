#!/usr/bin/env python3
"""
Simple DFI search test to understand result structure
"""

import requests
from bs4 import BeautifulSoup
import time

def test_simple_dfi_search():
    print('🔍 Simple DFI Search Test')
    print('=' * 30)
    
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
        
        # Step 3: Simple search for any businesses with "LLC" in the name
        form_data.update({
            'ctl00$cpContent$txtSearchString': 'LLC',
            'ctl00$cpContent$rblTextSearchType': 'StartsWith',
            'ctl00$cpContent$rblNameSet': 'Entities',
            'ctl00$cpContent$rblIncludeActiveEntities': 'Only',
            'ctl00$cpContent$btnSearch': 'Search Records'
        })
        
        print('🔍 Searching for businesses starting with "LLC"...')
        time.sleep(2)  # Rate limiting
        
        search_response = session.post(search_url, data=form_data)
        
        if search_response.status_code == 200:
            print('✅ Search successful')
            
            # Save and analyze response
            with open('dfi_simple_search_results.html', 'w', encoding='utf-8') as f:
                f.write(search_response.text)
            
            # Parse for results
            result_soup = BeautifulSoup(search_response.content, 'html.parser')
            
            # Look for common result indicators
            text_content = search_response.text.lower()
            
            if 'no records found' in text_content:
                print('ℹ️  No records found message detected')
            elif 'records found' in text_content:
                print('✅ Records found message detected')
            elif 'results' in text_content:
                print('✅ Results text found')
            
            # Look for tables with data
            tables = result_soup.find_all('table')
            print(f'📊 Found {len(tables)} tables')
            
            for i, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) > 2:  # Tables with actual data
                    print(f'📋 Table {i+1}: {len(rows)} rows')
                    
                    # Check if this might be a results table
                    headers = table.find_all('th')
                    if headers:
                        header_text = [h.get_text(strip=True) for h in headers]
                        print(f'   Headers: {header_text}')
                    
                    # Show sample data rows
                    data_rows = [r for r in rows if r.find_all('td')]
                    if data_rows:
                        print(f'   Data rows: {len(data_rows)}')
                        # Show first data row
                        first_row = data_rows[0]
                        cells = [td.get_text(strip=True) for td in first_row.find_all('td')]
                        print(f'   Sample row: {cells[:5]}')  # First 5 cells
            
            # Look for pagination or result counts
            if 'displaying' in text_content:
                print('📄 Pagination text found')
            
            # Check for specific DFI result patterns
            links = result_soup.find_all('a')
            business_links = [link for link in links if 'detail' in str(link.get('href', '')).lower()]
            if business_links:
                print(f'🔗 Found {len(business_links)} potential business detail links')
                
        else:
            print(f'❌ Search failed: {search_response.status_code}')
            
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_dfi_search()