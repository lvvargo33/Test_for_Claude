#!/usr/bin/env python3
"""
Test real DFI data collection
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta

def test_dfi_real_search():
    print('🔍 Testing Real DFI Data Collection')
    print('=' * 50)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    base_url = "https://apps.dfi.wi.gov/apps/corpsearch/"
    search_url = base_url + "Advanced.aspx"
    
    try:
        print(f"📥 Loading DFI advanced search form...")
        response = session.get(search_url)
        
        if response.status_code != 200:
            print(f"❌ Failed to load search form: {response.status_code}")
            return False
        
        print(f"✅ Loaded search form successfully")
        
        # Parse the form to understand the structure
        soup = BeautifulSoup(response.content, 'html.parser')
        
        print("\n📋 Analyzing form structure...")
        
        # Find all input elements
        inputs = soup.find_all('input')
        print(f"Found {len(inputs)} input elements:")
        
        for inp in inputs:
            name = inp.get('name', '')
            inp_type = inp.get('type', '')
            inp_id = inp.get('id', '')
            value = inp.get('value', '')
            
            if name or inp_id:
                print(f"   • {inp_type}: name='{name}' id='{inp_id}' value='{value[:50]}...' ")
        
        # Look specifically for date fields
        print(f"\n🗓️  Looking for date fields...")
        date_inputs = soup.find_all('input', {'type': ['text', 'date']})
        for inp in date_inputs:
            name = inp.get('name', '')
            inp_id = inp.get('id', '')
            if 'date' in name.lower() or 'date' in inp_id.lower():
                print(f"   📅 Date field: name='{name}' id='{inp_id}'")
        
        # Find the form element
        form = soup.find('form')
        if form:
            action = form.get('action', '')
            method = form.get('method', 'GET')
            print(f"\n📝 Form details: method={method}, action='{action}'")
        
        # Look for ViewState (ASP.NET)
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        if viewstate:
            print(f"✅ Found ASP.NET ViewState (length: {len(viewstate.get('value', ''))})")
        else:
            print(f"ℹ️  No ViewState found")
        
        # Try a simple search to see what happens
        print(f"\n🧪 Attempting test search...")
        
        # Extract form data
        form_data = {}
        
        # Get all hidden fields (especially important for ASP.NET)
        for hidden in soup.find_all('input', type='hidden'):
            name = hidden.get('name')
            value = hidden.get('value', '')
            if name:
                form_data[name] = value
        
        # Add search parameters - let's try searching for entities with no specific criteria first
        # to see what happens
        form_data.update({
            'ctl00$MainContent$EntityStatusList': 'InExistence',  # Active businesses
            'ctl00$MainContent$EntityNamesList': 'Current',      # Current names
            'ctl00$MainContent$SearchButton': 'Search'
        })
        
        print(f"📤 Submitting test search with {len(form_data)} form fields...")
        
        # Submit the search
        time.sleep(2)  # Rate limiting
        search_response = session.post(search_url, data=form_data)
        
        if search_response.status_code == 200:
            print(f"✅ Search submitted successfully")
            
            # Parse the results
            result_soup = BeautifulSoup(search_response.content, 'html.parser')
            
            # Look for results table
            tables = result_soup.find_all('table')
            print(f"📊 Found {len(tables)} tables in response")
            
            # Look for specific result indicators
            if "results" in search_response.text.lower():
                print(f"✅ Response contains 'results' text")
            
            if "no records found" in search_response.text.lower():
                print(f"ℹ️  No records found message detected")
            
            # Save response for analysis
            with open('dfi_search_response.html', 'w', encoding='utf-8') as f:
                f.write(search_response.text)
            print(f"💾 Saved full response to 'dfi_search_response.html' for analysis")
            
            # Look for any table that might contain results
            for i, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) > 1:  # More than just header
                    print(f"📋 Table {i+1}: {len(rows)} rows")
                    
                    # Show first few cells of first data row
                    if len(rows) > 1:
                        first_row = rows[1]
                        cells = first_row.find_all(['td', 'th'])
                        if cells:
                            cell_texts = [cell.get_text(strip=True)[:50] for cell in cells[:5]]
                            print(f"     Sample cells: {cell_texts}")
            
        else:
            print(f"❌ Search failed: {search_response.status_code}")
            print(f"Response: {search_response.text[:500]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing DFI search: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_dfi_real_search()