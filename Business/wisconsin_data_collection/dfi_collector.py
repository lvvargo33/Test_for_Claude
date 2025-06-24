"""
Wisconsin DFI Business Registration Collector
=============================================

Collects recent business registrations from Wisconsin Department of Financial Institutions
for location-critical businesses (restaurants, retail, services, etc.)
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from dataclasses import dataclass
import yaml

@dataclass
class DFIBusinessRecord:
    """Wisconsin DFI business registration record"""
    business_name: str
    entity_type: str
    registration_date: str
    status: str
    business_id: str
    agent_name: Optional[str] = None
    business_address: Optional[str] = None
    city: Optional[str] = None
    state: str = 'WI'
    zip_code: Optional[str] = None
    county: Optional[str] = None
    naics_code: Optional[str] = None
    business_type: Optional[str] = None
    source: str = 'DFI'
    
class DFIBusinessCollector:
    """Collector for Wisconsin DFI business registration data"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Load configuration
        with open('data_sources.yaml', 'r') as f:
            config = yaml.safe_load(f)
            self.config = config['states']['wisconsin']['business_registrations']['primary']
            
        self.base_url = "https://apps.dfi.wi.gov/apps/corpsearch/"
        self.search_url = self.base_url + "Advanced.aspx"
        self.rate_limit_delay = 12  # 5 requests per minute = 12 seconds between requests
        
    def classify_business_type(self, business_name: str, naics_code: str = None) -> Optional[str]:
        """
        Classify business type using NAICS codes first, then business name keywords
        
        Args:
            business_name: Name of the business
            naics_code: NAICS code if available
            
        Returns:
            Business type category or None
        """
        business_name_lower = business_name.lower()
        
        # First try NAICS code classification
        if naics_code:
            naics_3digit = naics_code[:3] if len(naics_code) >= 3 else naics_code
            
            for category, details in self.config['target_business_types'].items():
                if naics_3digit in details.get('naics_codes', []):
                    return category
        
        # Then try keyword matching
        for category, details in self.config['target_business_types'].items():
            keywords = details.get('keywords', [])
            for keyword in keywords:
                if keyword in business_name_lower:
                    return category
                    
        return None
    
    def is_target_business(self, business_name: str, naics_code: str = None) -> bool:
        """Check if business matches our target types"""
        return self.classify_business_type(business_name, naics_code) is not None
    
    def _check_for_duplicates(self, business_name: str, registration_date: str) -> bool:
        """
        Check if business already exists in BigQuery to prevent duplicates
        
        Args:
            business_name: Name of the business
            registration_date: Registration date in MM/DD/YYYY format
            
        Returns:
            True if duplicate exists, False otherwise
        """
        try:
            from google.cloud import bigquery
            
            client = bigquery.Client()
            
            # Parse date for comparison
            try:
                reg_date = datetime.strptime(registration_date, '%m/%d/%Y').date()
            except:
                return False  # If date parsing fails, allow the record
            
            query = f"""
            SELECT COUNT(*) as count
            FROM `location-optimizer-1.raw_business_data.dfi_business_registrations`
            WHERE business_name = @business_name
            AND registration_date = @registration_date
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("business_name", "STRING", business_name),
                    bigquery.ScalarQueryParameter("registration_date", "DATE", reg_date),
                ]
            )
            
            results = client.query(query, job_config=job_config).result()
            
            for row in results:
                if row.count > 0:
                    return True  # Duplicate found
                    
            return False  # No duplicate
            
        except Exception as e:
            self.logger.warning(f"Error checking for duplicates: {e}")
            return False  # If check fails, allow the record
    
    def get_search_form_data(self, start_date: str, end_date: str) -> Dict:
        """
        Get form data for DFI advanced search
        
        Args:
            start_date: Start date in MM/DD/YYYY format
            end_date: End date in MM/DD/YYYY format
            
        Returns:
            Form data dictionary
        """
        # Get the search form first to extract any required hidden fields
        try:
            response = self.session.get(self.search_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract ASP.NET ViewState and other hidden fields
            form_data = {}
            
            for hidden in soup.find_all('input', type='hidden'):
                name = hidden.get('name')
                value = hidden.get('value', '')
                if name:
                    form_data[name] = value
            
            # Add our search parameters using correct field names from real DFI form
            form_data.update({
                'ctl00$cpContent$rblIncludeActiveEntities': 'Only',  # Active businesses only
                'ctl00$cpContent$rblIncludeOldNames': 'Exclude',     # Current names only
                'ctl00$cpContent$txtIncorporationDateStart': start_date,  # Start date
                'ctl00$cpContent$txtIncorporationDateEnd': end_date,      # End date
                'ctl00$cpContent$btnSearch2': 'Search Records'           # Search button
            })
            
            return form_data
            
        except Exception as e:
            self.logger.error(f"Error getting form data: {e}")
            return {}
    
    def search_registrations_by_keyword(self, keyword: str, start_date: str, end_date: str, max_results: int = 100) -> List[DFIBusinessRecord]:
        """
        Search for business registrations by keyword and date range
        
        Args:
            keyword: Search keyword (e.g., "RESTAURANT", "SALON", etc.)
            start_date: Start date in MM/DD/YYYY format
            end_date: End date in MM/DD/YYYY format
            max_results: Maximum number of results to return
            
        Returns:
            List of DFI business records
        """
        businesses = []
        
        try:
            self.logger.info(f"Searching DFI registrations for '{keyword}' from {start_date} to {end_date}")
            
            # Get the search form first to extract any required hidden fields
            response = self.session.get(self.search_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract ASP.NET ViewState and other hidden fields
            form_data = {}
            
            for hidden in soup.find_all('input', type='hidden'):
                name = hidden.get('name')
                value = hidden.get('value', '')
                if name:
                    form_data[name] = value
            
            # Add search parameters
            form_data.update({
                'ctl00$cpContent$txtSearchString': keyword,              # Search keyword
                'ctl00$cpContent$rblTextSearchType': 'AllWords',         # Using all words
                'ctl00$cpContent$rblNameSet': 'Entities',                # Search entity names
                'ctl00$cpContent$rblIncludeActiveEntities': 'Only',      # Active businesses only
                'ctl00$cpContent$rblIncludeOldNames': 'Exclude',         # Current names only
                'ctl00$cpContent$txtIncorporationDateStart': start_date, # Start date
                'ctl00$cpContent$txtIncorporationDateEnd': end_date,     # End date
                'ctl00$cpContent$btnSearch2': 'Search Records'           # Search button
            })
            
            # Submit search
            time.sleep(self.rate_limit_delay)  # Rate limiting
            search_response = self.session.post(self.search_url, data=form_data)
            
            if search_response.status_code != 200:
                self.logger.error(f"Search request failed: {search_response.status_code}")
                return businesses
            
            # Parse results
            businesses = self.parse_search_results(search_response.content, max_results)
            
            self.logger.info(f"Found {len(businesses)} '{keyword}' business registrations")
            
        except Exception as e:
            self.logger.error(f"Error searching registrations for '{keyword}': {e}")
            
        return businesses
    
    def parse_search_results(self, html_content: bytes, max_results: int) -> List[DFIBusinessRecord]:
        """Parse search results from DFI response"""
        businesses = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find results table - based on real DFI response structure
            results_table = soup.find('table', {'id': 'results'})
            
            if not results_table:
                self.logger.warning("No results table found in response")
                # Save response for debugging
                with open('dfi_debug_response.html', 'w', encoding='utf-8') as f:
                    f.write(soup.prettify())
                return businesses
            
            rows = results_table.find_all('tr')[1:]  # Skip header row
            self.logger.info(f"Found {len(rows)} result rows to parse")
            
            for i, row in enumerate(rows[:max_results]):
                try:
                    cells = row.find_all('td')
                    if len(cells) >= 4:  # Expected columns: ID, Name/Type, Date, Status
                        
                        # Extract business ID from first column
                        business_id = cells[0].get_text(strip=True)
                        
                        # Extract business name and entity type from second column
                        name_cell = cells[1]
                        name_link = name_cell.find('span', class_='name')
                        if name_link and name_link.find('a'):
                            business_name = name_link.find('a').get_text(strip=True)
                        else:
                            business_name = name_cell.get_text(strip=True).split('\n')[0].strip()
                        
                        # Extract entity type description
                        type_span = name_cell.find('span', class_='typeDescription')
                        if type_span:
                            entity_type = type_span.get_text(strip=True)
                        else:
                            # Fallback: look for type in the cell text
                            cell_text = name_cell.get_text()
                            lines = [line.strip() for line in cell_text.split('\n') if line.strip()]
                            entity_type = lines[1] if len(lines) > 1 else ''
                        
                        # Extract registration date from third column
                        registration_date = cells[2].get_text(strip=True)
                        
                        # Extract status from fourth column
                        status_cell = cells[3]
                        status_span = status_cell.find('span', class_='statusDescription')
                        if status_span:
                            status = status_span.get_text(strip=True)
                        else:
                            status = status_cell.get_text(strip=True).split('\n')[0].strip()
                        
                        # Only process target businesses
                        if self.is_target_business(business_name):
                            # Check for duplicates
                            if not self._check_for_duplicates(business_name, registration_date):
                                business_type = self.classify_business_type(business_name)
                                
                                record = DFIBusinessRecord(
                                    business_name=business_name,
                                    entity_type=entity_type,
                                    registration_date=registration_date,
                                    status=status,
                                    business_id=business_id,
                                    business_type=business_type,
                                    source='DFI'
                                )
                                
                                businesses.append(record)
                                self.logger.debug(f"Found target business: {business_name} ({business_type})")
                            else:
                                self.logger.debug(f"Skipping duplicate business: {business_name}")
                        else:
                            self.logger.debug(f"Skipping non-target business: {business_name}")
                
                except Exception as e:
                    self.logger.warning(f"Error parsing row {i}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error parsing search results: {e}")
        
        return businesses
    
    def collect_recent_registrations(self, days_back: int = 90) -> List[DFIBusinessRecord]:
        """
        Collect recent business registrations for target business types
        
        Args:
            days_back: Number of days to look back for registrations
            
        Returns:
            List of recent business registrations
        """
        all_businesses = []
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Format dates for DFI search (MM/DD/YYYY)
            start_date_str = start_date.strftime('%m/%d/%Y')
            end_date_str = end_date.strftime('%m/%d/%Y')
            
            self.logger.info(f"Collecting DFI registrations from {start_date_str} to {end_date_str}")
            
            # Define search keywords for target business types
            search_keywords = [
                'RESTAURANT', 'PIZZA', 'CAFE', 'BAR', 'GRILL', 'FOOD',
                'SALON', 'SPA', 'BEAUTY', 'HAIR',
                'FITNESS', 'GYM', 'YOGA',
                'AUTO', 'REPAIR', 'AUTOMOTIVE',
                'RETAIL', 'STORE', 'SHOP',
                'GAS', 'FUEL', 'STATION',
                'HOTEL', 'MOTEL', 'INN',
                'CLEANING', 'LAUNDRY',
                'CHILD', 'DAYCARE', 'CARE'
            ]
            
            # Search using each keyword
            for keyword in search_keywords:
                try:
                    businesses = self.search_registrations_by_keyword(keyword, start_date_str, end_date_str)
                    all_businesses.extend(businesses)
                    
                    # Rate limiting between searches
                    if keyword != search_keywords[-1]:  # Don't wait after last search
                        time.sleep(self.rate_limit_delay)
                        
                except Exception as e:
                    self.logger.warning(f"Error searching for '{keyword}': {e}")
                    continue
            
            # Remove duplicates based on business_id
            unique_businesses = []
            seen_ids = set()
            
            for business in all_businesses:
                if business.business_id not in seen_ids:
                    unique_businesses.append(business)
                    seen_ids.add(business.business_id)
                else:
                    self.logger.debug(f"Removing duplicate business: {business.business_name} (ID: {business.business_id})")
            
            # Filter for target counties if specified
            target_counties = self.config.get('target_counties', [])
            if target_counties:
                filtered_businesses = []
                for business in unique_businesses:
                    # This is a simplified county check - in practice we'd need address parsing
                    # For now, we'll include all businesses and handle county filtering later
                    filtered_businesses.append(business)
                unique_businesses = filtered_businesses
            
            self.logger.info(f"Collected {len(unique_businesses)} unique target businesses from DFI")
            
        except Exception as e:
            self.logger.error(f"Error collecting recent registrations: {e}")
        
        return unique_businesses

if __name__ == "__main__":
    # Test the collector
    logging.basicConfig(level=logging.INFO)
    
    collector = DFIBusinessCollector()
    print("🔄 Testing DFI Business Collector...")
    
    # Test business type classification
    test_names = [
        "Joe's Pizza LLC",
        "Madison Hair Salon Inc",
        "Auto Repair Services LLC",
        "Milwaukee Consulting Corp",
        "Fitness Center of Green Bay"
    ]
    
    print("\n📋 Business Type Classification Test:")
    for name in test_names:
        business_type = collector.classify_business_type(name)
        target = "✅" if collector.is_target_business(name) else "❌"
        print(f"   {target} {name} → {business_type}")
    
    print("\n🎯 Ready for real DFI data collection!")