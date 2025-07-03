#!/usr/bin/env python3
"""
Seasonal Demand Data Collector
===============================

Collects seasonal consumer spending and demand pattern data including:
- BLS Consumer Expenditure Survey - Quarterly patterns
- Census Monthly Retail Trade Survey - Seasonal variations
- National retail spending patterns by category

Provides data for Section 1.3 Market Demand analysis - Seasonal Patterns Assessment
"""

import requests
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
import yaml
from pathlib import Path

try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None


class DataCollectionError(Exception):
    """Custom exception for data collection errors"""
    pass


class SeasonalDemandCollector:
    """Collector for seasonal consumer spending and demand patterns"""
    
    def __init__(self, config_path: str = "data_sources.yaml"):
        self.config_path = config_path
        self.api_key = "c177d400482b4df282ff74850f23a7d9"  # BLS API key
        self.base_url = "https://api.bls.gov/publicAPI/v2"
        self.census_base_url = "https://api.census.gov/data"
        self.logger = self._setup_logging()
        
        # Rate limiting (BLS allows 500 queries per day with API key)
        self.request_delay = 0.5  # 500ms between requests
        self.max_retries = 3
        
        # Industry/category codes for seasonal analysis
        self.spending_categories = self._get_spending_categories()
        
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    def _get_spending_categories(self) -> Dict[str, str]:
        """Consumer Price Index categories for seasonal analysis"""
        return {
            'food_away_from_home': 'CUUR0000SEFV',  # Food away from home
            'food_at_home': 'CUUR0000SAF1',  # Food at home
            'apparel': 'CUUR0000SAA',  # Apparel
            'recreation': 'CUUR0000SAR',  # Recreation
            'alcoholic_beverages': 'CUUR0000SAF116',  # Alcoholic beverages
            'transportation': 'CUUR0000SAT',  # Transportation
            'housing': 'CUUR0000SAH1',  # Housing
            'medical_care': 'CUUR0000SAM',  # Medical care
            'education': 'CUUR0000SAE1',  # Education and communication
            'other_goods_services': 'CUUR0000SAG1'  # Other goods and services
        }

    def _make_bls_request(self, series_ids: List[str], start_year: int, end_year: int) -> Dict[str, Any]:
        """Make BLS API request with error handling"""
        url = f"{self.base_url}/timeseries/data/"
        
        headers = {'Content-type': 'application/json'}
        data = {
            'seriesid': series_ids,
            'startyear': str(start_year),
            'endyear': str(end_year),
            'registrationkey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.request_delay)
                response = requests.post(url, json=data, headers=headers, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                if result.get('status') == 'REQUEST_SUCCEEDED':
                    return result
                else:
                    self.logger.warning(f"BLS API returned status: {result.get('status')}")
                    self.logger.warning(f"BLS API response: {result}")
                    return result  # Return even if not successful to debug
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise DataCollectionError(f"Failed to fetch BLS data after {self.max_retries} attempts")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise DataCollectionError("Failed to get successful response from BLS API")

    def _make_census_request(self, url: str) -> Dict[str, Any]:
        """Make Census API request with error handling"""
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.request_delay)
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                return result
                    
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Census request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise DataCollectionError(f"Failed to fetch Census data after {self.max_retries} attempts")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise DataCollectionError("Failed to get successful response from Census API")

    def collect_consumer_price_monthly(self, years: int = 3) -> List[Dict[str, Any]]:
        """
        Collect BLS Consumer Price Index monthly data for seasonal analysis
        
        Args:
            years: Number of years of historical data
            
        Returns:
            List of monthly price index records
        """
        current_year = datetime.now().year
        start_year = current_year - years
        
        price_data = []
        
        for category_name, series_id in self.spending_categories.items():
            try:
                self.logger.info(f"Collecting monthly price data for {category_name}")
                self.logger.info(f"Series ID: {series_id}")
                
                result = self._make_bls_request([series_id], start_year, current_year)
                
                # Process monthly data
                for series in result.get('Results', {}).get('series', []):
                    for data_point in series.get('data', []):
                        # Determine quarter and month from period
                        period = data_point['period']
                        quarter = self._parse_quarter(period)
                        month = self._parse_month(period)
                        
                        record = {
                            'category': category_name,
                            'series_id': series_id,
                            'year': int(data_point['year']),
                            'period': period,
                            'month': month,
                            'quarter': quarter,
                            'value': data_point['value'],
                            'collected_at': datetime.now().isoformat()
                        }
                        price_data.append(record)
                
                self.logger.info(f"Collected {len([r for r in price_data if r['category'] == category_name])} records for {category_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to collect data for {category_name}: {e}")
                continue
        
        return price_data

    def _parse_quarter(self, period: str) -> Optional[int]:
        """Parse BLS period code to quarter number"""
        if period.startswith('Q'):
            try:
                return int(period[2:])  # Q01 -> 1, Q02 -> 2, etc.
            except (ValueError, IndexError):
                return None
        elif period in ['M01', 'M02', 'M03']:
            return 1
        elif period in ['M04', 'M05', 'M06']:
            return 2
        elif period in ['M07', 'M08', 'M09']:
            return 3
        elif period in ['M10', 'M11', 'M12']:
            return 4
        else:
            return None

    def _parse_month(self, period: str) -> Optional[int]:
        """Parse BLS period code to month number"""
        if period.startswith('M'):
            try:
                return int(period[1:])  # M01 -> 1, M02 -> 2, etc.
            except (ValueError, IndexError):
                return None
        else:
            return None

    def collect_monthly_retail_trade(self, years: int = 3) -> List[Dict[str, Any]]:
        """
        Collect Census Monthly Retail Trade Survey data
        
        Args:
            years: Number of years of historical data
            
        Returns:
            List of monthly retail trade records
        """
        current_year = datetime.now().year
        start_year = current_year - years
        
        retail_data = []
        
        # Retail trade categories that align with restaurant business
        retail_categories = {
            'food_services_drinking': '722',  # Food services and drinking places
            'general_retail': '44-45',  # Retail trade
            'gasoline_stations': '447',  # Gasoline stations (convenience spending)
            'grocery_stores': '4451'  # Grocery stores
        }
        
        for year in range(start_year, current_year + 1):
            for category_name, naics_code in retail_categories.items():
                try:
                    # Census Monthly Retail Trade API endpoint
                    # Note: This is a placeholder URL - actual Census retail trade API may have different structure
                    url = f"{self.census_base_url}/timeseries/eits/marts?get=cell_value,data_type_code,time_slot_id,error_data,category_code&for=us:*&NAICS2017={naics_code}&time={year}"
                    
                    self.logger.info(f"Collecting monthly retail data for {category_name} ({year})")
                    
                    # Note: This is simplified - actual Census API structure may differ
                    # Using placeholder data structure for now
                    result = self._make_census_request(url)
                    
                    # Process monthly data (placeholder structure)
                    if isinstance(result, list) and len(result) > 1:
                        headers = result[0]
                        for row in result[1:]:
                            if len(row) >= len(headers):
                                record = {
                                    'category': category_name,
                                    'naics_code': naics_code,
                                    'year': year,
                                    'value': row[0] if row[0] != 'null' else None,
                                    'data_type': row[1] if len(row) > 1 else None,
                                    'time_slot': row[2] if len(row) > 2 else None,
                                    'collected_at': datetime.now().isoformat()
                                }
                                retail_data.append(record)
                    
                    self.logger.info(f"Collected monthly retail data for {category_name} ({year})")
                    
                except Exception as e:
                    self.logger.error(f"Failed to collect retail data for {category_name} ({year}): {e}")
                    continue
        
        return retail_data

    def create_seasonal_patterns(self, expenditure_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze seasonal patterns from expenditure data
        
        Args:
            expenditure_data: Raw quarterly expenditure data
            
        Returns:
            Seasonal pattern analysis
        """
        if not expenditure_data:
            return {}
        
        df = pd.DataFrame(expenditure_data)
        seasonal_analysis = {}
        
        # Analyze patterns by category
        for category in df['category'].unique():
            category_df = df[df['category'] == category]
            
            if len(category_df) >= 4:  # Need at least 4 quarters
                # Calculate average by quarter
                quarterly_avg = category_df.groupby('quarter')['value'].apply(
                    lambda x: pd.to_numeric(x, errors='coerce').mean()
                ).to_dict()
                
                # Calculate seasonal indices (Q1 = 100 baseline)
                if 1 in quarterly_avg and quarterly_avg[1] > 0:
                    seasonal_indices = {
                        q: round((avg / quarterly_avg[1]) * 100, 1) 
                        for q, avg in quarterly_avg.items()
                    }
                    
                    # Identify peak and low seasons
                    peak_quarter = max(quarterly_avg.keys(), key=lambda k: quarterly_avg[k])
                    low_quarter = min(quarterly_avg.keys(), key=lambda k: quarterly_avg[k])
                    
                    seasonal_analysis[category] = {
                        'quarterly_averages': {k: float(v) for k, v in quarterly_avg.items()},
                        'seasonal_indices': seasonal_indices,
                        'peak_quarter': int(peak_quarter),
                        'low_quarter': int(low_quarter),
                        'seasonality_strength': round(
                            (quarterly_avg[peak_quarter] - quarterly_avg[low_quarter]) / quarterly_avg[low_quarter] * 100, 1
                        ),
                        'data_points': len(category_df)
                    }
        
        return seasonal_analysis

    def generate_seasonal_insights(self, seasonal_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate business insights from seasonal patterns
        
        Args:
            seasonal_analysis: Seasonal pattern analysis
            
        Returns:
            Business insights and recommendations
        """
        insights = {
            'peak_seasons': {},
            'low_seasons': {},
            'recommendations': [],
            'seasonal_trends': {}
        }
        
        # Map quarters to seasons
        quarter_seasons = {
            1: 'Winter (Q1)',
            2: 'Spring (Q2)', 
            3: 'Summer (Q3)',
            4: 'Fall (Q4)'
        }
        
        for category, analysis in seasonal_analysis.items():
            peak_q = analysis['peak_quarter']
            low_q = analysis['low_quarter']
            strength = analysis['seasonality_strength']
            
            insights['peak_seasons'][category] = quarter_seasons[peak_q]
            insights['low_seasons'][category] = quarter_seasons[low_q]
            insights['seasonal_trends'][category] = {
                'strength': strength,
                'pattern': 'highly_seasonal' if strength > 20 else 'moderately_seasonal' if strength > 10 else 'stable'
            }
        
        # Generate recommendations for restaurant business
        if 'food_away_from_home' in seasonal_analysis:
            food_analysis = seasonal_analysis['food_away_from_home']
            peak_q = food_analysis['peak_quarter']
            
            if peak_q == 4:  # Fall peak
                insights['recommendations'].append("Fall season shows peak dining demand - consider seasonal menu offerings")
            elif peak_q == 3:  # Summer peak
                insights['recommendations'].append("Summer season shows peak dining demand - outdoor seating and seasonal items recommended")
            elif peak_q == 2:  # Spring peak
                insights['recommendations'].append("Spring season shows peak dining demand - fresh seasonal ingredients and promotions")
            else:  # Winter peak
                insights['recommendations'].append("Winter season shows peak dining demand - comfort food and holiday catering opportunities")
        
        return insights

    def save_to_csv(self, data: List[Dict[str, Any]], filename: str) -> str:
        """Save collected data to CSV file"""
        if not data:
            self.logger.warning("No data to save")
            return ""
        
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{filename}_{timestamp}.csv"
        
        df.to_csv(output_file, index=False)
        self.logger.info(f"Saved {len(data)} records to {output_file}")
        return output_file

    def collect_all_seasonal_data(self, years: int = 3) -> Dict[str, Any]:
        """
        Collect comprehensive seasonal demand data
        
        Args:
            years: Number of years of historical data
            
        Returns:
            Comprehensive seasonal demand analysis
        """
        self.logger.info("Starting comprehensive seasonal demand data collection")
        
        # Collect monthly price data
        price_data = self.collect_consumer_price_monthly(years)
        
        # Collect monthly retail trade data (may have limited success)
        try:
            retail_data = self.collect_monthly_retail_trade(years)
        except Exception as e:
            self.logger.warning(f"Retail trade collection failed: {e}")
            retail_data = []
        
        # Analyze seasonal patterns
        seasonal_analysis = self.create_seasonal_patterns(price_data)
        
        # Generate business insights
        insights = self.generate_seasonal_insights(seasonal_analysis)
        
        # Save raw data
        price_file = self.save_to_csv(price_data, "monthly_price_data")
        retail_file = self.save_to_csv(retail_data, "monthly_retail_data") if retail_data else ""
        
        # Create summary
        summary = {
            'collection_date': datetime.now().isoformat(),
            'years_analyzed': years,
            'categories_analyzed': len(self.spending_categories),
            'price_records': len(price_data),
            'retail_records': len(retail_data),
            'seasonal_analysis': seasonal_analysis,
            'business_insights': insights,
            'output_files': {
                'price_data': price_file,
                'retail_data': retail_file
            }
        }
        
        # Save summary
        summary_file = f"seasonal_demand_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        self.logger.info(f"Seasonal demand data collection complete. Summary saved to {summary_file}")
        return summary


def main():
    """Test the seasonal demand collector"""
    collector = SeasonalDemandCollector()
    
    # Collect seasonal data
    summary = collector.collect_all_seasonal_data(years=3)
    
    print(f"Collection Summary:")
    print(f"- Years analyzed: {summary['years_analyzed']}")
    print(f"- Categories analyzed: {summary['categories_analyzed']}")
    print(f"- Price records: {summary['price_records']}")
    print(f"- Retail records: {summary['retail_records']}")
    print(f"- Seasonal patterns found: {len(summary['seasonal_analysis'])}")


if __name__ == "__main__":
    main()