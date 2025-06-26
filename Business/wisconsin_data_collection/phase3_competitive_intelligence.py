#!/usr/bin/env python3
"""
Phase 3: Competitive Intelligence - Ad-hoc Analysis Tools
Collects real-time competitive data: foot traffic, reviews, pricing
"""

import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import time
import re
import random
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CompetitiveIntelligenceCollector:
    def __init__(self):
        """Initialize competitive intelligence collector"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Cache for avoiding repeated API calls
        self.cache = {}
        self.cache_duration = 24 * 60 * 60  # 24 hours in seconds
    
    def get_google_popular_times(self, place_name: str, location: str) -> Dict:
        """
        Get Google Popular Times data for a business
        Note: This requires Google Places API or web scraping
        """
        logger.info(f"Getting popular times for {place_name} in {location}")
        
        # Cache key
        cache_key = f"popular_times_{place_name}_{location}"
        
        # Check cache first
        if self._check_cache(cache_key):
            return self.cache[cache_key]['data']
        
        # In production, this would use Google Places API
        # For demo, generating realistic sample data
        popular_times = self._generate_sample_popular_times(place_name)
        
        # Cache the result
        self._store_cache(cache_key, popular_times)
        
        return popular_times
    
    def _generate_sample_popular_times(self, place_name: str) -> Dict:
        """Generate realistic popular times data based on business type"""
        business_type = self._classify_business_type(place_name)
        
        # Different patterns by business type
        patterns = {
            'restaurant': {
                'peak_hours': [12, 13, 18, 19, 20],  # Lunch and dinner
                'busy_days': ['Friday', 'Saturday', 'Sunday'],
                'base_traffic': 30
            },
            'coffee': {
                'peak_hours': [7, 8, 9, 14, 15],  # Morning and afternoon
                'busy_days': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
                'base_traffic': 25
            },
            'retail': {
                'peak_hours': [14, 15, 16, 17, 18],  # Afternoon/evening
                'busy_days': ['Saturday', 'Sunday'],
                'base_traffic': 35
            },
            'fitness': {
                'peak_hours': [6, 7, 17, 18, 19],  # Before/after work
                'busy_days': ['Monday', 'Tuesday', 'Wednesday', 'Thursday'],
                'base_traffic': 40
            }
        }
        
        pattern = patterns.get(business_type, patterns['retail'])
        
        # Generate weekly pattern
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekly_data = {}
        
        for day in days:
            day_multiplier = 1.3 if day in pattern['busy_days'] else 0.8
            hourly_data = []
            
            for hour in range(24):
                base = pattern['base_traffic']
                hour_multiplier = 1.5 if hour in pattern['peak_hours'] else 0.6
                
                # Add some randomness
                traffic = int(base * day_multiplier * hour_multiplier * (0.8 + random.random() * 0.4))
                traffic = max(0, min(100, traffic))  # Keep between 0-100
                
                hourly_data.append({
                    'hour': hour,
                    'traffic_percent': traffic,
                    'wait_time_minutes': max(0, (traffic - 50) // 10) if traffic > 50 else 0
                })
            
            weekly_data[day] = hourly_data
        
        # Calculate insights
        insights = self._analyze_popular_times(weekly_data, pattern)
        
        return {
            'business_name': place_name,
            'business_type': business_type,
            'weekly_patterns': weekly_data,
            'insights': insights,
            'last_updated': datetime.now().isoformat(),
            'data_source': 'Google Places API (simulated)'
        }
    
    def _analyze_popular_times(self, weekly_data: Dict, pattern: Dict) -> Dict:
        """Analyze popular times data for business insights"""
        all_traffic = []
        peak_times = []
        low_times = []
        
        for day, hours in weekly_data.items():
            for hour_data in hours:
                traffic = hour_data['traffic_percent']
                all_traffic.append(traffic)
                
                if traffic >= 70:
                    peak_times.append(f"{day} {hour_data['hour']}:00")
                elif traffic <= 20:
                    low_times.append(f"{day} {hour_data['hour']}:00")
        
        avg_traffic = np.mean(all_traffic)
        
        # Find optimal times for different activities
        lunch_hours = []
        dinner_hours = []
        
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
            day_data = weekly_data[day]
            
            # Lunch opportunity (11am-2pm)
            lunch_traffic = [h['traffic_percent'] for h in day_data[11:15]]
            if max(lunch_traffic) < 60:  # Under-served lunch
                lunch_hours.append(f"{day} 11-2pm")
            
            # Dinner opportunity (5pm-8pm)  
            dinner_traffic = [h['traffic_percent'] for h in day_data[17:21]]
            if max(dinner_traffic) < 60:  # Under-served dinner
                dinner_hours.append(f"{day} 5-8pm")
        
        return {
            'average_traffic': round(avg_traffic, 1),
            'peak_times': peak_times[:5],  # Top 5
            'low_traffic_opportunities': low_times[:5],  # Top 5
            'underserved_lunch_times': lunch_hours,
            'underserved_dinner_times': dinner_hours,
            'busiest_day': max(weekly_data.keys(), key=lambda d: sum(h['traffic_percent'] for h in weekly_data[d])),
            'quietest_day': min(weekly_data.keys(), key=lambda d: sum(h['traffic_percent'] for h in weekly_data[d]))
        }
    
    def get_social_velocity(self, business_name: str, location: str, platform: str = 'google') -> Dict:
        """
        Track social velocity - new reviews per month, rating trends
        """
        logger.info(f"Getting social velocity for {business_name} on {platform}")
        
        cache_key = f"social_velocity_{business_name}_{location}_{platform}"
        
        if self._check_cache(cache_key):
            return self.cache[cache_key]['data']
        
        # Generate sample social velocity data
        velocity_data = self._generate_sample_social_velocity(business_name, platform)
        
        self._store_cache(cache_key, velocity_data)
        return velocity_data
    
    def _generate_sample_social_velocity(self, business_name: str, platform: str) -> Dict:
        """Generate realistic social velocity data"""
        # Simulate 6 months of review data
        months = []
        current_date = datetime.now()
        
        for i in range(6):
            month_date = current_date - timedelta(days=30 * i)
            month_name = month_date.strftime('%Y-%m')
            
            # Generate review data with trends
            base_reviews = random.randint(8, 25)
            trend_factor = 1 + (0.1 * (6 - i))  # Slight upward trend
            
            reviews_this_month = int(base_reviews * trend_factor)
            rating_this_month = 3.5 + random.random() * 1.5  # 3.5-5.0 rating
            
            months.append({
                'month': month_name,
                'new_reviews': reviews_this_month,
                'average_rating': round(rating_this_month, 1),
                'total_reviews': sum(m.get('new_reviews', 0) for m in months) + reviews_this_month,
                'response_rate': random.randint(60, 95),  # % of reviews responded to
                'sentiment_positive': random.randint(70, 90),
                'sentiment_negative': random.randint(5, 15)
            })
        
        months.reverse()  # Chronological order
        
        # Calculate velocity metrics
        recent_3mo = months[-3:]
        prev_3mo = months[:3]
        
        recent_reviews = sum(m['new_reviews'] for m in recent_3mo)
        prev_reviews = sum(m['new_reviews'] for m in prev_3mo)
        
        velocity_trend = ((recent_reviews - prev_reviews) / prev_reviews * 100) if prev_reviews > 0 else 0
        
        return {
            'business_name': business_name,
            'platform': platform,
            'monthly_data': months,
            'velocity_metrics': {
                'reviews_per_month_recent': round(recent_reviews / 3, 1),
                'reviews_per_month_previous': round(prev_reviews / 3, 1),
                'velocity_trend_percent': round(velocity_trend, 1),
                'current_rating': months[-1]['average_rating'],
                'rating_trend': 'Improving' if months[-1]['average_rating'] > months[-2]['average_rating'] else 'Declining',
                'response_rate': months[-1]['response_rate'],
                'engagement_score': min(100, recent_reviews * 2 + months[-1]['response_rate'])
            },
            'last_updated': datetime.now().isoformat(),
            'data_source': f'{platform.title()} Reviews API (simulated)'
        }
    
    def get_competitor_pricing(self, business_type: str, location: str, competitors: List[str]) -> Dict:
        """
        Analyze competitor pricing in the area
        """
        logger.info(f"Analyzing {business_type} pricing in {location}")
        
        cache_key = f"pricing_{business_type}_{location}"
        
        if self._check_cache(cache_key):
            return self.cache[cache_key]['data']
        
        pricing_data = self._generate_sample_pricing_data(business_type, competitors, location)
        
        self._store_cache(cache_key, pricing_data)
        return pricing_data
    
    def _generate_sample_pricing_data(self, business_type: str, competitors: List[str], location: str) -> Dict:
        """Generate realistic competitive pricing data"""
        
        # Price ranges by business type
        price_ranges = {
            'restaurant': {
                'lunch_special': (8, 16),
                'dinner_entree': (12, 28),
                'appetizer': (6, 12),
                'beverage': (2, 6)
            },
            'coffee': {
                'coffee_small': (2, 4),
                'coffee_large': (3, 6),
                'specialty_drink': (4, 7),
                'pastry': (2, 5)
            },
            'fitness': {
                'monthly_membership': (25, 85),
                'class_drop_in': (15, 30),
                'personal_training': (50, 120),
                'initiation_fee': (0, 150)
            },
            'salon': {
                'haircut_basic': (25, 65),
                'color_service': (75, 200),
                'styling': (35, 85),
                'manicure': (20, 50)
            }
        }
        
        price_range = price_ranges.get(business_type, price_ranges['restaurant'])
        
        competitor_data = []
        market_prices = {}
        
        for competitor in competitors:
            competitor_prices = {}
            
            for item, (min_price, max_price) in price_range.items():
                # Add some market positioning variety
                position_multiplier = random.uniform(0.8, 1.3)
                price = (min_price + random.random() * (max_price - min_price)) * position_multiplier
                competitor_prices[item] = round(price, 2)
                
                if item not in market_prices:
                    market_prices[item] = []
                market_prices[item].append(price)
            
            competitor_data.append({
                'name': competitor,
                'prices': competitor_prices,
                'positioning': self._classify_price_positioning(competitor_prices, price_range),
                'last_updated': datetime.now().isoformat()
            })
        
        # Calculate market insights
        market_insights = {}
        for item, prices in market_prices.items():
            market_insights[item] = {
                'min_price': round(min(prices), 2),
                'max_price': round(max(prices), 2),
                'avg_price': round(np.mean(prices), 2),
                'median_price': round(np.median(prices), 2),
                'price_gap_opportunity': self._identify_price_gaps(prices)
            }
        
        return {
            'business_type': business_type,
            'location': location,
            'competitor_analysis': competitor_data,
            'market_insights': market_insights,
            'recommendations': self._generate_pricing_recommendations(market_insights),
            'last_updated': datetime.now().isoformat(),
            'data_source': 'Web scraping + Manual research (simulated)'
        }
    
    def _classify_price_positioning(self, prices: Dict, price_range: Dict) -> str:
        """Classify competitor's price positioning"""
        avg_price = np.mean(list(prices.values()))
        market_avg = np.mean([np.mean(range_val) for range_val in price_range.values()])
        
        if avg_price > market_avg * 1.2:
            return 'Premium'
        elif avg_price < market_avg * 0.8:
            return 'Budget'
        else:
            return 'Mid-Market'
    
    def _identify_price_gaps(self, prices: List[float]) -> str:
        """Identify pricing gap opportunities"""
        sorted_prices = sorted(prices)
        gaps = [sorted_prices[i+1] - sorted_prices[i] for i in range(len(sorted_prices)-1)]
        
        if max(gaps) > np.mean(sorted_prices) * 0.3:
            return f"Large gap between ${min(sorted_prices):.2f} and ${max(sorted_prices):.2f}"
        else:
            return "No significant pricing gaps"
    
    def _generate_pricing_recommendations(self, market_insights: Dict) -> List[str]:
        """Generate pricing strategy recommendations"""
        recommendations = []
        
        for item, insights in market_insights.items():
            min_p, max_p, avg_p = insights['min_price'], insights['max_price'], insights['avg_price']
            
            if max_p - min_p > avg_p * 0.5:  # Large price spread
                recommendations.append(f"High price variance in {item} ({min_p}-{max_p}) - opportunity for value positioning")
            
            if "Large gap" in insights['price_gap_opportunity']:
                recommendations.append(f"Price gap opportunity in {item} market")
        
        return recommendations[:3]  # Top 3 recommendations
    
    def _classify_business_type(self, business_name: str) -> str:
        """Classify business type from name"""
        name_lower = business_name.lower()
        
        if any(word in name_lower for word in ['pizza', 'restaurant', 'grill', 'bistro', 'cafe']):
            return 'restaurant'
        elif any(word in name_lower for word in ['coffee', 'espresso', 'brew', 'bean']):
            return 'coffee'
        elif any(word in name_lower for word in ['fitness', 'gym', 'yoga', 'crossfit']):
            return 'fitness'
        elif any(word in name_lower for word in ['salon', 'spa', 'beauty', 'hair']):
            return 'salon'
        else:
            return 'retail'
    
    def _check_cache(self, key: str) -> bool:
        """Check if cached data is still valid"""
        if key not in self.cache:
            return False
        
        cache_time = self.cache[key]['timestamp']
        return (time.time() - cache_time) < self.cache_duration
    
    def _store_cache(self, key: str, data: Dict):
        """Store data in cache with timestamp"""
        self.cache[key] = {
            'data': data,
            'timestamp': time.time()
        }
    
    def run_competitive_analysis(self, target_location: str, competitors: List[str], 
                                business_type: str = 'restaurant') -> Dict:
        """
        Run comprehensive competitive analysis for a location
        """
        logger.info(f"Running competitive analysis for {target_location}")
        
        analysis_results = {
            'location': target_location,
            'analysis_date': datetime.now().isoformat(),
            'competitors_analyzed': len(competitors),
            'popular_times': {},
            'social_velocity': {},
            'pricing_analysis': {},
            'competitive_insights': []
        }
        
        # Analyze each competitor
        for competitor in competitors:
            logger.info(f"Analyzing {competitor}")
            
            # Get popular times
            popular_times = self.get_google_popular_times(competitor, target_location)
            analysis_results['popular_times'][competitor] = popular_times
            
            # Get social velocity
            social_velocity = self.get_social_velocity(competitor, target_location)
            analysis_results['social_velocity'][competitor] = social_velocity
            
            # Small delay to avoid rate limiting
            time.sleep(1)
        
        # Get pricing analysis
        pricing_analysis = self.get_competitor_pricing(business_type, target_location, competitors)
        analysis_results['pricing_analysis'] = pricing_analysis
        
        # Generate competitive insights
        insights = self._generate_competitive_insights(analysis_results)
        analysis_results['competitive_insights'] = insights
        
        return analysis_results
    
    def _generate_competitive_insights(self, analysis_results: Dict) -> List[str]:
        """Generate actionable competitive insights"""
        insights = []
        
        # Analyze popular times patterns
        all_peak_times = []
        all_low_times = []
        
        for competitor, data in analysis_results['popular_times'].items():
            all_peak_times.extend(data['insights']['peak_times'])
            all_low_times.extend(data['insights']['low_traffic_opportunities'])
        
        if all_low_times:
            insights.append(f"Opportunity: Multiple competitors have low traffic during {all_low_times[0]} - potential gap for new entrant")
        
        # Analyze social velocity
        review_velocities = []
        for competitor, data in analysis_results['social_velocity'].items():
            velocity = data['velocity_metrics']['reviews_per_month_recent']
            review_velocities.append((competitor, velocity))
        
        review_velocities.sort(key=lambda x: x[1], reverse=True)
        
        if len(review_velocities) >= 2:
            leader = review_velocities[0]
            insights.append(f"Social leader: {leader[0]} gaining {leader[1]} reviews/month - investigate their engagement strategy")
        
        # Analyze pricing
        pricing = analysis_results.get('pricing_analysis', {})
        if pricing.get('recommendations'):
            insights.extend(pricing['recommendations'])
        
        return insights[:5]  # Top 5 insights

def main():
    """Demo the competitive intelligence system"""
    collector = CompetitiveIntelligenceCollector()
    
    # Example analysis
    location = "Madison, WI"
    competitors = [
        "Graze Restaurant",
        "The Old Fashioned", 
        "L'Etoile Restaurant",
        "Merchant Public House"
    ]
    
    print("🔍 Running Competitive Intelligence Analysis...")
    print("=" * 60)
    
    results = collector.run_competitive_analysis(location, competitors, "restaurant")
    
    # Print summary
    print(f"\n📍 Location: {results['location']}")
    print(f"📊 Competitors Analyzed: {results['competitors_analyzed']}")
    print(f"📅 Analysis Date: {results['analysis_date']}")
    
    print(f"\n🎯 Key Competitive Insights:")
    for i, insight in enumerate(results['competitive_insights'], 1):
        print(f"  {i}. {insight}")
    
    # Show sample popular times
    if results['popular_times']:
        first_competitor = list(results['popular_times'].keys())[0]
        popular_data = results['popular_times'][first_competitor]
        print(f"\n⏰ {first_competitor} Popular Times:")
        print(f"  Busiest Day: {popular_data['insights']['busiest_day']}")
        print(f"  Average Traffic: {popular_data['insights']['average_traffic']}%")
        print(f"  Peak Times: {', '.join(popular_data['insights']['peak_times'][:3])}")
    
    # Show pricing insights
    pricing = results.get('pricing_analysis', {})
    if pricing.get('market_insights'):
        print(f"\n💰 Pricing Analysis:")
        for item, insights in list(pricing['market_insights'].items())[:3]:
            print(f"  {item}: ${insights['min_price']}-${insights['max_price']} (avg: ${insights['avg_price']})")

if __name__ == "__main__":
    main()