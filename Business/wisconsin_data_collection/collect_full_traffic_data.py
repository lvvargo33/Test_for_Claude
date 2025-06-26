"""
Collect Full Wisconsin Traffic Data
===================================

Collect comprehensive traffic data for the entire state of Wisconsin.
"""

import pandas as pd
from datetime import datetime
from traffic_data_collector import WisconsinTrafficDataCollector

def main():
    print("COLLECTING FULL WISCONSIN TRAFFIC DATA")
    print("=" * 50)
    
    try:
        # Initialize collector
        collector = WisconsinTrafficDataCollector()
        
        # Collect full dataset
        print("Collecting comprehensive traffic data...")
        records = collector.collect_highway_traffic_data(max_records=25000)  # Get most/all records
        
        if not records:
            print("No traffic records collected")
            return
        
        print(f"Collected {len(records)} traffic records")
        
        # Convert to DataFrame
        data = [record.model_dump() for record in records]  # Use model_dump instead of dict()
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = f"wisconsin_traffic_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        
        print(f"Saved to: {output_file}")
        
        # Comprehensive statistics
        print(f"\n" + "="*50)
        print("WISCONSIN TRAFFIC DATA ANALYSIS")
        print("="*50)
        
        print(f"Dataset Overview:")
        print(f"- Total Records: {len(df):,}")
        print(f"- Counties Covered: {df['county'].nunique()}")
        print(f"- Date Range: {df['measurement_year'].min()} - {df['measurement_year'].max()}")
        print(f"- Average Data Quality: {df['data_quality_score'].mean():.1f}/100")
        
        print(f"\nHighway Type Distribution:")
        highway_dist = df['highway_type'].value_counts()
        for highway_type, count in highway_dist.items():
            pct = (count / len(df)) * 100
            print(f"  {highway_type}: {count:,} ({pct:.1f}%)")
        
        print(f"\nTraffic Volume Analysis:")
        volume_dist = df['traffic_volume_category'].value_counts()
        for volume_cat, count in volume_dist.items():
            pct = (count / len(df)) * 100
            avg_aadt = df[df['traffic_volume_category'] == volume_cat]['aadt'].mean()
            print(f"  {volume_cat}: {count:,} locations ({pct:.1f}%) - Avg AADT: {avg_aadt:,.0f}")
        
        print(f"\nAADT Statistics:")
        print(f"  Minimum: {df['aadt'].min():,}")
        print(f"  Maximum: {df['aadt'].max():,}")
        print(f"  Average: {df['aadt'].mean():,.0f}")
        print(f"  Median: {df['aadt'].median():,.0f}")
        
        print(f"\nTop 15 Counties by Traffic Locations:")
        county_counts = df['county'].value_counts().head(15)
        for county, count in county_counts.items():
            avg_aadt = df[df['county'] == county]['aadt'].mean()
            print(f"  {county}: {count:,} locations - Avg AADT: {avg_aadt:,.0f}")
        
        print(f"\nHigh-Traffic Locations (>50,000 AADT):")
        high_traffic = df[df['aadt'] > 50000].sort_values('aadt', ascending=False)
        if not high_traffic.empty:
            display_cols = ['route_name', 'county', 'aadt', 'highway_type']
            print(high_traffic[display_cols].head(10).to_string(index=False))
        else:
            print("  No locations with >50,000 AADT found")
        
        print(f"\nBusiness Location Relevance:")
        print(f"Summary for site analysis reports:")
        
        # Traffic accessibility metrics
        high_access = len(df[df['aadt'] > 20000])
        medium_access = len(df[(df['aadt'] >= 5000) & (df['aadt'] <= 20000)])
        low_access = len(df[df['aadt'] < 5000])
        
        print(f"- High Accessibility (>20K AADT): {high_access:,} locations")
        print(f"- Medium Accessibility (5K-20K AADT): {medium_access:,} locations") 
        print(f"- Low Accessibility (<5K AADT): {low_access:,} locations")
        
        # Highway access for business types
        interstate_access = len(df[df['highway_type'] == 'Interstate'])
        major_highway_access = len(df[df['highway_type'].isin(['Interstate', 'US Highway'])])
        
        print(f"- Interstate Access Points: {interstate_access:,}")
        print(f"- Major Highway Access: {major_highway_access:,}")
        
        return output_file
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    main()