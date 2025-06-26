"""
Export Traffic Data Sample
==========================

Export a sample of the collected traffic data to CSV for review.
"""

import pandas as pd
from datetime import datetime
from traffic_data_collector import WisconsinTrafficDataCollector

def main():
    print("EXPORTING WISCONSIN TRAFFIC DATA SAMPLE")
    print("=" * 50)
    
    try:
        # Initialize collector
        collector = WisconsinTrafficDataCollector()
        
        # Collect sample data
        print("Collecting traffic data sample...")
        records = collector.collect_highway_traffic_data(max_records=500)
        
        if not records:
            print("No traffic records collected")
            return
        
        print(f"Collected {len(records)} traffic records")
        
        # Convert to DataFrame
        data = [record.dict() for record in records]
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = f"wisconsin_traffic_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        
        print(f"Saved to: {output_file}")
        
        # Show summary statistics
        print(f"\nTraffic Data Summary:")
        print(f"- Total Records: {len(df):,}")
        print(f"- Counties: {df['county'].nunique()}")
        print(f"- Highway Types: {df['highway_type'].value_counts().to_dict()}")
        print(f"- AADT Range: {df['aadt'].min():,} - {df['aadt'].max():,}")
        print(f"- Average Data Quality Score: {df['data_quality_score'].mean():.1f}")
        
        # Show sample records
        print(f"\nSample Records:")
        sample_cols = ['route_name', 'county', 'aadt', 'highway_type', 'traffic_volume_category', 'data_quality_score']
        print(df[sample_cols].head(10).to_string(index=False))
        
        # Show geographic distribution
        print(f"\nGeographic Distribution (Top 10 Counties):")
        county_counts = df['county'].value_counts().head(10)
        for county, count in county_counts.items():
            print(f"  {county}: {count} locations")
        
        return output_file
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    main()