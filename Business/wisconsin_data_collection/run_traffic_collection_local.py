"""
Run Traffic Collection with Local Storage
==========================================

Collect traffic data and save it locally regardless of BigQuery status.
"""

import pandas as pd
from datetime import datetime
from traffic_data_collector import WisconsinTrafficDataCollector

def main():
    print("WISCONSIN TRAFFIC DATA COLLECTION")
    print("=" * 50)
    print(f"Collection started: {datetime.now()}")
    
    try:
        # Initialize collector
        collector = WisconsinTrafficDataCollector()
        print("✓ Traffic data collector initialized")
        
        # Collect comprehensive traffic data
        print("\nCollecting Wisconsin DOT traffic data...")
        records = collector.collect_highway_traffic_data(max_records=25000)
        
        print(f"✓ Collected {len(records)} traffic records")
        
        if not records:
            print("No records collected")
            return
        
        # Convert to DataFrame
        print("Processing and saving data...")
        data = [record.model_dump() for record in records]
        df = pd.DataFrame(data)
        
        # Generate output files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save full dataset as CSV
        csv_file = f"wisconsin_traffic_data_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        print(f"✓ Saved CSV: {csv_file}")
        
        # Save as JSON for data analysis
        json_file = f"wisconsin_traffic_data_{timestamp}.json"
        df.to_json(json_file, orient='records', indent=2)
        print(f"✓ Saved JSON: {json_file}")
        
        # Create summary statistics
        summary = {
            'collection_timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'counties_covered': df['county'].nunique(),
            'date_range': {
                'earliest': int(df['measurement_year'].min()),
                'latest': int(df['measurement_year'].max())
            },
            'highway_types': df['highway_type'].value_counts().to_dict(),
            'traffic_volumes': df['traffic_volume_category'].value_counts().to_dict(),
            'aadt_statistics': {
                'min': int(df['aadt'].min()),
                'max': int(df['aadt'].max()),
                'mean': float(df['aadt'].mean()),
                'median': float(df['aadt'].median())
            },
            'top_counties': df['county'].value_counts().head(10).to_dict(),
            'data_quality': {
                'average_score': float(df['data_quality_score'].mean()),
                'high_quality_pct': float((df['data_quality_score'] >= 80).mean() * 100)
            }
        }
        
        # Save summary
        import json
        summary_file = f"wisconsin_traffic_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"✓ Saved summary: {summary_file}")
        
        # Print key statistics
        print(f"\n" + "="*50)
        print("COLLECTION SUMMARY")
        print("="*50)
        print(f"Total Records: {summary['total_records']:,}")
        print(f"Counties: {summary['counties_covered']}")
        print(f"Date Range: {summary['date_range']['earliest']}-{summary['date_range']['latest']}")
        print(f"Average AADT: {summary['aadt_statistics']['mean']:,.0f}")
        print(f"Data Quality: {summary['data_quality']['average_score']:.1f}/100")
        
        print(f"\nHighway Distribution:")
        for highway_type, count in summary['highway_types'].items():
            pct = (count / summary['total_records']) * 100
            print(f"  {highway_type}: {count:,} ({pct:.1f}%)")
        
        print(f"\nTop 5 Counties:")
        for county, count in list(summary['top_counties'].items())[:5]:
            print(f"  {county}: {count:,} locations")
        
        print(f"\nFiles created:")
        print(f"  • {csv_file}")
        print(f"  • {json_file}")
        print(f"  • {summary_file}")
        
        return csv_file, json_file, summary_file
        
    except Exception as e:
        print(f"ERROR: {e}")
        return None

if __name__ == "__main__":
    main()