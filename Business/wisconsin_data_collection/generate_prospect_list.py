#!/usr/bin/env python3
"""
Prospect List Generator - Offline Mode
=====================================

Generates a realistic prospect list using sample data to show what your
outreach list would look like in a real business scenario.
"""

import csv
import random
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from wisconsin_collector import WisconsinDataCollector
from models import BusinessType

def generate_prospect_list():
    """Generate a prospect list with realistic sample data"""
    print("🎯 Generating Wisconsin Business Prospect List")
    print("=" * 50)
    
    # Create collector instance for data generation
    collector = object.__new__(WisconsinDataCollector)
    collector.county_mappings = {
        'Milwaukee': 'Milwaukee',
        'Madison': 'Dane', 
        'Green Bay': 'Brown',
        'Kenosha': 'Kenosha',
        'Racine': 'Racine',
        'Appleton': 'Outagamie',
        'Waukesha': 'Waukesha'
    }
    
    # Generate sample data
    businesses = collector._generate_realistic_wi_businesses(50)
    sba_loans = collector._generate_sample_sba_loans(30)
    licenses = collector._generate_sample_milwaukee_licenses(25)
    
    prospects = []
    
    # Process SBA loan recipients (HIGH PRIORITY)
    print("📊 Processing SBA loan recipients...")
    for loan in sba_loans:
        if loan.loan_amount >= 100000:  # Focus on larger loans
            days_since = random.randint(1, 60)  # Recent approvals
            
            prospect = {
                'source': 'SBA_LOAN',
                'business_name': loan.borrower_name,
                'city': loan.borrower_city,
                'state': loan.borrower_state,
                'contact_approach': f"Phone: Congratulations on your ${loan.loan_amount:,.0f} SBA approval! Free location analysis available.",
                'value_indicator': f"${loan.loan_amount:,.0f}",
                'days_since_trigger': days_since,
                'lead_quality': 'HIGH' if loan.loan_amount >= 300000 else 'MEDIUM',
                'priority_score': 1,
                'franchise_info': loan.franchise_name if loan.franchise_name else 'Independent',
                'program_type': loan.program_type,
                'next_action': 'Call within 48 hours - congratulate and offer market insights'
            }
            prospects.append(prospect)
    
    # Process new business registrations (MEDIUM PRIORITY)
    print("📊 Processing new business registrations...")
    target_types = [BusinessType.RESTAURANT, BusinessType.RETAIL, BusinessType.FRANCHISE, BusinessType.FITNESS]
    
    for business in businesses:
        if business.business_type in target_types:
            days_since = random.randint(1, 30)  # Recently registered
            
            prospect = {
                'source': 'NEW_BUSINESS',
                'business_name': business.business_name,
                'city': business.city,
                'state': business.state,
                'contact_approach': f"Email: Welcome to Wisconsin! Complimentary market analysis for new {business.business_type.value} businesses.",
                'value_indicator': f"{business.confidence_score}% confidence",
                'days_since_trigger': days_since,
                'lead_quality': 'HIGH' if business.business_type == BusinessType.FRANCHISE else 'MEDIUM',
                'priority_score': 2,
                'franchise_info': 'Franchise' if business.business_type == BusinessType.FRANCHISE else business.business_type.value.title(),
                'program_type': 'Business Registration',
                'next_action': 'Send welcome email with market insights attachment'
            }
            prospects.append(prospect)
    
    # Process active business licenses (QUALIFIED LEADS)
    print("📊 Processing business license holders...")
    restaurant_licenses = [lic for lic in licenses if 'restaurant' in lic.license_type.lower() or 'food' in lic.license_type.lower()]
    
    for license in restaurant_licenses[:10]:  # Top 10 restaurant licenses
        days_since = random.randint(1, 45)
        
        prospect = {
            'source': 'BUSINESS_LICENSE',
            'business_name': license.business_name,
            'city': license.city,
            'state': license.state,
            'contact_approach': f"Email: Local {license.license_type} expansion opportunities available.",
            'value_indicator': license.status,
            'days_since_trigger': days_since,
            'lead_quality': 'QUALIFIED',
            'priority_score': 3,
            'franchise_info': license.license_type,
            'program_type': 'Active License',
            'next_action': 'Send expansion opportunity case studies'
        }
        prospects.append(prospect)
    
    # Sort prospects by priority and value
    prospects.sort(key=lambda x: (x['priority_score'], -x['days_since_trigger']))
    
    return prospects

def save_prospect_list(prospects, filename="wisconsin_prospects_sample.csv"):
    """Save prospects to CSV file"""
    print(f"💾 Saving {len(prospects)} prospects to {filename}...")
    
    # Define CSV headers
    headers = [
        'Priority', 'Source', 'Business Name', 'City', 'State',
        'Lead Quality', 'Value Indicator', 'Days Since Trigger',
        'Contact Approach', 'Franchise Info', 'Next Action'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for i, prospect in enumerate(prospects, 1):
            writer.writerow([
                i,
                prospect['source'],
                prospect['business_name'],
                prospect['city'],
                prospect['state'],
                prospect['lead_quality'],
                prospect['value_indicator'],
                prospect['days_since_trigger'],
                prospect['contact_approach'],
                prospect['franchise_info'],
                prospect['next_action']
            ])
    
    print(f"✅ Prospect list saved to {filename}")
    return filename

def display_top_prospects(prospects, count=10):
    """Display top prospects in a formatted table"""
    print(f"\n🎯 TOP {count} PROSPECTS FOR OUTREACH")
    print("=" * 80)
    
    for i, prospect in enumerate(prospects[:count], 1):
        print(f"\n{i}. {prospect['business_name']} ({prospect['city']}, {prospect['state']})")
        print(f"   📊 Source: {prospect['source']} | Quality: {prospect['lead_quality']}")
        print(f"   💡 Value: {prospect['value_indicator']}")
        print(f"   📅 Days Since Trigger: {prospect['days_since_trigger']}")
        print(f"   📞 Approach: {prospect['contact_approach'][:70]}...")
        print(f"   ⭐ Next Action: {prospect['next_action']}")
        print("-" * 80)

def show_summary_stats(prospects):
    """Show summary statistics"""
    print(f"\n📈 PROSPECT LIST SUMMARY")
    print("=" * 30)
    
    # Count by source
    source_counts = {}
    quality_counts = {}
    
    for prospect in prospects:
        source = prospect['source']
        quality = prospect['lead_quality']
        
        source_counts[source] = source_counts.get(source, 0) + 1
        quality_counts[quality] = quality_counts.get(quality, 0) + 1
    
    print(f"📊 Total Prospects: {len(prospects)}")
    print(f"")
    print(f"📋 By Source:")
    for source, count in source_counts.items():
        source_name = source.replace('_', ' ').title()
        print(f"   • {source_name}: {count}")
    
    print(f"")
    print(f"⭐ By Lead Quality:")
    for quality, count in quality_counts.items():
        print(f"   • {quality}: {count}")
    
    # Calculate urgency
    urgent_prospects = [p for p in prospects if p['days_since_trigger'] <= 7]
    priority_prospects = [p for p in prospects if p['priority_score'] == 1]
    
    print(f"")
    print(f"🚨 Urgent (≤7 days): {len(urgent_prospects)}")
    print(f"🔥 High Priority (SBA loans): {len(priority_prospects)}")

def main():
    """Main function"""
    print("🚀 Wisconsin Business Prospect List Generator")
    print("=" * 55)
    print("⚡ Generating realistic sample prospect data...")
    print("")
    
    try:
        # Generate prospects
        prospects = generate_prospect_list()
        
        # Show summary
        show_summary_stats(prospects)
        
        # Display top prospects
        display_top_prospects(prospects)
        
        # Save to CSV
        filename = save_prospect_list(prospects)
        
        print(f"\n🎉 SUCCESS!")
        print(f"📋 Generated {len(prospects)} qualified prospects")
        print(f"📁 Saved to: {filename}")
        print(f"💡 This shows what your real prospect list would look like!")
        
        print(f"\n📞 IMMEDIATE ACTIONS:")
        urgent_count = len([p for p in prospects if p['days_since_trigger'] <= 7])
        print(f"   • Call {urgent_count} urgent prospects (≤7 days old)")
        print(f"   • Email welcome messages to new business registrations")
        print(f"   • Send market insights to SBA loan recipients")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating prospect list: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)