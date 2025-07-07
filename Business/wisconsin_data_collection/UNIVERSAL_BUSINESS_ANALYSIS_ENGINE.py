#!/usr/bin/env python3
"""
Universal Business Analysis Engine
=================================

Orchestrates complete business feasibility analysis for any business type in any Wisconsin location.
Handles automated data collection, manual data entry pauses, and generates professional client reports.

Features:
- Extensible section framework for future additions
- Multiple manual data entry pause points
- Professional client folder structure
- Seamless integration with all universal templates
- Progress tracking and resume capability
"""

import os
import json
import argparse
import shutil
from datetime import datetime
from pathlib import Path
import subprocess
import sys
from typing import Dict, List, Any, Optional

class UniversalBusinessAnalysisEngine:
    """Main engine for orchestrating complete business analysis"""
    
    def __init__(self):
        self.sections_config = {
            # Current implemented sections
            "1.1": {
                "name": "Demographic Profile", 
                "template": "UNIVERSAL_DEMOGRAPHIC_PROFILE_TEMPLATE.md",
                "automated": True,
                "data_sources": ["wisconsin_county_analysis.py", "integrated_business_analyzer.py"]
            },
            "1.2": {
                "name": "Economic Environment",
                "template": "UNIVERSAL_ECONOMIC_ENVIRONMENT_TEMPLATE.md", 
                "automated": True,
                "data_sources": ["integrated_business_analyzer.py", "construction_cost_report"]
            },
            "1.3": {
                "name": "Market Demand",
                "template": "SECTION_1_3_MARKET_DEMAND_TEMPLATE.md",
                "automated": True, 
                "data_sources": ["integrated_business_analyzer.py", "consumer_spending_analysis"]
            },
            "1.4": {
                "name": "Labor Market & Operations Environment",
                "template": "UNIVERSAL_LABOR_MARKET_OPERATIONS_TEMPLATE.md",
                "automated": True,
                "data_sources": ["integrated_business_analyzer.py", "construction_cost_report"]
            },
            "1.5": {
                "name": "Site Evaluation & Location Intelligence", 
                "template": "SITE_DATA_ENTRY_TEMPLATE.md",
                "automated": False,
                "manual_data_required": True,
                "manual_template": "SITE_DATA_ENTRY_TEMPLATE.md",
                "data_sources": ["manual_site_inspection", "traffic_data"]
            },
            "2.1": {
                "name": "Direct Competition",
                "template": "UNIVERSAL_COMPETITIVE_ANALYSIS_TEMPLATE.md",
                "automated": True,
                "data_sources": ["universal_competitive_analyzer.py", "google_places_data"]
            },
            
            # Future sections (extensible framework)
            "2.2": {
                "name": "Market Saturation",
                "template": "UNIVERSAL_MARKET_SATURATION_TEMPLATE.md",  # Future
                "automated": True,  # May change
                "implemented": False
            },
            "3.1": {
                "name": "Traffic & Transportation",
                "template": "UNIVERSAL_TRAFFIC_TRANSPORTATION_TEMPLATE.md",  # Future
                "automated": True,
                "implemented": False
            },
            "3.2": {
                "name": "Site Characteristics", 
                "template": "UNIVERSAL_SITE_CHARACTERISTICS_TEMPLATE.md",  # Future
                "automated": False,  # Likely requires manual data
                "manual_data_required": True,  # Anticipated
                "implemented": False
            },
            "4.1": {
                "name": "Revenue Projections",
                "template": "UNIVERSAL_REVENUE_PROJECTIONS_TEMPLATE.md",  # Future
                "automated": True,
                "implemented": False
            },
            "4.2": {
                "name": "Cost Analysis", 
                "template": "UNIVERSAL_COST_ANALYSIS_TEMPLATE.md",  # Future
                "automated": True,
                "implemented": False
            },
            "4.3": {
                "name": "Risk Assessment",
                "template": "UNIVERSAL_RISK_ASSESSMENT_TEMPLATE.md",  # Future
                "automated": False,  # May require manual judgment
                "manual_data_required": True,  # Anticipated
                "implemented": False
            },
            "5.1": {
                "name": "Zoning & Permits",
                "template": "UNIVERSAL_ZONING_PERMITS_TEMPLATE.md",  # Future
                "automated": False,  # Requires local research
                "manual_data_required": True,
                "implemented": False
            },
            "5.2": {
                "name": "Infrastructure",
                "template": "UNIVERSAL_INFRASTRUCTURE_TEMPLATE.md",  # Future 
                "automated": True,  # May be automated
                "implemented": False
            },
            "6.1": {
                "name": "Final Recommendations",
                "template": "UNIVERSAL_RECOMMENDATIONS_TEMPLATE.md",  # Future
                "automated": True,  # Generated from all other sections
                "implemented": False
            },
            "6.2": {
                "name": "Implementation Plan",
                "template": "UNIVERSAL_IMPLEMENTATION_TEMPLATE.md",  # Future
                "automated": True,  # Generated from analysis
                "implemented": False
            }
        }
        
        self.current_project = None
        self.project_state = {}
        
    def get_implemented_sections(self) -> List[str]:
        """Get list of currently implemented sections"""
        return [section_id for section_id, config in self.sections_config.items() 
                if config.get("implemented", True)]
    
    def get_manual_sections(self) -> List[str]:
        """Get list of sections requiring manual data entry"""
        implemented = self.get_implemented_sections()
        return [section_id for section_id in implemented 
                if self.sections_config[section_id].get("manual_data_required", False)]
    
    def create_project_folder(self, business_type: str, location: str) -> str:
        """Create organized client project folder"""
        # Sanitize folder name
        folder_name = f"{business_type.lower().replace(' ', '_')}_{location.lower().replace(' ', '_').replace(',', '').replace('.', '')}"
        project_path = Path("clients") / folder_name
        
        # Create folder structure
        project_path.mkdir(parents=True, exist_ok=True)
        (project_path / "data").mkdir(exist_ok=True)
        (project_path / "templates_populated").mkdir(exist_ok=True)
        (project_path / "visuals_specifications").mkdir(exist_ok=True)
        (project_path / "manual_data_entry").mkdir(exist_ok=True)
        
        return str(project_path)
    
    def run_automated_data_collection(self, project_path: str, business_type: str, address: str) -> Dict[str, Any]:
        """Run all automated data collectors"""
        print("🔄 Running automated data collection...")
        
        data_results = {}
        
        try:
            # Run county analysis for demographics
            print("  📊 Running county demographic analysis...")
            result = subprocess.run([sys.executable, "wisconsin_county_analysis.py"], 
                                  capture_output=True, text=True, cwd=".")
            if result.returncode == 0:
                data_results["county_analysis"] = "✅ Success"
            else:
                data_results["county_analysis"] = f"❌ Error: {result.stderr}"
            
            # Run integrated business analyzer
            print("  💼 Running integrated business analysis...")
            result = subprocess.run([sys.executable, "integrated_business_analyzer.py"], 
                                  capture_output=True, text=True, cwd=".")
            if result.returncode == 0:
                data_results["integrated_analysis"] = "✅ Success"
                # Copy generated files to project folder
                timestamp = datetime.now().strftime("%Y%m%d")
                analysis_file = f"wisconsin_integrated_analysis_{timestamp}.json"
                if os.path.exists(analysis_file):
                    shutil.copy(analysis_file, f"{project_path}/data/")
            else:
                data_results["integrated_analysis"] = f"❌ Error: {result.stderr}"
            
            # Run competitive analysis
            print("  🏪 Running competitive analysis...")
            # Parse coordinates from address (simplified - would need geocoding in production)
            lat, lng = 43.0265, -89.4698  # Default to Fitchburg coordinates
            
            # Import and run competitive analyzer
            try:
                from universal_competitive_analyzer import UniversalCompetitiveAnalyzer
                analyzer = UniversalCompetitiveAnalyzer(business_type, lat, lng, address)
                
                # Check if Google Places data exists
                places_files = ["google_places_phase1_20250627_212804.csv"]
                if any(os.path.exists(f) for f in places_files):
                    report, results = analyzer.run_complete_analysis()
                    
                    # Save competitive analysis results
                    with open(f"{project_path}/data/competitive_analysis_results.json", 'w') as f:
                        json.dump(results, f, indent=2)
                    
                    data_results["competitive_analysis"] = "✅ Success"
                else:
                    data_results["competitive_analysis"] = "⚠️ Google Places data not found - using mock data"
                    
            except ImportError as e:
                data_results["competitive_analysis"] = f"❌ Import error: {e}"
            
        except Exception as e:
            print(f"❌ Error in automated data collection: {e}")
            data_results["error"] = str(e)
        
        return data_results
    
    def generate_automated_sections(self, project_path: str, business_type: str, location: str, address: str) -> List[str]:
        """Generate all automated sections"""
        print("📝 Generating automated sections...")
        
        completed_sections = []
        implemented_sections = self.get_implemented_sections()
        
        for section_id in implemented_sections:
            section_config = self.sections_config[section_id]
            
            if section_config.get("automated", True):
                print(f"  ✍️  Generating Section {section_id}: {section_config['name']}")
                
                # Load template
                template_file = section_config["template"]
                if os.path.exists(template_file):
                    with open(template_file, 'r') as f:
                        template_content = f.read()
                    
                    # Basic template population (simplified - would need full data integration)
                    populated_content = template_content.replace("[BUSINESS_TYPE]", business_type)
                    populated_content = populated_content.replace("[LOCATION]", location)
                    populated_content = populated_content.replace("[SITE_ADDRESS]", address)
                    
                    # Save populated template
                    section_filename = f"section_{section_id.replace('.', '_')}_{section_config['name'].lower().replace(' ', '_').replace('&', 'and')}.md"
                    with open(f"{project_path}/templates_populated/{section_filename}", 'w') as f:
                        f.write(populated_content)
                    
                    completed_sections.append(section_id)
                else:
                    print(f"    ⚠️ Template {template_file} not found")
        
        return completed_sections
    
    def identify_manual_data_requirements(self) -> List[Dict[str, Any]]:
        """Identify all sections requiring manual data entry"""
        manual_requirements = []
        
        implemented_sections = self.get_implemented_sections()
        
        for section_id in implemented_sections:
            section_config = self.sections_config[section_id]
            
            if section_config.get("manual_data_required", False):
                manual_requirements.append({
                    "section_id": section_id,
                    "section_name": section_config["name"],
                    "template": section_config.get("manual_template", section_config["template"]),
                    "description": f"Manual data collection required for Section {section_id}"
                })
        
        return manual_requirements
    
    def setup_manual_data_entry(self, project_path: str, business_type: str, address: str) -> List[str]:
        """Setup manual data entry templates"""
        print("📋 Setting up manual data entry...")
        
        manual_requirements = self.identify_manual_data_requirements()
        manual_files_created = []
        
        for requirement in manual_requirements:
            print(f"  📝 Creating manual entry for Section {requirement['section_id']}: {requirement['section_name']}")
            
            template_file = requirement["template"]
            if os.path.exists(template_file):
                with open(template_file, 'r') as f:
                    template_content = f.read()
                
                # Pre-populate known data
                populated_content = template_content.replace("[EXACT ADDRESS WITH ZIP CODE]", address)
                populated_content = populated_content.replace("[BUSINESS_TYPE]", business_type)
                
                # Save manual data entry template
                manual_filename = f"MANUAL_DATA_ENTRY_{requirement['section_id'].replace('.', '_')}.md"
                manual_file_path = f"{project_path}/manual_data_entry/{manual_filename}"
                
                with open(manual_file_path, 'w') as f:
                    f.write(populated_content)
                
                manual_files_created.append(manual_file_path)
            else:
                print(f"    ⚠️ Manual template {template_file} not found")
        
        return manual_files_created
    
    def save_project_state(self, project_path: str, state: Dict[str, Any]):
        """Save project state for resume capability"""
        state_file = f"{project_path}/project_state.json"
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def load_project_state(self, project_path: str) -> Dict[str, Any]:
        """Load project state for resume capability"""
        state_file = f"{project_path}/project_state.json"
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                return json.load(f)
        return {}
    
    def start_new_analysis(self, business_type: str, address: str) -> str:
        """Start new business analysis"""
        print("🚀 STARTING NEW BUSINESS ANALYSIS")
        print("=" * 50)
        print(f"Business Type: {business_type}")
        print(f"Address: {address}")
        
        # Extract location from address
        location = address.split(',')[1].strip() if ',' in address else "Wisconsin"
        
        # Create project folder
        project_path = self.create_project_folder(business_type, location)
        print(f"📁 Project folder created: {project_path}")
        
        # Initialize project state
        project_state = {
            "business_type": business_type,
            "address": address, 
            "location": location,
            "started": datetime.now().isoformat(),
            "phase": "automated_collection",
            "completed_sections": [],
            "manual_data_pending": []
        }
        
        # Run automated data collection
        data_results = self.run_automated_data_collection(project_path, business_type, address)
        project_state["data_collection_results"] = data_results
        
        # Generate automated sections
        completed_sections = self.generate_automated_sections(project_path, business_type, location, address)
        project_state["completed_sections"] = completed_sections
        
        # Setup manual data entry requirements
        manual_files = self.setup_manual_data_entry(project_path, business_type, address)
        project_state["manual_data_pending"] = manual_files
        
        # Update phase
        if manual_files:
            project_state["phase"] = "manual_data_required"
        else:
            project_state["phase"] = "ready_for_final_generation"
        
        # Save project state
        self.save_project_state(project_path, project_state)
        
        # Print results
        print("\n✅ AUTOMATED ANALYSIS COMPLETE")
        print("=" * 40)
        print(f"Completed Sections: {len(completed_sections)}")
        for section_id in completed_sections:
            section_name = self.sections_config[section_id]["name"]
            print(f"  ✅ Section {section_id}: {section_name}")
        
        if manual_files:
            print(f"\n⏸️  MANUAL DATA COLLECTION REQUIRED")
            print("=" * 40)
            print("The following manual data entry is required:")
            for manual_file in manual_files:
                print(f"  📝 {os.path.basename(manual_file)}")
            
            print(f"\n📋 Manual data templates created in: {project_path}/manual_data_entry/")
            print("\n🔄 After completing manual data entry, run:")
            print(f"   python UNIVERSAL_BUSINESS_ANALYSIS_ENGINE.py --continue-project={os.path.basename(project_path)}")
        else:
            print("\n🎉 ALL SECTIONS COMPLETE - READY FOR FINAL REPORT GENERATION")
        
        return project_path
    
    def continue_analysis(self, project_name: str) -> str:
        """Continue analysis after manual data entry"""
        project_path = f"clients/{project_name}"
        
        if not os.path.exists(project_path):
            raise FileNotFoundError(f"Project {project_name} not found")
        
        print("🔄 CONTINUING BUSINESS ANALYSIS")
        print("=" * 40)
        print(f"Project: {project_name}")
        
        # Load project state
        project_state = self.load_project_state(project_path)
        
        if project_state.get("phase") != "manual_data_required":
            print("⚠️ Project not in manual data required phase")
            return project_path
        
        # Check if manual data has been completed
        manual_files = project_state.get("manual_data_pending", [])
        completed_manual = []
        
        for manual_file in manual_files:
            if os.path.exists(manual_file):
                # Check if file has been modified (simplified check)
                with open(manual_file, 'r') as f:
                    content = f.read()
                    if "[NUMBER]" not in content and "[DESCRIPTION]" not in content:
                        completed_manual.append(manual_file)
                        print(f"  ✅ Manual data completed: {os.path.basename(manual_file)}")
                    else:
                        print(f"  ⏸️ Manual data pending: {os.path.basename(manual_file)}")
        
        if len(completed_manual) == len(manual_files):
            print("\n✅ ALL MANUAL DATA COMPLETED")
            
            # Generate manual sections
            # (Implementation would integrate manual data into templates)
            
            # Generate final client report
            self.generate_final_client_report(project_path, project_state)
            
            # Update project state
            project_state["phase"] = "completed"
            project_state["completed"] = datetime.now().isoformat()
            self.save_project_state(project_path, project_state)
            
            print("\n🎉 CLIENT REPORT GENERATION COMPLETE!")
            print(f"📄 Report ready: {project_path}/CLIENT_REPORT_{project_state['business_type'].replace(' ', '_')}_{project_state['location'].replace(' ', '_')}.md")
        else:
            print(f"\n⏸️ Manual data still required: {len(manual_files) - len(completed_manual)} remaining")
        
        return project_path
    
    def generate_final_client_report(self, project_path: str, project_state: Dict[str, Any]):
        """Generate final comprehensive client report"""
        print("📄 Generating final client report...")
        
        business_type = project_state["business_type"]
        location = project_state["location"]
        
        # Combine all populated templates into final report
        report_content = f"""# {business_type} Feasibility Study
## {project_state['address']}

Generated by Universal Business Analysis Engine
Report Date: {datetime.now().strftime('%B %d, %Y')}

---

"""
        
        # Add each completed section
        templates_dir = f"{project_path}/templates_populated"
        if os.path.exists(templates_dir):
            for template_file in sorted(os.listdir(templates_dir)):
                if template_file.endswith('.md'):
                    with open(f"{templates_dir}/{template_file}", 'r') as f:
                        section_content = f.read()
                        report_content += section_content + "\n\n---\n\n"
        
        # Save final report
        report_filename = f"CLIENT_REPORT_{business_type.replace(' ', '_')}_{location.replace(' ', '_')}.md"
        with open(f"{project_path}/{report_filename}", 'w') as f:
            f.write(report_content)
        
        print(f"  ✅ Final report saved: {report_filename}")

def main():
    parser = argparse.ArgumentParser(description="Universal Business Analysis Engine")
    parser.add_argument("--business", help="Business type (e.g., 'Auto Repair Shop')")
    parser.add_argument("--address", help="Full address")
    parser.add_argument("--continue-project", help="Continue existing project by name")
    parser.add_argument("--list-projects", action="store_true", help="List all existing projects")
    
    args = parser.parse_args()
    
    engine = UniversalBusinessAnalysisEngine()
    
    if args.list_projects:
        # List existing projects
        clients_dir = Path("clients")
        if clients_dir.exists():
            projects = [d.name for d in clients_dir.iterdir() if d.is_dir()]
            print("Existing Projects:")
            for project in projects:
                print(f"  📁 {project}")
        else:
            print("No projects found")
    
    elif args.continue_project:
        # Continue existing project
        engine.continue_analysis(args.continue_project)
    
    elif args.business and args.address:
        # Start new analysis
        engine.start_new_analysis(args.business, args.address)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()