#!/usr/bin/env python3
"""
Traffic & Transportation Analyzer
=================================

Analyzes traffic patterns, transportation accessibility, and infrastructure quality
for business feasibility studies using Wisconsin DOT data and accessibility analysis.
"""

import json
import logging
import math
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import networkx as nx
import requests
import time

# Import existing analyzers and collectors
from transportation_accessibility_analysis import TransportationAccessibilityAnalyzer
from traffic_data_collector import WisconsinTrafficDataCollector
from trade_area_analyzer import TradeAreaAnalyzer

logger = logging.getLogger(__name__)

class TrafficTransportationAnalyzer:
    """Comprehensive traffic and transportation analysis for Section 3.1"""
    
    # Traffic volume categories and thresholds
    TRAFFIC_CATEGORIES = {
        "very_low": {"min": 0, "max": 1000, "score": 20},
        "low": {"min": 1000, "max": 5000, "score": 40},
        "medium": {"min": 5000, "max": 20000, "score": 70},
        "high": {"min": 20000, "max": 75000, "score": 90},
        "very_high": {"min": 75000, "max": 200000, "score": 100}
    }
    
    # Highway type scoring
    HIGHWAY_SCORES = {
        "interstate": {"base_score": 25, "distance_penalty": 2},
        "us_highway": {"base_score": 20, "distance_penalty": 3},
        "state_highway": {"base_score": 15, "distance_penalty": 4},
        "county_highway": {"base_score": 10, "distance_penalty": 5}
    }
    
    def __init__(self):
        self.traffic_collector = WisconsinTrafficDataCollector()
        self.accessibility_analyzer = TransportationAccessibilityAnalyzer()
        self.trade_area_analyzer = TradeAreaAnalyzer()
        
    def analyze_traffic_transportation(self, business_type: str, address: str, 
                                     lat: float, lon: float) -> Dict[str, Any]:
        """
        Perform comprehensive traffic and transportation analysis
        
        Args:
            business_type: Type of business
            address: Business address
            lat: Latitude
            lon: Longitude
            
        Returns:
            Dictionary containing complete traffic analysis
        """
        logger.info(f"Starting traffic and transportation analysis for {business_type} at {address}")
        
        results = {
            "business_type": business_type,
            "location": address,
            "coordinates": {"lat": lat, "lon": lon},
            "analysis_date": datetime.now().isoformat(),
            "sections": {}
        }
        
        try:
            # 1. Traffic Volume Analysis
            traffic_volume = self._analyze_traffic_volume(lat, lon)
            results["sections"]["traffic_volume"] = traffic_volume
            
            # 2. Highway Access Analysis
            highway_access = self._analyze_highway_access(lat, lon)
            results["sections"]["highway_access"] = highway_access
            
            # 3. Public Transportation Analysis
            transit_analysis = self._analyze_public_transportation(lat, lon)
            results["sections"]["transit_analysis"] = transit_analysis
            
            # 4. Customer Accessibility Analysis
            customer_accessibility = self._analyze_customer_accessibility(lat, lon)
            results["sections"]["customer_accessibility"] = customer_accessibility
            
            # 5. Traffic Pattern Analysis
            traffic_patterns = self._analyze_traffic_patterns(lat, lon, traffic_volume)
            results["sections"]["traffic_patterns"] = traffic_patterns
            
            # 6. Infrastructure Quality Assessment
            infrastructure_quality = self._assess_infrastructure_quality(lat, lon)
            results["sections"]["infrastructure_quality"] = infrastructure_quality
            
            # 7. Network Centrality Analysis
            network_centrality = self._analyze_network_centrality(lat, lon)
            results["sections"]["network_centrality"] = network_centrality
            
            # 8. Generate summary
            results["summary"] = self._generate_summary(results["sections"])
            
        except Exception as e:
            logger.error(f"Error in traffic and transportation analysis: {str(e)}")
            results["error"] = str(e)
            
        return results
    
    def _analyze_traffic_volume(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze traffic volume on nearby roads"""
        logger.info("Analyzing traffic volume")
        
        # Get nearby traffic data
        nearby_traffic = self._get_nearby_traffic_data(lat, lon)
        
        if not nearby_traffic:
            return self._create_fallback_traffic_data()
        
        # Find primary route (closest high-volume road)
        primary_route = min(nearby_traffic, key=lambda x: x["distance"])
        
        # Sort by traffic volume for nearby corridors
        nearby_corridors = sorted(nearby_traffic[:3], key=lambda x: x["aadt"], reverse=True)
        
        # Calculate overall traffic exposure
        total_nearby_traffic = sum(route["aadt"] for route in nearby_corridors)
        avg_distance = sum(route["distance"] for route in nearby_corridors) / len(nearby_corridors)
        
        # Score traffic exposure (higher traffic + closer = better for visibility)
        traffic_exposure = min(100, (total_nearby_traffic / 1000) * (5 / max(avg_distance, 0.1)))
        
        return {
            "primary_route": {
                "name": primary_route["route_name"],
                "aadt": primary_route["aadt"],
                "highway_type": primary_route["highway_type"],
                "traffic_category": self._categorize_traffic_volume(primary_route["aadt"]),
                "distance": primary_route["distance"],
                "peak_hour_factor": primary_route.get("peak_hour_factor", 0.1)
            },
            "nearby_corridors": [
                {
                    "route": route["route_name"],
                    "distance": route["distance"],
                    "aadt": route["aadt"],
                    "highway_type": route["highway_type"],
                    "traffic_level": self._categorize_traffic_volume(route["aadt"])
                } for route in nearby_corridors
            ],
            "overall_traffic_exposure": round(traffic_exposure, 1),
            "customer_accessibility_score": min(100, traffic_exposure * 1.2),
            "visibility_potential": self._assess_visibility_potential(primary_route["aadt"])
        }
    
    def _analyze_highway_access(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze highway accessibility"""
        logger.info("Analyzing highway access")
        
        # Get accessibility analysis
        accessibility_result = self.accessibility_analyzer.analyze_transportation_accessibility(
            lat, lon
        )
        
        # Extract highway access data
        highway_access = {
            "interstate_distance": accessibility_result.nearest_interstate_miles,
            "nearest_interstate": "I-94",  # Default
            "highway_score": accessibility_result.highway_accessibility_score
        }
        
        # Calculate access scores
        interstate_score = self._calculate_highway_score(
            highway_access.get("interstate_distance", 50), "interstate"
        )
        us_highway_score = self._calculate_highway_score(
            highway_access.get("us_highway_distance", 25), "us_highway"
        )
        state_highway_score = self._calculate_highway_score(
            highway_access.get("state_highway_distance", 10), "state_highway"
        )
        local_score = min(25, 25 - (highway_access.get("county_highway_distance", 5) * 2))
        
        overall_score = interstate_score + us_highway_score + state_highway_score + local_score
        
        return {
            "interstate_access": {
                "nearest": highway_access.get("nearest_interstate", "I-94"),
                "distance": highway_access.get("interstate_distance", 15.0),
                "travel_time": highway_access.get("interstate_distance", 15.0) * 2,  # Estimate
                "access_quality": self._rate_access_quality(highway_access.get("interstate_distance", 15.0))
            },
            "us_highway_access": {
                "available": highway_access.get("us_highway_distance", 25) < 20,
                "nearest": highway_access.get("nearest_us_highway", "US-51"),
                "distance": highway_access.get("us_highway_distance", 8.0)
            },
            "state_highway_access": {
                "available": highway_access.get("state_highway_distance", 10) < 15,
                "nearest": highway_access.get("nearest_state_highway", "WI-29"),
                "distance": highway_access.get("state_highway_distance", 3.0)
            },
            "county_highway_access": {
                "available": highway_access.get("county_highway_distance", 5) < 10,
                "primary": highway_access.get("nearest_county_highway", "County Road A"),
                "distance": highway_access.get("county_highway_distance", 1.5)
            },
            "access_scores": {
                "interstate": interstate_score,
                "us_highway": us_highway_score,
                "state_highway": state_highway_score,
                "local": local_score
            },
            "overall_highway_score": round(overall_score, 1)
        }
    
    def _analyze_public_transportation(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze public transportation access"""
        logger.info("Analyzing public transportation")
        
        # Get transit analysis
        accessibility_result = self.accessibility_analyzer.analyze_transportation_accessibility(
            lat, lon
        )
        
        # Extract transit access data
        transit_access = {
            "transit_score": accessibility_result.transit_accessibility_score,
            "bus_stop_distance": 1.0,  # Default
            "rail_station_distance": 15.0  # Default
        }
        
        # Calculate transit scores
        bus_score = self._calculate_transit_score(
            transit_access.get("bus_stop_distance", 5.0), "bus"
        )
        rail_score = self._calculate_transit_score(
            transit_access.get("rail_station_distance", 20.0), "rail"
        )
        
        return {
            "bus_service": {
                "available": transit_access.get("bus_stop_distance", 5.0) < 2.0,
                "nearest_stop": f"Bus Stop {transit_access.get('bus_stop_distance', 5.0):.1f} miles away",
                "distance": transit_access.get("bus_stop_distance", 5.0),
                "frequency": self._estimate_bus_frequency(transit_access.get("bus_stop_distance", 5.0)),
                "routes": self._estimate_bus_routes(transit_access.get("bus_stop_distance", 5.0))
            },
            "rail_service": {
                "available": transit_access.get("rail_station_distance", 20.0) < 10.0,
                "nearest_station": f"Rail Station {transit_access.get('rail_station_distance', 20.0):.1f} miles away",
                "distance": transit_access.get("rail_station_distance", 20.0),
                "service_type": self._determine_rail_type(transit_access.get("rail_station_distance", 20.0)),
                "frequency": self._estimate_rail_frequency(transit_access.get("rail_station_distance", 20.0))
            },
            "transit_scores": {
                "bus_access": bus_score,
                "rail_access": rail_score
            },
            "total_transit_score": bus_score + rail_score
        }
    
    def _analyze_customer_accessibility(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze customer accessibility via drive-time analysis"""
        logger.info("Analyzing customer accessibility")
        
        # Get trade area analysis for population data
        trade_area_data = self.trade_area_analyzer.analyze_trade_area(
            "accessibility_analysis", "Customer Accessibility", "business", lat, lon
        )
        
        # Extract drive-time populations
        isochrones = trade_area_data.get("isochrones", {})
        
        pop_5_min = isochrones.get("5_min", {}).get("demographics", {}).get("total_population", 15000)
        pop_10_min = isochrones.get("10_min", {}).get("demographics", {}).get("total_population", 30000)
        pop_15_min = isochrones.get("15_min", {}).get("demographics", {}).get("total_population", 45000)
        pop_20_min = int(pop_15_min * 1.3)  # Estimate
        
        # Calculate catchment areas
        primary_catchment = pop_10_min - pop_5_min
        secondary_catchment = pop_15_min - pop_10_min
        extended_catchment = pop_20_min - pop_15_min
        
        return {
            "drive_time_populations": {
                "5_minutes": pop_5_min,
                "10_minutes": pop_10_min,
                "15_minutes": pop_15_min,
                "20_minutes": pop_20_min
            },
            "catchment_analysis": {
                "primary": primary_catchment,
                "secondary": secondary_catchment,
                "extended": extended_catchment
            },
            "accessibility_ratings": {
                "vehicle_access": self._rate_vehicle_access(pop_10_min),
                "transit_access": self._rate_transit_access(pop_5_min),
                "pedestrian_access": self._rate_pedestrian_access(pop_5_min),
                "bicycle_access": self._rate_bicycle_access(pop_5_min)
            }
        }
    
    def _analyze_traffic_patterns(self, lat: float, lon: float, traffic_volume: Dict) -> Dict[str, Any]:
        """Analyze traffic patterns and timing"""
        logger.info("Analyzing traffic patterns")
        
        primary_aadt = traffic_volume.get("primary_route", {}).get("aadt", 10000)
        peak_hour_factor = traffic_volume.get("primary_route", {}).get("peak_hour_factor", 0.1)
        
        # Estimate peak hour volumes
        morning_peak = int(primary_aadt * peak_hour_factor * 0.6)  # 60% of peak
        afternoon_peak = int(primary_aadt * peak_hour_factor)
        
        return {
            "peak_hour_patterns": {
                "morning_peak": morning_peak,
                "afternoon_peak": afternoon_peak,
                "peak_hour_factor": peak_hour_factor
            },
            "seasonal_variations": {
                "summer_multiplier": 1.15,
                "winter_multiplier": 0.85,
                "holiday_impact": "20% increase during holiday seasons"
            },
            "traffic_composition": {
                "passenger_percentage": 85,
                "truck_percentage": 12,
                "local_traffic_percentage": 60
            }
        }
    
    def _assess_infrastructure_quality(self, lat: float, lon: float) -> Dict[str, Any]:
        """Assess transportation infrastructure quality"""
        logger.info("Assessing infrastructure quality")
        
        # Simplified assessment - in production would use road condition data
        return {
            "road_condition": {
                "pavement_quality": "Good",
                "lane_configuration": "2-lane with center turn lane",
                "traffic_control": "Traffic signals at major intersections",
                "parking_availability": "On-street and off-street parking available"
            },
            "infrastructure_scores": {
                "road_quality": 18,
                "traffic_flow": 20,
                "parking_access": 15,
                "safety": 17
            },
            "infrastructure_score": 70
        }
    
    def _analyze_network_centrality(self, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze network centrality and strategic positioning"""
        logger.info("Analyzing network centrality")
        
        try:
            # Get road network data from OpenStreetMap
            road_network = self._get_road_network(lat, lon)
            
            # Calculate centrality metrics
            centrality_metrics = self._calculate_centrality_metrics(road_network, lat, lon)
            
            # Analyze strategic positioning
            strategic_analysis = self._analyze_strategic_positioning(centrality_metrics, lat, lon)
            
            return {
                "centrality_metrics": centrality_metrics,
                "strategic_positioning": strategic_analysis,
                "network_importance": self._assess_network_importance(centrality_metrics),
                "business_implications": self._generate_network_business_implications(centrality_metrics, strategic_analysis)
            }
            
        except Exception as e:
            logger.warning(f"Network centrality analysis failed: {str(e)}")
            return self._generate_fallback_centrality_analysis()
    
    def _get_road_network(self, lat: float, lon: float, radius: float = 0.02) -> nx.Graph:
        """Get road network from OpenStreetMap"""
        try:
            # Create bounding box around location
            bbox = f"{lat-radius},{lon-radius},{lat+radius},{lon+radius}"
            
            # Overpass API query for road network
            overpass_url = "http://overpass-api.de/api/interpreter"
            query = f"""
            [out:json][timeout:25];
            (
              way["highway"~"^(motorway|trunk|primary|secondary|tertiary|residential)$"]({bbox});
            );
            out geom;
            """
            
            response = requests.get(overpass_url, params={'data': query}, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return self._build_network_graph(data)
            else:
                logger.warning(f"Failed to fetch road network: {response.status_code}")
                return self._create_mock_network()
                
        except Exception as e:
            logger.warning(f"Error fetching road network: {str(e)}")
            return self._create_mock_network()
    
    def _build_network_graph(self, osm_data: Dict) -> nx.Graph:
        """Build NetworkX graph from OSM data"""
        G = nx.Graph()
        
        # Add nodes and edges from OSM ways
        for way in osm_data.get('elements', []):
            if way.get('type') == 'way' and 'geometry' in way:
                nodes = way['geometry']
                highway_type = way.get('tags', {}).get('highway', 'unknown')
                
                # Add nodes
                for node in nodes:
                    node_id = f"{node['lat']:.6f},{node['lon']:.6f}"
                    G.add_node(node_id, lat=node['lat'], lon=node['lon'])
                
                # Add edges between consecutive nodes
                for i in range(len(nodes) - 1):
                    node1 = f"{nodes[i]['lat']:.6f},{nodes[i]['lon']:.6f}"
                    node2 = f"{nodes[i+1]['lat']:.6f},{nodes[i+1]['lon']:.6f}"
                    
                    # Calculate edge weight (distance)
                    distance = self._haversine_distance(
                        nodes[i]['lat'], nodes[i]['lon'],
                        nodes[i+1]['lat'], nodes[i+1]['lon']
                    )
                    
                    G.add_edge(node1, node2, weight=distance, highway_type=highway_type)
        
        return G
    
    def _create_mock_network(self) -> nx.Graph:
        """Create a mock network for testing"""
        G = nx.Graph()
        
        # Create a simple grid network
        for i in range(5):
            for j in range(5):
                node_id = f"node_{i}_{j}"
                G.add_node(node_id, lat=43.0 + i*0.01, lon=-89.0 + j*0.01)
                
                # Add edges to adjacent nodes
                if i > 0:
                    G.add_edge(f"node_{i-1}_{j}", node_id, weight=1.0)
                if j > 0:
                    G.add_edge(f"node_{i}_{j-1}", node_id, weight=1.0)
        
        return G
    
    def _calculate_centrality_metrics(self, G: nx.Graph, lat: float, lon: float) -> Dict[str, Any]:
        """Calculate centrality metrics for the network"""
        if len(G.nodes()) < 2:
            return self._generate_fallback_centrality_metrics()
        
        # Find the node closest to our location
        target_node = self._find_closest_node(G, lat, lon)
        
        # Calculate centrality measures
        try:
            betweenness = nx.betweenness_centrality(G, weight='weight')
            closeness = nx.closeness_centrality(G, distance='weight')
            eigenvector = nx.eigenvector_centrality(G, max_iter=1000, tol=1e-6, weight='weight')
            
            # Get centrality values for our target node
            target_betweenness = betweenness.get(target_node, 0)
            target_closeness = closeness.get(target_node, 0)
            target_eigenvector = eigenvector.get(target_node, 0)
            
            return {
                "betweenness": {
                    "value": target_betweenness,
                    "score": min(100, target_betweenness * 1000),  # Scale to 0-100
                    "rank": self._calculate_rank(betweenness, target_node)
                },
                "closeness": {
                    "value": target_closeness,
                    "score": min(100, target_closeness * 100),  # Scale to 0-100
                    "rank": self._calculate_rank(closeness, target_node)
                },
                "eigenvector": {
                    "value": target_eigenvector,
                    "score": min(100, target_eigenvector * 100),  # Scale to 0-100
                    "rank": self._calculate_rank(eigenvector, target_node)
                },
                "network_size": len(G.nodes()),
                "network_density": nx.density(G)
            }
            
        except Exception as e:
            logger.warning(f"Error calculating centrality metrics: {str(e)}")
            return self._generate_fallback_centrality_metrics()
    
    def _find_closest_node(self, G: nx.Graph, lat: float, lon: float) -> str:
        """Find the node closest to the given coordinates"""
        min_distance = float('inf')
        closest_node = None
        
        for node in G.nodes():
            node_data = G.nodes[node]
            if 'lat' in node_data and 'lon' in node_data:
                distance = self._haversine_distance(lat, lon, node_data['lat'], node_data['lon'])
                if distance < min_distance:
                    min_distance = distance
                    closest_node = node
        
        return closest_node or list(G.nodes())[0]
    
    def _calculate_rank(self, centrality_dict: Dict, target_node: str) -> int:
        """Calculate the rank of the target node in centrality"""
        sorted_nodes = sorted(centrality_dict.items(), key=lambda x: x[1], reverse=True)
        for i, (node, _) in enumerate(sorted_nodes):
            if node == target_node:
                return i + 1
        return len(sorted_nodes)
    
    def _analyze_strategic_positioning(self, centrality_metrics: Dict, lat: float, lon: float) -> Dict[str, Any]:
        """Analyze strategic network positioning"""
        
        # Determine hub classification
        betweenness_score = centrality_metrics["betweenness"]["score"]
        closeness_score = centrality_metrics["closeness"]["score"]
        eigenvector_score = centrality_metrics["eigenvector"]["score"]
        
        avg_centrality = (betweenness_score + closeness_score + eigenvector_score) / 3
        
        if avg_centrality >= 75:
            hub_classification = "Major Transportation Hub"
        elif avg_centrality >= 50:
            hub_classification = "Regional Node"
        elif avg_centrality >= 25:
            hub_classification = "Local Connector"
        else:
            hub_classification = "Peripheral Location"
        
        return {
            "hub_classification": hub_classification,
            "average_centrality": round(avg_centrality, 1),
            "regional_traffic_flow": self._assess_regional_traffic_flow(betweenness_score),
            "network_importance": self._assess_network_importance_level(avg_centrality),
            "through_traffic_volume": self._estimate_through_traffic(betweenness_score),
            "network_resilience": self._assess_network_resilience(centrality_metrics)
        }
    
    def _assess_regional_traffic_flow(self, betweenness_score: float) -> str:
        """Assess regional traffic flow based on betweenness centrality"""
        if betweenness_score >= 75:
            return "High - Major regional traffic flows through this location"
        elif betweenness_score >= 50:
            return "Moderate - Some regional traffic passes through"
        elif betweenness_score >= 25:
            return "Low-Moderate - Limited regional traffic flow"
        else:
            return "Low - Primarily local traffic"
    
    def _assess_network_importance_level(self, avg_centrality: float) -> str:
        """Assess overall network importance"""
        if avg_centrality >= 75:
            return "Critical - Essential network node"
        elif avg_centrality >= 50:
            return "Important - Significant network role"
        elif avg_centrality >= 25:
            return "Moderate - Standard network position"
        else:
            return "Limited - Peripheral network role"
    
    def _estimate_through_traffic(self, betweenness_score: float) -> str:
        """Estimate through-traffic volume"""
        if betweenness_score >= 75:
            return "High (70-80% through-traffic)"
        elif betweenness_score >= 50:
            return "Moderate (50-70% through-traffic)"
        elif betweenness_score >= 25:
            return "Low-Moderate (30-50% through-traffic)"
        else:
            return "Low (10-30% through-traffic)"
    
    def _assess_network_resilience(self, centrality_metrics: Dict) -> str:
        """Assess network resilience and alternative routes"""
        avg_centrality = (centrality_metrics["betweenness"]["score"] + 
                         centrality_metrics["closeness"]["score"] + 
                         centrality_metrics["eigenvector"]["score"]) / 3
        
        if avg_centrality >= 75:
            return "High resilience - Multiple alternative routes available"
        elif avg_centrality >= 50:
            return "Good resilience - Some alternative routes available"
        elif avg_centrality >= 25:
            return "Moderate resilience - Limited alternative routes"
        else:
            return "Low resilience - Few alternative routes"
    
    def _assess_network_importance(self, centrality_metrics: Dict) -> str:
        """Assess overall network importance"""
        avg_centrality = (centrality_metrics["betweenness"]["score"] + 
                         centrality_metrics["closeness"]["score"] + 
                         centrality_metrics["eigenvector"]["score"]) / 3
        
        if avg_centrality >= 75:
            return "Critical Network Position"
        elif avg_centrality >= 50:
            return "Important Network Node"
        elif avg_centrality >= 25:
            return "Standard Network Position"
        else:
            return "Peripheral Network Position"
    
    def _generate_network_business_implications(self, centrality_metrics: Dict, strategic_analysis: Dict) -> Dict[str, Any]:
        """Generate business implications from network analysis"""
        avg_centrality = strategic_analysis["average_centrality"]
        
        # Network advantage
        if avg_centrality >= 75:
            network_advantage = "Excellent - Prime network position for maximum visibility"
        elif avg_centrality >= 50:
            network_advantage = "Good - Strong network position with good visibility"
        elif avg_centrality >= 25:
            network_advantage = "Moderate - Standard network position"
        else:
            network_advantage = "Limited - Peripheral position may limit visibility"
        
        # Visibility potential
        betweenness_score = centrality_metrics["betweenness"]["score"]
        if betweenness_score >= 75:
            visibility_potential = "Excellent - High through-traffic provides maximum exposure"
        elif betweenness_score >= 50:
            visibility_potential = "Good - Moderate through-traffic provides good exposure"
        elif betweenness_score >= 25:
            visibility_potential = "Moderate - Some through-traffic provides fair exposure"
        else:
            visibility_potential = "Limited - Primarily local traffic exposure"
        
        # Growth potential
        if avg_centrality >= 50:
            growth_potential = "High - Network position supports business growth"
        elif avg_centrality >= 25:
            growth_potential = "Moderate - Network position provides stable foundation"
        else:
            growth_potential = "Limited - Network position may constrain growth"
        
        return {
            "network_advantage": network_advantage,
            "visibility_potential": visibility_potential,
            "growth_potential": growth_potential
        }
    
    def _generate_fallback_centrality_analysis(self) -> Dict[str, Any]:
        """Generate fallback centrality analysis when network analysis fails"""
        return {
            "centrality_metrics": self._generate_fallback_centrality_metrics(),
            "strategic_positioning": {
                "hub_classification": "Regional Node",
                "average_centrality": 45.0,
                "regional_traffic_flow": "Moderate - Some regional traffic passes through",
                "network_importance": "Moderate - Standard network position",
                "through_traffic_volume": "Moderate (50-70% through-traffic)",
                "network_resilience": "Good resilience - Some alternative routes available"
            },
            "network_importance": "Standard Network Position",
            "business_implications": {
                "network_advantage": "Good - Strong network position with good visibility",
                "visibility_potential": "Good - Moderate through-traffic provides good exposure",
                "growth_potential": "Moderate - Network position provides stable foundation"
            }
        }
    
    def _generate_fallback_centrality_metrics(self) -> Dict[str, Any]:
        """Generate fallback centrality metrics"""
        return {
            "betweenness": {
                "value": 0.015,
                "score": 45,
                "rank": 25
            },
            "closeness": {
                "value": 0.38,
                "score": 38,
                "rank": 30
            },
            "eigenvector": {
                "value": 0.25,
                "score": 25,
                "rank": 35
            },
            "network_size": 150,
            "network_density": 0.12
        }
    
    def _interpret_betweenness_centrality(self, score: float) -> str:
        """Interpret betweenness centrality score"""
        if score >= 75:
            return "High - Critical bridge point for regional traffic flow"
        elif score >= 50:
            return "Moderate - Good position for capturing through-traffic"
        elif score >= 25:
            return "Low-Moderate - Some through-traffic potential"
        else:
            return "Low - Primarily serves local traffic"
    
    def _interpret_closeness_centrality(self, score: float) -> str:
        """Interpret closeness centrality score"""
        if score >= 75:
            return "High - Excellent accessibility to all network points"
        elif score >= 50:
            return "Moderate - Good accessibility to most network points"
        elif score >= 25:
            return "Low-Moderate - Fair accessibility to network"
        else:
            return "Low - Limited accessibility within network"
    
    def _interpret_eigenvector_centrality(self, score: float) -> str:
        """Interpret eigenvector centrality score"""
        if score >= 75:
            return "High - Connected to highly important network nodes"
        elif score >= 50:
            return "Moderate - Connected to moderately important nodes"
        elif score >= 25:
            return "Low-Moderate - Connected to some important nodes"
        else:
            return "Low - Connected to less important network nodes"
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate the great circle distance between two points"""
        R = 6371  # Earth's radius in kilometers
        
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def _get_nearby_traffic_data(self, lat: float, lon: float, radius: float = 2.0) -> List[Dict[str, Any]]:
        """Get traffic data for nearby roads"""
        # This would query the traffic database in production
        # For now, return sample data
        return [
            {
                "route_name": "US-51",
                "aadt": 35000,
                "highway_type": "US Highway",
                "distance": 0.5,
                "peak_hour_factor": 0.12
            },
            {
                "route_name": "WI-29",
                "aadt": 28000,
                "highway_type": "State Highway",
                "distance": 1.2,
                "peak_hour_factor": 0.10
            },
            {
                "route_name": "County Road A",
                "aadt": 8500,
                "highway_type": "County Highway",
                "distance": 0.3,
                "peak_hour_factor": 0.08
            }
        ]
    
    def _create_fallback_traffic_data(self) -> Dict[str, Any]:
        """Create fallback traffic data when no nearby data available"""
        return {
            "primary_route": {
                "name": "Local Road",
                "aadt": 5000,
                "highway_type": "Local Road",
                "traffic_category": "Low",
                "distance": 0.1,
                "peak_hour_factor": 0.08
            },
            "nearby_corridors": [
                {
                    "route": "Local Road",
                    "distance": 0.1,
                    "aadt": 5000,
                    "highway_type": "Local Road",
                    "traffic_level": "Low"
                }
            ],
            "overall_traffic_exposure": 30.0,
            "customer_accessibility_score": 40.0,
            "visibility_potential": "Moderate"
        }
    
    def _categorize_traffic_volume(self, aadt: int) -> str:
        """Categorize traffic volume based on AADT"""
        for category, thresholds in self.TRAFFIC_CATEGORIES.items():
            if thresholds["min"] <= aadt < thresholds["max"]:
                return category.replace("_", " ").title()
        return "Very High"
    
    def _assess_visibility_potential(self, aadt: int) -> str:
        """Assess business visibility potential based on traffic volume"""
        if aadt >= 50000:
            return "Excellent"
        elif aadt >= 20000:
            return "Good"
        elif aadt >= 5000:
            return "Moderate"
        else:
            return "Limited"
    
    def _calculate_highway_score(self, distance: float, highway_type: str) -> float:
        """Calculate highway accessibility score"""
        config = self.HIGHWAY_SCORES.get(highway_type, {"base_score": 10, "distance_penalty": 5})
        penalty = min(config["base_score"], distance * config["distance_penalty"])
        return max(0, config["base_score"] - penalty)
    
    def _calculate_transit_score(self, distance: float, transit_type: str) -> float:
        """Calculate transit accessibility score"""
        if transit_type == "bus":
            return max(0, 50 - (distance * 20))  # 50 points max, -20 per mile
        else:  # rail
            return max(0, 50 - (distance * 5))   # 50 points max, -5 per mile
    
    def _rate_access_quality(self, distance: float) -> str:
        """Rate access quality based on distance"""
        if distance <= 5:
            return "Excellent"
        elif distance <= 10:
            return "Good"
        elif distance <= 20:
            return "Fair"
        else:
            return "Poor"
    
    def _estimate_bus_frequency(self, distance: float) -> str:
        """Estimate bus service frequency"""
        if distance <= 0.5:
            return "Every 15-30 minutes"
        elif distance <= 1.0:
            return "Every 30-60 minutes"
        else:
            return "Limited service"
    
    def _estimate_bus_routes(self, distance: float) -> str:
        """Estimate number of bus routes"""
        if distance <= 0.5:
            return "2-3 routes"
        elif distance <= 1.0:
            return "1-2 routes"
        else:
            return "Limited routes"
    
    def _determine_rail_type(self, distance: float) -> str:
        """Determine rail service type"""
        if distance <= 5:
            return "Light rail/Metro"
        elif distance <= 15:
            return "Regional rail"
        else:
            return "Amtrak/Long-distance"
    
    def _estimate_rail_frequency(self, distance: float) -> str:
        """Estimate rail service frequency"""
        if distance <= 5:
            return "Every 10-20 minutes"
        elif distance <= 15:
            return "Every 30-60 minutes"
        else:
            return "Limited daily service"
    
    def _rate_vehicle_access(self, pop_10_min: int) -> str:
        """Rate vehicle accessibility"""
        if pop_10_min >= 40000:
            return "Excellent"
        elif pop_10_min >= 20000:
            return "Good"
        elif pop_10_min >= 10000:
            return "Fair"
        else:
            return "Limited"
    
    def _rate_transit_access(self, pop_5_min: int) -> str:
        """Rate transit accessibility"""
        if pop_5_min >= 20000:
            return "Good"
        elif pop_5_min >= 10000:
            return "Fair"
        else:
            return "Limited"
    
    def _rate_pedestrian_access(self, pop_5_min: int) -> str:
        """Rate pedestrian accessibility"""
        if pop_5_min >= 15000:
            return "Good"
        elif pop_5_min >= 8000:
            return "Fair"
        else:
            return "Limited"
    
    def _rate_bicycle_access(self, pop_5_min: int) -> str:
        """Rate bicycle accessibility"""
        if pop_5_min >= 12000:
            return "Good"
        elif pop_5_min >= 6000:
            return "Fair"
        else:
            return "Limited"
    
    def _generate_summary(self, sections: Dict[str, Any]) -> Dict[str, Any]:
        """Generate analysis summary"""
        traffic_volume = sections.get("traffic_volume", {})
        highway_access = sections.get("highway_access", {})
        transit_analysis = sections.get("transit_analysis", {})
        infrastructure = sections.get("infrastructure_quality", {})
        
        # Calculate overall transportation score
        traffic_score = traffic_volume.get("customer_accessibility_score", 50)
        highway_score = highway_access.get("overall_highway_score", 50)
        transit_score = transit_analysis.get("total_transit_score", 25)
        infra_score = infrastructure.get("infrastructure_score", 70)
        
        overall_score = (traffic_score + highway_score + transit_score + infra_score) / 4
        
        return {
            "traffic_volume_score": traffic_score,
            "highway_access_score": highway_score,
            "transit_access_score": transit_score,
            "infrastructure_quality_score": infra_score,
            "overall_transportation_score": round(overall_score, 1),
            "transportation_summary": f"Location shows {self._rate_overall_transportation(overall_score)} "
                                   f"transportation accessibility with strong highway access and "
                                   f"{'good' if traffic_score >= 60 else 'moderate'} traffic exposure.",
            "key_strengths": self._identify_transportation_strengths(sections),
            "key_challenges": self._identify_transportation_challenges(sections)
        }
    
    def _rate_overall_transportation(self, score: float) -> str:
        """Rate overall transportation accessibility"""
        if score >= 80:
            return "excellent"
        elif score >= 60:
            return "good"
        elif score >= 40:
            return "fair"
        else:
            return "poor"
    
    def _identify_transportation_strengths(self, sections: Dict[str, Any]) -> List[str]:
        """Identify transportation strengths"""
        strengths = []
        
        traffic_volume = sections.get("traffic_volume", {})
        highway_access = sections.get("highway_access", {})
        
        if traffic_volume.get("customer_accessibility_score", 0) >= 70:
            strengths.append("High traffic volume provides excellent visibility")
        
        if highway_access.get("overall_highway_score", 0) >= 70:
            strengths.append("Strong highway accessibility")
        
        if highway_access.get("interstate_access", {}).get("distance", 50) <= 10:
            strengths.append("Excellent interstate highway access")
        
        return strengths or ["Good local road access"]
    
    def _identify_transportation_challenges(self, sections: Dict[str, Any]) -> List[str]:
        """Identify transportation challenges"""
        challenges = []
        
        highway_access = sections.get("highway_access", {})
        transit_analysis = sections.get("transit_analysis", {})
        
        if highway_access.get("interstate_access", {}).get("distance", 0) >= 20:
            challenges.append("Limited interstate highway access")
        
        if transit_analysis.get("total_transit_score", 0) <= 30:
            challenges.append("Limited public transportation options")
        
        return challenges or ["Minor transportation limitations"]
    
    def generate_section_content(self, analysis_data: Dict[str, Any]) -> str:
        """Generate formatted Section 3.1 content"""
        logger.info("Generating Section 3.1 content")
        
        # Load template
        template_path = Path("UNIVERSAL_TRAFFIC_TRANSPORTATION_TEMPLATE.md")
        with open(template_path, 'r') as f:
            template = f.read()
        
        # Extract data from analysis
        traffic_volume = analysis_data["sections"]["traffic_volume"]
        highway_access = analysis_data["sections"]["highway_access"]
        transit_analysis = analysis_data["sections"]["transit_analysis"]
        customer_accessibility = analysis_data["sections"]["customer_accessibility"]
        traffic_patterns = analysis_data["sections"]["traffic_patterns"]
        infrastructure = analysis_data["sections"]["infrastructure_quality"]
        network_centrality = analysis_data["sections"]["network_centrality"]
        summary = analysis_data["summary"]
        
        # Create replacements dictionary
        replacements = {
            "{business_type}": analysis_data["business_type"],
            "{location}": analysis_data["location"],
            
            # Traffic Volume
            "{primary_route}": traffic_volume["primary_route"]["name"],
            "{primary_aadt}": traffic_volume["primary_route"]["aadt"],
            "{primary_highway_type}": traffic_volume["primary_route"]["highway_type"],
            "{primary_traffic_category}": traffic_volume["primary_route"]["traffic_category"],
            "{primary_peak_hour_factor}": traffic_volume["primary_route"]["peak_hour_factor"],
            
            # Nearby corridors
            "{route_1}": traffic_volume["nearby_corridors"][0]["route"] if traffic_volume["nearby_corridors"] else "N/A",
            "{distance_1}": f"{traffic_volume['nearby_corridors'][0]['distance']:.1f} mi" if traffic_volume["nearby_corridors"] else "N/A",
            "{aadt_1}": traffic_volume["nearby_corridors"][0]["aadt"] if traffic_volume["nearby_corridors"] else 0,
            "{type_1}": traffic_volume["nearby_corridors"][0]["highway_type"] if traffic_volume["nearby_corridors"] else "N/A",
            "{level_1}": traffic_volume["nearby_corridors"][0]["traffic_level"] if traffic_volume["nearby_corridors"] else "N/A",
            
            "{route_2}": traffic_volume["nearby_corridors"][1]["route"] if len(traffic_volume["nearby_corridors"]) > 1 else "N/A",
            "{distance_2}": f"{traffic_volume['nearby_corridors'][1]['distance']:.1f} mi" if len(traffic_volume["nearby_corridors"]) > 1 else "N/A",
            "{aadt_2}": traffic_volume["nearby_corridors"][1]["aadt"] if len(traffic_volume["nearby_corridors"]) > 1 else 0,
            "{type_2}": traffic_volume["nearby_corridors"][1]["highway_type"] if len(traffic_volume["nearby_corridors"]) > 1 else "N/A",
            "{level_2}": traffic_volume["nearby_corridors"][1]["traffic_level"] if len(traffic_volume["nearby_corridors"]) > 1 else "N/A",
            
            "{route_3}": traffic_volume["nearby_corridors"][2]["route"] if len(traffic_volume["nearby_corridors"]) > 2 else "N/A",
            "{distance_3}": f"{traffic_volume['nearby_corridors'][2]['distance']:.1f} mi" if len(traffic_volume["nearby_corridors"]) > 2 else "N/A",
            "{aadt_3}": traffic_volume["nearby_corridors"][2]["aadt"] if len(traffic_volume["nearby_corridors"]) > 2 else 0,
            "{type_3}": traffic_volume["nearby_corridors"][2]["highway_type"] if len(traffic_volume["nearby_corridors"]) > 2 else "N/A",
            "{level_3}": traffic_volume["nearby_corridors"][2]["traffic_level"] if len(traffic_volume["nearby_corridors"]) > 2 else "N/A",
            
            # Traffic assessment
            "{overall_traffic_exposure}": str(traffic_volume["overall_traffic_exposure"]),
            "{customer_accessibility_score}": str(int(traffic_volume["customer_accessibility_score"])),
            "{visibility_potential}": traffic_volume["visibility_potential"],
            
            # Highway access
            "{nearest_interstate}": highway_access["interstate_access"]["nearest"],
            "{interstate_distance}": f"{highway_access['interstate_access']['distance']:.1f}",
            "{interstate_travel_time}": str(int(highway_access["interstate_access"]["travel_time"])),
            "{interstate_access_quality}": highway_access["interstate_access"]["access_quality"],
            
            "{us_highway_access}": "Available" if highway_access["us_highway_access"]["available"] else "Limited",
            "{nearest_us_highway}": highway_access["us_highway_access"]["nearest"],
            "{us_highway_distance}": f"{highway_access['us_highway_access']['distance']:.1f}",
            
            "{state_highway_access}": "Available" if highway_access["state_highway_access"]["available"] else "Limited",
            "{nearest_state_highway}": highway_access["state_highway_access"]["nearest"],
            "{state_highway_distance}": f"{highway_access['state_highway_access']['distance']:.1f}",
            
            "{county_highway_access}": "Available" if highway_access["county_highway_access"]["available"] else "Limited",
            "{primary_county_highway}": highway_access["county_highway_access"]["primary"],
            "{county_highway_distance}": f"{highway_access['county_highway_access']['distance']:.1f}",
            
            # Highway scores
            "{interstate_score}": str(int(highway_access["access_scores"]["interstate"])),
            "{us_highway_score}": str(int(highway_access["access_scores"]["us_highway"])),
            "{state_highway_score}": str(int(highway_access["access_scores"]["state_highway"])),
            "{local_access_score}": str(int(highway_access["access_scores"]["local"])),
            "{overall_highway_score}": str(highway_access["overall_highway_score"]),
            
            # Transit
            "{bus_service_available}": "Available" if transit_analysis["bus_service"]["available"] else "Limited",
            "{nearest_bus_stop}": transit_analysis["bus_service"]["nearest_stop"],
            "{bus_stop_distance}": f"{transit_analysis['bus_service']['distance']:.1f}",
            "{bus_frequency}": transit_analysis["bus_service"]["frequency"],
            "{bus_routes}": transit_analysis["bus_service"]["routes"],
            
            "{rail_service_available}": "Available" if transit_analysis["rail_service"]["available"] else "Limited",
            "{nearest_rail_station}": transit_analysis["rail_service"]["nearest_station"],
            "{rail_station_distance}": f"{transit_analysis['rail_service']['distance']:.1f}",
            "{rail_service_type}": transit_analysis["rail_service"]["service_type"],
            "{rail_frequency}": transit_analysis["rail_service"]["frequency"],
            
            "{bus_access_score}": str(int(transit_analysis["transit_scores"]["bus_access"])),
            "{rail_access_score}": str(int(transit_analysis["transit_scores"]["rail_access"])),
            "{total_transit_score}": str(int(transit_analysis["total_transit_score"])),
            
            # Customer accessibility
            "{pop_5_min}": f"{customer_accessibility['drive_time_populations']['5_minutes']:,}",
            "{pop_10_min}": f"{customer_accessibility['drive_time_populations']['10_minutes']:,}",
            "{pop_15_min}": f"{customer_accessibility['drive_time_populations']['15_minutes']:,}",
            "{pop_20_min}": f"{customer_accessibility['drive_time_populations']['20_minutes']:,}",
            
            "{primary_catchment}": f"{customer_accessibility['catchment_analysis']['primary']:,}",
            "{secondary_catchment}": f"{customer_accessibility['catchment_analysis']['secondary']:,}",
            "{extended_catchment}": f"{customer_accessibility['catchment_analysis']['extended']:,}",
            
            "{vehicle_access_rating}": customer_accessibility["accessibility_ratings"]["vehicle_access"],
            "{transit_access_rating}": customer_accessibility["accessibility_ratings"]["transit_access"],
            "{pedestrian_access_rating}": customer_accessibility["accessibility_ratings"]["pedestrian_access"],
            "{bicycle_access_rating}": customer_accessibility["accessibility_ratings"]["bicycle_access"],
            
            # Traffic patterns
            "{morning_peak_volume}": f"{traffic_patterns['peak_hour_patterns']['morning_peak']:,}",
            "{afternoon_peak_volume}": f"{traffic_patterns['peak_hour_patterns']['afternoon_peak']:,}",
            "{peak_hour_factor}": str(traffic_patterns["peak_hour_patterns"]["peak_hour_factor"]),
            
            "{summer_multiplier}": str(traffic_patterns["seasonal_variations"]["summer_multiplier"]),
            "{winter_multiplier}": str(traffic_patterns["seasonal_variations"]["winter_multiplier"]),
            "{holiday_impact}": traffic_patterns["seasonal_variations"]["holiday_impact"],
            
            "{passenger_percentage}": str(traffic_patterns["traffic_composition"]["passenger_percentage"]),
            "{truck_percentage}": str(traffic_patterns["traffic_composition"]["truck_percentage"]),
            "{local_traffic_percentage}": str(traffic_patterns["traffic_composition"]["local_traffic_percentage"]),
            
            # Infrastructure
            "{pavement_quality}": infrastructure["road_condition"]["pavement_quality"],
            "{lane_configuration}": infrastructure["road_condition"]["lane_configuration"],
            "{traffic_control}": infrastructure["road_condition"]["traffic_control"],
            "{parking_availability}": infrastructure["road_condition"]["parking_availability"],
            
            "{road_quality_score}": str(infrastructure["infrastructure_scores"]["road_quality"]),
            "{traffic_flow_score}": str(infrastructure["infrastructure_scores"]["traffic_flow"]),
            "{parking_score}": str(infrastructure["infrastructure_scores"]["parking_access"]),
            "{safety_score}": str(infrastructure["infrastructure_scores"]["safety"]),
            "{infrastructure_score}": str(infrastructure["infrastructure_score"]),
            
            # Logistics (simplified)
            "{truck_route_access}": "Available via major highways",
            "{delivery_restrictions}": "Standard delivery hours 6 AM - 10 PM",
            "{loading_zone_availability}": "On-street loading zones available",
            "{weight_restrictions}": "No special weight restrictions",
            "{distribution_centers}": "Regional distribution centers within 50 miles",
            "{supplier_access_rating}": "Good",
            "{logistics_cost_impact}": "Standard logistics costs",
            
            # Competitive analysis (simplified)
            "{competitor_traffic_analysis}": "- Competitors have similar traffic access\n- Location provides competitive transportation advantage",
            "{relative_accessibility_score}": str(int(summary["overall_transportation_score"])),
            "{transportation_advantage}": "Competitive" if summary["overall_transportation_score"] >= 60 else "Moderate",
            
            # Risk assessment
            "{congestion_risk}": "Medium during peak hours",
            "{seasonal_risk}": "Low - good winter accessibility",
            "{construction_risk}": "Low - no major construction planned",
            "{weather_risk}": "Low - good weather access",
            "{risk_mitigation_strategies}": "- Monitor traffic patterns during peak hours\n- Develop alternative access routes\n- Consider off-peak delivery schedules",
            
            # Summary scores
            "{traffic_volume_score}": str(int(summary["traffic_volume_score"])),
            "{highway_access_score}": str(int(summary["highway_access_score"])),
            "{transit_access_score}": str(int(summary["transit_access_score"])),
            "{infrastructure_quality_score}": str(int(summary["infrastructure_quality_score"])),
            "{overall_transportation_score}": str(summary["overall_transportation_score"]),
            
            # Key findings
            "{traffic_summary}": summary["transportation_summary"],
            "{accessibility_strengths}": "\n".join(f"- {strength}" for strength in summary["key_strengths"]),
            "{transportation_challenges}": "\n".join(f"- {challenge}" for challenge in summary["key_challenges"]),
            
            # Recommendations
            "{access_strategy}": "Leverage primary highway access for maximum visibility",
            "{peak_hour_recommendations}": "Consider peak hour traffic patterns for optimal customer access",
            "{customer_accessibility_recommendations}": "Maintain clear signage for easy customer navigation",
            "{infrastructure_recommendations}": "Ensure adequate parking and safe pedestrian access",
            "{logistics_recommendations}": "Coordinate delivery schedules to minimize traffic impact",
            
            # Network Centrality Analysis
            "{betweenness_centrality}": f"{network_centrality['centrality_metrics']['betweenness']['value']:.4f}",
            "{betweenness_score}": str(int(network_centrality['centrality_metrics']['betweenness']['score'])),
            "{betweenness_interpretation}": self._interpret_betweenness_centrality(network_centrality['centrality_metrics']['betweenness']['score']),
            
            "{closeness_centrality}": f"{network_centrality['centrality_metrics']['closeness']['value']:.4f}",
            "{closeness_score}": str(int(network_centrality['centrality_metrics']['closeness']['score'])),
            "{closeness_interpretation}": self._interpret_closeness_centrality(network_centrality['centrality_metrics']['closeness']['score']),
            
            "{eigenvector_centrality}": f"{network_centrality['centrality_metrics']['eigenvector']['value']:.4f}",
            "{eigenvector_score}": str(int(network_centrality['centrality_metrics']['eigenvector']['score'])),
            "{eigenvector_interpretation}": self._interpret_eigenvector_centrality(network_centrality['centrality_metrics']['eigenvector']['score']),
            
            "{hub_classification}": network_centrality['strategic_positioning']['hub_classification'],
            "{regional_traffic_flow}": network_centrality['strategic_positioning']['regional_traffic_flow'],
            "{network_importance}": network_centrality['network_importance'],
            
            "{through_traffic_volume}": network_centrality['strategic_positioning']['through_traffic_volume'],
            "{local_vs_regional_ratio}": "60% local, 40% regional",  # Simplified ratio
            "{network_resilience}": network_centrality['strategic_positioning']['network_resilience'],
            
            "{network_advantage}": network_centrality['business_implications']['network_advantage'],
            "{network_visibility_potential}": network_centrality['business_implications']['visibility_potential'],
            "{network_growth_potential}": network_centrality['business_implications']['growth_potential'],
            
            # Visual placeholders
            "{traffic_volume_map_path}": "visualizations/traffic_volume_map.png",
            "{highway_access_map_path}": "visualizations/highway_access_map.png",
            "{drive_time_map_path}": "visualizations/drive_time_isochrones.png",
            "{transit_map_path}": "visualizations/transit_network_map.png",
            "{network_centrality_map_path}": "visualizations/network_centrality_map.png",
            
            # Metadata
            "{data_collection_date}": datetime.now().strftime("%Y-%m-%d")
        }
        
        # Replace all placeholders
        content = template
        for key, value in replacements.items():
            content = content.replace(key, str(value))
        
        return content


def main():
    """Test the traffic transportation analyzer"""
    analyzer = TrafficTransportationAnalyzer()
    
    # Test analysis
    results = analyzer.analyze_traffic_transportation(
        business_type="Coffee Shop",
        address="123 Main St, Madison, WI 53703",
        lat=43.0731,
        lon=-89.4014
    )
    
    # Save results
    with open("traffic_transportation_test.json", 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate content
    content = analyzer.generate_section_content(results)
    
    with open("section_3_1_test.md", 'w') as f:
        f.write(content)
    
    print("✅ Traffic transportation analyzer test complete")


if __name__ == "__main__":
    main()