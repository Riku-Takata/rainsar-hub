
import os
import sys
import requests
import json
import logging
import time
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.ops import transform
import pyproj

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

# Import from common_utils if needed, but we can rely on standard libs mostly
from common_utils import setup_logger, HUB_DIR

logger = setup_logger("fetch_osm_rivers")

# Configuration
OSM_RIVERS_DIR = HUB_DIR / "mask-data" / "osm_rivers"
OSM_RIVERS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_GEOJSON = OSM_RIVERS_DIR / "osm_rivers_japan.geojson"
OUTPUT_SUMMARY = OSM_RIVERS_DIR / "osm_rivers_summary.csv"

# Minimum area in square kilometers for selection
MIN_AREA_KM2 = 0.5 

# Overpass API URL
OVERPASS_URL = "http://overpass-api.de/api/interpreter"

# List of Prefectures (Japanese names for reliable OSM area matching)
PREFECTURES_JP = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県",
    "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
]

def fetch_rivers_for_area(area_name, retries=3):
    """
    Fetch river polygons (waterway=riverbank OR (natural=water AND water=river))
    for a specific area name using Overpass API.
    """
    query = f"""
    [out:json][timeout:300];
    area["name"="{area_name}"]["admin_level"="4"]->.searchArea;
    (
      way["waterway"="riverbank"]["name"](area.searchArea);
      relation["waterway"="riverbank"]["name"](area.searchArea);
      way["natural"="water"]["water"="river"]["name"](area.searchArea);
      relation["natural"="water"]["water"="river"]["name"](area.searchArea);
    );
    out geom;
    """
    
    for attempt in range(retries):
        try:
            logger.info(f"Fetching rivers for {area_name} (Attempt {attempt+1}/{retries})...")
            response = requests.post(OVERPASS_URL, data={'data': query}, timeout=350)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"  Error fetching {area_name}: {e}")
            time.sleep(5) # Wait before retry
            
    logger.error(f"Failed to fetch data for {area_name} after {retries} attempts.")
    return None

def process_osm_elements(elements):
    """
    Convert OSM elements (ways/relations) to a list of dicts suitable for GeoDataFrame.
    Handles closed ways and relation multipolygons.
    """
    features = []
    
    for el in elements:
        tags = el.get('tags', {})
        geom_type = el.get('type')
        osm_id = el.get('id')
        
        name = tags.get('name', tags.get('name:en', 'Unknown'))
        
        geometry = None
        
        if geom_type == 'way':
            # Create Polygon from way coordinates
            coords = [(p['lon'], p['lat']) for p in el.get('geometry', [])]
            if len(coords) < 3: continue
            try:
                geometry = Polygon(coords)
            except Exception:
                continue
                
        elif geom_type == 'relation':
            # Handle Multipolygon relations
            members = el.get('members', [])
            outer_lines = []
            
            for m in members:
                if m.get('role') == 'outer' and m.get('type') == 'way':
                    m_geom = m.get('geometry', [])
                    coords = [(p['lon'], p['lat']) for p in m_geom]
                    if len(coords) >= 2:
                        from shapely.geometry import LineString
                        outer_lines.append(LineString(coords))
            
            if outer_lines:
                from shapely.ops import linemerge, polygonize, unary_union
                try:
                    # Merge lines into longer strings (potentially rings)
                    merged = linemerge(outer_lines)
                    # Polygonize to find closed rings
                    polys = list(polygonize(merged))
                    
                    if not polys:
                        # Sometimes linemerge returns a MultiLineString if not all connected
                        # Or maybe the data is just lines.
                        pass
                    
                    if polys:
                        geometry = unary_union(polys)
                except Exception as e:
                    # logger.warning(f"Failed to build polygon for relation {osm_id}: {e}")
                    pass

        if geometry and not geometry.is_empty:
             features.append({
                 'osm_id': osm_id,
                 'name': name,
                 'type': geom_type,
                 'geometry': geometry
             })
             
    return features

def calculate_area_km2(gdf):
    """
    Calculate area in km2 using an equal-area projection or UTM.
    For Japan, we use EPSG:32654 (UTM 54N) as a good approximation for order of magnitude.
    """
    # EPSG:32654 is WGS 84 / UTM zone 54N
    # Warning: Distortions increase far from zone 54, but acceptable for filtering.
    gdf_proj = gdf.to_crs("EPSG:32654")
    areas_sq_m = gdf_proj.geometry.area
    return areas_sq_m / 1e6

def main():
    logger.info("Starting OSM River extraction for Japan...")
    all_features = []

    for pref in PREFECTURES_JP:
        data = fetch_rivers_for_area(pref)
        if data and 'elements' in data:
            feats = process_osm_elements(data['elements'])
            logger.info(f"  {pref}: Found {len(feats)} potential river features.")
            all_features.extend(feats)
        else:
            logger.warning(f"  {pref}: No data returned.")
        
        time.sleep(10) # Respect API rate limits

    if not all_features:
        logger.error("No features found at all.")
        return

    # Create GeoDataFrame
    logger.info(f"Creating GeoDataFrame from {len(all_features)} features...")
    gdf = gpd.GeoDataFrame(all_features, crs="EPSG:4326")
    
    # Calculate Area
    logger.info("Calculating areas...")
    gdf['area_km2'] = calculate_area_km2(gdf)
    
    # Filter by Area
    logger.info(f"Filtering rivers > {MIN_AREA_KM2} km2...")
    gdf_large = gdf[gdf['area_km2'] >= MIN_AREA_KM2].copy()
    
    logger.info(f"Selected {len(gdf_large)} river features.")
    
    if gdf_large.empty:
        logger.warning("No rivers met the area criteria.")
        return

    # Sort by Area
    gdf_large = gdf_large.sort_values('area_km2', ascending=False)
    
    # Save to GeoJSON
    logger.info(f"Saving to {OUTPUT_GEOJSON}...")
    gdf_large.to_file(OUTPUT_GEOJSON, driver="GeoJSON")
    
    # Save Summary CSV
    summary_cols = ['name', 'area_km2', 'osm_id', 'type']
    # Add centroid
    logger.info("Calculating centroids...")
    # Warning: centroid on geographic CRS is not geometrically perfect but OK for point location
    centroids = gdf_large.geometry.centroid
    gdf_large['lat'] = centroids.y
    gdf_large['lon'] = centroids.x
    
    summary_df = gdf_large[['name', 'area_km2', 'lat', 'lon', 'osm_id', 'type']]
    summary_df.to_csv(OUTPUT_SUMMARY, index=False)
    logger.info(f"Saved summary to {OUTPUT_SUMMARY}")
    
    # Print top 10
    print("\nTop 10 Largest Rivers found:")
    print(summary_df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
