import pandas as pd
import requests
import json
import time
import logging
from pathlib import Path
from shapely.geometry import box, shape, LineString, Polygon, MultiPolygon
from shapely.ops import transform
import rasterio
from rasterio.features import rasterize
import numpy as np
import pyproj
import geopandas as gpd

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_50_grids_v4_strict.csv"
DEBUG_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "grid_pixel_stats.csv"

# Resolution
RES_M = 10
GRID_SIZE_DEG = 0.05
# Approx 5km x 5km -> 500x500 = 250,000 pixels

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("select_v4")

def decode_grid_id(grid_id):
    # N03445E13605 -> lat=34.45, lon=136.05 (Bottom-Left)
    try:
        lat_str = grid_id[1:6] # 03445
        lon_str = grid_id[7:13] # 13605
        lat = float(lat_str) / 100
        lon = float(lon_str) / 100
        return lat, lon
    except:
        return None, None

def fetch_osm_motorways(lat, lon, size=0.05):
    # Overpass API
    overpass_url = "http://overpass-api.de/api/interpreter"
    bbox = f"{lat},{lon},{lat+size},{lon+size}"
    query = f"""
    [out:json];
    (
      way["highway"="motorway"]({bbox});
      way["highway"="trunk"]({bbox}); 
    );
    out geom;
    """
    # Note: user said "Motorway only", but usually "trunk" is also vital. 
    # I'll stick to motorway for strictness if requested, but falling back to trunk is safer.
    # User said: "最初は高速道路のみを対象" -> Strictly Motorway.
    
    strict_query = f"""
    [out:json];
    way["highway"="motorway"]({bbox});
    out geom;
    """
    
    try:
        response = requests.get(overpass_url, params={'data': strict_query}, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"  OSM Error: {e}")
    return None

def fetch_fude_polygons(lat, lon):
    # This requires local files. Simulation for now?
    # Actually, we have a script `analyze_fude_pixels.py` (hypothetically) or similar.
    # Accessing heavy ZIPs for every candidate is slow.
    # Strategy: Use a simplified check or assume we verify this later?
    # No, user asked to "Select based on 10000 pixels".
    # I must check it.
    # But Fude is massive. 
    # Let's assume for this "Planning" step, I will filter Roads FIRST (fast API).
    # Then for those passing Road > 10k, I check Fude.
    pass

def calculate_pixels_osm(osm_data, lat, lon, size=0.05):
    if not osm_data or 'elements' not in osm_data:
        return 0
        
    lines = []
    for el in osm_data['elements']:
        if 'geometry' in el:
            coords = [(pt['lon'], pt['lat']) for pt in el['geometry']]
            lines.append(LineString(coords))
            
    if not lines:
        return 0
        
    # Rasterize
    # Bounds
    minx, miny, maxx, maxy = lon, lat, lon+size, lat+size
    
    # Simple transform to approx meters (Plane) or work in deg?
    # Rasterize in meters is better for pixel count.
    # Reproject to JGD2011 (EPSG:6668) or UTM?
    # Let's use simple approximation for checking.
    # 1 deg ~ 111km. 0.05 deg ~ 5.5km.
    # 5.5km / 10m = 550 pixels.
    # Width ~ 550, Height ~ 550.
    
    height = int(size * 111000 / RES_M)
    width = int(size * 111000 * np.cos(np.radians(lat)) / RES_M)
    
    transform_affine = rasterio.transform.from_bounds(minx, miny, maxx, maxy, width, height)
    
    # We need to burn lines.
    # Issue: rasterize works on polygons or lines.
    # For lines, we need thickness?
    # Rasterize usually burns "touched" pixels or center.
    # Ideally, we buffer the lines by Resolution/2 (5m) to simulate 10m width?
    # User asked for "10000 pixels". One pixel is 100m^2.
    # If we burn lines 'all_touched=True', we get roughly length * 1px width.
    
    shapes = [(l, 1) for l in lines]
    
    mask = rasterio.features.rasterize(
        shapes,
        out_shape=(height, width),
        transform=transform_affine,
        all_touched=True, # Critical for thin lines
        fill=0,
        dtype=np.uint8
    )
    
    return np.sum(mask)

def main():
    logger.info("Starting Strict Selection (V4)...")
    df = pd.read_csv(PAIRS_CSV)
    
    # 1. Count events per grid (Rain>=10)
    # User said: "Remove filtered events? Or just select grids?"
    # Use strict rain filter for counting events too.
    df_heavy = df[df['max_gauge_mm_h'] >= 10.0]
    
    grid_counts = df_heavy.groupby('grid_id').size().reset_index(name='event_count')
    # Filter >= 5 events to start
    candidates = grid_counts[grid_counts['event_count'] >= 5].sort_values('event_count', ascending=False)
    
    logger.info(f"Candidates with >=5 Heavy Rain events: {len(candidates)}")
    
    results = []
    
    # Process top candidates
    count = 0
    for _, row in candidates.iterrows():
        if count >= 200: break # Limit processing
        
        gid = row['grid_id']
        lat, lon = decode_grid_id(gid)
        
        # 1. Check Motorway Pixels
        osm_data = fetch_osm_motorways(lat, lon)
        road_pixels = calculate_pixels_osm(osm_data, lat, lon)
        
        # 2. Heuristic for Mountain/Paddy
        # If road pixels are low, likely mountain or rural without highway.
        # User constraint: Road > 10000.
        # That is HUGE. 10,000 pixels = 100km length (if 1px width) or 1km^2 area.
        # Let's count and see.
        
        logger.info(f"Checking {gid}: Roads={road_pixels} px")
        
        results.append({
            'grid_id': gid,
            'event_count': row['event_count'],
            'road_pixels': road_pixels
        })
        
        count += 1
        time.sleep(1) # API rate limit
        
    df_res = pd.DataFrame(results)
    df_res.to_csv(DEBUG_CSV, index=False)
    logger.info(f"Saved stats to {DEBUG_CSV}")
    
    # Check max
    logger.info(f"Max Road Pixels found: {df_res['road_pixels'].max()}")
    
    # Filter
    valid = df_res[df_res['road_pixels'] >= 10000]
    logger.info(f"Grids with > 10k Road Pixels: {len(valid)}")

if __name__ == "__main__":
    main()
