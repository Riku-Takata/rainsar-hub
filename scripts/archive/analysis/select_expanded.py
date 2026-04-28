import pandas as pd
import requests
import time
import logging
import zipfile
import re
import numpy as np
import rasterio
from rasterio.features import rasterize
from shapely.geometry import shape, box, LineString
from shapely.strtree import STRtree
from pathlib import Path
import fiona
from concurrent.futures import ThreadPoolExecutor

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_DIR = BASE_DIR / "data_vv" / "analysis" / "final_selection"
OUTPUT_CSV = OUTPUT_DIR / "best_expanded_grids.csv"
FUDE_DIR = Path("D:/sotsuron/fude-polygon")

# Constraints for Phase 2
MIN_RAIN = 10.0
MAX_DELAY = 12.0
MIN_EVENT_COUNT = 5 # Relaxed
MIN_ROAD_PX = 1000  # Strict
MIN_PADDY_PX = 20000 # Strict
RES_M = 10

# Limits
TARGET_GRID_COUNT = 150 # Aim for 100-150 grids

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("select_v6")

def decode_grid(gid):
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, gid)
    if not m: return None, None
    ns, lat_s, ew, lon_s = m.groups()
    lat = float(lat_s)/100.0
    if ns=='S': lat = -lat
    lon = float(lon_s)/100.0
    if ew=='W': lon = -lon
    return lat, lon

def get_grid_poly(lat, lon):
    s, w, n, e = lat - 0.05, lon - 0.05, lat + 0.05, lon + 0.05
    return box(w, s, e, n), (s, w, n, e)

def fetch_osm_motorways(bbox_poly):
    minx, miny, maxx, maxy = bbox_poly.bounds
    s, w, n, e = miny, minx, maxy, maxx
    overpass_url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json][timeout:25];
    way["highway"~"motorway|trunk"]({s},{w},{n},{e});
    out geom;
    """
    try:
        response = requests.get(overpass_url, params={'data': query}, timeout=30)
        if response.status_code == 200:
            return response.json()
    except: pass
    return None

def calc_pixels_osm(osm_data, bbox_tuple, height, width):
    if not osm_data or 'elements' not in osm_data: return 0
    lines = []
    for el in osm_data['elements']:
        if 'geometry' in el:
            coords = [(pt['lon'], pt['lat']) for pt in el['geometry']]
            if len(coords) > 1:
                lines.append(LineString(coords))
    
    if not lines: return 0
    
    s, w, n, e = bbox_tuple
    transform_affine = rasterio.transform.from_bounds(w, s, e, n, width, height)
    
    shapes = [(l, 1) for l in lines]
    mask = rasterize(shapes, out_shape=(height, width), transform=transform_affine, all_touched=True, fill=0, dtype=np.uint8)
    return np.sum(mask)

def scan_zip_bounds(zf):
    """Worker function to scan a single zip for all GeoJSON bounds."""
    sub_indices = []
    try:
        with zipfile.ZipFile(zf, 'r') as z:
            names = [n for n in z.namelist() if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
            for name in names:
                try:
                    vsi_path = f"/vsizip/{str(zf).replace('\\', '/')}/{name}"
                    with fiona.open(vsi_path) as src:
                        b = src.bounds
                        # Store (zip_path, filename, geom_box)
                        sub_indices.append((zf, name, box(b[0], b[1], b[2], b[3])))
                except: pass
    except: pass
    return sub_indices

def index_fude_zips_parallel():
    if not FUDE_DIR.exists(): return [], None
    logger.info(f"Scanning {FUDE_DIR} for *.zip (Parallel)...")
    zip_files = list(FUDE_DIR.glob("*.zip"))
    
    all_indices = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(scan_zip_bounds, zip_files)
        for res in results:
            all_indices.extend(res)
            
    logger.info(f"Indexed {len(all_indices)} Fude Sub-areas. Building Spatial Tree...")
    
    geoms = [item[2] for item in all_indices]
    data_items = [(item[0], item[1]) for item in all_indices]
    
    tree = STRtree(geoms)
    return data_items, tree

def calc_pixels_fude(grid_poly, bbox_tuple, height, width, fude_data, fude_tree):
    s, w, n, e = bbox_tuple
    transform_affine = rasterio.transform.from_bounds(w, s, e, n, width, height)
    paddies = []
    
    fiona_bbox = (w, s, e, n)
    
    # STRtree Query
    candidate_indices = fude_tree.query(grid_poly)
    
    for idx in candidate_indices:
        zf, name = fude_data[idx]
        try:
             vsi_path = f"/vsizip/{str(zf).replace('\\', '/')}/{name}"
             with fiona.open(vsi_path) as src:
                for feat in src.filter(bbox=fiona_bbox):
                    props = feat.get('properties', {})
                    lt = props.get('land_type') or props.get('LAND_TYPE')
                    if str(lt) == '100':
                        geom = shape(feat['geometry'])
                        if geom.intersects(grid_poly):
                            paddies.append(geom)
        except: pass
    
    if not paddies: return 0
    shapes = [(p, 1) for p in paddies]
    mask = rasterize(shapes, out_shape=(height, width), transform=transform_affine, all_touched=True, default_value=1, dtype=np.uint8)
    return np.sum(mask)

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not INPUT_PAIRS_CSV.exists():
        logger.error("Input CSV missing")
        return

    logger.info("Loading pairs...")
    df = pd.read_csv(INPUT_PAIRS_CSV)
    
    # 1. Strict Event Filter
    df = df[
        (df['max_gauge_mm_h'] >= MIN_RAIN) &
        (df['delay_h'] <= MAX_DELAY) &
        (df['after_scene_id'].notna()) &
        (df['before_scene_id'].notna())
    ].copy()
    
    # 2. Aggregation (Relaxed Uniformity)
    candidates_list = []
    grouped = df.groupby('grid_id')
    
    for gid, group in grouped:
        if len(group) < MIN_EVENT_COUNT: continue
        
        # Unique check
        pairs = list(zip(group['after_scene_id'], group['before_scene_id']))
        if len(pairs) != len(set(pairs)): continue
            
        delays = sorted(group['delay_h'].tolist())
        
        candidates_list.append({
            'grid_id': gid,
            'event_count': len(group),
            'delays': delays
        })
            
    candidates = pd.DataFrame(candidates_list).sort_values('event_count', ascending=False)
    logger.info(f"Candidates passing Event >= {MIN_EVENT_COUNT}: {len(candidates)}")
    
    if len(candidates) == 0:
        logger.error("No grids match event criteria!")
        return

    # 3. Geometry Check (Optimized)
    fude_data, fude_tree = index_fude_zips_parallel()
    if not fude_tree:
        logger.error("Failed to build Fude Index.")
        return
        
    selected = []
    checked = 0
    # Process candidates
    # Prioritize higher event counts
    
    for _, row in candidates.iterrows():
        if len(selected) >= TARGET_GRID_COUNT: break
        
        gid = row['grid_id']
        lat, lon = decode_grid(gid)
        if not lat: continue
        
        grid_poly, bbox = get_grid_poly(lat, lon)
        s, w, n, e = bbox
        
        # Calc Resolution (Same as before)
        h_m = (n - s) * 111000
        w_m = (e - w) * 111000 * np.cos(np.radians(lat))
        height = int(h_m / RES_M)
        width = int(w_m / RES_M)
        
        # Road Check (OSM API is slow, careful of rate limits)
        osm_data = fetch_osm_motorways(grid_poly) # Pass poly for bounds
        road_px = calc_pixels_osm(osm_data, bbox, height, width)
        time.sleep(0.5) # Gentle on OSM
        
        if road_px < MIN_ROAD_PX:
            checked += 1
            if checked % 50 == 0: logger.info(f"Checked {checked}, Selected {len(selected)}")
            continue
            
        # Paddy Check (Fast)
        paddy_px = calc_pixels_fude(grid_poly, bbox, height, width, fude_data, fude_tree)
        
        if paddy_px >= MIN_PADDY_PX:
            logger.info(f"  [SELECTED] {gid}: Road={road_px}, Paddy={paddy_px}, Events={row['event_count']}")
            selected.append({
                'grid_id': gid,
                'event_count': row['event_count'],
                'road_pixels': road_px,
                'paddy_pixels': paddy_px,
                'delays': str(row['delays'])
            })
        else:
             # logger.info(f"  [Reject] {gid}: Paddy Low ({paddy_px})")
             pass
            
        checked += 1
        if checked % 50 == 0: logger.info(f"Checked {checked}, Selected {len(selected)}")

    if selected:
        out_df = pd.DataFrame(selected)
        out_df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"Saved {len(out_df)} grids to {OUTPUT_CSV}")
    else:
        logger.error("No grids selected.")

if __name__ == "__main__":
    main()
