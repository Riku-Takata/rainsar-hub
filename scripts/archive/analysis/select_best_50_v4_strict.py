import pandas as pd
import requests
import time
import logging
import zipfile
import re
import numpy as np
import rasterio
from rasterio.features import rasterize
from shapely.geometry import shape, box, LineString, Polygon
from shapely.ops import transform
from pathlib import Path
import fiona

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_50_grids_v4_strict.csv"
FUDE_DIR = Path("D:/sotsuron/fude-polygon")

# Constraints
MIN_RAIN = 10.0
MAX_DELAY = 12.0
MIN_ROAD_PX = 500
MIN_PADDY_PX = 30000
RES_M = 10
GRID_SIZE_DEG = 0.05 # +/- 0.05 around center => 0.1 deg box ~ 10km. 
# Wait, grid ID N3035... is center or corner? 
# Usually JMA/Standard grids are defined by corners.
# Let's assume standard 0.05 deg buffer implies ~10km width if ID is center.
# If ID is BL corner, we need correct bounding box.
# Assuming ID is BL corner for safety as per standard mesh codes roughly.
# Actually, let's stick to the +/- 0.05 buffer around the decoded point to match previous scripts.

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("select_v4")

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
    # Buffer 0.05 deg ~ 5.5km radius? Or box.
    # We want a 5km x 5km area approx.
    # 0.05 deg is about 5.5km. +/- 0.025 would be 5km width.
    # Previous scripts used +/- 0.05, so let's stick to that to be consistent with "Grid" definition used so far.
    s, w, n, e = lat - 0.05, lon - 0.05, lat + 0.05, lon + 0.05
    return box(w, s, e, n), (s, w, n, e)

def fetch_osm_motorways(bbox):
    s, w, n, e = bbox
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

def calc_pixels_osm(osm_data, bbox, height, width):
    if not osm_data or 'elements' not in osm_data: return 0
    lines = []
    for el in osm_data['elements']:
        if 'geometry' in el:
            coords = [(pt['lon'], pt['lat']) for pt in el['geometry']]
            if len(coords) > 1:
                lines.append(LineString(coords))
    
    if not lines: return 0
    
    s, w, n, e = bbox
    transform_affine = rasterio.transform.from_bounds(w, s, e, n, width, height)
    
    shapes = [(l, 1) for l in lines]
    mask = rasterize(shapes, out_shape=(height, width), transform=transform_affine, all_touched=True, fill=0, dtype=np.uint8)
    return np.sum(mask)

def index_fude_zips():
    # Return list of (ZipPath, Box)
    indices = []
    if not FUDE_DIR.exists(): return []
    
    logger.info(f"Scanning {FUDE_DIR} for *.zip...")
    zip_files = list(FUDE_DIR.glob("*.zip"))
    logger.info(f"Found {len(zip_files)} zip files.")
    
    for zf in zip_files:
        try:
            with zipfile.ZipFile(zf, 'r') as z:
                # Check .geojson AND .json
                names = [n for n in z.namelist() if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
                if not names:
                    # logger.warning(f"Skipping {zf.name}: No geojson/json found.")
                    continue
                
                # Use VSISZIP
                for name in names:
                    try:
                        vsi_path = f"/vsizip/{str(zf).replace('\\', '/')}/{name}"
                        with fiona.open(vsi_path) as src:
                            b = src.bounds # minx, miny, maxx, maxy
                            if len(indices) == 0:
                                logger.info(f"DEBUG: First File {name} bounds: {b}")
                            indices.append((zf, name, box(b[0], b[1], b[2], b[3])))
                    except: pass
        except Exception as e:
            logger.warning(f"Error indexing {zf.name}: {e}")
    logger.info(f"Indexed {len(indices)} Fude Sub-areas.")
    return indices

def calc_pixels_fude(grid_poly, bbox, height, width, fude_index):
    # Find intersecting Zips
    s, w, n, e = bbox
    transform_affine = rasterio.transform.from_bounds(w, s, e, n, width, height)
    
    paddies = []
    fiona_bbox = (w, s, e, n)
    
    # Iterate Index
    for zf, fname, zpoly in fude_index:
        if grid_poly.intersects(zpoly):
             # Found intersecting municipality
             try:
                 vsi_path = f"/vsizip/{str(zf).replace('\\', '/')}/{fname}"
                 with fiona.open(vsi_path) as src:
                    for feat in src.filter(bbox=fiona_bbox):
                        props = feat.get('properties', {})
                        # 100 = Paddy
                        lt = props.get('land_type') or props.get('LAND_TYPE')
                        if str(lt) == '100':
                            geom = shape(feat['geometry'])
                            if geom.intersects(grid_poly):
                                paddies.append(geom)
             except: pass
    
    # Debug log
    logger.info(f"  Debug: Found {len(paddies)} paddy features.")
            
    if not paddies: return 0
    
    shapes = [(p, 1) for p in paddies]
    mask = rasterize(shapes, out_shape=(height, width), transform=transform_affine, all_touched=True, default_value=1, dtype=np.uint8)
    return np.sum(mask)

def main():
    if not INPUT_PAIRS_CSV.exists():
        logger.error("Input CSV missing")
        return

    logger.info("Loading pairs...")
    df = pd.read_csv(INPUT_PAIRS_CSV)
    
    # Strict Event Filter
    df = df[
        (df['max_gauge_mm_h'] >= MIN_RAIN) &
        (df['delay_h'] <= MAX_DELAY)
    ]
    
    # Aggregation
    stats = df.groupby('grid_id').size().reset_index(name='event_count')
    candidates = stats[stats['event_count'] >= 5].sort_values('event_count', ascending=False)
    
    logger.info(f"Candidates (>=5 events, Rain>=10, Delay<=12): {len(candidates)}")
    
    # Index Fude
    fude_index = index_fude_zips()
    
    selected = []
    checked = 0
    
    for _, row in candidates.iterrows():
        if len(selected) >= 50: break
        
        gid = row['grid_id']
        count = row['event_count']
        lat, lon = decode_grid(gid)
        
        # Grid Params
        grid_poly, bbox = get_grid_poly(lat, lon)
        s, w, n, e = bbox
        
        # Raster Size (10m res)
        # Lat deg to meters ~ 111000
        # Lon deg to meters ~ 111000 * cos(lat)
        h_m = (n - s) * 111000
        w_m = (e - w) * 111000 * np.cos(np.radians(lat))
        height = int(h_m / RES_M)
        width = int(w_m / RES_M)
        
        # 1. Road Check (Fast)
        osm_data = fetch_osm_motorways(bbox)
        road_px = calc_pixels_osm(osm_data, bbox, height, width)
        time.sleep(1)
        
        if road_px < MIN_ROAD_PX:
            # logger.info(f"  [Reject] {gid}: Road {road_px} < {MIN_ROAD_PX}")
            checked += 1
            if checked % 10 == 0: print(f"Checked {checked}, Selected {len(selected)}")
            continue
            
        # 2. Paddy Check (Slow)
        paddy_px = calc_pixels_fude(grid_poly, bbox, height, width, fude_index)
        
        if paddy_px < MIN_PADDY_PX:
            logger.info(f"  [Reject] {gid}: Road OK ({road_px}), Paddy {paddy_px} < {MIN_PADDY_PX}")
        else:
            logger.info(f"  [SELECTED] {gid}: Road={road_px}, Paddy={paddy_px}, Events={count}")
            selected.append({
                'grid_id': gid,
                'event_count': count,
                'road_pixels': road_px,
                'paddy_pixels': paddy_px
            })
            
        checked += 1
        if checked % 10 == 0: print(f"Checked {checked}, Selected {len(selected)}")

    if selected:
        out_df = pd.DataFrame(selected)
        out_df.to_csv(OUTPUT_CSV, index=False)
        print("\nTop Selected:")
        print(out_df.head())
        logger.info(f"Saved {len(out_df)} grids to {OUTPUT_CSV}")
    else:
        logger.error("No grids met strict criteria.")

if __name__ == "__main__":
    main()
