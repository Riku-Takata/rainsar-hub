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
from pathlib import Path
import fiona

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_DIR = BASE_DIR / "data_vv" / "analysis" / "final_selection"
OUTPUT_CSV = OUTPUT_DIR / "best_final_grids.csv"
FUDE_DIR = Path("D:/sotsuron/fude-polygon")

# Constraints
MIN_RAIN = 10.0
MAX_DELAY = 12.0 # Implied by context
MIN_EVENT_COUNT = 8
MIN_ROAD_PX = 1000
MIN_PADDY_PX = 20000
RES_M = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("select_v5")

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
    # +/- 0.05 Buffer
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
    indices = []
    if not FUDE_DIR.exists(): return []
    logger.info(f"Scanning {FUDE_DIR} for *.zip...")
    zip_files = list(FUDE_DIR.glob("*.zip"))
    
    # Iterate Zips
    for zf in zip_files:
        try:
            with zipfile.ZipFile(zf, 'r') as z:
                names = [n for n in z.namelist() if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
                if not names: continue
                
                # Check EVERY file to build a granular index
                # (zf, filename, bbox)
                for name in names:
                    try:
                        vsi_path = f"/vsizip/{str(zf).replace('\\', '/')}/{name}"
                        with fiona.open(vsi_path) as src:
                            b = src.bounds
                            indices.append((zf, name, box(b[0], b[1], b[2], b[3])))
                    except: pass
        except: pass
        
    logger.info(f"Indexed {len(indices)} Fude Sub-areas.")
    return indices

def calc_pixels_fude(grid_poly, bbox, height, width, fude_index):
    s, w, n, e = bbox
    transform_affine = rasterio.transform.from_bounds(w, s, e, n, width, height)
    paddies = []
    
    fiona_bbox = (w, s, e, n)
    
    # Check Index (Granular)
    matched_files = 0
    total_features = 0
    
    for zf, name, region_poly in fude_index:
        if grid_poly.intersects(region_poly):
             # Match found
             matched_files += 1
             try:
                 vsi_path = f"/vsizip/{str(zf).replace('\\', '/')}/{name}"
                 with fiona.open(vsi_path) as src:
                    found_in_file = 0
                    for feat in src.filter(bbox=fiona_bbox):
                        found_in_file += 1
                        props = feat.get('properties', {})
                        lt = props.get('land_type') or props.get('LAND_TYPE')
                        if str(lt) == '100':
                            geom = shape(feat['geometry'])
                            if geom.intersects(grid_poly):
                                paddies.append(geom)
                    total_features += found_in_file
             except: pass
    
    if not paddies: return 0
    shapes = [(p, 1) for p in paddies]
    mask = rasterize(shapes, out_shape=(height, width), transform=transform_affine, all_touched=True, default_value=1, dtype=np.uint8)
    return np.sum(mask)

def check_uniformity(delays):
    # Check distribution of delays
    # 0-12h. Ideally spread out.
    # Metric: Coverage of bins [0-3], [3-6], [6-9], [9-12]
    # At least 3 bins must be occupied?
    bins = [0, 0, 0, 0]
    for d in delays:
        if 0 <= d < 3: bins[0] = 1
        elif 3 <= d < 6: bins[1] = 1
        elif 6 <= d < 9: bins[2] = 1
        elif 9 <= d <= 12: bins[3] = 1
    
    score = sum(bins)
    return score >= 3, score

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
    
    # 2. Aggregation & Uniformity Check
    candidates_list = []
    grouped = df.groupby('grid_id')
    
    for gid, group in grouped:
        if len(group) < MIN_EVENT_COUNT: continue
        
        # Check Unique Mapping (Scene IDs should be unique per event? Or pair unique?)
        # User: "Multiple events not mapped to 1 pair".
        # Check duplicates in scene pairs
        pairs = list(zip(group['after_scene_id'], group['before_scene_id']))
        if len(pairs) != len(set(pairs)):
            # Has duplicates
            # Actually, cleaning script kept unique pairs.
            # But if distinct events map to same pair, that's what user dislikes.
            # Since pairs are (After, Before), if they are same, scene IDs are same.
            # So len(unique pairs) should be == len(events).
            continue
            
        # Check Uniformity
        delays = group['delay_h'].tolist()
        is_uniform, score = check_uniformity(delays)
        
        if is_uniform:
            candidates_list.append({
                'grid_id': gid,
                'event_count': len(group),
                'delays': sorted(delays),
                'score': score
            })
            
    candidates = pd.DataFrame(candidates_list).sort_values('event_count', ascending=False)
    logger.info(f"Candidates passing Event/Uniformity Criteria: {len(candidates)}")
    
    if len(candidates) == 0:
        logger.error("No grids match event criteria!")
        return

    # 3. Geometry Check
    fude_index = index_fude_zips()
    
    msg_list = []
    
    selected = []
    checked = 0
    
    for _, row in candidates.iterrows():
        # if len(selected) >= 50: break # Keep gathering for "Final" set
        if checked >= 200: break # Safety limit
        
        gid = row['grid_id']
        lat, lon = decode_grid(gid)
        if not lat: continue
        
        grid_poly, bbox = get_grid_poly(lat, lon)
        s, w, n, e = bbox
        
        h_m = (n - s) * 111000
        w_m = (e - w) * 111000 * np.cos(np.radians(lat))
        height = int(h_m / RES_M)
        width = int(w_m / RES_M)
        
        # Road Check
        osm_data = fetch_osm_motorways(bbox)
        road_px = calc_pixels_osm(osm_data, bbox, height, width)
        time.sleep(1)
        
        if road_px < MIN_ROAD_PX:
            checked += 1
            continue
            
        # Paddy Check
        paddy_px = calc_pixels_fude(grid_poly, bbox, height, width, fude_index)
        
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
            logger.info(f"  [Reject] {gid}: Road OK ({road_px}), Paddy {paddy_px}")
            
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
