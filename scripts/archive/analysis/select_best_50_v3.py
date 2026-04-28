import pandas as pd
import requests
import time
import zipfile
import math
import re
import logging
from pathlib import Path
from shapely.geometry import shape
import fiona

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_50_grids_v3.csv"
FUDE_DIR = Path("D:/sotsuron/fude-polygon")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("select_v3")

def decode_grid(gid):
    # N3035E13065 -> Lat 30.35, Lon 130.65
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, gid)
    if not m: return None, None
    ns, lat_s, ew, lon_s = m.groups()
    lat = float(lat_s)/100.0
    if ns=='S': lat = -lat
    lon = float(lon_s)/100.0
    if ew=='W': lon = -lon
    return lat, lon

def get_grid_bbox(lat, lon):
    # Approx 0.1 degree grid? Or 0.05? 
    # Usually these grids are roughly 10km. 
    # Let's assume +/- 0.05 deg for query safety to catch intersecting roads.
    # Grid ID typically center or BL corner. 
    # RainSAR grids are 0.1 deg x 0.1 deg? 
    # N3035E13065 -> 30.35, 130.65.
    # If center, bbox is [lat-0.05, lon-0.05, lat+0.05, lon+0.05]
    # Let's query a slightly smaller box to ensure the road goes *through* it, 
    # or match the exact grid definition if known. Assuming ID is center.
    offset = 0.05
    return (lat - offset, lon - offset, lat + offset, lon + offset)

def check_osm_highway(lat, lon):
    # Overpass API query
    # south, west, north, east
    s, w, n, e = get_grid_bbox(lat, lon)
    
    # Query for motorway
    query = f"""
    [out:json][timeout:15];
    way["highway"~"motorway|trunk"]({s},{w},{n},{e});
    out count;
    """
    
    mirrors = [
        "https://overpass.kumi.systems/api/interpreter",
        "http://overpass-api.de/api/interpreter",
        "https://maps.mail.ru/osm/tools/overpass/api/interpreter"
    ]
    
    for url in mirrors:
        try:
            # logger.info(f"Querying {url}...")
            response = requests.get(url, params={'data': query}, timeout=25)
            
            if response.status_code == 200:
                data = response.json()
                for elem in data.get('elements', []):
                    if elem.get('type') == 'count':
                        ways = int(elem.get('tags', {}).get('ways', 0))
                        if ways > 0: return True
                return False
                
            elif response.status_code == 429:
                time.sleep(2)
                continue # Try next mirror
            
        except Exception:
            continue # Try next mirror
            
    logger.warning("  [Warn] All Overpass mirrors failed/timed out.")
    return False

def get_fude_list():
    logger.info("Scanning Fude Polygons for centroids...")
    centroids = []
    if FUDE_DIR.exists():
        for zf in FUDE_DIR.glob("*.zip"):
            try:
                with zipfile.ZipFile(zf, 'r') as z:
                    names = z.namelist()
                    spatial = [n for n in names if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
                    if not spatial: continue
                    # Just reading one feature is enough to get general location of the prefecture/area
                    # Heuristic: If grid is close to ANY fude file content, it likely has paddies 
                    # (since these files are nationwide paddy maps).
                    # Actually, we should check if THIS grid is near THIS file's area.
                    
                    # Optimization: Extract prefecture code from filename? 
                    # 2022_40.zip -> Fukuoka.
                    # Creating a map of Pref centroid -> list of zips might be better but let's stick to the previous proximity scan
                    # reading first geometry of each zip.
                    
                    first = spatial[0]
                    abs_path = str(zf.resolve()).replace("\\", "/")
                    vsi_path = f"/vsizip/{abs_path}/{first}"
                    
                    with fiona.open(vsi_path) as src:
                        try:
                            # Read simple bounds or first feature
                            # bounds = src.bounds # (minx, miny, maxx, maxy)
                            # center = ((bounds[1]+bounds[3])/2, (bounds[0]+bounds[2])/2)
                            # centroids.append(center)
                            
                            # Better: read the first feature
                            feat = next(iter(src))
                            geom = shape(feat['geometry'])
                            c = geom.centroid
                            centroids.append((c.y, c.x))
                        except: pass
            except: pass
    logger.info(f"Loaded {len(centroids)} Fude region markers.")
    return centroids

def main():
    if not INPUT_PAIRS_CSV.exists():
        logger.error("Input CSV not found.")
        return

    logger.info("Loading cleaned pairs...")
    df = pd.read_csv(INPUT_PAIRS_CSV)
    
    # 1. Filter Delay
    df = df[df['delay_h'] <= 12.0]
    
    # 2. Aggregation
    stats = df.groupby('grid_id').agg(
        event_count=('delay_h', 'count'),
        min_delay=('delay_h', 'min')
    ).sort_values('event_count', ascending=False)
    
    candidates = stats.reset_index()
    logger.info(f"Total Grids with <=12h events: {len(candidates)}")
    
    # 3. Filter Loop
    selected = []
    fude_points = get_fude_list()
    
    # Process candidates in order (highest count first)
    checked_count = 0
    
    for idx, row in candidates.iterrows():
        if len(selected) >= 50:
            break
            
        gid = row['grid_id']
        count = row['event_count']
        
        # Heuristic to skip low counts if we haven't found enough yet?
        # No, just check top down.
        
        lat, lon = decode_grid(gid)
        if not lat: continue
        
        # A. Fude Check (Local Proximity)
        # 0.5 degrees ~ 50km. A crude check that we are in a 'mapped' agricultural area.
        has_paddy = False
        for flat, flon in fude_points:
            dist = math.sqrt((lat-flat)**2 + (lon-flon)**2)
            if dist < 0.5:
                has_paddy = True
                break
        
        if not has_paddy:
            # logger.info(f"  [Skip] {gid}: No nearby Fude data")
            continue
            
        # B. Highway Check (Live API)
        has_highway = check_osm_highway(lat, lon)
        time.sleep(1) # Be nice to API
        
        status = "OK" if has_highway else "No Highway"
        if has_highway:
            selected.append(row)
            logger.info(f"  [SELECTED] #{len(selected)} {gid} (Events: {count})")
        else:
            # logger.info(f"  [Reject] {gid}: {status}")
            pass
            
        checked_count += 1
        if checked_count % 10 == 0:
            print(f"Checked {checked_count} grids... Found {len(selected)} so far.")

    # Save
    if selected:
        final_df = pd.DataFrame(selected)
        final_df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"Saved best {len(final_df)} grids to {OUTPUT_CSV}")
        print("\nTop 10 Selected:")
        print(final_df.head(10))
    else:
        logger.error("No grids met criteria!")

if __name__ == "__main__":
    main()
