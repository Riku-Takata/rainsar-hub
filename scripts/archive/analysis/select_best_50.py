import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
# Use the pairs file to filter specific events by delay
INPUT_PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_50_grids_v2.csv"

def main():
    if not INPUT_PAIRS_CSV.exists():
        print(f"Error: {INPUT_PAIRS_CSV} not found.")
        return

    # Load all pairs
    # Expected cols: grid_id, after_scene_id, before_scene_id, delay_h, ...
    print(f"Loading {INPUT_PAIRS_CSV}...")
    df = pd.read_csv(INPUT_PAIRS_CSV)
    print(f"Total pairs/events: {len(df)}")
    
    # 0. Filter by Delay <= 12h (User request)
    df = df[df['delay_h'] <= 12.0].copy()
    print(f"Events with delay <= 12h: {len(df)}")
    
    # 1. Aggregate by Grid
    # We need count and min_delay for sorting
    grid_stats = df.groupby('grid_id').agg(
        pair_count=('delay_h', 'count'),
        min_delay=('delay_h', 'min'),
        mean_delay=('delay_h', 'mean')
    ).reset_index()
    
    print(f"Unique grids with <=12h events: {len(grid_stats)}")

    # 2. Filter by Event Count (10 <= count <= 15)
    filtered = grid_stats[(grid_stats['pair_count'] >= 10) & (grid_stats['pair_count'] <= 15)].copy()
    print(f"Grids with 10-15 qualifying events: {len(filtered)}")
    
    if len(filtered) < 50:
        print("Warning: Fewer than 50 grids meet the criteria. Relaxing upper bound...")
        filtered = grid_stats[grid_stats['pair_count'] >= 10].copy()
        print(f"Grids with >=10 qualifying events: {len(filtered)}")

    # 3. Filter by Land Cover Availability (Fude Proxy)
    # Scan Fude Zips to get centroids
    print("Checking Fude Polygon coverage...")
    fude_dir = Path("D:/sotsuron/fude-polygon")
    fude_centroids = []
    
    if fude_dir.exists():
        import zipfile
        import math
        import re
        import fiona
        from shapely.geometry import shape

        for zf in fude_dir.glob("*.zip"):
            try:
                # Peek first spatial file
                with zipfile.ZipFile(zf, 'r') as z:
                    names = z.namelist()
                    spatial = [n for n in names if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
                    if not spatial: continue
                    
                    first = spatial[0]
                    # Read using vsizip
                    # Use a fresh env or subprocess if fiona locks? Just try.
                    # We might need to handle the path correctly.
                    # D:/... needs to be compatible with GDAL
                    abs_path = str(zf.resolve()).replace("\\", "/")
                    vsi_path = f"/vsizip/{abs_path}/{first}"
                    
                    with fiona.open(vsi_path) as src:
                         try:
                             feat = next(iter(src))
                             geom = shape(feat['geometry'])
                             c = geom.centroid
                             fude_centroids.append((c.y, c.x)) # lat, lon
                         except:
                             pass
            except Exception as e:
                # print(f"Error peeking {zf.name}: {e}")
                pass
    
    print(f"Found {len(fude_centroids)} Fude data points.")
    
    def decode_grid(gid):
        # Decode N3565E13965 -> Lat, Lon
        pattern = r"([NS])(\d{5})([EW])(\d{5})"
        m = re.match(pattern, gid)
        if not m: return None, None
        ns, lat_s, ew, lon_s = m.groups()
        lat = float(lat_s)/100.0
        if ns=='S': lat = -lat
        lon = float(lon_s)/100.0
        if ew=='W': lon = -lon
        return lat, lon

    # Filter grids
    # Criteria: Must be within 0.5 deg (~50km) of ANY Fude centroid
    # Fude Zips are usually per prefecture or sub-pref. 0.5 deg is conservative.
    
    valid_grids_mask = []
    for gid in filtered['grid_id']:
        lat, lon = decode_grid(gid)
        if lat is None:
            valid_grids_mask.append(False)
            continue
            
        # Check dist
        is_near = False
        for flat, flon in fude_centroids:
            dist = math.sqrt((lat-flat)**2 + (lon-flon)**2)
            if dist < 0.5: # 0.5 deg
                is_near = True
                break
        valid_grids_mask.append(is_near)
        
    filtered = filtered[valid_grids_mask].copy()
    print(f"Grids with Fude coverage (proxy): {len(filtered)}")

    # 4. Sort by Quality (Immediacy: min_delay)
    filtered = filtered.sort_values(by=['min_delay', 'mean_delay'], ascending=[True, True])
    
    # 5. Select Top 50
    top_50 = filtered.head(50)
    
    # 6. Save
    top_50.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved best 50 grids to {OUTPUT_CSV}")
    print(top_50[['grid_id', 'pair_count', 'min_delay']].head(10))

if __name__ == "__main__":
    main()
