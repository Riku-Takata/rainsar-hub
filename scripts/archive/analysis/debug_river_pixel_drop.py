import rasterio
import numpy as np
import pandas as pd
from pathlib import Path
import geopandas as gpd
from rasterio.features import geometry_mask, rasterize
from shapely.geometry import box

# Config
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"
RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data/river_polygon_2320001.geojson",
    BASE_DIR / "mask-data/river_polygon_2320061.geojson"
]
PIXEL_COUNTS_CSV = BASE_DIR / "data/analysis/aug_oct_pixel_counts_with_river_detailed.csv"

# Target Delay 0h (where drop is large: 682 -> 39)
TARGET_DELAY = 0
TARGET_MONTH = 8

def main():
    print(f"=== Debugging Pixel Drop for Delay {TARGET_DELAY}h (Month {TARGET_MONTH}) ===")
    
    # Load River Polygons
    gdfs = [gpd.read_file(p) for p in RIVER_GEOJSON_PATHS if p.exists()]
    RIVER_GDFS = pd.concat(gdfs, ignore_index=True)
    # Re-list for loop usage similar to script
    RIVER_GDFS_LIST = [gpd.read_file(p) for p in RIVER_GEOJSON_PATHS if p.exists()]
    
    # Load Counts to pick an event
    df_counts = pd.read_csv(PIXEL_COUNTS_CSV)
    subset = df_counts[(df_counts['month'] == TARGET_MONTH) & (df_counts['delay_int'] == TARGET_DELAY)]
    
    if subset.empty:
        print("No events found in CSV.")
        return
        
    print(f"Found {len(subset)} events in CSV.")
    
    total_after_only = 0
    total_before_match = 0
    total_before_valid = 0
    
    for _, row in subset.iterrows():
        grid_id = row['grid_id']
        event_dir_name = row['event_dir']
        event_dir = SAMPLES_DIR / grid_id / event_dir_name
        
        path_af = event_dir / "after_vv.tif"
        path_be = event_dir / "before_vv.tif"
        
        if not path_af.exists():
            print(f"SKIP: {event_dir_name} (No After VV)")
            continue
            
        with rasterio.open(path_af) as src_af:
            af_data = src_af.read(1)
            h, w = af_data.shape
            bounds = src_af.bounds
            
            # 1. After Valid Mask (Count script logic)
            # count script: valid_s1 = (s1_data != s1_nodata) & (~np.isnan(s1_data))
            # It does NOT check > 0 or anything else.
            valid_af = ~np.isnan(af_data)
            if src_af.nodata is not None:
                valid_af &= (af_data != src_af.nodata)
                
            # River Mask
            s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            river_shapes = []
            for river_gdf in RIVER_GDFS_LIST:
                possible_idx = list(river_gdf.sindex.intersection(s1_box.bounds))
                if possible_idx:
                    matches = river_gdf.iloc[possible_idx]
                    matches = matches[matches.intersects(s1_box)]
                    if not matches.empty:
                        river_shapes.extend(matches.geometry)
            
            if river_shapes:
                river_m = rasterize(river_shapes, out_shape=(h, w), transform=src_af.transform, fill=0, default_value=1, dtype=rasterio.uint8) > 0
            else:
                river_m = np.zeros((h, w), dtype=bool)
                
            count_af_only = np.sum(river_m & valid_af)
            total_after_only += count_af_only
            
            # --- Check Before ---
            if not path_be.exists():
                print(f"  {event_dir_name}: After={count_af_only}, Before=MISSING FILE")
                continue
                
            with rasterio.open(path_be) as src_be:
                be_data = src_be.read(1)
                
                # Check shape match
                if be_data.shape != af_data.shape:
                    min_h = min(h, be_data.shape[0])
                    min_w = min(w, be_data.shape[1])
                    be_data = be_data[:min_h, :min_w]
                    # Cut others
                    # ... simplified check
                
                # 2. Before Valid Check (Analysis logic)
                # valid = (~np.isnan(vv_af)) & (~np.isnan(vv_be))
                valid_be = ~np.isnan(be_data)
                
                # Combined
                if be_data.shape == af_data.shape:
                    valid_combined = valid_af & valid_be
                    count_combined = np.sum(river_m & valid_combined)
                    total_before_match += count_combined
                    
                    if count_af_only != count_combined:
                        diff = count_af_only - count_combined
                        if diff > 0:
                             print(f"  {event_dir_name}: AfterCount={count_af_only} -> CombinedCount={count_combined} (Drop: {diff})")
                             # Diagnose drops
                             # Are they NaNs in Before?
                             dropped_mask = (river_m & valid_af) & (~valid_be)
                             if np.sum(dropped_mask) > 0:
                                 print(f"    -> Drop reason: NaNs in Before image")
                    else:
                        # print(f"  {event_dir_name}: Count match ({count_af_only})")
                        pass
                else:
                    print(f"  {event_dir_name}: Shape mismatch! After{af_data.shape} vs Before{be_data.shape}")

    print("-" * 30)
    print(f"Summary for Delay {TARGET_DELAY}h:")
    print(f"  Total Pixels (Count Script Logic / After Only): {total_after_only}")
    print(f"  Total Pixels (Analysis Logic / With Before):   {total_before_match}")
    print(f"  Retention Rate: {total_before_match / total_after_only * 100:.1f}%" if total_after_only > 0 else "  Retention Rate: N/A")

if __name__ == "__main__":
    main()
