import os
import re
import pandas as pd
import numpy as np
import rasterio
from rasterio import features
import geopandas as gpd
from pathlib import Path
import concurrent.futures
from shapely.geometry import box

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
MASK_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks")
OUTPUT_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\aug_oct_backscatter_stats.csv")

RIVER_FILES = [
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson"),
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320061.geojson")
]

TARGET_MONTHS = [8, 10]

# Global cache for river polygons (populated in worker)
RIVER_GDFS = []

def init_worker():
    global RIVER_GDFS
    if not RIVER_GDFS:
        for p in RIVER_FILES:
            if p.exists():
                try:
                    gdf = gpd.read_file(p)
                    _ = gdf.sindex
                    RIVER_GDFS.append(gdf)
                except Exception as e:
                    print(f"Error loading {p}: {e}")

def db_to_linear(db_arr):
    return np.power(10, db_arr / 10.0)

def process_grid(grid_dir):
    global RIVER_GDFS
    if not RIVER_GDFS:
        init_worker()
        
    grid_results = []
    
    # Pre-checks for masks - Accept ANY mask (OR logic, not AND)
    local_mask_dir = MASK_DIR / grid_dir.name
    mask_road_tif = local_mask_dir / f"{grid_dir.name}_road_mask.tif"
    mask_paddy_tif = local_mask_dir / f"{grid_dir.name}_paddy_mask.tif"
    mask_road_json = local_mask_dir / f"{grid_dir.name}_motorway.geojson"
    mask_paddy_json = local_mask_dir / f"{grid_dir.name}_paddy.geojson"
    
    # Check each mask type independently
    has_road_json = mask_road_json.exists()
    has_paddy_json = mask_paddy_json.exists()
    has_road_tif = mask_road_tif.exists()
    has_paddy_tif = mask_paddy_tif.exists()
    
    # River masks are loaded globally (RIVER_GDFS), so we always have potential river coverage
    has_any_road_paddy_mask = has_road_json or has_paddy_json or has_road_tif or has_paddy_tif
    
    # Even if no local road/paddy mask, we can still process for river class (global masks)
    # So we continue processing - the extract_stats calls will handle missing individual masks
    
    # Cache Local GeoDataFrames (Lazy load, load each independently)
    gdf_road = None
    gdf_paddy = None
    
    if has_road_json:
        try:
            gdf_road = gpd.read_file(mask_road_json)
        except:
            has_road_json = False
            
    if has_paddy_json:
        try:
            gdf_paddy = gpd.read_file(mask_paddy_json)
        except:
            has_paddy_json = False

    for event_dir in grid_dir.iterdir():
        if not event_dir.is_dir():
            continue
        
        dir_name = event_dir.name
        match = re.match(r"delay_([\d\.]+)h_(\d{8})", dir_name)
        if not match:
            continue
            
        delay_float = float(match.group(1))
        date_str = match.group(2)
        delay_int = int(delay_float)
        
        if not (0 <= delay_int <= 11):
            continue
        
        try:
            dt = pd.to_datetime(date_str, format="%Y%m%d")
            month = dt.month
        except:
            continue
            
        if month not in TARGET_MONTHS:
            continue

        # Paths for VV and VH
        # VV
        s1_after_vv_path = event_dir / "after_vv.tif"
        s1_before_vv_path = event_dir / "before_vv.tif"
        # VH
        s1_after_vh_path = event_dir / "after_vh.tif"
        s1_before_vh_path = event_dir / "before_vh.tif"
        
        # We process if at least VV exists, VH is optional but highly likely exists
        if not s1_after_vv_path.exists() or not s1_before_vv_path.exists():
            continue
            
        try:
            # --- Load VV ---
            with rasterio.open(s1_after_vv_path) as src:
                vv_after_db = src.read(1)
                nodata = src.nodata
                height_a, width_a = vv_after_db.shape
                transform = src.transform
                bounds = src.bounds
            with rasterio.open(s1_before_vv_path) as src:
                vv_before_db = src.read(1)
                height_b, width_b = vv_before_db.shape
                
            # --- Handle Shape Mismatch (Crop to Intersection) ---
            common_h = min(height_a, height_b)
            common_w = min(width_a, width_b)
            
            if (height_a != common_h) or (width_a != common_w):
                vv_after_db = vv_after_db[:common_h, :common_w]
                
            if (height_b != common_h) or (width_b != common_w):
                vv_before_db = vv_before_db[:common_h, :common_w]
            
            # Update global height/width used for mask rasterization
            height, width = common_h, common_w
                
            # --- Load VH (if exists) ---
            process_vh = False
            if s1_after_vh_path.exists() and s1_before_vh_path.exists():
                with rasterio.open(s1_after_vh_path) as src:
                    vh_after_db = src.read(1)
                    ha, wa = vh_after_db.shape
                with rasterio.open(s1_before_vh_path) as src:
                    vh_before_db = src.read(1)
                    hb, wb = vh_before_db.shape
                
                # Crop VH as well
                vh_after_db = vh_after_db[:common_h, :common_w]
                vh_before_db = vh_before_db[:common_h, :common_w]
                
                process_vh = True
            
            # --- Valid Mask (based on VV) ---
            if nodata is not None:
                valid_mask = (vv_after_db != nodata) & (vv_before_db != nodata) & (~np.isnan(vv_after_db)) & (~np.isnan(vv_before_db))
            else:
                valid_mask = (~np.isnan(vv_after_db)) & (~np.isnan(vv_before_db))
            
            if process_vh:
                 # Extend valid mask to VH if present
                 valid_mask &= (~np.isnan(vh_after_db)) & (~np.isnan(vh_before_db))

            if not np.any(valid_mask):
                continue
            
            # --- Detect data format and conditionally convert to Linear ---
            # Heuristic: dB data has negative mean (typically -20 to 0)
            # Linear data has positive mean close to 0-1
            vv_valid_vals = vv_after_db[valid_mask]
            vv_mean = np.mean(vv_valid_vals)
            
            if vv_mean < -1:
                # Data is in dB format, convert to linear
                vv_after_lin = db_to_linear(vv_after_db)
                vv_before_lin = db_to_linear(vv_before_db)
            else:
                # Data is already in linear format, use as-is
                vv_after_lin = vv_after_db
                vv_before_lin = vv_before_db
                
            vv_diff_lin = vv_after_lin - vv_before_lin
            
            if process_vh:
                vh_valid_vals = vh_after_db[valid_mask]
                vh_mean = np.mean(vh_valid_vals)
                
                if vh_mean < -1:
                    # VH is in dB format
                    vh_after_lin = db_to_linear(vh_after_db)
                    vh_before_lin = db_to_linear(vh_before_db)
                else:
                    # VH is already linear
                    vh_after_lin = vh_after_db
                    vh_before_lin = vh_before_db
                    
                vh_diff_lin = vh_after_lin - vh_before_lin

            # Helper to extract stats
            def extract_stats(mask_bool, class_name):
                final_mask = mask_bool & valid_mask
                if not np.any(final_mask):
                    return None
                
                # Extract values
                # VV
                vals_vv_after = vv_after_lin[final_mask]
                vals_vv_diff = vv_diff_lin[final_mask]
                
                # Filter outliers: Linear > 2.0 is considered outlier
                OUTLIER_THRESHOLD = 2.0
                valid_outlier_mask = vals_vv_after <= OUTLIER_THRESHOLD
                vals_vv_after = vals_vv_after[valid_outlier_mask]
                vals_vv_diff = vals_vv_diff[valid_outlier_mask]
                
                if len(vals_vv_after) == 0:
                    return None
                
                res = {
                    "grid_id": grid_dir.name,
                    "event_dir": dir_name,
                    "month": month,
                    "delay_int": delay_int,
                    "class": class_name,
                    "pixel_count": len(vals_vv_after),
                    # VV Stats
                    "vv_mean_after": np.mean(vals_vv_after),
                    "vv_std_after": np.std(vals_vv_after),
                    "vv_mean_diff": np.mean(vals_vv_diff),
                    "vv_std_diff": np.std(vals_vv_diff),
                }
                
                # VH Stats
                if process_vh:
                    vals_vh_after = vh_after_lin[final_mask]
                    vals_vh_diff = vh_diff_lin[final_mask]
                    
                    # Apply same outlier mask (based on VV)
                    vals_vh_after = vals_vh_after[valid_outlier_mask]
                    vals_vh_diff = vals_vh_diff[valid_outlier_mask]
                    
                    res.update({
                        "vh_mean_after": np.mean(vals_vh_after),
                        "vh_std_after": np.std(vals_vh_after),
                        "vh_mean_diff": np.mean(vals_vh_diff),
                        "vh_std_diff": np.std(vals_vh_diff),
                    })
                
                return res

            # 1. Road / Paddy Masks - Handle each independently
            def rasterize_json(gdf):
                 if gdf is None or gdf.empty: 
                     return np.zeros((height, width), dtype=bool)
                 arr = features.rasterize(
                    shapes=gdf.geometry,
                    out_shape=(height, width),
                    transform=transform,
                    fill=0,
                    default_value=1,
                    dtype=rasterio.uint8
                 )
                 return arr > 0
            
            # Road mask
            if has_road_json and gdf_road is not None:
                m_road = rasterize_json(gdf_road)
                s = extract_stats(m_road, "road")
                if s: grid_results.append(s)
            elif has_road_tif:
                try:
                    with rasterio.open(mask_road_tif) as src_m:
                        m_road = src_m.read(1)
                        # Check shape match
                        if m_road.shape == (height, width):
                            s = extract_stats(m_road > 0, "road")
                            if s: grid_results.append(s)
                except:
                    pass
                    
            # Paddy mask
            if has_paddy_json and gdf_paddy is not None:
                m_paddy = rasterize_json(gdf_paddy)
                s = extract_stats(m_paddy, "paddy")
                if s: grid_results.append(s)
            elif has_paddy_tif:
                try:
                    with rasterio.open(mask_paddy_tif) as src_m:
                        m_paddy = src_m.read(1)
                        # Check shape match
                        if m_paddy.shape == (height, width):
                            s = extract_stats(m_paddy > 0, "paddy")
                            if s: grid_results.append(s)
                except:
                    pass

            # 2. River Mask
            s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            river_shapes = []
            for river_gdf in RIVER_GDFS:
                # Need to be careful about sindex access if multiple threads share object? 
                # Read-only should be fine.
                try:
                    possible_matches_index = list(river_gdf.sindex.intersection(s1_box.bounds))
                    if possible_matches_index:
                        possible_matches = river_gdf.iloc[possible_matches_index]
                        possible_matches = possible_matches[possible_matches.intersects(s1_box)]
                        if not possible_matches.empty:
                            river_shapes.extend(possible_matches.geometry)
                except:
                    pass
            
            if river_shapes:
                river_mask_arr = features.rasterize(
                    shapes=river_shapes,
                    out_shape=(height, width),
                    transform=transform,
                    fill=0,
                    default_value=1,
                    dtype=rasterio.uint8
                )
                s = extract_stats(river_mask_arr > 0, "river")
                if s: grid_results.append(s)

        except Exception as e:
            pass
            
    return grid_results

def main():
    print("Starting Linear Backscatter Stats Extraction (VV & VH) for Aug/Oct...")
    
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(grid_dirs)} grid directories.")

    results = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, initializer=init_worker) as executor:
        futures = {executor.submit(process_grid, d): d for d in grid_dirs}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            grid_data = future.result()
            if grid_data:
                results.extend(grid_data)
                
            if (i + 1) % 50 == 0:
                print(f"Processed {i + 1}/{len(grid_dirs)} grids...")

    df = pd.DataFrame(results)
    print(f"Stats extracted from {len(df)} entries.")
    
    OUTPUT_CSV_LIN = Path(r"D:\sotsuron\rainsar-hub\data\analysis\aug_oct_linear_backscatter_stats.csv")
    
    if len(df) > 0:
        df.to_csv(OUTPUT_CSV_LIN, index=False)
        print(f"Saved linear stats to {OUTPUT_CSV_LIN}")
    else:
        print("No stats extracted.")

if __name__ == "__main__":
    main()
