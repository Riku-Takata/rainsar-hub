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
OUTPUT_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\aug_oct_pixel_counts_with_river.csv")
OUTPUT_DETAILED_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\aug_oct_pixel_counts_with_river_detailed.csv")

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
        print(f"Worker {os.getpid()} loading river masks...")
        for p in RIVER_FILES:
            if p.exists():
                try:
                    gdf = gpd.read_file(p)
                    # Create sindex explicitly if needed, usually done on read/access
                    _ = gdf.sindex
                    RIVER_GDFS.append(gdf)
                except Exception as e:
                    print(f"Error loading {p}: {e}")

def process_grid(grid_dir):
    # Ensure river masks are loaded (if not using initializer or just to be safe)
    global RIVER_GDFS
    if not RIVER_GDFS:
        init_worker()
        
    grid_results = []
    
    # Pre-calculate mask paths
    # (Assuming we use GeoJSON or TIF for Road/Paddy)
    local_mask_dir = MASK_DIR / grid_dir.name
    mask_road_tif = local_mask_dir / f"{grid_dir.name}_road_mask.tif"
    mask_paddy_tif = local_mask_dir / f"{grid_dir.name}_paddy_mask.tif"
    mask_road_json = local_mask_dir / f"{grid_dir.name}_motorway.geojson"
    mask_paddy_json = local_mask_dir / f"{grid_dir.name}_paddy.geojson"
    
    # Check what exists
    has_tif = mask_road_tif.exists() and mask_paddy_tif.exists()
    has_json = mask_road_json.exists() and mask_paddy_json.exists()
    
    if not (has_tif or has_json):
         # Skipping grid if no local mask
         return []

    # Cache Local GeoDataFrames (Lazy load)
    gdf_road = None
    gdf_paddy = None
    
    if not has_tif and has_json:
        try:
            gdf_road = gpd.read_file(mask_road_json)
            gdf_paddy = gpd.read_file(mask_paddy_json)
        except:
            return []

    # Iterate over event directories
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

        s1_path = event_dir / "after_vv.tif"
        if not s1_path.exists():
            continue
            
        try:
            with rasterio.open(s1_path) as src_s1:
                s1_data = src_s1.read(1)
                s1_nodata = src_s1.nodata
                height, width = s1_data.shape
                transform = src_s1.transform
                bounds = src_s1.bounds
                
                # Valid S1 mask
                if s1_nodata is not None:
                    valid_s1 = (s1_data != s1_nodata) & (~np.isnan(s1_data))
                else:
                    valid_s1 = ~np.isnan(s1_data)
                
                # Optimization: Skip if image is empty
                if not np.any(valid_s1):
                    continue

            # --- Count Road/Paddy ---
            n_road = 0
            n_paddy = 0
            
            def count_intersection_tif(mask_path, valid_s1_mask):
                if not mask_path.exists(): return 0
                with rasterio.open(mask_path) as src_m:
                    mask_data = src_m.read(1)
                    mh, mw = mask_data.shape
                    if mh != height or mw != width:
                        min_h = min(height, mh)
                        min_w = min(width, mw)
                        mask_cut = mask_data[:min_h, :min_w]
                        s1_cut = valid_s1_mask[:min_h, :min_w]
                        return np.sum((mask_cut > 0) & s1_cut)
                    else:
                        return np.sum((mask_data > 0) & valid_s1_mask)

            def count_intersection_json(gdf, valid_s1_mask, shape, transform):
                if gdf is None or gdf.empty: return 0
                mask_arr = features.rasterize(
                    shapes=gdf.geometry,
                    out_shape=shape,
                    transform=transform,
                    fill=0,
                    default_value=1,
                    dtype=rasterio.uint8
                )
                return np.sum((mask_arr > 0) & valid_s1_mask)

            if has_tif:
                n_road = count_intersection_tif(mask_road_tif, valid_s1)
                n_paddy = count_intersection_tif(mask_paddy_tif, valid_s1)
            elif has_json:
                n_road = count_intersection_json(gdf_road, valid_s1, (height, width), transform)
                n_paddy = count_intersection_json(gdf_paddy, valid_s1, (height, width), transform)
                
            # --- Count River Soil ---
            n_river = 0
            
            # Create S1 bounding box geometry
            s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            
            river_shapes = []
            
            for river_gdf in RIVER_GDFS:
                # Use spatial index intersection
                # sindices are created on read usually
                possible_matches_index = list(river_gdf.sindex.intersection(s1_box.bounds))
                if possible_matches_index:
                    possible_matches = river_gdf.iloc[possible_matches_index]
                    possible_matches = possible_matches[possible_matches.intersects(s1_box)]
                    
                    if not possible_matches.empty:
                        river_shapes.extend(possible_matches.geometry)
            
            if river_shapes:
                river_mask_arr = features.rasterize(
                    shapes=river_shapes,
                    out_shape=(height, width),
                    transform=transform,
                    fill=0,
                    default_value=1,
                    dtype=rasterio.uint8
                )
                n_river = np.sum((river_mask_arr > 0) & valid_s1)
            
            # Append if ANY of the classes have pixels
            if n_road > 0 or n_paddy > 0 or n_river > 0:
                grid_results.append({
                    "grid_id": grid_dir.name,
                    "event_dir": dir_name,
                    "month": month,
                    "delay_int": delay_int,
                    "road_pixels": n_road,
                    "paddy_pixels": n_paddy,
                    "river_pixels": n_river
                })
                
        except Exception as e:
            # print(e)
            pass
            
    return grid_results

def main():
    print("Starting pixel count with Rivers for months [8, 10]...")
    # init_river_masks() # Removed from main, done in process/initializer
    
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(grid_dirs)} grid directories.")

    results = []
    
    # Use initializer to load river data in each subprocess once
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, initializer=init_worker) as executor:
        futures = {executor.submit(process_grid, d): d for d in grid_dirs}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            grid_data = future.result()
            if grid_data:
                results.extend(grid_data)
                
            if (i + 1) % 50 == 0:
                print(f"Processed {i + 1}/{len(grid_dirs)} grids...")

    df = pd.DataFrame(results)
    print(f"Processed {len(df)} events total.")
    
    if len(df) == 0:
        print("No valid events found.")
        return

    # Aggregate by Month and Delay
    agg_df = df.groupby(['month', 'delay_int'])[['road_pixels', 'paddy_pixels', 'river_pixels']].sum().reset_index()
    agg_df['total_events'] = df.groupby(['month', 'delay_int']).size().values
    
    agg_df = agg_df.sort_values(['month', 'delay_int'])
    
    print("\nAggregate Pixel Counts:")
    print(agg_df)
    
    df.to_csv(OUTPUT_DETAILED_CSV, index=False)
    print(f"Saved detailed counts to {OUTPUT_DETAILED_CSV}")
    
    agg_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved aggregated counts to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
