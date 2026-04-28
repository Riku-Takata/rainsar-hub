import os
import re
import pandas as pd
import numpy as np
import rasterio
from pathlib import Path


# Paths
DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
OUTPUT_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\monthly_delay_pixel_counts.csv")

def main():
    print("Starting pixel count analysis...")
    
    results = []
    
    # Iterate over grid directories
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(grid_dirs)} grid directories.")

import concurrent.futures

def process_grid(grid_dir):
    grid_results = []
    
    # Pre-calculate mask paths
    mask_dir = DATA_DIR.parent / "masks" / grid_dir.name
    mask_road_tif = mask_dir / f"{grid_dir.name}_road_mask.tif"
    mask_paddy_tif = mask_dir / f"{grid_dir.name}_paddy_mask.tif"
    mask_road_json = mask_dir / f"{grid_dir.name}_motorway.geojson"
    mask_paddy_json = mask_dir / f"{grid_dir.name}_paddy.geojson"
    
    # Check what exists
    has_tif = mask_road_tif.exists() and mask_paddy_tif.exists()
    has_json = mask_road_json.exists() and mask_paddy_json.exists()
    
    if not (has_tif or has_json):
         return []

    # Cache GeoDataFrames if needed (optimization: load once per grid)
    gdf_road = None
    gdf_paddy = None
    
    if not has_tif and has_json:
        try:
            import geopandas as gpd
            from rasterio import features
            gdf_road = gpd.read_file(mask_road_json)
            gdf_paddy = gpd.read_file(mask_paddy_json)
        except Exception as e:
            # print(f"Error reading GeoJSON for {grid_dir.name}: {e}")
            return []

    # Iterate over event directories
    for event_dir in grid_dir.iterdir():
        if not event_dir.is_dir():
            continue
        
        dir_name = event_dir.name
        
        # Parse directory name: delay_{delay}h_{YYYYMMDD}
        match = re.match(r"delay_([\d\.]+)h_(\d{8})", dir_name)
        if not match:
            continue
            
        delay_float = float(match.group(1))
        date_str = match.group(2)
        
        # Round delay to nearest integer (floor)
        delay_int = int(delay_float)
        
        # Filter Delay 0-11
        if not (0 <= delay_int <= 11):
            continue
        
        try:
            dt = pd.to_datetime(date_str, format="%Y%m%d")
            month = dt.month
        except:
            continue
            
        if month not in [4, 8, 9, 10]:
            # Temporarily keeping this comment but removing the continue to process all
            # continue
            pass

        # Load S1 image
        s1_path = event_dir / "after_vv.tif"
        if not s1_path.exists():
            continue
            
        n_road = 0
        n_paddy = 0
        
        try:
            with rasterio.open(s1_path) as src_s1:
                s1_data = src_s1.read(1)
                s1_meta = src_s1.meta
                s1_nodata = src_s1.nodata
                
                # Create Valid Mask
                if s1_nodata is not None:
                    valid_s1 = (s1_data != s1_nodata) & (~np.isnan(s1_data))
                else:
                    valid_s1 = ~np.isnan(s1_data)
                
                height, width = s1_data.shape
                transform = src_s1.transform

            # Function to count intersection
            def count_intersection_tif(mask_path, valid_s1_mask):
                if not mask_path.exists():
                    return 0
                with rasterio.open(mask_path) as src_m:
                    mask_data = src_m.read(1)
                    # Resize/Crop
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
                if gdf is None or gdf.empty:
                    return 0
                
                # Rasterize
                # Using 1 for mask value, 0 for fill
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
            
        except Exception as e:
            # print(f"Error processing {dir_name}: {e}")
            pass
        
        if n_road > 0 or n_paddy > 0:
            grid_results.append({
                "grid_id": grid_dir.name,
                "event_dir": dir_name,
                "month": month,
                "delay_int": delay_int,
                "delay_float": delay_float,
                "road_pixels": n_road,
                "paddy_pixels": n_paddy
            })
            
    return grid_results

def main():
    print("Starting pixel count analysis for ALL months...")
    
    # Iterate over grid directories
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(grid_dirs)} grid directories.")

    results = []
    
    # Parallel Processing
    # Adjust max_workers as needed. Disk I/O might be the bottleneck.
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        # Map returns in order
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
    agg_df = df.groupby(['month', 'delay_int'])[['road_pixels', 'paddy_pixels']].sum().reset_index()
    agg_df['total_events'] = df.groupby(['month', 'delay_int']).size().values
    
    # Sort
    agg_df = agg_df.sort_values(['month', 'delay_int'])
    
    print("\nAggregate Pixel Counts:")
    print(agg_df)
    
    agg_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved aggregate counts to {OUTPUT_CSV}")
    
    # Save detailed
    detailed_csv = OUTPUT_CSV.with_name("monthly_delay_pixel_counts_detailed.csv")
    df.to_csv(detailed_csv, index=False)
    print(f"Saved detailed counts to {detailed_csv}")

if __name__ == "__main__":
    main()
