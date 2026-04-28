import pandas as pd
import numpy as np
import rasterio
from rasterio import features
import geopandas as gpd
from pathlib import Path
from shapely.geometry import box
import concurrent.futures
import re

# --- Configuration ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATA_DIR = BASE_DIR / "data" / "analysis"
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
MASK_DATA_DIR = BASE_DIR / "mask-data"

# River Mask Sources
RIVER_PATHS = [
    MASK_DATA_DIR / "river_polygon_2320001.geojson",
    MASK_DATA_DIR / "river_polygon_2320061.geojson"
]

INPUT_CSV = DATA_DIR / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_SUMMARY_CSV = DATA_DIR / "october_delay_summary_stats_recalculated.csv"
OUTPUT_DETAILED_CSV = DATA_DIR / "october_event_pixel_stats_recalculated.csv"

# Global Cache for River Shapes (populated in worker)
RIVER_SHAPES = None

def init_worker():
    global RIVER_SHAPES
    if RIVER_SHAPES is None:
        gdfs = []
        for p in RIVER_PATHS:
            if p.exists():
                try:
                    gdfs.append(gpd.read_file(p))
                except: pass
        if gdfs:
            combined = pd.concat(gdfs, ignore_index=True)
            # Spatial index is helpful for clipping but we'll use polygons directly for rasterization
            RIVER_SHAPES = combined
        else:
            RIVER_SHAPES = gpd.GeoDataFrame()

def count_river_pixels(grid_id, event_dir):
    """Calculate river pixels for a specific event using the unified river dataset."""
    ref_tif = SAMPLES_DIR / grid_id / event_dir / "after_vv.tif"
    if not ref_tif.exists():
        return 0
        
    try:
        with rasterio.open(ref_tif) as src:
            nodata = src.nodata
            data = src.read(1)
            valid = (data != nodata) if nodata is not None else ~np.isnan(data)
            h, w = data.shape
            transform = src.transform
            bounds = src.bounds
            crs = src.crs

        # Intersect with global polygons
        s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        
        # Filter shapes to those in box (using spatial index if available, or just clip)
        if RIVER_SHAPES.empty: return 0
        
        # Ensure CRS match
        proj_gdf = RIVER_SHAPES.to_crs(crs)
        intersecting = proj_gdf[proj_gdf.intersects(s1_box)]
        
        if intersecting.empty:
            return 0
            
        # Rasterize
        mask_arr = features.rasterize(
            shapes=intersecting.geometry,
            out_shape=(h, w),
            transform=transform,
            fill=0,
            default_value=1,
            dtype=rasterio.uint8
        )
        
        return int(np.sum((mask_arr > 0) & valid))
    except Exception as e:
        # print(f"Error {grid_id}/{event_dir}: {e}")
        return 0

def process_batch(rows):
    res = []
    for _, row in rows.iterrows():
        n_river = count_river_pixels(row['grid_id'], row['event_dir'])
        res.append({
            'delay_int': row['delay_int'],
            'grid_id': row['grid_id'],
            'event_dir': row['event_dir'],
            'road_pixels': row['road_pixels'],
            'paddy_pixels': row['paddy_pixels'],
            'river_pixels': n_river
        })
    return res

def main():
    print("Recalculating October stats with Unified River Logic...")
    df = pd.read_csv(INPUT_CSV)
    oct_df = df[df['month'] == 10].copy()
    print(f"Total events to process: {len(oct_df)}")

    # Split into chunks for parallel processing
    n_workers = 4
    chunks = np.array_split(oct_df, n_workers)
    
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_workers, initializer=init_worker) as executor:
        futures = [executor.submit(process_batch, chunk) for chunk in chunks]
        for future in concurrent.futures.as_completed(futures):
            results.extend(future.result())

    final_detailed = pd.DataFrame(results)
    
    # Sort for output
    final_detailed = final_detailed.sort_values(['delay_int', 'grid_id', 'event_dir'])
    
    # Save Detailed
    final_detailed.to_csv(OUTPUT_DETAILED_CSV, index=False)
    print(f"Saved recalculated detailed stats to: {OUTPUT_DETAILED_CSV}")
    
    # --- Generate Summary ---
    summary = final_detailed.groupby('delay_int').agg({
        'event_dir': 'count',
        'road_pixels': 'sum',
        'paddy_pixels': 'sum',
        'river_pixels': 'sum'
    }).rename(columns={
        'event_dir': 'n_events',
        'road_pixels': 'total_road_pixels',
        'paddy_pixels': 'total_paddy_pixels',
        'river_pixels': 'total_river_pixels'
    })
    
    # Add month column for consistency
    summary.insert(0, 'month', 10)
    
    # Save Summary
    summary.to_csv(OUTPUT_SUMMARY_CSV)
    print(f"Saved recalculated summary to: {OUTPUT_SUMMARY_CSV}")
    
    print("\n--- Summary for October (Recalculated) ---")
    print(summary)

if __name__ == "__main__":
    main()
