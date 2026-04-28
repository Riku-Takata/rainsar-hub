import os
import sys
import logging
import pandas as pd
import numpy as np
import rasterio
from rasterio import features
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime

# Setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
EXPANDED_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
INPUT_CSV = BASE_DIR / "data" / "result" / "event_distribution" / "delay_values.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "event_distribution"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PixelAnalysis")

def rasterize_mask_for_tif(geojson_path, src):
    """Rasterize GeoJSON mask to match specific TIFF geometry"""
    try:
        if not geojson_path.exists():
            return None
            
        gdf = gpd.read_file(geojson_path)
        if gdf.empty:
            return None
            
        shape = src.shape
        transform = src.transform
            
        mask = features.rasterize(
            shapes=gdf.geometry,
            out_shape=shape,
            transform=transform,
            fill=0,
            default_value=1,
            dtype=rasterio.uint8,
            all_touched=True
        )
        return mask
        
    except Exception as e:
        # logger.error(f"  Rasterize Error {geojson_path.name}: {e}")
        return None

def find_event_dir(grid_id, event_date_str):
    """Find event directory by matching dates."""
    grid_dir = EXPANDED_DIR / grid_id
    if not grid_dir.exists():
        return None
    
    # Target date string YYYYMMDD
    target_date = event_date_str.replace("-", "")[0:8]
    
    for d in grid_dir.iterdir():
        if d.is_dir() and target_date in d.name:
            return d
    return None

def process_event(row):
    grid_id = row['grid_id']
    delay_h = row['delay_h']
    
    # Event end ts is "YYYY-MM-DD HH:MM:SS"
    # We mainly need date for folder matching
    event_ts_str = str(row['event_end_ts_utc'])
    if " " in event_ts_str:
        date_part = event_ts_str.split(" ")[0]
    else:
        date_part = event_ts_str
        
    event_dir = find_event_dir(grid_id, date_part)
    if not event_dir:
        return None

    # Load Tif
    # Use after_vv.tif as reference
    tif_path = event_dir / "after_vv.tif"
    if not tif_path.exists():
        return None
        
    try:
        with rasterio.open(tif_path) as src:
            # Read just valid mask (not data yet)
            # Or read data to check nan
            data = src.read(1)
            nodata = src.nodata
            
            valid_mask = ~np.isnan(data) & ~np.isinf(data)
            if nodata is not None:
                valid_mask &= (data != nodata)
            
            # Masks
            road_geojson = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
            paddy_geojson = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
            
            road_mask = rasterize_mask_for_tif(road_geojson, src)
            paddy_mask = rasterize_mask_for_tif(paddy_geojson, src)
            
            road_pixels = 0
            paddy_pixels = 0
            
            if road_mask is not None:
                road_pixels = np.sum(valid_mask & (road_mask == 1))
                
            if paddy_mask is not None:
                paddy_pixels = np.sum(valid_mask & (paddy_mask == 1))
            
            return {
                'grid_id': grid_id,
                'event_end_ts_utc': row['event_end_ts_utc'],
                'month': row['month'],
                'delay_h': delay_h,
                'road_pixels': road_pixels,
                'paddy_pixels': paddy_pixels
            }
            
    except Exception as e:
        logger.error(f"Error reading {tif_path}: {e}")
        return None

def main():
    if not INPUT_CSV.exists():
        logger.error(f"Input CSV not found: {INPUT_CSV}")
        return

    logger.info("Loading event list...")
    df_events = pd.read_csv(INPUT_CSV)
    
    logger.info(f"Processing {len(df_events)} events...")
    
    results = []
    
    # Process
    # Can interpret date properly
    # df_events['event_end_ts_utc'] is likely string
    
    count = 0
    for idx, row in df_events.iterrows():
        res = process_event(row)
        if res:
            results.append(res)
        
        count += 1
        if count % 100 == 0:
            logger.info(f"Processed {count}/{len(df_events)}")
            
    if not results:
        logger.warning("No pixel counts extracted.")
        return
        
    df_results = pd.DataFrame(results)
    
    # Save raw pixel counts
    output_csv = OUTPUT_DIR / "event_pixel_counts.csv"
    df_results.to_csv(output_csv, index=False)
    logger.info(f"Saved raw counts to: {output_csv}")
    
    # Aggregate by Month and Delay Bin
    # Bin delay: 0-1, 1-2, ..., 11-12
    df_results['delay_bin'] = df_results['delay_h'].apply(lambda x: int(x)) # 0.5 -> 0, 1.5 -> 1
    
    # Group by Month, Delay Bin
    agg_df = df_results.groupby(['month', 'delay_bin'])[['road_pixels', 'paddy_pixels']].sum().reset_index()
    agg_df['event_count'] = df_results.groupby(['month', 'delay_bin']).size().values
    
    agg_csv = OUTPUT_DIR / "monthly_delay_pixel_counts.csv"
    agg_df.to_csv(agg_csv, index=False)
    logger.info(f"Saved aggregated counts to: {agg_csv}")
    
    # Visualizations
    
    # Heatmap: Road Pixels
    pivot_road = agg_df.pivot(index='delay_bin', columns='month', values='road_pixels').fillna(0)
    # Ensure all moths/delays present for nicer plot
    for m in range(1, 13):
        if m not in pivot_road.columns:
            pivot_road[m] = 0
    pivot_road = pivot_road[sorted(pivot_road.columns)]
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_road, cmap="Oranges", annot=True, fmt='g', linewidths=.5)
    plt.title("Total Road Pixels by Month and Delay (h)")
    plt.xlabel("Month")
    plt.ylabel("Delay (h) [Bin Start]")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "heatmap_road_pixels.png")
    plt.close()

    # Heatmap: Paddy Pixels
    pivot_paddy = agg_df.pivot(index='delay_bin', columns='month', values='paddy_pixels').fillna(0)
    for m in range(1, 13):
        if m not in pivot_paddy.columns:
            pivot_paddy[m] = 0
    pivot_paddy = pivot_paddy[sorted(pivot_paddy.columns)]

    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot_paddy, cmap="Greens", annot=True, fmt='g', linewidths=.5)
    plt.title("Total Paddy Pixels by Month and Delay (h)")
    plt.xlabel("Month")
    plt.ylabel("Delay (h) [Bin Start]")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "heatmap_paddy_pixels.png")
    plt.close()
    
    # Heatmap: Avg Pixels per Event?
    # Maybe useful to see data density/quality
    
    logger.info("Visualizations saved.")

if __name__ == "__main__":
    main()
