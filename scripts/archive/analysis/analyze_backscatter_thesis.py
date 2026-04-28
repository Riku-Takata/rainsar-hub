"""
Analyze Backscatter Intensity (Thesis)
- Inputs: 
  - Preprocessed TIFFs (data/expanded/samples/{grid_id}/{event_id}/*.tif)
  - Mask GeoJSONs (data/expanded/masks/{grid_id}/*.geojson)
- Outputs:
  - Statistics CSV (data/result/sigma/{grid_id}/{event_id}/stats.csv)
  - Histograms (data/result/sigma/{grid_id}/{event_id}/histogram_{pol}.png)
  - Pixel Data (data/result/sigma/{grid_id}/{event_id}/pixels_{type}_{pol}.csv)
- Logic:
  1. Load GeoJSON masks and rasterize them to match TIFF geometry.
  2. Extract pixels for Road and Paddy.
  3. Calculate statistics (Mean, Median, Std, etc.).
  4. Generate plots and save data.
"""

import os
import sys
import logging
import json
import csv
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
from rasterio.windows import Window
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
EXPANDED_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
RESULT_DIR = BASE_DIR / "data" / "result" / "sigma"
GRID_LIST = BASE_DIR / "data" / "thesis_grids_with_masks.txt"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("AnalyzeBackscatter")

def rasterize_mask_for_tif(geojson_path, src):
    """Rasterize GeoJSON mask to match specific TIFF geometry"""
    try:
        # Load GeoJSON (cached if possible, but reading is fast enough)
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
        logger.error(f"  Rasterize Error {geojson_path.name}: {e}")
        return None

def sigma_clip(data, sigma=3.0):
    """
    Apply sigma clipping to remove outliers.
    """
    if len(data) == 0: return data
    
    mean = np.mean(data)
    std = np.std(data)
    
    # Clip
    lower = mean - sigma * std
    upper = mean + sigma * std
    
    # Filter
    return data[(data >= lower) & (data <= upper)]

def calculate_stats(data):
    """Calculate statistics for valid data"""
    if len(data) == 0:
        return None
        
    return {
        'count': len(data),
        'mean': float(np.mean(data)),
        'median': float(np.median(data)),
        'std': float(np.std(data)),
        'min': float(np.min(data)),
        'max': float(np.max(data)),
        'q25': float(np.percentile(data, 25)),
        'q75': float(np.percentile(data, 75))
    }

def process_event(grid_id, event_name, event_dir, mask_dir):
    """Process a single event"""
    results = []
    output_dir = RESULT_DIR / grid_id / event_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define file pairs
    pairs = [
        ('after', 'vv'), ('after', 'vh'),
        ('before', 'vv'), ('before', 'vh')
    ]
    
    # Store pixel data for plotting
    pixel_data = {
        'vv': {'road': [], 'paddy': [], 'road_label': 'Road', 'paddy_label': 'Paddy'},
        'vh': {'road': [], 'paddy': [], 'road_label': 'Road', 'paddy_label': 'Paddy'}
    }
    
    # Load GeoJSON paths
    road_geojson = mask_dir / f"{grid_id}_motorway.geojson"
    paddy_geojson = mask_dir / f"{grid_id}_paddy.geojson"
    
    for timing, pol in pairs:
        tif_name = f"{timing}_{pol}.tif"
        tif_path = event_dir / tif_name
        
        if not tif_path.exists():
            continue
            
        try:
            with rasterio.open(tif_path) as src:
                data = src.read(1)
                nodata = src.nodata
                
                # Rasterize masks specificaly for this TIFF
                masks = {}
                masks['road'] = rasterize_mask_for_tif(road_geojson, src)
                masks['paddy'] = rasterize_mask_for_tif(paddy_geojson, src)
                
            # Flatten and filter nodata/inf/nan
            valid_mask = ~np.isnan(data) & ~np.isinf(data)
            if nodata is not None:
                valid_mask &= (data != nodata)
                
            # Apply masks
            for mask_type, mask_array in masks.items():
                if mask_array is None: continue
                
                # Apply Type Mask
                target_pixels = data[valid_mask & (mask_array == 1)]
                
                if len(target_pixels) > 0:
                    # Apply 3-Sigma Clipping
                    filtered_pixels = sigma_clip(target_pixels, sigma=3.0)
                    
                    if len(filtered_pixels) > 0:
                        stats = calculate_stats(filtered_pixels)
                        if stats:
                            row = {
                                'grid_id': grid_id,
                                'event_name': event_name,
                                'timing': timing,
                                'pol': pol,
                                'type': mask_type,
                                **stats
                            }
                            results.append(row)
                            
                            # Store for plot (subsampling if too large)
                            if len(filtered_pixels) > 10000:
                                sample = np.random.choice(filtered_pixels, 10000, replace=False)
                            else:
                                sample = filtered_pixels
                            
                            pixel_data[pol][mask_type].append(sample)

        except Exception as e:
            logger.error(f"  Error processing {tif_path.name}: {e}")
            
    # Plot Histograms
    for pol in ['vv', 'vh']:
        plt.figure(figsize=(10, 6))
        has_data = False
        
        # Plot Road (After/Before)
        road_data = pixel_data[pol]['road']
        if len(road_data) == 2: # Has before and after
            sns.histplot(road_data[0], color='red', alpha=0.3, label='Road (After)', kde=True, stat="density", element="step")
            sns.histplot(road_data[1], color='darkred', alpha=0.3, label='Road (Before)', kde=True, stat="density", element="step")
            has_data = True
            
        # Plot Paddy (After/Before)
        paddy_data = pixel_data[pol]['paddy']
        if len(paddy_data) == 2:
            sns.histplot(paddy_data[0], color='blue', alpha=0.3, label='Paddy (After)', kde=True, stat="density", element="step")
            sns.histplot(paddy_data[1], color='navy', alpha=0.3, label='Paddy (Before)', kde=True, stat="density", element="step")
            has_data = True
            
        if has_data:
            plt.title(f"Backscatter Distribution ({pol.upper()}) - {grid_id} {event_name}")
            plt.xlabel("Backscatter Intensity (dB)")
            plt.ylabel("Density")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.xlim(-30, 10) # Typical range
            
            plot_path = output_dir / f"histogram_{pol}.png"
            plt.savefig(plot_path)
            
        plt.close()
        
    # Save Stats
    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_dir / "stats.csv", index=False)
        return df
    return None

def process_grid(grid_id):
    """Process all events in a grid"""
    grid_dir = EXPANDED_DIR / grid_id
    mask_dir = MASKS_DIR / grid_id
    
    if not grid_dir.exists():
        return None
        
    events = [d for d in grid_dir.iterdir() if d.is_dir() and d.name.startswith("delay_")]
    if not events:
        return None
        
    logger.info(f"Processing {grid_id} ({len(events)} events)")
    
    # 2. Process Events
    grid_results = []
    for event_dir in events:
        df = process_event(grid_id, event_dir.name, event_dir, mask_dir)
        if df is not None:
            grid_results.append(df)
            
    if grid_results:
        return pd.concat(grid_results)
    return None

def perform_global_analysis(all_results):
    """
    Calculate and plot global statistics across all grids/events.
    """
    if not all_results: return

    global_df = pd.concat(all_results)
    output_dir = RESULT_DIR / "global_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save Combined CSV
    global_df.to_csv(output_dir / "all_events_stats.csv", index=False)
    
    # Calculate Global Statistics (aggregating the event-level stats)
    # Note: This is "Mean of Means", but sufficient for global trend check
    global_stats = []
    
    for mtype in ['road', 'paddy']:
        for pol in ['vv', 'vh']:
            subset = global_df[(global_df['type'] == mtype) & (global_df['pol'] == pol)]
            if len(subset) == 0: continue
            
            stats = {
                'type': mtype,
                'pol': pol,
                'total_events': len(subset),
                'mean_of_means': subset['mean'].mean(),
                'std_of_means': subset['mean'].std(),
                'mean_of_stds': subset['std'].mean(),
                'min_value_recorded': subset['min'].min(),
                'max_value_recorded': subset['max'].max()
            }
            global_stats.append(stats)
            
            # Plot Distribution of Means
            plt.figure(figsize=(10, 6))
            sns.histplot(subset['mean'], kde=True)
            plt.title(f"Distribution of Mean Backscatter ({mtype.upper()} {pol.upper()})")
            plt.xlabel("Mean Backscatter (dB)")
            plt.savefig(output_dir / f"dist_means_{mtype}_{pol}.png")
            plt.close()

    pd.DataFrame(global_stats).to_csv(output_dir / "global_summary_stats.csv", index=False)
    logger.info(f"Global analysis saved to {output_dir}")

def main():
    # Load Grids
    with open(GRID_LIST) as f:
        grids = [l.strip() for l in f if l.strip()]
        
    logger.info(f"Start Analysis: {len(grids)} grids")
    
    # Process
    all_results = []
    
    # Sequential for now
    for i, grid_id in enumerate(grids, 1):
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(grids)}")
            
        try:
            df = process_grid(grid_id)
            if df is not None:
                all_results.append(df)
        except Exception as e:
            logger.error(f"Failed to process {grid_id}: {e}")
            
    # Combine and Save Nationwide Stats and Perform Global Analysis
    if all_results:
        final_df = pd.concat(all_results)
        final_df.to_csv(RESULT_DIR / "nationwide_stats_thesis.csv", index=False)
        logger.info(f"Saved nationwide stats to {RESULT_DIR / 'nationwide_stats_thesis.csv'}")
        
        # New: Global Analysis
        perform_global_analysis(all_results)
    else:
        logger.warning("No results generated.")

if __name__ == "__main__":
    main()

