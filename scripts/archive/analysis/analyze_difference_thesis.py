"""
Analyze Backscatter Difference (Thesis)
- Inputs: 
  - Preprocessed TIFFs (data/expanded/samples/{grid_id}/{event_id}/*.tif)
  - Mask GeoJSONs (data/expanded/masks/{grid_id}/*.geojson)
  - Outlier Thresholds (data/result/outliers/delay_outlier_thresholds.csv)
- Outputs:
  - Difference Statistics CSV (data/result/diff/{grid_id}/{event_id}/diff_stats.csv)
  - Difference Histograms (data/result/diff/{grid_id}/{event_id}/diff_histogram_{pol}.png)
- Logic:
  1. Load Delay-based Outlier Thresholds.
  2. Load After and Before TIFFs.
  3. Identify Delay from event name.
  4. Rasterize masks to match geometry.
  5. Apply Mask AND Outlier Thresholds (Normal range) to pixels.
  6. Calculate pixel-wise difference (After - Before).
  7. Calculate statistics and generate plots.
"""

import os
import sys
import logging
import json
import csv
import re
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
EXPANDED_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
RESULT_DIR = BASE_DIR / "data" / "result" / "diff"
GRID_LIST = BASE_DIR / "data" / "thesis_grids_with_masks.txt"
THRESHOLD_CSV = BASE_DIR / "data" / "result" / "outliers" / "delay_outlier_thresholds.csv"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("AnalyzeDifference")

GLOBAL_THRESHOLDS = {}

def load_thresholds():
    """Load outlier thresholds from CSV"""
    global GLOBAL_THRESHOLDS
    if not THRESHOLD_CSV.exists():
        logger.warning(f"Threshold CSV not found: {THRESHOLD_CSV}")
        return

    try:
        df = pd.read_csv(THRESHOLD_CSV)
        # Key: (type, pol, timing, delay) -> (lower, upper)
        for _, row in df.iterrows():
            key = (row['type'], row['pol'], row['timing'], int(row['delay']))
            GLOBAL_THRESHOLDS[key] = (row['lower_threshold'], row['upper_threshold'])
        logger.info(f"Loaded thresholds for {len(GLOBAL_THRESHOLDS)} conditions")
    except Exception as e:
        logger.error(f"Failed to load thresholds: {e}")

def rasterize_mask_for_tif(geojson_path, src):
    """Rasterize GeoJSON mask to match specific TIFF geometry"""
    try:
        gdf = gpd.read_file(geojson_path)
        if gdf.empty: return None
            
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
    if len(data) == 0: return None
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

def extract_delay(name):
    m = re.search(r'delay_(\d+)h_', name)
    return int(m.group(1)) if m else -1

def process_event(grid_id, event_name, event_dir, mask_dir):
    """Process a single event for difference analysis"""
    
    # Load thresholds if not already loaded
    if not GLOBAL_THRESHOLDS:
        load_thresholds()
        
    delay = extract_delay(event_name)

    results = []
    output_dir = RESULT_DIR / grid_id / event_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load GeoJSON paths
    road_geojson = mask_dir / f"{grid_id}_motorway.geojson"
    paddy_geojson = mask_dir / f"{grid_id}_paddy.geojson"
    
    # Store difference data for plotting
    diff_data = {
        'vv': {'road': [], 'paddy': []},
        'vh': {'road': [], 'paddy': []}
    }
    
    for pol in ['vv', 'vh']:
        after_path = event_dir / f"after_{pol}.tif"
        before_path = event_dir / f"before_{pol}.tif"
        
        if not (after_path.exists() and before_path.exists()):
            continue
            
        try:
            # Read After
            with rasterio.open(after_path) as src_a:
                data_a = src_a.read(1)
                nodata_a = src_a.nodata
                
                # Masks based on After geometry
                masks = {}
                masks['road'] = rasterize_mask_for_tif(road_geojson, src_a)
                masks['paddy'] = rasterize_mask_for_tif(paddy_geojson, src_a)
                
            # Read Before (Assumed same geometry as After due to coregistration/terrain correction)
            with rasterio.open(before_path) as src_b:
                data_b = src_b.read(1)
                nodata_b = src_b.nodata
                
                # Check shape match and crop if necessary
                if data_a.shape != data_b.shape:
                    logger.warning(f"  Shape mismatch {pol}: {data_a.shape} vs {data_b.shape}. Cropping to overlap.")
                    
                    min_h = min(data_a.shape[0], data_b.shape[0])
                    min_w = min(data_a.shape[1], data_b.shape[1])
                    
                    data_a = data_a[:min_h, :min_w]
                    data_b = data_b[:min_h, :min_w]
                    
                    masks['road'] = masks['road'][:min_h, :min_w] if masks['road'] is not None else None
                    masks['paddy'] = masks['paddy'][:min_h, :min_w] if masks['paddy'] is not None else None

            # Base Valid Mask (NoData etc)
            base_valid = ~np.isnan(data_a) & ~np.isinf(data_a) & ~np.isnan(data_b) & ~np.isinf(data_b)
            if nodata_a is not None: base_valid &= (data_a != nodata_a)
            if nodata_b is not None: base_valid &= (data_b != nodata_b)
            
            # Diff Image
            diff_img = np.zeros_like(data_a)
            diff_img[base_valid] = data_a[base_valid] - data_b[base_valid]
            
            # Apply Masks and Outlier Filtering
            for mask_type, mask_array in masks.items():
                if mask_array is None: continue
                
                # Check thresholds for this specific condition
                key_a = (mask_type, pol, 'after', delay)
                key_b = (mask_type, pol, 'before', delay)
                
                # Start with pixels in the mask and generally valid
                target_mask = base_valid & (mask_array == 1)
                
                # Apply Threshold Filtering (IQR based)
                if key_a in GLOBAL_THRESHOLDS:
                    low, high = GLOBAL_THRESHOLDS[key_a]
                    target_mask &= (data_a >= low) & (data_a <= high)
                    
                if key_b in GLOBAL_THRESHOLDS:
                    low, high = GLOBAL_THRESHOLDS[key_b]
                    target_mask &= (data_b >= low) & (data_b <= high)
                
                # Extract Difference
                target_diffs = diff_img[target_mask]
                
                if len(target_diffs) > 0:
                    # Apply 3-Sigma Clipping (Double cleanup)
                    filtered_diffs = sigma_clip(target_diffs, sigma=3.0)
                    
                    if len(filtered_diffs) > 0:
                        stats = calculate_stats(filtered_diffs)
                        if stats:
                            row = {
                                'grid_id': grid_id,
                                'event_name': event_name,
                                'delay': delay,
                                'pol': pol,
                                'type': mask_type,
                                **stats
                            }
                            results.append(row)
                            
                            # Store for plot (subsampling)
                            if len(filtered_diffs) > 10000:
                                sample = np.random.choice(filtered_diffs, 10000, replace=False)
                            else:
                                sample = filtered_diffs
                            diff_data[pol][mask_type] = sample

        except Exception as e:
            logger.error(f"  Error processing {event_name} {pol}: {e}")

    # Plot Difference Histograms
    for pol in ['vv', 'vh']:
        road_diff = diff_data[pol]['road']
        paddy_diff = diff_data[pol]['paddy']
        
        if len(road_diff) > 0 or len(paddy_diff) > 0:
            plt.figure(figsize=(10, 6))
            
            if len(road_diff) > 0:
                sns.histplot(road_diff, color='red', alpha=0.4, label='Road', kde=True, stat="density", element="step")
                
            if len(paddy_diff) > 0:
                sns.histplot(paddy_diff, color='blue', alpha=0.4, label='Paddy', kde=True, stat="density", element="step")
                
            plt.title(f"Backscatter Difference (After - Before) [{pol.upper()}] - {grid_id}")
            plt.xlabel("Difference (dB)")
            plt.ylabel("Density")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.axvline(x=0, color='black', linestyle='--', alpha=0.5)
            plt.xlim(-15, 15)
            
            plot_path = output_dir / f"diff_histogram_{pol}.png"
            plt.savefig(plot_path)
            plt.close()

    # Save Stats
    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_dir / "diff_stats.csv", index=False)
        return df
    return None

def process_grid(grid_id):
    """Process all events in a grid"""
    grid_dir = EXPANDED_DIR / grid_id
    mask_dir = MASKS_DIR / grid_id
    
    if not grid_dir.exists(): return None
        
    events = [d for d in grid_dir.iterdir() if d.is_dir() and d.name.startswith("delay_")]
    if not events: return None
        
    logger.info(f"Processing {grid_id} ({len(events)} events)")
    
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
    Calculate and plot global difference statistics across all grids/events.
    """
    if not all_results: return
    
    global_df = pd.concat(all_results)
    output_dir = RESULT_DIR / "global_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save Combined CSV
    global_df.to_csv(output_dir / "all_diff_stats.csv", index=False)
    
    global_stats = []
    
    for mtype in ['road', 'paddy']:
        for pol in ['vv', 'vh']:
            subset = global_df[(global_df['type'] == mtype) & (global_df['pol'] == pol)]
            if len(subset) == 0: continue
            
            # Mean Difference Statistics
            stats = {
                'type': mtype,
                'pol': pol,
                'total_events': len(subset),
                'mean_of_means': subset['mean'].mean(),
                'std_of_means': subset['mean'].std(),
                'mean_of_medians': subset['median'].mean(),
                'min_diff_recorded': subset['min'].min(),
                'max_diff_recorded': subset['max'].max()
            }
            global_stats.append(stats)
            
            # Plot Distribution of Mean Differences
            plt.figure(figsize=(10, 6))
            sns.histplot(subset['mean'], kde=True)
            plt.axvline(x=0, color='red', linestyle='--')
            plt.title(f"Distribution of Mean Difference ({mtype.upper()} {pol.upper()})")
            plt.xlabel("Mean Difference (After - Before) [dB]")
            plt.savefig(output_dir / f"dist_mean_diff_{mtype}_{pol}.png")
            plt.close()
            
    pd.DataFrame(global_stats).to_csv(output_dir / "global_diff_summary.csv", index=False)
    logger.info(f"Global analysis saved to {output_dir}")

def main():
    if not GRID_LIST.exists():
        logger.warning(f"Grid list not found: {GRID_LIST}")
        return

    with open(GRID_LIST) as f:
        grids = [l.strip() for l in f if l.strip()]
        
    logger.info(f"Start Difference Analysis: {len(grids)} grids")
    
    all_results = []
    
    for i, grid_id in enumerate(grids, 1):
        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(grids)}")
        try:
            df = process_grid(grid_id)
            if df is not None:
                all_results.append(df)
        except Exception as e:
            logger.error(f"Failed to process {grid_id}: {e}")
            
    if all_results:
        final_df = pd.concat(all_results)
        final_df.to_csv(RESULT_DIR / "nationwide_diff_stats_thesis.csv", index=False)
        logger.info(f"Saved stats to {RESULT_DIR / 'nationwide_diff_stats_thesis.csv'}")
        
        # Global Analysis
        perform_global_analysis(all_results)

if __name__ == "__main__":
    main()
