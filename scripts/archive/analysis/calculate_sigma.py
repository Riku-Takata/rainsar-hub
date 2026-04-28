"""
後方散乱強度（Sigma）算出スクリプト

各イベントのGeoTIFFファイルから道路・田んぼマスクを適用し、
後方散乱強度の統計値を計算・可視化する。

Usage:
    python calculate_sigma.py --polarization vv
    python calculate_sigma.py --polarization vh
"""

import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import rasterio
from rasterio.mask import mask
from shapely.geometry import shape
import logging

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
SAMPLES_DIR = DATA_DIR / "expanded" / "samples"
MASKS_DIR = DATA_DIR / "expanded" / "masks"
TARGET_GRIDS_FILE = DATA_DIR / "analysis_target_grids.txt"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def load_mask_geometries(mask_file):
    """Load GeoJSON mask and extract geometries"""
    if not mask_file.exists():
        return []
    
    with open(mask_file, encoding='utf-8') as f:
        data = json.load(f)
    
    geometries = []
    for feature in data.get('features', []):
        geom = feature.get('geometry')
        if geom:
            geometries.append(geom)
    
    return geometries

def extract_masked_values(tif_file, geometries):
    """Extract pixel values using mask geometries"""
    if not geometries:
        return np.array([])
    
    with rasterio.open(tif_file) as src:
        # Mask the raster
        masked_data, _ = mask(src, geometries, crop=False, filled=False)
        
        # Extract valid values
        masked_values = masked_data[0]  # First band
        
        # Convert to regular numpy array if masked
        if hasattr(masked_values, 'compressed'):
            valid_values = masked_values.compressed()
        else:
            valid_values = masked_values.flatten()
        
        # Filter nodata values
        if src.nodata is not None:
            valid_values = valid_values[valid_values != src.nodata]
        
        # Filter NaN and Inf
        valid_values = valid_values[~np.isnan(valid_values) & ~np.isinf(valid_values)]
        
        # Filter zero values (often nodata/background)
        valid_values = valid_values[valid_values != 0]
        
        return valid_values

def remove_outliers_iqr(data, factor=1.5):
    """Remove outliers using IQR method"""
    if len(data) == 0:
        return data
    
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    iqr = q3 - q1
    
    lower_bound = q1 - factor * iqr
    upper_bound = q3 + factor * iqr
    
    filtered = data[(data >= lower_bound) & (data <= upper_bound)]
    return filtered

def calculate_statistics(values, label=""):
    """Calculate statistical metrics"""
    if len(values) == 0:
        return {
            'label': label,
            'count': 0,
            'mean': np.nan,
            'median': np.nan,
            'std': np.nan,
            'min': np.nan,
            'max': np.nan,
            'q25': np.nan,
            'q75': np.nan
        }
    
    return {
        'label': label,
        'count': len(values),
        'mean': np.mean(values),
        'median': np.median(values),
        'std': np.std(values),
        'min': np.min(values),
        'max': np.max(values),
        'q25': np.percentile(values, 25),
        'q75': np.percentile(values, 75)
    }

def create_histogram(data_dict, output_file, title="Backscatter Intensity Distribution"):
    """Create histogram comparison"""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(title, fontsize=16)
    
    labels = ['After Road', 'After Paddy', 'Before Road', 'Before Paddy']
    keys = ['after_road', 'after_paddy', 'before_road', 'before_paddy']
    
    for ax, label, key in zip(axes.flat, labels, keys):
        values = data_dict.get(key, np.array([]))
        
        if len(values) > 0:
            ax.hist(values, bins=50, alpha=0.7, edgecolor='black')
            ax.axvline(np.mean(values), color='r', linestyle='--', label=f'Mean: {np.mean(values):.2f}')
            ax.axvline(np.median(values), color='g', linestyle='--', label=f'Median: {np.median(values):.2f}')
            ax.set_xlabel('Sigma0 (dB)')
            ax.set_ylabel('Frequency')
            ax.set_title(f'{label} (n={len(values)})')
            ax.legend()
            ax.grid(True, alpha=0.3)
        else:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f'{label} (n=0)')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

def process_event(grid_id, event_dir, polarization, output_dir):
    """Process single event"""
    event_id = event_dir.name
    
    # Input files
    after_file = event_dir / f"after_{polarization}.tif"
    before_file = event_dir / f"before_{polarization}.tif"
    
    if not after_file.exists() or not before_file.exists():
        logger.warning(f"Missing files for {grid_id}/{event_id}")
        return None
    
    # Load masks
    grid_masks = MASKS_DIR / grid_id
    road_mask = grid_masks / f"{grid_id}_motorway.geojson"
    paddy_mask = grid_masks / f"{grid_id}_paddy.geojson"
    
    road_geoms = load_mask_geometries(road_mask)
    paddy_geoms = load_mask_geometries(paddy_mask)
    
    if not road_geoms or not paddy_geoms:
        logger.warning(f"Missing masks for {grid_id}")
        return None
    
    # Extract values
    after_road = extract_masked_values(after_file, road_geoms)
    after_paddy = extract_masked_values(after_file, paddy_geoms)
    before_road = extract_masked_values(before_file, road_geoms)
    before_paddy = extract_masked_values(before_file, paddy_geoms)
    
    # Remove outliers
    after_road_clean = remove_outliers_iqr(after_road)
    after_paddy_clean = remove_outliers_iqr(after_paddy)
    before_road_clean = remove_outliers_iqr(before_road)
    before_paddy_clean = remove_outliers_iqr(before_paddy)
    
    # Calculate statistics
    stats_raw = {
        'after_road': calculate_statistics(after_road, 'After Road (Raw)'),
        'after_paddy': calculate_statistics(after_paddy, 'After Paddy (Raw)'),
        'before_road': calculate_statistics(before_road, 'Before Road (Raw)'),
        'before_paddy': calculate_statistics(before_paddy, 'Before Paddy (Raw)')
    }
    
    stats_clean = {
        'after_road': calculate_statistics(after_road_clean, 'After Road (Clean)'),
        'after_paddy': calculate_statistics(after_paddy_clean, 'After Paddy (Clean)'),
        'before_road': calculate_statistics(before_road_clean, 'Before Road (Clean)'),
        'before_paddy': calculate_statistics(before_paddy_clean, 'Before Paddy (Clean)')
    }
    
    # Create output directory
    event_output = output_dir / grid_id / event_id
    event_output.mkdir(parents=True, exist_ok=True)
    
    # Save raw pixel values
    pd.DataFrame({'after_road': after_road}).to_csv(event_output / 'after_road_sigma.csv', index=False)
    pd.DataFrame({'after_paddy': after_paddy}).to_csv(event_output / 'after_paddy_sigma.csv', index=False)
    pd.DataFrame({'before_road': before_road}).to_csv(event_output / 'before_road_sigma.csv', index=False)
    pd.DataFrame({'before_paddy': before_paddy}).to_csv(event_output / 'before_paddy_sigma.csv', index=False)
    
    # Save statistics
    stats_df = pd.DataFrame([
        stats_raw['after_road'],
        stats_raw['after_paddy'],
        stats_raw['before_road'],
        stats_raw['before_paddy'],
        stats_clean['after_road'],
        stats_clean['after_paddy'],
        stats_clean['before_road'],
        stats_clean['before_paddy']
    ])
    stats_df.to_csv(event_output / 'statistics.csv', index=False)
    
    # Create histogram
    data_for_hist = {
        'after_road': after_road_clean,
        'after_paddy': after_paddy_clean,
        'before_road': before_road_clean,
        'before_paddy': before_paddy_clean
    }
    create_histogram(data_for_hist, event_output / 'histograms.png', 
                    title=f'{grid_id} - {event_id} ({polarization.upper()})')
    
    logger.info(f"  ✓ {event_id}: Road={len(after_road)}, Paddy={len(after_paddy)}")
    
    return stats_clean

def main():
    parser = argparse.ArgumentParser(description='Calculate backscatter sigma values')
    parser.add_argument('--polarization', type=str, required=True, choices=['vv', 'vh'],
                       help='Polarization to process (vv or vh)')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: process only first 3 grids')
    args = parser.parse_args()
    
    pol = args.polarization.lower()
    
    logger.info("="*60)
    logger.info(f"Backscatter Sigma Calculation ({pol.upper()})")
    logger.info("="*60)
    
    # Output directory
    output_base = DATA_DIR / "result" / pol / "sigma"
    output_base.mkdir(parents=True, exist_ok=True)
    
    # Load target grids
    with open(TARGET_GRIDS_FILE) as f:
        target_grids = [line.strip() for line in f if line.strip()]
    
    if args.test:
        target_grids = target_grids[:3]
        logger.info(f"TEST MODE: Processing only {len(target_grids)} grids\n")
    
    logger.info(f"Target grids: {len(target_grids)}\n")
    
    # Process each grid
    all_stats = []
    processed_count = 0
    
    for i, grid_id in enumerate(target_grids, 1):
        grid_dir = SAMPLES_DIR / grid_id
        
        if not grid_dir.exists():
            logger.warning(f"[{i}/{len(target_grids)}] {grid_id}: Directory not found")
            continue
        
        events = [d for d in grid_dir.iterdir() if d.is_dir()]
        
        logger.info(f"[{i}/{len(target_grids)}] {grid_id}: {len(events)} events")
        
        for event_dir in events:
            stats = process_event(grid_id, event_dir, pol, output_base)
            if stats:
                for key, stat in stats.items():
                    stat['grid_id'] = grid_id
                    stat['event_id'] = event_dir.name
                    stat['category'] = key
                    all_stats.append(stat)
                processed_count += 1
    
    # Save overall statistics
    if all_stats:
        overall_df = pd.DataFrame(all_stats)
        overall_file = output_base / 'overall_statistics.csv'
        overall_df.to_csv(overall_file, index=False)
        logger.info(f"\n✓ Overall statistics saved: {overall_file}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing Complete")
    logger.info(f"{'='*60}")
    logger.info(f"Grids processed: {len(target_grids)}")
    logger.info(f"Events processed: {processed_count}")
    logger.info(f"Output directory: {output_base}")

if __name__ == "__main__":
    main()
