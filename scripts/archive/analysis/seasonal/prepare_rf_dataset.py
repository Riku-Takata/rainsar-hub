"""
RF Dataset Preparation Script (Corrected Version)

Key Changes:
1. Extract ALL Road and Paddy pixels independently (no per-event downsampling)
2. Downsample AFTER aggregating by Month/Delay bin
3. Ensure data matches `seasonal_stats` counts
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
import rasterio
from rasterio import features
import geopandas as gpd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json

# Setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
EXPANDED_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "rf_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
GRID_JSON = BASE_DIR / "data" / "thesis_grids_final_filtered.json"

# Rain Filter Config
RAIN_THRESHOLD_MM_H = 1.0 
SIGMA_LIMIT = 3.0
TARGET_MONTHS = [4, 8, 9, 10]

# DB Access
env_path = BASE_DIR / "backend" / ".env"
load_dotenv(env_path)
DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RF_DataPrep")

def rasterize_mask_for_tif(geojson_path, src):
    try:
        if not geojson_path.exists():
            return None
        gdf = gpd.read_file(geojson_path)
        if gdf.empty:
            return None
        mask = features.rasterize(
            shapes=gdf.geometry,
            out_shape=src.shape,
            transform=src.transform,
            fill=0,
            default_value=1,
            dtype=rasterio.uint8,
            all_touched=True
        )
        return mask
    except:
        return None

def sigma_clip_mask(data, sigma=3.0):
    """Return boolean mask of valid pixels within sigma range"""
    if len(data) == 0: 
        return np.array([], dtype=bool)
    mean = np.mean(data)
    std = np.std(data)
    if std == 0:
        return np.ones(len(data), dtype=bool)
    lower = mean - sigma * std
    upper = mean + sigma * std
    return (data >= lower) & (data <= upper)

def find_event_dir(grid_id, event_date_str):
    grid_dir = EXPANDED_DIR / grid_id
    if not grid_dir.exists(): 
        return None
    target_date = event_date_str.replace("-", "")[0:8]
    for d in grid_dir.iterdir():
        if d.is_dir() and target_date in d.name:
            return d
    return None

def extract_pixels_for_class(diff_vv, diff_vh, class_mask, valid_mask, label, sigma_limit):
    """
    Extract pixels for a single class (Road or Paddy).
    Returns DataFrame with diff_vv, diff_vh, label columns, or None if no valid pixels.
    """
    target_bool = valid_mask & (class_mask == 1)
    target_diffs_vv = diff_vv[target_bool]
    
    if len(target_diffs_vv) == 0:
        return None
    
    # Sigma clip
    keep_mask = sigma_clip_mask(target_diffs_vv, sigma_limit)
    
    vv_vals = target_diffs_vv[keep_mask]
    
    if diff_vh is not None:
        vh_vals = diff_vh[target_bool][keep_mask]
    else:
        vh_vals = np.full_like(vv_vals, np.nan)
    
    n_pixels = len(vv_vals)
    if n_pixels == 0:
        return None
    
    return pd.DataFrame({
        'diff_vv': vv_vals,
        'diff_vh': vh_vals,
        'label': label
    })

def process_event(row):
    """
    Extract ALL valid pixels for Road and Paddy from a single event.
    NO downsampling at this stage.
    """
    grid_id = row['grid_id']
    max_gauge = row['max_gauge_mm_h']
    
    # Rain Filter
    if max_gauge < RAIN_THRESHOLD_MM_H:
        return None

    total_rain = row['total_gauge_mm']
    duration = row['duration_hours']
    
    event_ts_str = str(row['event_end_ts_utc'])
    date_part = event_ts_str.split(" ")[0] if " " in event_ts_str else event_ts_str
    event_dir = find_event_dir(grid_id, date_part)
    if not event_dir: 
        return None

    ref_tif = event_dir / "after_vv.tif"
    if not ref_tif.exists(): 
        return None
    
    try:
        with rasterio.open(ref_tif) as src:
            road_geojson = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
            paddy_geojson = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
            road_mask = rasterize_mask_for_tif(road_geojson, src)
            paddy_mask = rasterize_mask_for_tif(paddy_geojson, src)
            
            # Allow events with at least one class
            if road_mask is None and paddy_mask is None:
                return None
    except:
        return None

    # Load VV (required)
    after_vv_path = event_dir / "after_vv.tif"
    before_vv_path = event_dir / "before_vv.tif"
    if not after_vv_path.exists() or not before_vv_path.exists():
        return None
    
    try:
        with rasterio.open(after_vv_path) as src_a, rasterio.open(before_vv_path) as src_b:
            vv_a = src_a.read(1)
            vv_b = src_b.read(1)
            nodata = src_a.nodata
            
            h, w = min(vv_a.shape[0], vv_b.shape[0]), min(vv_a.shape[1], vv_b.shape[1])
            vv_a = vv_a[:h, :w]
            vv_b = vv_b[:h, :w]
            
            # Load VH if available
            vh_a, vh_b = None, None
            after_vh_path = event_dir / "after_vh.tif"
            before_vh_path = event_dir / "before_vh.tif"
            if after_vh_path.exists() and before_vh_path.exists():
                with rasterio.open(after_vh_path) as vh_src_a, rasterio.open(before_vh_path) as vh_src_b:
                    vha = vh_src_a.read(1)
                    vhb = vh_src_b.read(1)
                    vh_a = vha[:h, :w]
                    vh_b = vhb[:h, :w]

            # Crop masks
            if road_mask is not None:
                road_mask = road_mask[:h, :w]
            if paddy_mask is not None:
                paddy_mask = paddy_mask[:h, :w]
            
            # Valid Mask
            valid = ~np.isnan(vv_a) & ~np.isinf(vv_a) & ~np.isnan(vv_b) & ~np.isinf(vv_b)
            if vh_a is not None:
                valid &= ~np.isnan(vh_a) & ~np.isinf(vh_a) & ~np.isnan(vh_b) & ~np.isinf(vh_b)
            if nodata is not None:
                valid &= (vv_a != nodata)
            
            # Calculate Diff
            diff_vv = np.full_like(vv_a, np.nan)
            diff_vv[valid] = vv_a[valid] - vv_b[valid]
            
            diff_vh = None
            if vh_a is not None:
                diff_vh = np.full_like(vh_a, np.nan)
                diff_vh[valid] = vh_a[valid] - vh_b[valid]

            results = []
            
            # Extract Road (label=0)
            if road_mask is not None:
                road_df = extract_pixels_for_class(diff_vv, diff_vh, road_mask, valid, label=0, sigma_limit=SIGMA_LIMIT)
                if road_df is not None:
                    results.append(road_df)
            
            # Extract Paddy (label=1) - INDEPENDENTLY
            if paddy_mask is not None:
                paddy_df = extract_pixels_for_class(diff_vv, diff_vh, paddy_mask, valid, label=1, sigma_limit=SIGMA_LIMIT)
                if paddy_df is not None:
                    results.append(paddy_df)
                    
    except Exception as e:
        logger.error(f"Error processing {grid_id}: {e}")
        return None

    if not results:
        return None
        
    event_df = pd.concat(results, ignore_index=True)
    
    # Attach metadata
    event_df['total_rain'] = total_rain
    event_df['duration'] = duration
    event_df['month'] = row['month']
    event_df['delay_bin'] = int(row['delay_h'])
    event_df['grid_id'] = grid_id
    event_df['event_id'] = f"{grid_id}_{date_part}"
    
    return event_df

def downsample_by_bin(df):
    """
    For each (month, delay_bin) group, downsample the majority class to match minority.
    This ensures balanced classes per bin for training.
    """
    balanced_chunks = []
    
    for (month, delay), group in df.groupby(['month', 'delay_bin']):
        road_df = group[group['label'] == 0]
        paddy_df = group[group['label'] == 1]
        
        n_road = len(road_df)
        n_paddy = len(paddy_df)
        
        if n_road == 0 or n_paddy == 0:
            logger.warning(f"M{month} D{delay}: Skipping (Road={n_road}, Paddy={n_paddy})")
            continue
        
        min_count = min(n_road, n_paddy)
        
        # Downsample
        road_sampled = road_df.sample(n=min_count, random_state=42)
        paddy_sampled = paddy_df.sample(n=min_count, random_state=42)
        
        balanced_chunks.append(road_sampled)
        balanced_chunks.append(paddy_sampled)
        
        logger.info(f"M{month} D{delay}: Balanced to {min_count} per class")
    
    if balanced_chunks:
        return pd.concat(balanced_chunks, ignore_index=True)
    return pd.DataFrame()

def main():
    # 1. Get Grids
    with open(GRID_JSON, 'r') as f:
        grids = json.load(f)
    grid_ids = [g['grid_id'] for g in grids]
    
    # 2. Load Selected Events (Master List from earlier analysis)
    SELECTED_CSV = BASE_DIR / "data" / "result" / "event_distribution" / "delay_values.csv"
    if not SELECTED_CSV.exists():
        logger.error(f"Selected list not found: {SELECTED_CSV}")
        return

    selected_df = pd.read_csv(SELECTED_CSV)
    selected_df['event_end_ts_utc'] = pd.to_datetime(selected_df['event_end_ts_utc'])
    selected_keys = set(zip(selected_df['grid_id'], selected_df['event_end_ts_utc']))
    logger.info(f"Master List: {len(selected_keys)} events")

    # 3. Query DB for Rain metadata
    engine = create_engine(DATABASE_URL)
    query = text("""
        SELECT 
            s.grid_id, 
            s.delay_h, 
            s.event_end_ts_utc, 
            s.max_gauge_mm_h,
            g.sum_gauge_mm_h as total_gauge_mm,
            g.hit_hours as duration_hours
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.delay_h >= 0 AND s.delay_h <= 12.0
        AND s.grid_id IN :grids
        AND s.before_scene_id IS NOT NULL
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": grid_ids})
        
    df['event_end_ts_utc'] = pd.to_datetime(df['event_end_ts_utc'])
    
    # Filter by Master List
    labels = list(map(tuple, df[['grid_id', 'event_end_ts_utc']].values))
    mask = [x in selected_keys for x in labels]
    df = df[mask].copy()
    
    df['month'] = df['event_end_ts_utc'].dt.month
    df_target = df[df['month'].isin(TARGET_MONTHS)].copy()
    logger.info(f"Target Events: {len(df_target)}")
    
    # 4. Extract ALL pixels (parallel)
    from joblib import Parallel, delayed
    
    logger.info("Extracting pixels (parallel)...")
    results = Parallel(n_jobs=-1, backend="loky")(
        delayed(process_event)(row) for idx, row in df_target.iterrows()
    )
    
    all_chunks = [r for r in results if r is not None]
    
    if not all_chunks:
        logger.warning("No data extracted!")
        return
        
    full_df = pd.concat(all_chunks, ignore_index=True)
    full_df.dropna(subset=['diff_vv'], inplace=True)
    
    logger.info(f"Total Extracted: {len(full_df)} pixels")
    logger.info(f"  Road: {len(full_df[full_df['label']==0])}")
    logger.info(f"  Paddy: {len(full_df[full_df['label']==1])}")
    
    # Save raw (for debugging)
    raw_path = OUTPUT_DIR / "rf_dataset_raw.csv"
    full_df.to_csv(raw_path, index=False)
    logger.info(f"Saved raw dataset: {raw_path}")
    
    # 5. Downsample by Month/Delay bin
    logger.info("Downsampling by Month/Delay...")
    balanced_df = downsample_by_bin(full_df)
    
    if balanced_df.empty:
        logger.warning("No balanced data generated!")
        return
        
    out_path = OUTPUT_DIR / "rf_dataset_balanced.csv"
    balanced_df.to_csv(out_path, index=False)
    logger.info(f"Saved balanced dataset: {out_path} (Rows: {len(balanced_df)})")

if __name__ == "__main__":
    main()
