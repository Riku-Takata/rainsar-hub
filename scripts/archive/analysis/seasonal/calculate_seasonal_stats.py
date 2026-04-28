
import os
import sys
import logging
import pandas as pd
import numpy as np
import rasterio
from rasterio import features
import geopandas as gpd
from pathlib import Path
import math
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup paths
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
EXPANDED_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
GRID_JSON = BASE_DIR / "data" / "thesis_grids_final_filtered.json"

# Load Env
env_path = BASE_DIR / "backend" / ".env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Parameters
TARGET_MONTHS = [4, 8, 9, 10]
RAIN_THRESHOLD_MM_H = 1.0 
SIGMA_LIMIT = 3.0

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SeasonalStats")

def db_to_linear(db_array):
    return np.power(10.0, db_array / 10.0)

def linear_to_db(linear_val):
    if linear_val <= 0: return np.nan
    return 10.0 * math.log10(linear_val)

def rasterize_mask_for_tif(geojson_path, src):
    try:
        if not geojson_path.exists():
            return None, 0
        gdf = gpd.read_file(geojson_path)
        if gdf.empty:
            return None, 0
        
        # Fude Count (Total features in mask)
        fude_count = len(gdf)
            
        mask = features.rasterize(
            shapes=gdf.geometry,
            out_shape=src.shape,
            transform=src.transform,
            fill=0,
            default_value=1,
            dtype=rasterio.uint8,
            all_touched=True
        )
        return mask, fude_count
    except Exception:
        return None, 0

def sigma_clip(data, sigma=3.0):
    if len(data) == 0: return data
    mean = np.mean(data)
    std = np.std(data)
    lower = mean - sigma * std
    upper = mean + sigma * std
    return data[(data >= lower) & (data <= upper)]

def calc_stats(pixels_db):
    if len(pixels_db) == 0:
        return {}
    
    mean_db_arith = np.mean(pixels_db)
    pixels_lin = db_to_linear(pixels_db)
    mean_lin = np.mean(pixels_lin)
    mean_db_lin = linear_to_db(mean_lin)
    
    return {
        'mean_db': mean_db_lin,
        'mean_db_arith': mean_db_arith,
        'std_db': np.std(pixels_db),
        'count': len(pixels_db)
    }

def find_event_dir(grid_id, event_date_str):
    grid_dir = EXPANDED_DIR / grid_id
    if not grid_dir.exists(): return None
    target_date = event_date_str.replace("-", "")[0:8]
    for d in grid_dir.iterdir():
        if d.is_dir() and target_date in d.name:
            return d
    return None

def process_event(row):
    grid_id = row['grid_id']
    month = row['month']
    delay_h = row['delay_h']
    max_gauge = row['max_gauge_mm_h']
    
    # Pre-check Rain
    is_rain_valid = (max_gauge >= RAIN_THRESHOLD_MM_H)
    
    # Find Directory
    event_ts_str = str(row['event_end_ts_utc'])
    date_part = event_ts_str.split(" ")[0] if " " in event_ts_str else event_ts_str
    event_dir = find_event_dir(grid_id, date_part)
    if not event_dir:
        return None

    ref_tif = event_dir / "after_vv.tif"
    if not ref_tif.exists(): return None
    
    # Load Masks
    road_mask = None
    paddy_mask = None
    paddy_fude_count = 0
    
    try:
        with rasterio.open(ref_tif) as src:
            road_geojson = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
            paddy_geojson = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
            road_mask, _ = rasterize_mask_for_tif(road_geojson, src)
            paddy_mask, paddy_fude_count = rasterize_mask_for_tif(paddy_geojson, src)
    except:
        return None

    results = []
    
    for pol in ['vv', 'vh']:
        after_p = event_dir / f"after_{pol}.tif"
        before_p = event_dir / f"before_{pol}.tif"
        
        if not (after_p.exists() and before_p.exists()): continue
        
        try:
            with rasterio.open(after_p) as src_a, rasterio.open(before_p) as src_b:
                data_a = src_a.read(1)
                data_b = src_b.read(1)
                
                # Align
                if data_a.shape != data_b.shape:
                    mh, mw = min(data_a.shape[0], data_b.shape[0]), min(data_a.shape[1], data_b.shape[1])
                    data_a = data_a[:mh, :mw]
                    data_b = data_b[:mh, :mw]
                    if road_mask is not None: road_mask = road_mask[:mh, :mw]
                    if paddy_mask is not None: paddy_mask = paddy_mask[:mh, :mw]
                
                # Base Valid
                invalid = np.isnan(data_a) | np.isinf(data_a) | np.isnan(data_b) | np.isinf(data_b)
                if src_a.nodata: invalid |= (data_a == src_a.nodata)
                if src_b.nodata: invalid |= (data_b == src_b.nodata)
                valid = ~invalid
                
                diff = np.zeros_like(data_a)
                diff[valid] = data_a[valid] - data_b[valid]
                
                for m_name, m_arr, f_count in [('road', road_mask, 0), ('paddy', paddy_mask, paddy_fude_count)]:
                    if m_arr is None: continue
                    
                    target_mask = valid & (m_arr == 1)
                    pixels_a = data_a[target_mask]
                    pixels_diff = diff[target_mask]
                    
                    raw_count = len(pixels_a)
                    # Raw Stats
                    raw_stats_a = calc_stats(pixels_a)
                    raw_stats_diff = calc_stats(pixels_diff)
                    
                    # Clean Stats
                    clean_count = 0
                    clean_stats_a = {}
                    clean_stats_diff = {}
                    
                    if is_rain_valid and raw_count > 0:
                        # Clean Sigma (Clip on Sigma)
                        clean_pixels_a = sigma_clip(pixels_a, SIGMA_LIMIT)
                        # Clean Diff (Clip on Diff)
                        clean_pixels_diff = sigma_clip(pixels_diff, SIGMA_LIMIT)
                        
                        clean_count = len(clean_pixels_a) # Using Sigma count as reference
                        clean_stats_a = calc_stats(clean_pixels_a)
                        clean_stats_diff = calc_stats(clean_pixels_diff)
                    
                    rec = {
                        'grid_id': grid_id,
                        'event_end_ts_utc': row['event_end_ts_utc'],
                        'month': month,
                        'delay_h': delay_h,
                        'max_gauge_mm_h': max_gauge,
                        'pol': pol,
                        'type': m_name,
                        'fude_count': f_count,
                        
                        'raw_pixel_count': raw_count,
                        'raw_sigma_mean': raw_stats_a.get('mean_db', np.nan),
                        'raw_diff_mean':  raw_stats_diff.get('mean_db_arith', np.nan),
                        
                        'clean_pixel_count': clean_count,
                        'clean_sigma_mean': clean_stats_a.get('mean_db', np.nan),
                        'clean_diff_mean':  clean_stats_diff.get('mean_db_arith', np.nan)
                    }
                    results.append(rec)
                    
        except Exception as e:
            logger.error(f"Err {grid_id} {pol}: {e}")
            
    return results

def main():
    # 1. Get Grids
    with open(GRID_JSON, 'r') as f:
        grids = json.load(f)
    grid_ids = [g['grid_id'] for g in grids]
    logger.info(f"Target Grids: {len(grid_ids)}")
    
    # 2. Query DB
    engine = create_engine(DATABASE_URL)
    query = text("""
        SELECT grid_id, delay_h, after_start_ts_utc, event_end_ts_utc, max_gauge_mm_h
        FROM s1_pairs
        WHERE source = 'cdse_nationwide_search'
        AND delay_h >= 0 AND delay_h <= 12.0
        AND max_gauge_mm_h >= 10.0
        AND grid_id IN :grids
        AND before_scene_id IS NOT NULL
        ORDER BY event_end_ts_utc
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": grid_ids})
        
    df['event_end_ts_utc'] = pd.to_datetime(df['event_end_ts_utc'])
    df['month'] = df['event_end_ts_utc'].dt.month
    
    # Filter Months
    df_target = df[df['month'].isin(TARGET_MONTHS)].copy()
    logger.info(f"Events in Target Months {TARGET_MONTHS}: {len(df_target)} / {len(df)}")
    
    all_rows = []
    count = 0
    total = len(df_target)
    
    for idx, row in df_target.iterrows():
        res = process_event(row)
        if res: all_rows.extend(res)
        count += 1
        if count % 50 == 0:
            logger.info(f"Progress: {count}/{total}")
            
    if all_rows:
        res_df = pd.DataFrame(all_rows)
        out_csv = OUTPUT_DIR / "seasonal_stats_all.csv"
        res_df.to_csv(out_csv, index=False)
        logger.info(f"Saved {out_csv}")
        
        # Aggregation
        res_df['delay_bin'] = res_df['delay_h'].apply(lambda x: int(x))
        agg = res_df.groupby(['month', 'delay_bin', 'type', 'pol']).agg({
            'raw_pixel_count': 'sum',
            'clean_pixel_count': 'sum',
            'fude_count': 'sum',
            'grid_id': 'count',
            'raw_sigma_mean': 'mean', # Mean of Means (Approx)
            'clean_sigma_mean': 'mean',
            'clean_diff_mean': 'mean'
        }).rename(columns={'grid_id': 'event_count'})
        
        agg.to_csv(OUTPUT_DIR / "seasonal_aggregated_counts.csv")
        logger.info("Saved Aggregated Counts.")
    else:
        logger.warning("No results.")

if __name__ == "__main__":
    main()
