"""
Week 1 Analysis: Permeability Classification (Method A)
- Input: data/expanded/analysis/evolution/evolution_data_combined.csv
- Logic: Multi-temporal Permeability Estimation (Relaxed Sampling)
- Output: data/expanded/analysis/week1/results_method_a.csv
"""

import sys
import numpy as np
import pandas as pd
import rasterio
from scipy.stats import linregress
from pathlib import Path
from sklearn.metrics import roc_auc_score, confusion_matrix, classification_report
import logging
import time

# Config
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
DATA_DIR = BASE_DIR / "data"
SAMPLES_DIR = DATA_DIR / "expanded" / "samples"
EVO_CSV = DATA_DIR / "expanded" / "analysis" / "evolution" / "evolution_data_combined.csv"
OUT_DIR = DATA_DIR / "expanded" / "analysis" / "week1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Week1_Analysis")

def load_pixel_data(grid_id, event_name):
    """Load valid pixel data for one event"""
    event_dir = SAMPLES_DIR / grid_id / event_name
    
    paths = {
        'after': event_dir / "after.tif",
        'before': event_dir / "before.tif",
        'road': event_dir / "mask_road.tif",
        'paddy': event_dir / "mask_paddy.tif"
    }
    
    # Check existence
    if not all(p.exists() for p in paths.values()):
        return None
        
    try:
        data = {}
        for k, p in paths.items():
            with rasterio.open(p) as src:
                data[k] = src.read(1)
        
        # Crop to min shape
        min_h = min(d.shape[0] for d in data.values())
        min_w = min(d.shape[1] for d in data.values())
        
        for k in data:
            data[k] = data[k][:min_h, :min_w]
            
        # Calc Diff
        data['diff'] = data['after'] - data['before']
        
        # Valid Mask
        # Range: -50 to 20 dB typically
        valid_mask = (
            (~np.isnan(data['diff'])) &
            (data['after'] > -50) & (data['after'] < 30) &
            (data['before'] > -50) & (data['before'] < 30)
        )
        data['valid'] = valid_mask
        
        return data
        
    except Exception as e:
        logger.debug(f"Error loading {grid_id}/{event_name}: {e}")
        return None

def process_grid_method_a(grid_id, events_df, n_pixels=5000):
    """Process a single grid using Method A logic"""
    
    # Load all event data
    event_data = []
    for _, row in events_df.iterrows():
        d = load_pixel_data(grid_id, row['event'])
        if d:
            event_data.append({
                'data': d,
                'delay': row['delay_h'],
                'rain_max': row['rain_max_mm_h'],
                'rain_total': row['rain_total_est_mm']
            })
            
    if len(event_data) < 3:
        return None
        
    # Standard Shape
    h, w = event_data[0]['data']['diff'].shape
    
    # Sampling: Candidates
    # Sample from Road and Paddy specifically to ensure balance? 
    # Or random sample?
    # Original logic: Random sample from valid pixels.
    # To improve ROAD samples (which are sparse), we should specifically target road pixels if possible.
    
    # Identify Road & Paddy candidate indices
    # We use a "Union" mask concept or just pick from any event?
    # Let's use the first available mask as reference, or merge masks?
    # Masks should be identical if from same OSM/Fude? Actually yes.
    
    # Simplified: Use mask from first valid event
    road_mask_ref = event_data[0]['data']['road']
    paddy_mask_ref = event_data[0]['data']['paddy']
    
    # Valid pixels reference (must be valid in at least 3 events)
    # This is expensive to compute for full image.
    # Strategy: Pick Random coordinates, check criteria.
    
    collected_results = []
    
    # Try to get 50% Road, 50% Paddy
    target_road = n_pixels // 2
    target_paddy = n_pixels // 2
    
    # Get all road indices
    road_indices = np.argwhere((road_mask_ref == 1))
    paddy_indices = np.argwhere((paddy_mask_ref == 1))
    
    # Helper to process a batch of pixels
    def process_pixels(indices, label):
        results = []
        if len(indices) == 0: return results
        
        # Shuffle
        np.random.shuffle(indices)
        
        processed_count = 0
        for idx in indices:
            r, c = idx
            
            # Check derived validity (valid in >=3 events)
            valid_events = [ev for ev in event_data if ev['data']['valid'][r, c]]
            
            if len(valid_events) >= 3:
                # Time Series
                delays = np.array([ev['delay'] for ev in valid_events])
                diffs = np.array([ev['data']['diff'][r, c] for ev in valid_events])
                
                # Regression
                slope, intercept, r_value, p_value, std_err = linregress(delays, diffs)
                
                # Features
                res = {
                    'grid_id': grid_id,
                    'row': r, 'col': c,
                    'ground_truth': label, # 1=Road, 0=Paddy
                    'decay_rate': -slope,
                    'decay_r2': r_value**2,
                    'n_events': len(valid_events),
                    'start_diff': diffs[0], # Approx
                    'mean_diff': np.mean(diffs)
                }
                results.append(res)
                processed_count += 1
                
                if processed_count >= (target_road if label==1 else target_paddy):
                    break
        return results

    # Run logic
    logger.debug(f"  Grid {grid_id}: Found {len(road_indices)} road, {len(paddy_indices)} paddy candidates")
    
    road_results = process_pixels(road_indices, 1)
    paddy_results = process_pixels(paddy_indices, 0)
    
    return road_results + paddy_results

def main():
    if not EVO_CSV.exists():
        logger.error("Evolution CSV missing. Run P03_compile_metadata first.")
        return

    df_meta = pd.read_csv(EVO_CSV)
    logger.info(f"Loaded {len(df_meta)} events from metadata.")
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_results = []
    
    # Group by Grid
    for grid_id, group in df_meta.groupby('grid_id'):
        logger.info(f"Processing {grid_id} ({len(group)} events)...")
        
        res = process_grid_method_a(grid_id, group)
        if res:
            all_results.extend(res)
            logger.info(f"  => {len(res)} samples ({sum(r['ground_truth']==1 for r in res)} Road)")
            
    if not all_results:
        logger.error("No results generated.")
        return
        
    df_res = pd.DataFrame(all_results)
    out_csv = OUT_DIR / "results_method_a.csv"
    df_res.to_csv(out_csv, index=False)
    
    logger.info("="*30)
    logger.info(f"Saved results to {out_csv}")
    logger.info(f"Total Samples: {len(df_res)}")
    logger.info(f"Road Samples: {len(df_res[df_res['ground_truth']==1])}")
    logger.info("="*30)
    
    # Evaluation
    valid_df = df_res.dropna(subset=['decay_rate', 'decay_r2'])
    logger.info(f"Valid Regression Samples: {len(valid_df)}")
    
    # Simple Threshold Evaluation (e.g. Decay > 0.05 is Road?)
    # or just metrics
    
    # Week 1 focus: Sample count increase validation
    
if __name__ == "__main__":
    main()
