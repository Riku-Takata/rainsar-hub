import os
import sys
import re
import logging
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("negative_pixel_analysis")

# Settings
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
S1_SAFE_DIR = BASE_DIR / "s1_safe"
RESULT_DIR = BASE_DIR / "rainsar-hub" / "result" / "distributions" / "negative_analysis"

TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

SUFFIX_HIGHWAY = "_highway_mask.tif"
SUFFIX_PADDY = "_paddy_mask.tif"

def parse_summary_txt(grid_id):
    summary_path = S1_SAFE_DIR / grid_id / "summary_delay.txt"
    if not summary_path.exists(): return []

    with open(summary_path, 'r', encoding='utf-8') as f:
        content = f.read()

    events = []
    blocks = content.split('-' * 60)
    for block in blocks:
        if "Event Start" not in block: continue
        data = {}
        start_m = re.search(r"Event Start \(UTC\) : ([\d\- :]+)", block)
        after_m = re.search(r"After Scene\s*:? (S1\w+)", block)
        before_m = re.search(r"Before Scene\s*:? (S1\w+)", block)
        delay_m = re.search(r"Delay \(Hours\)\s+: ([\d\.]+)", block)

        if start_m and after_m and before_m and delay_m:
            data['date'] = start_m.group(1).split(' ')[0]
            data['after_scene'] = after_m.group(1)
            data['before_scene'] = before_m.group(1)
            data['delay'] = float(delay_m.group(1))
            events.append(data)
    return events

def read_linear_values(tif_path):
    if not tif_path.exists(): return None
    with rasterio.open(tif_path) as src:
        data = src.read(1)
        valid_mask = ~np.isnan(data)
        if not np.any(valid_mask): return None
        valid_data_db = data[valid_mask]
        valid_data_linear = 10 ** (valid_data_db / 10.0)
        return valid_data_linear

def main():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_stats = []
    
    for grid_id in TARGET_GRIDS:
        events = parse_summary_txt(grid_id)
        
        # Filter for < 1h events
        short_events = [e for e in events if e['delay'] < 1.0]
        
        if not short_events:
            continue
            
        logger.info(f"Processing Grid: {grid_id} ({len(short_events)} short-term events)")
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        for evt in short_events:
            stem_after = f"{evt['after_scene']}_proc"
            stem_before = f"{evt['before_scene']}_proc"
            
            # Process both types
            for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
                path_after = grid_dir / f"{stem_after}{suffix}"
                path_before = grid_dir / f"{stem_before}{suffix}"
                
                arr_a = read_linear_values(path_after)
                arr_b = read_linear_values(path_before)
                
                if arr_a is not None and arr_b is not None:
                    min_len = min(len(arr_a), len(arr_b))
                    arr_a = arr_a[:min_len]
                    arr_b = arr_b[:min_len]
                    diff = arr_a - arr_b
                    
                    # --- FILTER: Negative Diff Only ---
                    neg_mask = diff < 0
                    neg_diff = diff[neg_mask]
                    
                    if len(neg_diff) > 0:
                        stats = {
                            'Grid': grid_id,
                            'Delay': evt['delay'],
                            'Type': type_name,
                            'Total_Pixels': len(diff),
                            'Negative_Pixels': len(neg_diff),
                            'Negative_Ratio': len(neg_diff) / len(diff),
                            'Mean_Diff': np.mean(neg_diff),
                            'Median_Diff': np.median(neg_diff),
                            'Std_Diff': np.std(neg_diff)
                        }
                        all_stats.append(stats)

    if all_stats:
        df = pd.DataFrame(all_stats)
        output_csv = RESULT_DIR / "negative_pixel_stats.csv"
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        logger.info(f"Saved stats to {output_csv}")
        
        # Print Summary
        print("\n=== Negative Pixel Analysis Summary (<1h) ===")
        print(df.groupby('Type')[['Negative_Ratio', 'Mean_Diff', 'Median_Diff', 'Std_Diff']].mean())
    else:
        logger.warning("No data found.")

if __name__ == "__main__":
    main()
