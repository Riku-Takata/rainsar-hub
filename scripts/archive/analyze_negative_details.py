import os
import sys
import re
import logging
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("negative_details")

# Settings
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
S1_SAFE_DIR = BASE_DIR / "s1_safe"
RESULT_DIR = BASE_DIR / "rainsar-hub" / "result" / "distributions" / "negative_analysis_v2"

TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

SUFFIX_HIGHWAY = "_highway_mask.tif"
SUFFIX_PADDY = "_paddy_mask.tif"

def configure_plotting():
    sns.set_style("whitegrid")
    target_fonts = ['MS Gothic', 'Meiryo', 'Yu Gothic', 'SimHei', 'Arial Unicode MS']
    found_font = None
    for font_name in target_fonts:
        try:
            if fm.findfont(font_name, fallback_to_default=False) != fm.findfont("NonExistentFont"):
                found_font = font_name
                break
        except:
            continue
    if found_font:
        plt.rcParams['font.family'] = found_font
        plt.rcParams['axes.unicode_minus'] = False

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
        delay_m = re.search(r"Delay \(Hours\)\s+: ([\d\.]+)", block)
        after_m = re.search(r"After Scene\s*:? (S1\w+)", block)
        before_m = re.search(r"Before Scene\s*:? (S1\w+)", block)
        
        if delay_m and after_m and before_m:
            data['delay'] = float(delay_m.group(1))
            data['after_scene'] = after_m.group(1)
            data['before_scene'] = before_m.group(1)
            events.append(data)
    return events

def read_linear_values(tif_path):
    if not tif_path.exists(): return None
    with rasterio.open(tif_path) as src:
        data = src.read(1)
        valid_mask = ~np.isnan(data)
        if not np.any(valid_mask): return None
        return 10 ** (data[valid_mask] / 10.0)

def main():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    configure_plotting()
    
    # Store aggregated negative pixel data
    # { 'Short': {'Before': [], 'After': []}, 'Long': ... }
    agg_data = {
        'Short': {'Paddy_Before': [], 'Paddy_After': [], 'Road_Before': [], 'Road_After': []},
        'Long':  {'Paddy_Before': [], 'Paddy_After': [], 'Road_Before': [], 'Road_After': []}
    }
    
    # Store Diff stats for validation
    diff_stats = []

    for grid_id in TARGET_GRIDS:
        events = parse_summary_txt(grid_id)
        if not events: continue
        
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        for evt in events:
            delay = evt['delay']
            
            # Categorize
            if delay <= 1.5: cat = 'Short'
            elif delay > 6.0: cat = 'Long'
            else: continue
            
            stem_after = f"{evt['after_scene']}_proc"
            stem_before = f"{evt['before_scene']}_proc"
            
            for type_name, suffix, key_prefix in [('道路', SUFFIX_HIGHWAY, 'Road'), ('田んぼ', SUFFIX_PADDY, 'Paddy')]:
                path_after = grid_dir / f"{stem_after}{suffix}"
                path_before = grid_dir / f"{stem_before}{suffix}"
                
                arr_a = read_linear_values(path_after)
                arr_b = read_linear_values(path_before)
                
                if arr_a is not None and arr_b is not None:
                    min_len = min(len(arr_a), len(arr_b))
                    arr_a = arr_a[:min_len]
                    arr_b = arr_b[:min_len]
                    diff = arr_a - arr_b
                    
                    # NEGATIVE PIXELS ONLY
                    neg_mask = diff < 0
                    neg_count = np.sum(neg_mask)
                    total_count = len(diff)
                    
                    if neg_count > 0:
                        neg_before = arr_b[neg_mask]
                        neg_after = arr_a[neg_mask]
                        
                        # Subsample if too large to save memory/speed
                        if len(neg_before) > 5000:
                            indices = np.random.choice(len(neg_before), 5000, replace=False)
                            neg_before = neg_before[indices]
                            neg_after = neg_after[indices]
                        
                        agg_data[cat][f'{key_prefix}_Before'].extend(neg_before)
                        agg_data[cat][f'{key_prefix}_After'].extend(neg_after)
                        
                        diff_stats.append({
                            'Grid': grid_id, 'Delay': delay, 'Cat': cat, 'Type': type_name,
                            'Neg_Ratio': neg_count / total_count,
                            'Mean_Diff': np.mean(arr_a - arr_b), # Overall
                            'Neg_Mean_Diff': np.mean(neg_after - neg_before)
                        })

    # --- Analysis & Visualization ---
    
    # 1. Histogram of Negative Pixels (Before vs After) for Short Term
    # Focus on Paddy as it had strong signal
    plt.figure(figsize=(10, 6))
    sns.histplot(agg_data['Short']['Paddy_Before'], color='green', label='Before (Valid)', kde=True, stat='density', alpha=0.4)
    sns.histplot(agg_data['Short']['Paddy_After'], color='blue', label='After (Valid)', kde=True, stat='density', alpha=0.4)
    plt.title("短時間・負の差分ピクセルの元強度分布 (田んぼ)")
    plt.xlabel("Backscatter Intensity (Linear)")
    plt.ylabel("Density")
    plt.xlim(0, 0.5) # Limiting mainly for noise visibility
    plt.legend()
    plt.savefig(RESULT_DIR / "hist_short_paddy_before_after.png")
    plt.close()

    # 2. Histogram for Road (Short Term) - Control
    plt.figure(figsize=(10, 6))
    sns.histplot(agg_data['Short']['Road_Before'], color='orange', label='Before', kde=True, stat='density')
    sns.histplot(agg_data['Short']['Road_After'], color='red', label='After', kde=True, stat='density')
    plt.title("短時間・負の差分ピクセルの元強度分布 (道路)")
    plt.xlabel("Backscatter Intensity (Linear)")
    plt.xlim(0, 0.5)
    plt.legend()
    plt.savefig(RESULT_DIR / "hist_short_road_before_after.png")
    plt.close()

    # 3. Long Term Comparison
    # Compare Negative Diff magnitude in Short vs Long
    df_stats = pd.DataFrame(diff_stats)
    df_stats.to_csv(RESULT_DIR / "negative_stats_comparison.csv", index=False)
    
    print("\n=== Long-term Negative Pixel Trend ===")
    print(df_stats.groupby(['Cat', 'Type'])[['Neg_Ratio', 'Neg_Mean_Diff']].mean())
    
    # Plot Neg Mean Diff
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df_stats, x='Cat', y='Neg_Mean_Diff', hue='Type', order=['Short', 'Long'])
    plt.title("負の差分の深さ比較 (Short vs Long)")
    plt.ylabel("Mean Difference of Negative Pixels (Linear)")
    plt.tight_layout()
    plt.savefig(RESULT_DIR / "boxplot_neg_diff_comparison.png")
    plt.close()
    
    print(f"\nSaved results to {RESULT_DIR}")

if __name__ == "__main__":
    main()
