import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import sys

# Add current directory to sys.path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from common_utils import (
    setup_logger, parse_summary_txt, read_linear_values, linear_to_db,
    S1_SAMPLES_DIR, RESULT_DIR, TARGET_GRIDS
)

# Japanese font support
plt.rcParams['font.family'] = 'MS Gothic'

OUTPUT_DIR = RESULT_DIR / "20251212" / "delay_analysis"
STATS_FILE = OUTPUT_DIR / "delay_stats.csv"

def sigmaclip(a, low=3.0, high=3.0):
    c = np.asarray(a)
    delta = 1
    while delta:
        if c.size == 0: break
        c_mean = c.mean()
        c_std = c.std()
        size = c.size
        critlower = c_mean - c_std * low
        critupper = c_mean + c_std * high
        c = c[(c >= critlower) & (c <= critupper)]
        delta = size - c.size
    return c, c_mean, c_std

def main():
    logger = setup_logger("delay_stats_analysis")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_stats = []
    
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        events = parse_summary_txt(grid_id)
        if not events: continue
        
        for evt in events:
            before_s = evt['before_scene']
            after_s = evt['after_scene']
            delay = evt.get('delay', -1)
            
            # Determine Delay Class
            if delay < 1.5:
                delay_class = "Short (<1.5h)"
            elif delay > 6.0:
                delay_class = "Long (>6h)"
            elif delay >= 1.5 and delay <= 6.0:
                delay_class = "Medium (1.5-6h)"
            else:
                delay_class = "Unknown"
            
            for type_name, suffix in [('Road', '_highway_mask.tif'), ('Paddy', '_paddy_mask.tif')]:
                path_before = S1_SAMPLES_DIR / grid_id / f"{before_s}_proc{suffix}"
                path_after = S1_SAMPLES_DIR / grid_id / f"{after_s}_proc{suffix}"
                
                val_b = read_linear_values(path_before)
                val_a = read_linear_values(path_after)
                
                if val_b is None or val_a is None: continue
                
                min_n = min(len(val_b), len(val_a))
                val_b = val_b[:min_n]
                val_a = val_a[:min_n]
                
                db_b = linear_to_db(val_b)
                db_a = linear_to_db(val_a)
                diff_db = db_a - db_b
                
                # Apply Filtering
                clean_b, _, _ = sigmaclip(db_b)
                clean_a, _, _ = sigmaclip(db_a)
                clean_diff, _, _ = sigmaclip(diff_db)
                
                med_b = np.median(clean_b) if clean_b.size > 0 else np.nan
                med_a = np.median(clean_a) if clean_a.size > 0 else np.nan
                med_diff = np.median(clean_diff) if clean_diff.size > 0 else np.nan
                
                all_stats.append({
                    'GridID': grid_id,
                    'EventDate': evt['date'],
                    'Delay': delay,
                    'DelayClass': delay_class,
                    'Type': type_name,
                    'Median_Before': med_b,
                    'Median_After': med_a,
                    'Median_Diff': med_diff
                })
                
    if not all_stats: return
    df = pd.DataFrame(all_stats)
    df.to_csv(STATS_FILE, index=False, encoding='utf-8-sig')
    
    # --- Visualization ---
    
    # 1. Boxplot by Delay Class
    plt.figure(figsize=(10, 6))
    order = ['Short (<1.5h)', 'Medium (1.5-6h)', 'Long (>6h)']
    sns.boxplot(data=df, x='DelayClass', y='Median_Diff', hue='Type', order=order, palette={'Road': 'gray', 'Paddy': 'green'})
    plt.axhline(0, color='k', linestyle='--')
    plt.title("Backscatter Change by Delay Class")
    plt.ylabel("Change (After - Before) [dB]")
    plt.savefig(OUTPUT_DIR / "boxplot_change_by_delay.png")
    plt.close()
    
    # Summary Table
    summary = df.groupby(['DelayClass', 'Type'])['Median_Diff'].describe()[['mean', 'std', '50%']]
    print("\n=== Change Statistics by Delay Class ===")
    print(summary)
    
    summary.to_csv(OUTPUT_DIR / "delay_summary.csv")

if __name__ == "__main__":
    main()
