"""
Analyze Event Composition Bias for Delay 0h vs 1h.
1. Count unique events (Grid ID + Date).
2. Compare Rainfall Intensity distribution.
3. Check Grid Dominance (Top 5 grids contribution).
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from pathlib import Path

matplotlib.rcParams['font.family'] = 'MS Gothic'
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATASET_PATH = BASE_DIR / "data/result/seasonal/rf_data/rf_dataset_balanced.csv"
OUTPUT_DIR = BASE_DIR / "data/result/Aug"

def main():
    print("Loading balanced dataset...")
    df = pd.read_csv(DATASET_PATH)
    aug = df[df['month'] == 8].copy()
    
    # We need to reconstruct 'Event ID'.
    # Dataset has 'grid_id' and 'event_end_ts_utc' (waiting... does it?)
    # rf_dataset_balanced.csv columns: 
    #   grid_id, diff_vv, diff_vh, total_rain, duration, month, delay_bin, label...
    # It might NOT have event_end_ts explicitly if it was aggregating pixels.
    # But 'grid_id' + 'delay_bin' + 'month' roughly defines a set of events?
    # No, delay varies by hour.
    # Actually, `prepare_rf_dataset.py` saves `rf_dataset_raw.csv` with event_end?
    # Let's check columns of balanced dataset.
    
    print("Columns:", aug.columns.tolist())
    
    # Assuming 'event_end' or similar exists. If not, we use 'grid_id' and 'total_rain' as proxy for unique event.
    # Or just 'grid_id' count.
    
    # Adding Delay 3 to check if high accuracy there is also due to bias
    delays = [0, 1, 3]
    
    summary = []
    
    for delay in delays:
        subset = aug[aug['delay_bin'] == delay]
        
        # 1. Unique Events (Proxy: unique combinations of grid_id and total_rain)
        # Using total_rain because different events at same grid have different rain.
        subset['event_signature'] = subset['grid_id'] + "_" + subset['total_rain'].astype(str)
        n_events = subset['event_signature'].nunique()
        n_samples = len(subset)
        
        # 2. Rain Distribution
        mean_rain = subset['total_rain'].mean()
        max_rain = subset['total_rain'].max()
        
        # 3. Grid Dominance
        grid_counts = subset['grid_id'].value_counts(normalize=True)
        top_grid = grid_counts.index[0]
        top_share = grid_counts.iloc[0] * 100
        top_5_share = grid_counts.iloc[:5].sum() * 100
        
        print(f"Delay {delay}h:")
        print(f"  Samples: {n_samples}")
        print(f"  Unique Events: {n_events}")
        print(f"  Mean Rain: {mean_rain:.2f} mm")
        print(f"  Top Grid: {top_grid} ({top_share:.1f}%)")
        print(f"  Top 5 Share: {top_5_share:.1f}%")
        
        summary.append({
            'delay': delay,
            'n_samples': n_samples,
            'n_events': n_events,
            'mean_rain': mean_rain,
            'top_grid_share': top_share,
            'top_5_share': top_5_share
        })
        
        # Plot Top 10 Grids
        plt.figure(figsize=(10, 6))
        grid_counts.iloc[:15].plot(kind='bar', color='skyblue', edgecolor='black')
        plt.title(f"Delay {delay}h: Grid Dominance (Top 15)\nTotal Events: {n_events}, Samples: {n_samples}")
        plt.ylabel("Share of Total Samples")
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / f"8月_Grid偏り_Delay{delay}h.png")
        print(f"Saved: 8月_Grid偏り_Delay{delay}h.png")

    sum_df = pd.DataFrame(summary)
    sum_df.to_csv(OUTPUT_DIR / "8月_Delay0vs1_構成分析.csv", index=False)
    print("\nSummary:")
    print(sum_df)

if __name__ == "__main__":
    main()
