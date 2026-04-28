"""
Analyze Grid Bias for ALL Delays (0-12h) in August.
1. Count unique events.
2. Check Grid Dominance (Top 1 and Top 5 grids contribution).
3. Visualize bias metrics across delays.
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
def main():
    print("Loading balanced dataset...")
    df = pd.read_csv(DATASET_PATH)
    
    target_months = [4, 8, 9, 10]
    
    delays = range(13)
    
    for month in target_months:
        print(f"\n=== Processing Month {month} ===")
        month_df = df[df['month'] == month].copy()
        
        if len(month_df) == 0:
            print(f"No data for Month {month}")
            continue
            
        summary = []
        print(f"{'Delay':<5} | {'Samples':<8} | {'Events':<6} | {'Top Grid %':<10} | {'Top 5 %':<10}")
        print("-" * 55)
        
        for delay in delays:
            subset = month_df[month_df['delay_bin'] == delay]
            n_samples = len(subset)
            
            if n_samples == 0:
                summary.append({
                    'delay': delay,
                    'n_samples': 0,
                    'n_events': 0,
                    'top_grid_share': 0,
                    'top_5_share': 0
                })
                continue

            # Unique Events Proxy
            subset['event_signature'] = subset['grid_id'] + "_" + subset['total_rain'].astype(str)
            n_events = subset['event_signature'].nunique()
            
            # Grid Dominance
            grid_counts = subset['grid_id'].value_counts(normalize=True)
            top_share = grid_counts.iloc[0] * 100
            top_5_share = grid_counts.iloc[:5].sum() * 100
            
            print(f"{delay:<5} | {n_samples:<8} | {n_events:<6} | {top_share:<9.1f}% | {top_5_share:<9.1f}%")
            
            summary.append({
                'delay': delay,
                'n_samples': n_samples,
                'n_events': n_events,
                'top_grid_share': top_share,
                'top_5_share': top_5_share
            })

        sum_df = pd.DataFrame(summary)
        csv_path = OUTPUT_DIR.parent / f"{month}月_全Delay構成バイアス.csv"
        # Ensure parent dir exists (it should, as data/result/Aug vs seasonal)
        # Using seasonal/rain_continuity or create specific?
        # Let's save to data/result/seasonal/bias_analysis for clarity
        base_out = BASE_DIR / "data/result/seasonal/bias_analysis"
        base_out.mkdir(parents=True, exist_ok=True)
        
        sum_df.to_csv(base_out / f"{month}月_全Delay構成バイアス.csv", index=False)
        
        # Visualization
        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        color = 'tab:red'
        ax1.set_xlabel('Delay (h)')
        ax1.set_ylabel('Top 1 Grid Share (%)', color=color)
        ax1.plot(sum_df['delay'], sum_df['top_grid_share'], 'o-', color=color, label='Top 1 Grid Share')
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.set_ylim(0, 100)
        ax1.grid(True)
        
        ax2 = ax1.twinx()
        color = 'tab:blue'
        ax2.set_ylabel('Unique Events Count', color=color)
        # Handle 0 events for log scale or just linear? Linear is fine for count.
        ax2.bar(sum_df['delay'], sum_df['n_events'], alpha=0.3, color=color, label='Events Count')
        ax2.tick_params(axis='y', labelcolor=color)
        
        plt.title(f'{month}月 Delay別データ構成バイアス (イベント数 vs 特定Gridの支配率)')
        fig.tight_layout()
        plt.savefig(base_out / f"{month}月_全Delay構成バイアス.png")
        print(f"Saved plot: {base_out}/{month}月_全Delay構成バイアス.png")

if __name__ == "__main__":
    main()
