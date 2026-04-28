"""
Visualize RF Accuracy with Reliability Filtering.
Filter out delays with:
- Unique Events < threshold (e.g. 5)
- Top Grid Share > threshold (e.g. 40%)
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from pathlib import Path

matplotlib.rcParams['font.family'] = 'MS Gothic'
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
RF_RESULTS_PATH = BASE_DIR / "data/result/seasonal/rf_results/rf_accuracy_metrics.csv"
BIAS_DIR = BASE_DIR / "data/result/seasonal/bias_analysis"
OUTPUT_DIR = BASE_DIR / "data/result/seasonal/rf_results"

def main():
    # Load RF Accuracy
    if not RF_RESULTS_PATH.exists():
        print("RF Results not found.")
        return
    rf_df = pd.read_csv(RF_RESULTS_PATH)
    
    # Load Bias Data for all months
    bias_dfs = []
    for csv_file in BIAS_DIR.glob("*月_全Delay構成バイアス.csv"):
        # File name format: "{month}月_全Delay構成バイアス.csv"
        try:
            m_str = csv_file.name.split("月")[0]
            month = int(m_str)
            df = pd.read_csv(csv_file)
            df['month'] = month
            bias_dfs.append(df)
        except:
            continue
            
    if not bias_dfs:
        print("Bias data not found.")
        return
        
    bias_df = pd.concat(bias_dfs, ignore_index=True)
    
    # Merge Accuracy and Bias
    # rf_df has 'month', 'delay'
    # bias_df has 'month', 'delay'
    merged = pd.merge(rf_df, bias_df, on=['month', 'delay'], how='inner')
    
    # Define Reliability Criteria
    # Reliable if: Events >= 5 AND Top Grid Share <= 40%
    MIN_EVENTS = 5
    MAX_SHARE = 40.0
    
    merged['is_reliable'] = (merged['n_events'] >= MIN_EVENTS) & (merged['top_grid_share'] <= MAX_SHARE)
    
    print("Reliable Data Points:")
    print(merged[merged['is_reliable']][['month', 'delay', 'test_accuracy', 'n_events', 'top_grid_share']])
    
    # Plot
    plt.figure(figsize=(12, 7))
    
    months = sorted(merged['month'].unique())
    colors = sns.color_palette("tab10", n_colors=len(months))
    
    for i, month in enumerate(months):
        subset = merged[merged['month'] == month]
        reliable = subset[subset['is_reliable']]
        unreliable = subset[~subset['is_reliable']]
        
        # Plot Line (all points connected for continuity, but faint)
        plt.plot(subset['delay'], subset['test_accuracy'], '-', color=colors[i], alpha=0.3)
        
        # Plot Reliable Points (Large, Solid)
        plt.scatter(reliable['delay'], reliable['test_accuracy'], color=colors[i], s=100, label=f"{month}月 (信頼)", zorder=5)
        
        # Plot Unreliable Points (Small, X, Hollow)
        plt.scatter(unreliable['delay'], unreliable['test_accuracy'], color=colors[i], marker='x', s=50, alpha=0.5, label=f"{month}月 (除外)" if i==0 else "")
        
    plt.axhline(0.5, color='gray', linestyle='--')
    plt.title(f'季節別RF分類精度 (信頼性フィルタ適用)\n基準: イベント数>={MIN_EVENTS} かつ 特定Gridシェア<={MAX_SHARE}%', fontsize=14)
    plt.xlabel('Delay (h)', fontsize=12)
    plt.ylabel('テスト精度', fontsize=12)
    plt.grid(True)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.ylim(0.4, 1.0)
    plt.tight_layout()
    
    out_path = OUTPUT_DIR / "rf_accuracy_filtered_reliability.png"
    plt.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")
    
    merged.to_csv(OUTPUT_DIR / "rf_accuracy_reliability_merged.csv", index=False)

if __name__ == "__main__":
    main()
