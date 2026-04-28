import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

STATS_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\aug_oct_linear_backscatter_stats.csv")
OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")

def main():
    print("Plotting Linear Backscatter Stats...")
    
    if not STATS_CSV.exists():
        print(f"Error: {STATS_CSV} not found.")
        return
        
    df = pd.read_csv(STATS_CSV)
    
    # 1. Clean Data (Remove Inf/NaN if any overflow occurred)
    cols_to_check = [c for c in df.columns if 'mean' in c or 'std' in c]
    before_len = len(df)
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=cols_to_check)
    print(f"Loaded {before_len} entries. Valid entries: {len(df)}")

    # 2. Add Weighted Mean calculation helper
    # We want to aggregate by (Month, Delay, Class)
    # Weighted by pixel_count
    
    def weighted_avg(x, val_col, w_col):
        try:
            return np.average(x[val_col], weights=x[w_col])
        except ZeroDivisionError:
            return np.nan

    # Groupby
    group_cols = ['month', 'delay_int', 'class']
    
    # We want to plot:
    # - Mean Intensity (After)
    # - Mean Difference (After - Before)
    # For both VV and VH.
    
    # Aggregation
    agg_rows = []
    for name, group in df.groupby(group_cols):
        m, d, c = name
        row = {
            'month': m,
            'delay_int': d,
            'class': c,
            'count': group['pixel_count'].sum()
        }
        
        # VV
        if 'vv_mean_after' in group:
            row['vv_mean_after'] = weighted_avg(group, 'vv_mean_after', 'pixel_count')
            row['vv_mean_diff'] = weighted_avg(group, 'vv_mean_diff', 'pixel_count')
            
        # VH
        if 'vh_mean_after' in group:
            row['vh_mean_after'] = weighted_avg(group, 'vh_mean_after', 'pixel_count')
            row['vh_mean_diff'] = weighted_avg(group, 'vh_mean_diff', 'pixel_count')
            
        agg_rows.append(row)
        
    df_agg = pd.DataFrame(agg_rows)
    df_agg.to_csv(OUTPUT_DIR / "aug_oct_linear_stats_aggregated.csv", index=False)
    
    # 3. Plotting
    plt.rcParams['font.family'] = 'Meiryo'
    sns.set(font='Meiryo')
    
    # Map class to English/Japanese if needed, but 'class' is road/paddy/river
    # Filter to only Road and Paddy
    df_agg = df_agg[df_agg['class'].isin(['road', 'paddy'])]
    
    class_order = ['road', 'paddy']
    class_labels = {'road': '道路', 'paddy': '水田', 'river': '河川'}
    class_colors = {'road': 'gray', 'paddy': 'green'}
    
    # Create plots for each Month
    for month in [8, 10]:
        month_data = df_agg[df_agg['month'] == month]
        if month_data.empty: continue
        
        # --- Figure 1: Mean Intensity (After) ---
        fig, axes = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
        
        # VV
        sns.barplot(data=month_data, x='delay_int', y='vv_mean_after', hue='class', 
                    palette=class_colors, hue_order=class_order, ax=axes[0])
        axes[0].set_title(f"{month}月 VV偏波 後方散乱強度(Linear) - 降雨後", fontsize=14)
        axes[0].set_ylabel("後方散乱強度 (平均)", fontsize=12)
        axes[0].grid(axis='y', linestyle='--', alpha=0.7)
        axes[0].legend(title='クラス', labels=['道路', '水田'])

        # VH
        sns.barplot(data=month_data, x='delay_int', y='vh_mean_after', hue='class', 
                    palette=class_colors, hue_order=class_order, ax=axes[1])
        axes[1].set_title(f"{month}月 VH偏波 後方散乱強度(Linear) - 降雨後", fontsize=14)
        axes[1].set_ylabel("後方散乱強度 (平均)", fontsize=12)
        axes[1].set_xlabel("経過時間 (時間)", fontsize=12)
        axes[1].grid(axis='y', linestyle='--', alpha=0.7)
        axes[1].legend(title='クラス', labels=['道路', '水田'])
        
        out_path = OUTPUT_DIR / f"plot_linear_intensity_{month}m.png"
        plt.tight_layout()
        plt.savefig(out_path, dpi=300)
        print(f"Saved {out_path}")
        
        # --- Figure 2: Mean Difference (After - Before) ---
        fig, axes = plt.subplots(2, 1, figsize=(10, 12), sharex=True)
        
        # VV
        sns.barplot(data=month_data, x='delay_int', y='vv_mean_diff', hue='class', 
                    palette=class_colors, hue_order=class_order, ax=axes[0])
        axes[0].set_title(f"{month}月 VV偏波 差分(Linear) (降雨後 - 降雨前)", fontsize=14)
        axes[0].set_ylabel("差分 (平均)", fontsize=12)
        axes[0].axhline(0, color='black', linewidth=0.8)
        axes[0].grid(axis='y', linestyle='--', alpha=0.7)
        axes[0].legend(title='クラス', labels=['道路', '水田'])

        # VH
        sns.barplot(data=month_data, x='delay_int', y='vh_mean_diff', hue='class', 
                    palette=class_colors, hue_order=class_order, ax=axes[1])
        axes[1].set_title(f"{month}月 VH偏波 差分(Linear) (降雨後 - 降雨前)", fontsize=14)
        axes[1].set_ylabel("差分 (平均)", fontsize=12)
        axes[1].set_xlabel("経過時間 (時間)", fontsize=12)
        axes[1].axhline(0, color='black', linewidth=0.8)
        axes[1].grid(axis='y', linestyle='--', alpha=0.7)
        axes[1].legend(title='クラス', labels=['道路', '水田'])
        
        out_path = OUTPUT_DIR / f"plot_linear_diff_{month}m.png"
        plt.tight_layout()
        plt.savefig(out_path, dpi=300)
        print(f"Saved {out_path}")

if __name__ == "__main__":
    main()
