import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# --- CONFIG ---
STATS_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\aug_oct_linear_backscatter_stats.csv")
OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Japanese font
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def main():
    if not STATS_CSV.exists():
        print(f"Error: {STATS_CSV} not found.")
        return

    df = pd.read_csv(STATS_CSV)
    
    # 1. Clean Data
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=['vv_mean_diff', 'vh_mean_diff'])
    
    # Filter for October
    df = df[df['month'] == 10]
    print(f"Filtered for October: {len(df)} entries")

    # Filter classes
    target_classes = ['road', 'paddy']
    df = df[df['class'].isin(target_classes)]
    
    # Map class names for display
    class_map = {'road': '透水性が高い土質', 'paddy': '透水性が低い土質'}
    df['class_jp'] = df['class'].map(class_map)
    
    print(f"Loaded {len(df)} valid event-class entries.")

    # 2. Boxplots (All Delays 0-12h)
    palette = {'透水性が高い土質': 'blue', '透水性が低い土質': 'orange'}
    
    for pol in ['vv', 'vh']:
        col = f'{pol}_mean_diff'
        plt.figure(figsize=(15, 8))
        sns.boxplot(data=df, x='delay_int', y=col, hue='class_jp', 
                    palette=palette,
                    showfliers=False)
        
        plt.title(f"後方散乱強度差分の推移", fontsize=18)
        plt.xlabel("降雨終了からの経過時間 [時間後]", fontsize=14)
        plt.ylabel("強度差分", fontsize=14)
        plt.axhline(0, color='red', linestyle='--', alpha=0.5)
        plt.grid(True, axis='y', alpha=0.3)
        plt.legend(title='クラス')
        
        save_path = OUTPUT_DIR / f"dist_boxplot_{pol}_oct.png"
        plt.savefig(save_path, dpi=300)
        plt.close()
        print(f"Saved: {save_path}")

    # 3. Histograms (Faceted by Delay)
    # We'll plot 0-12h (13 panels)
    for pol in ['vv', 'vh']:
        col = f'{pol}_mean_diff'
        g = sns.FacetGrid(df, col="delay_int", col_wrap=4, hue="class_jp", 
                          palette=palette,
                          height=3.5, aspect=1.3, sharey=False)
        g.map(sns.histplot, col, bins=30, element="step", alpha=0.3)
        g.add_legend()
        g.set_axis_labels(f"{pol.upper()} Diff", "Event Count")
        
        limit = 0.2 if pol == 'vv' else 0.1
        for ax in g.axes.flat:
            ax.set_xlim(-limit, limit)
            ax.axvline(0, color='red', linestyle='--', alpha=0.5)
            
        plt.subplots_adjust(top=0.9)
        g.fig.suptitle(f"{pol.upper()}偏波 イベント別平均差分分布の推移", fontsize=20)
        
        save_path = OUTPUT_DIR / f"dist_histogram_{pol}_oct.png"
        plt.savefig(save_path, dpi=300)
        plt.close()
        print(f"Saved: {save_path}")

    # 4. Statistical Table (Save to CSV)
    stats_summary = df.groupby(['delay_int', 'class']).agg({
        'vv_mean_diff': ['mean', 'median', 'std', 'count'],
        'vh_mean_diff': ['mean', 'median', 'std']
    }).round(4)
    stats_summary.to_csv(OUTPUT_DIR / "dist_stats_summary_0_12h_oct.csv")
    print("Saved stats summary CSV for October.")

if __name__ == "__main__":
    main()
