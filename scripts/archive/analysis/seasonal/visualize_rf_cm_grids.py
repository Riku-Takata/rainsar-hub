
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import matplotlib

# Setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
METRICS_CSV = BASE_DIR / "data" / "result" / "seasonal" / "rf_results" / "rf_accuracy_metrics.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "rf_results" / "cm_grids"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Japanese font configuration
matplotlib.rcParams['font.family'] = 'MS Gothic'

# Labels in Japanese
LABELS = ["道路", "田んぼ"]

def plot_month_cm_grid(month_df, month):
    # Determine grid size. 
    # Delays 0-12 -> 13 plots. 4x4=16 OK.
    # Or 3x5.
    # User's image has 4 cols x 3 rows = 12 plots (Delay 0-11). 
    # Let's target 12 delays (0-11). If 12 exists, maybe extend.
    
    delays = sorted(month_df['delay'].unique())
    max_delay = max(delays)
    
    cols = 4
    rows = int(np.ceil(len(delays) / cols))
    
    fig, axes = plt.subplots(rows, cols, figsize=(4*cols, 4*rows))
    axes = axes.flatten()
    
    for i, delay in enumerate(delays):
        ax = axes[i]
        
        row = month_df[month_df['delay'] == delay].iloc[0]
        
        # CM Layout:
        # [[TN, FP],
        #  [FN, TP]]
        # Road=0, Paddy=1
        
        tn = int(row['tn'])
        fp = int(row['fp'])
        fn = int(row['fn'])
        tp = int(row['tp'])
        
        cm = np.array([[tn, fp], [fn, tp]])
        acc = row['test_accuracy']
        n_test = int(row['test_samples'])
        
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False, ax=ax,
                    xticklabels=LABELS, yticklabels=LABELS)
        
        ax.set_title(f"{delay}時間経過\n(n={n_test:,}, 精度={acc:.2f})")
        ax.set_ylabel("実測")
        ax.set_xlabel("予測")
        
    # Hide unused
    for j in range(len(delays), len(axes)):
        axes[j].axis('off')
        
    month_names = {4: '4月', 8: '8月', 9: '9月', 10: '10月'}
    plt.suptitle(f"{month_names.get(month, str(month)+'月')} 混同行列 (道路 vs 田んぼ)", fontsize=16)
    plt.tight_layout()
    
    out_path = OUTPUT_DIR / f"cm_grid_month_{month}.png"
    plt.savefig(out_path)
    print(f"Saved {out_path}")
    plt.close()

def main():
    if not METRICS_CSV.exists():
        print("Metrics CSV not found.")
        return
        
    df = pd.read_csv(METRICS_CSV)
    
    # Check if 'tn' column exists
    if 'tn' not in df.columns:
        print("Column 'tn' missing. Need to re-run training.")
        return
        
    months = sorted(df['month'].unique())
    
    for m in months:
        m_df = df[df['month'] == m]
        plot_month_cm_grid(m_df, m)

if __name__ == "__main__":
    main()
