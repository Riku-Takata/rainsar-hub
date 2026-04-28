import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from pathlib import Path

# Set Japanese font
mpl.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
METRICS_CSV = OUTPUT_DIR / "rf_classification_metrics.csv"
PLOT_DIR = OUTPUT_DIR / "rf_plots"
PLOT_DIR.mkdir(exist_ok=True)

def plot_cm_summary():
    if not METRICS_CSV.exists():
        print(f"Metrics file not found: {METRICS_CSV}")
        return

    df = pd.read_csv(METRICS_CSV)
    
    months = df['month'].unique()
    
    for month in months:
        subset = df[df['month'] == month].sort_values('delay_int')
        
        # Setup 3x4 grid for 12 delays (0-11)
        fig, axes = plt.subplots(3, 4, figsize=(20, 15))
        fig.suptitle(f"{month}月 Delay別 混同行列まとめ", fontsize=20, y=0.98)
        
        delays = range(12)
        axes = axes.flatten()
        
        for i, delay in enumerate(delays):
            ax = axes[i]
            row = subset[subset['delay_int'] == delay]
            
            if row.empty:
                ax.text(0.5, 0.5, "No Data", ha='center', va='center')
                ax.set_title(f"Delay {delay}h")
                continue
                
            # Reconstruct CM
            # cm_tn_road_road, cm_fp_road_paddy, cm_fn_paddy_road, cm_tp_paddy_paddy
            tn = row.iloc[0]['cm_tn_road_road']
            fp = row.iloc[0]['cm_fp_road_paddy']
            fn = row.iloc[0]['cm_fn_paddy_road']
            tp = row.iloc[0]['cm_tp_paddy_paddy']
            
            cm = np.array([[tn, fp], [fn, tp]])
            acc = row.iloc[0]['accuracy']
            
            sns.heatmap(cm, annot=True, fmt='.0f', cmap='Blues', ax=ax, cbar=False,
                        xticklabels=['道路', '水田'],
                        yticklabels=['道路', '水田'])
            
            ax.set_title(f"Delay {delay}h (Acc: {acc:.3f})")
            ax.set_ylabel("正解" if i % 4 == 0 else "")
            ax.set_xlabel("予測" if i >= 8 else "")
            
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        save_path = PLOT_DIR / f"cm_summary_month{month}.png"
        plt.savefig(save_path)
        print(f"Saved {save_path}")
        plt.close()

if __name__ == "__main__":
    plot_cm_summary()
