import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# --- CONFIG ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
STATS_CSV = BASE_DIR / "data" / "analysis" / "aug_oct_linear_backscatter_stats.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "classification_difficulty"
KDE_DELAY_DIR = OUTPUT_DIR / "delay_distributions"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
KDE_DELAY_DIR.mkdir(parents=True, exist_ok=True)

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def cohens_d(x, y):
    nx, ny = len(x), len(y)
    if nx < 2 or ny < 2: return 0
    dof = nx + ny - 2
    return (np.mean(x) - np.mean(y)) / np.sqrt(((nx-1)*np.std(x, ddof=1)**2 + (ny-1)*np.std(y, ddof=1)**2) / dof)

def m_statistic(x, y):
    mean_diff = abs(np.mean(x) - np.mean(y))
    sum_std = np.std(x) + np.std(y)
    if sum_std == 0: return 0
    return mean_diff / sum_std

def analyze_difficulty():
    if not STATS_CSV.exists():
        print(f"Stats file not found: {STATS_CSV}")
        return

    df = pd.read_csv(STATS_CSV)
    class_map = {'road': '道路', 'paddy': '水田', 'river': '河川'}
    df['class'] = df['class'].map(class_map)
    
    pair_map = {
        'Road vs Paddy': '道路 vs 水田',
        'Road vs River': '道路 vs 河川',
        'Paddy vs River': '水田 vs 河川'
    }

    results = []
    delays = sorted(df['delay_int'].unique())
    
    # --- Per-Delay Analysis & Plotting ---
    print("Generating per-delay KDE plots (Japanese)...")
    
    for pol in ['VV', 'VH']:
        col = f'{pol.lower()}_mean_diff'
        
        n_delays = len(delays)
        cols = 3
        rows = (n_delays + cols - 1) // cols
        
        fig, axes = plt.subplots(rows, cols, figsize=(18, 5 * rows), sharex=True)
        axes = axes.flatten()
        
        for i, delay in enumerate(delays):
            ax = axes[i]
            subset = df[df['delay_int'] == delay]
            
            for cls in ['道路', '水田', '河川']:
                data = subset[subset['class'] == cls][col].dropna()
                if len(data) > 3:
                    sns.kdeplot(data, ax=ax, label=cls, fill=True, alpha=0.3)
            
            ax.set_title(f"経過時間: {delay}時間後", fontsize=14)
            ax.set_xlim(-0.5, 0.5)
            ax.set_xlabel("後方散乱強度差分" if i >= (rows-1)*cols else "")
            ax.set_ylabel("密度" if i % cols == 0 else "")
            ax.grid(True, alpha=0.3)
            if i == 0: ax.legend()

            # Stats collection
            for pair_en in [('Road', 'Paddy'), ('Road', 'River'), ('Paddy', 'River')]:
                p1, p2 = pair_en
                c1_data = subset[subset['class'] == class_map[p1.lower()]][col].dropna()
                c2_data = subset[subset['class'] == class_map[p2.lower()]][col].dropna()
                if len(c1_data) >= 5 and len(c2_data) >= 5:
                    results.append({
                        'delay': delay,
                        'pair': pair_map[f"{p1} vs {p2}"],
                        'pol': pol,
                        'cohens_d': cohens_d(c1_data, c2_data),
                        'm_stat': m_statistic(c1_data, c2_data)
                    })
        
        # Hide empty subplots
        for j in range(i + 1, len(axes)):
            axes[j].axis('off')
            
        plt.tight_layout()
        plt.suptitle(f"{pol}偏波 経過時間別の強度差分分布", fontsize=20, y=1.02)
        plt.savefig(OUTPUT_DIR / f"kde_grid_{pol.lower()}.png", dpi=300, bbox_inches='tight')
        plt.close()

    res_df = pd.DataFrame(results)
    res_df.to_csv(OUTPUT_DIR / "separation_metrics.csv", index=False)

    # --- Trend Plot ---
    plt.figure(figsize=(12, 10))
    metrics = [('cohens_d', "Cohen's d (効果量)"), ('m_stat', "M統計量 (分離度)")]
    for i, (metric_key, metric_name) in enumerate(metrics):
        plt.subplot(2, 1, i+1)
        sns.lineplot(data=res_df, x='delay', y=metric_key, hue='pair', style='pol', markers=True, dashes=False)
        plt.title(f"分離度指標の推移 ({metric_name})", fontsize=16)
        plt.xlabel("降雨後経過時間 [時間後]", fontsize=12)
        plt.ylabel(metric_name, fontsize=12)
        plt.xticks(range(0, 13))
        plt.grid(True, alpha=0.3)
        plt.legend(title='クラス対', bbox_to_anchor=(1.05, 1), loc='upper left')
        if metric_key == 'm_stat': 
            plt.axhline(y=1.0, color='r', linestyle='--', alpha=0.5, label='分離可能 (M=1.0)')
        elif metric_key == 'cohens_d': 
            plt.axhline(y=0.8, color='g', linestyle='--', alpha=0.5, label='効果大 (d=0.8)')

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "separability_trend.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"All analysis images generated in {OUTPUT_DIR}")

if __name__ == "__main__":
    analyze_difficulty()
