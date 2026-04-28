"""
Analyze why Delay 0h has higher accuracy than 1h/2h.
Method:
1. Visualize distribution of diff_vv for Road vs Paddy at Delay 0, 1, 2, 3.
2. Calculate separation metrics (Cohen's d).
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

def calculate_cohens_d(group1, group2):
    """
    Calculate Cohen's d for independent samples.
    d = (mean1 - mean2) / pooled_std
    """
    n1, n2 = len(group1), len(group2)
    var1, var2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
    
    pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    d = (np.mean(group1) - np.mean(group2)) / pooled_std
    return abs(d)

def main():
    print("Loading balanced dataset...")
    df = pd.read_csv(DATASET_PATH)
    
    # Filter for August
    aug = df[df['month'] == 8].copy()
    
    delays = [0, 1, 2, 3]
    
    # Prepare figure for distributions
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    axes = axes.flatten()
    
    summary = []
    
    for i, delay in enumerate(delays):
        subset = aug[aug['delay_bin'] == delay]
        road = subset[subset['label'] == 0]['diff_vv']
        paddy = subset[subset['label'] == 1]['diff_vv']
        
        # Calculate stats
        mean_road = road.mean()
        mean_paddy = paddy.mean()
        std_road = road.std()
        std_paddy = paddy.std()
        
        # Calculate Separability (Cohen's d)
        cohen_d = calculate_cohens_d(road, paddy)
        
        print(f"Delay {delay}h: Road(n={len(road)}), Paddy(n={len(paddy)})")
        print(f"  Mean: Road={mean_road:.2f}, Paddy={mean_paddy:.2f} -> Diff={abs(mean_road - mean_paddy):.2f}")
        print(f"  Std : Road={std_road:.2f}, Paddy={std_paddy:.2f}")
        print(f"  Separation (Cohen's d): {cohen_d:.4f}")
        
        summary.append({
            'Delay': delay,
            'Road_Mean': mean_road,
            'Paddy_Mean': mean_paddy,
            'Mean_Diff': abs(mean_road - mean_paddy),
            'Cohens_d': cohen_d
        })
        
        # Plot
        ax = axes[i]
        sns.kdeplot(road, label='道路', fill=True, color='blue', alpha=0.3, ax=ax)
        sns.kdeplot(paddy, label='田んぼ', fill=True, color='green', alpha=0.3, ax=ax)
        
        ax.set_title(f"Delay {delay}h (d={cohen_d:.2f})")
        ax.set_xlabel("Diff VV (After - Before)")
        ax.set_xlim(-10, 10)
        ax.grid(True, alpha=0.3)
        ax.legend()
        
        # Add annotation
        ax.text(0.05, 0.95, f"Road μ={mean_road:.1f}\nPaddy μ={mean_paddy:.1f}\nDiff={abs(mean_road-mean_paddy):.1f}",
                transform=ax.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.suptitle("8月 Delay 0-3h 特徴量分布変化 (VV Difference)", fontsize=16)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_Delay0-3h_分布変化.png", dpi=150)
    print(f"Saved: {OUTPUT_DIR}/8月_Delay0-3h_分布変化.png")
    
    # Save summary
    sum_df = pd.DataFrame(summary)
    sum_df.to_csv(OUTPUT_DIR / "8月_Delay0-3h_分離度解析.csv", index=False)
    print("\nSummary:")
    print(sum_df)

if __name__ == "__main__":
    main()
