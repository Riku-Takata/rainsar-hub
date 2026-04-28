"""
Detailed analysis of Delay 0h vs 1h anomaly.
1. Calculate Overlapping Coefficient (OVL) for diff_vv.
2. Visualize 2D Scatter (diff_vv vs diff_vh).
3. Check Feature Importance.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from scipy.stats import gaussian_kde
from sklearn.ensemble import RandomForestClassifier
from pathlib import Path

matplotlib.rcParams['font.family'] = 'MS Gothic'
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATASET_PATH = BASE_DIR / "data/result/seasonal/rf_data/rf_dataset_balanced.csv"
OUTPUT_DIR = BASE_DIR / "data/result/Aug"

def calculate_overlap(x1, x2):
    """
    Calculate Overlapping Coefficient (OVL) between two distributions.
    OVL = Integral(min(f1(x), f2(x)) dx)
    """
    # Define range
    min_val = min(x1.min(), x2.min()) - 2
    max_val = max(x1.max(), x2.max()) + 2
    x_grid = np.linspace(min_val, max_val, 1000)
    
    kde1 = gaussian_kde(x1)
    kde2 = gaussian_kde(x2)
    
    y1 = kde1(x_grid)
    y2 = kde2(x_grid)
    
    overlap = np.minimum(y1, y2)
    ovl_score = np.trapz(overlap, x_grid)
    return ovl_score

def analyze_feature_importance(df, delay):
    """
    Train a quick RF and return feature importance.
    """
    features = ['diff_vv', 'diff_vh', 'rain_sum_12h', 'rain_max_12h']
    # If rain cols exist. Otherwise use what's available.
    # Checking CSV columns usually: diff_vv, diff_vh, month, delay_bin...
    # The balanced dataset might not have detailed rain info unless merged.
    # Let's check available columns.
    
    # Assuming basic features for now. 
    # If 'diff_vh' has NaNs, fill them.
    X = df[['diff_vv', 'diff_vh']].copy()
    X = X.fillna(0) # Simple fill
    y = df['label']
    
    clf = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
    clf.fit(X, y)
    
    return dict(zip(X.columns, clf.feature_importances_))

def main():
    print("Loading balanced dataset...")
    df = pd.read_csv(DATASET_PATH)
    aug = df[df['month'] == 8].copy()
    
    delays = [0, 1]
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    summary = []
    
    for i, delay in enumerate(delays):
        subset = aug[aug['delay_bin'] == delay]
        road = subset[subset['label'] == 0]['diff_vv']
        paddy = subset[subset['label'] == 1]['diff_vv']
        
        # 1. Calc OVL
        ovl = calculate_overlap(road, paddy)
        
        # 2. Feature Importance
        imp = analyze_feature_importance(subset, delay)
        
        print(f"Delay {delay}h:")
        print(f"  Overlap Coefficient: {ovl:.4f} (Lower means better separation)")
        print(f"  Feature Importance: {imp}")
        
        summary.append({
            'Delay': delay,
            'OVL': ovl,
            'Imp_VV': imp.get('diff_vv', 0),
            'Imp_VH': imp.get('diff_vh', 0)
        })
        
        # 3. 2D Scatter Plot
        ax = axes[i]
        
        # Sample for clean plot
        subset_sample = subset.sample(n=min(len(subset), 2000), random_state=42)
        
        sns.scatterplot(data=subset_sample, x='diff_vv', y='diff_vh', hue='label', 
                        style='label', alpha=0.5, ax=ax, palette={0: 'blue', 1: 'green'})
        
        ax.set_title(f"Delay {delay}h (OVL={ovl:.2f})")
        ax.set_xlabel("Diff VV")
        ax.set_ylabel("Diff VH")
        ax.grid(True, alpha=0.3)
        ax.set_xlim(-10, 5)
        ax.set_ylim(-10, 5)

    plt.suptitle("8月 Delay 0h vs 1h 詳細比較", fontsize=16)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_Delay0vs1_詳細分析.png", dpi=150)
    print(f"Saved: {OUTPUT_DIR}/8月_Delay0vs1_詳細分析.png")
    
    print("\nSummary:")
    print(pd.DataFrame(summary))

if __name__ == "__main__":
    main()
