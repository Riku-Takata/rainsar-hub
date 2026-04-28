"""
Experiment: Downsample all delays to the minimum sample count
Goal: Remove any sample size variation to see if accuracy patterns remain
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.metrics import accuracy_score
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import logging

matplotlib.rcParams['font.family'] = 'MS Gothic'

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("EqualSample")

BASE_DIR = r"D:\sotsuron\rainsar-hub"
DATASET_PATH = f"{BASE_DIR}/data/result/seasonal/rf_data/rf_dataset_balanced.csv"
OUTPUT_DIR = f"{BASE_DIR}/data/result/Aug"

def train_rf(X, y):
    if len(y.unique()) < 2 or y.value_counts().min() < 10:
        return None
    
    sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    train_idx, test_idx = next(sss.split(X, y))
    
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    
    pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('clf', RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1))
    ])
    
    pipe.fit(X_train, y_train)
    return accuracy_score(y_test, pipe.predict(X_test))

def main():
    logger.info("Loading dataset...")
    df = pd.read_csv(DATASET_PATH)
    
    # Focus on August
    aug_df = df[df['month'] == 8].copy()
    
    # Find minimum sample count per delay
    delay_counts = aug_df.groupby('delay_bin').size()
    min_count = delay_counts.min()
    min_delay = delay_counts.idxmin()
    
    logger.info(f"Minimum sample count: {min_count} (Delay {min_delay})")
    
    results = []
    
    delays = sorted(aug_df['delay_bin'].unique())
    
    for delay in delays:
        delay_data = aug_df[aug_df['delay_bin'] == delay]
        
        # Original (full balanced data)
        X_orig = delay_data[['diff_vv', 'diff_vh', 'total_rain', 'duration']]
        y_orig = delay_data['label']
        acc_orig = train_rf(X_orig, y_orig)
        
        # Downsampled to minimum
        if len(delay_data) > min_count:
            # Stratified sampling to maintain class balance
            delay_sampled = delay_data.groupby('label').apply(
                lambda x: x.sample(n=min(len(x), min_count // 2), random_state=42)
            ).reset_index(drop=True)
        else:
            delay_sampled = delay_data
        
        X_sampled = delay_sampled[['diff_vv', 'diff_vh', 'total_rain', 'duration']]
        y_sampled = delay_sampled['label']
        acc_sampled = train_rf(X_sampled, y_sampled)
        
        if acc_orig and acc_sampled:
            results.append({
                'delay': delay,
                'original_samples': len(delay_data),
                'sampled_to': len(delay_sampled),
                'acc_original': acc_orig,
                'acc_sampled': acc_sampled,
                'diff': acc_sampled - acc_orig
            })
            logger.info(f"D{delay}: Original={acc_orig:.3f} (n={len(delay_data)}), Sampled={acc_sampled:.3f} (n={len(delay_sampled)})")
    
    res_df = pd.DataFrame(results)
    res_df.to_csv(f"{OUTPUT_DIR}/8月_等サンプル実験.csv", index=False, encoding='utf-8-sig')
    
    # Visualization
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(res_df))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, res_df['acc_original'], width, label='元データ', color='steelblue')
    bars2 = ax.bar(x + width/2, res_df['acc_sampled'], width, label=f'均等サンプル (n≈{min_count})', color='orange')
    
    ax.set_xlabel('経過時間 (h)', fontsize=12)
    ax.set_ylabel('テスト精度', fontsize=12)
    ax.set_title('8月 データ数均等化実験', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(res_df['delay'])
    ax.set_ylim(0.4, 1.0)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/8月_等サンプル実験.png", dpi=150)
    logger.info("Saved plot")
    
    # Summary
    print("\n=== 結果サマリー ===")
    print(f"最小サンプル数: {min_count} (Delay {min_delay})")
    print(f"\n精度差の平均: {res_df['diff'].mean():.4f}")
    print(f"精度差の標準偏差: {res_df['diff'].std():.4f}")

if __name__ == "__main__":
    main()
