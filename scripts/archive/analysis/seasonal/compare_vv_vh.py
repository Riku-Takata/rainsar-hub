"""
Compare RF Classification using VV-only vs VH-only features
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.metrics import accuracy_score, f1_score
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import logging

# Japanese font
matplotlib.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATASET_PATH = BASE_DIR / "data" / "result" / "seasonal" / "rf_data" / "rf_dataset_balanced.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "rf_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VV_VH_Compare")

TARGET_MONTHS = [4, 8, 9, 10]
DELAYS = range(0, 12)

def train_and_eval(X, y, feature_name):
    """Train RF and return test accuracy"""
    # Check class balance
    if len(y.unique()) < 2:
        return None
    if y.value_counts().min() < 10:
        return None
    
    sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    try:
        train_idx, test_idx = next(sss.split(X, y))
    except:
        return None
    
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    
    if len(np.unique(y_train)) < 2:
        return None
    
    # Simple pipeline with imputer
    pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('clf', RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1))
    ])
    
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    
    return accuracy_score(y_test, y_pred)

def main():
    logger.info("Loading dataset...")
    df = pd.read_csv(DATASET_PATH)
    logger.info(f"Loaded {len(df)} rows")
    
    results = []
    
    for month in TARGET_MONTHS:
        logger.info(f"--- Month {month} ---")
        month_df = df[df['month'] == month]
        
        for delay in DELAYS:
            delay_df = month_df[month_df['delay_bin'] == delay]
            
            if len(delay_df) < 100:
                continue
            
            y = delay_df['label']
            
            # VV only
            X_vv = delay_df[['diff_vv', 'total_rain', 'duration']]
            acc_vv = train_and_eval(X_vv, y, "VV")
            
            # VH only
            X_vh = delay_df[['diff_vh', 'total_rain', 'duration']]
            acc_vh = train_and_eval(X_vh, y, "VH")
            
            # Both VV+VH
            X_both = delay_df[['diff_vv', 'diff_vh', 'total_rain', 'duration']]
            acc_both = train_and_eval(X_both, y, "VV+VH")
            
            if acc_vv is not None and acc_vh is not None:
                results.append({
                    'month': month,
                    'delay': delay,
                    'acc_vv': acc_vv,
                    'acc_vh': acc_vh,
                    'acc_both': acc_both
                })
                logger.info(f"  D{delay}: VV={acc_vv:.3f}, VH={acc_vh:.3f}, Both={acc_both:.3f}")
    
    if not results:
        logger.warning("No results!")
        return
    
    res_df = pd.DataFrame(results)
    res_df.to_csv(OUTPUT_DIR / "vv_vh_comparison.csv", index=False)
    logger.info(f"Saved comparison CSV")
    
    # Visualization
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()
    
    for i, month in enumerate(TARGET_MONTHS):
        ax = axes[i]
        m_df = res_df[res_df['month'] == month]
        
        if len(m_df) == 0:
            continue
        
        ax.plot(m_df['delay'], m_df['acc_vv'], 'o-', label='VVのみ', color='blue')
        ax.plot(m_df['delay'], m_df['acc_vh'], 's-', label='VHのみ', color='green')
        ax.plot(m_df['delay'], m_df['acc_both'], '^-', label='VV+VH', color='red')
        
        ax.set_title(f"{month}月")
        ax.set_xlabel("経過時間 (h)")
        ax.set_ylabel("テスト精度")
        ax.set_ylim(0.4, 1.0)
        ax.legend()
        ax.grid(True)
    
    plt.suptitle("VV vs VH 偏波比較 (道路 vs 田んぼ分類)", fontsize=14)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "plot_vv_vh_comparison.png")
    logger.info("Saved comparison plot")

if __name__ == "__main__":
    main()
