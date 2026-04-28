"""
Compare RF Accuracy: VV-only vs Combined (VV+VH)
To quantify the contribution of VH polarization, especially at Delay 0h.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.metrics import accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer

matplotlib.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATASET_PATH = BASE_DIR / "data/result/seasonal/rf_data/rf_dataset_balanced.csv"
OUTPUT_DIR = BASE_DIR / "data/result/Aug"

def build_model(features):
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median'))
    ])
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, features)
        ],
        remainder='drop'
    )
    
    rf = RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        class_weight='balanced_subsample',
        n_jobs=-1
    )
    
    pipe = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', rf)
    ])
    
    return pipe

def train_and_eval(df, features):
    X = df[features]
    y = df['label']
    
    if len(np.unique(y)) < 2:
        return np.nan
    
    sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    try:
        train_idx, test_idx = next(sss.split(X, y))
    except:
        return np.nan
        
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    
    model = build_model(features)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    return accuracy_score(y_test, y_pred)

def main():
    print("Loading dataset...")
    df = pd.read_csv(DATASET_PATH)
    aug = df[df['month'] == 8].copy()
    
    delays = range(13)
    results = []
    
    # Define feature sets
    feat_combined = ['diff_vv', 'diff_vh', 'total_rain', 'duration']
    feat_vv_only = ['diff_vv', 'total_rain', 'duration']
    feat_vh_only = ['diff_vh', 'total_rain', 'duration']
    
    for delay in delays:
        subset = aug[aug['delay_bin'] == delay]
        if len(subset) < 50:
            continue
            
        print(f"Processing Delay {delay}h (n={len(subset)})...")
        
        acc_combined = train_and_eval(subset, feat_combined)
        acc_vv = train_and_eval(subset, feat_vv_only)
        acc_vh = train_and_eval(subset, feat_vh_only)
        
        results.append({
            'delay': delay,
            'Combined': acc_combined,
            'VV_Only': acc_vv,
            'VH_Only': acc_vh
        })
        
    res_df = pd.DataFrame(results)
    print("\nResults:")
    print(res_df)
    
    # Plot
    plt.figure(figsize=(10, 6))
    plt.plot(res_df['delay'], res_df['Combined'], 'o-', label='Combined (VV+VH)', color='blue')
    plt.plot(res_df['delay'], res_df['VV_Only'], 'o--', label='VV Only', color='green')
    plt.plot(res_df['delay'], res_df['VH_Only'], 'o:', label='VH Only', color='red')
    
    plt.title('8月 偏波条件別 分類精度比較 (VV vs VH vs Combined)', fontsize=14)
    plt.xlabel('Delay (h)', fontsize=12)
    plt.ylabel('テスト精度', fontsize=12)
    plt.grid(True)
    plt.legend()
    plt.ylim(0.4, 1.0)
    
    plt.savefig(OUTPUT_DIR / "8月_Polarization_Comparison.png", dpi=150)
    print(f"Saved plot: {OUTPUT_DIR}/8月_Polarization_Comparison.png")
    res_df.to_csv(OUTPUT_DIR / "8月_Polarization_Comparison.csv", index=False)

if __name__ == "__main__":
    main()
