
import os
import sys
import logging
import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, f1_score, recall_score
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# Setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATASET_PATH = BASE_DIR / "data" / "result" / "seasonal" / "rf_data" / "rf_dataset_balanced.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "rf_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RF_Train")

# Config
TARGET_MONTHS = [4, 8, 9, 10]
DELAYS = range(0, 13) # 0-12

def build_pipeline():
    # Features: diff_vv, diff_vh, total_rain, duration
    # diff_vh might be NaN
    
    numeric_features = ['diff_vv', 'diff_vh', 'total_rain', 'duration']
    
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median'))
    ])
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features)
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

def train_and_eval(df_subset, month, delay, results_list):
    # Features
    X = df_subset[['diff_vv', 'diff_vh', 'total_rain', 'duration']]
    y = df_subset['label']
    groups = df_subset['grid_id']
    
    # Check class balance
    class_counts = y.value_counts()
    if len(class_counts) < 2:
        logger.warning(f"M{month} D{delay}: Only one class. Skipping.")
        return
    
    min_class_count = class_counts.min()
    if min_class_count < 10:
        logger.warning(f"M{month} D{delay}: Min class count {min_class_count} < 10. Skipping.")
        return
    
    # Use StratifiedShuffleSplit to ensure both classes in Train and Test
    sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    try:
        train_idx, test_idx = next(sss.split(X, y))
    except ValueError as e:
        logger.warning(f"M{month} D{delay}: Split failed ({e}). Skipping.")
        return
    
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
    
    # Check class balance
    train_dist = y_train.value_counts(normalize=True)
    test_dist = y_test.value_counts(normalize=True)
    # logger.info(f"M{month} D{delay} | Train Size: {len(X_train)} (Road: {train_dist.get(0,0):.2f}) | Test Size: {len(X_test)}")
    
    if len(np.unique(y_train)) < 2:
        logger.warning(f"M{month} D{delay}: Only one class in training set. Skipping.")
        return

    # Train
    model = build_pipeline()
    model.fit(X_train, y_train)
    
    # Evaluate on Train (Model Verification)
    y_train_pred = model.predict(X_train)
    train_acc = accuracy_score(y_train, y_train_pred)
    train_f1 = f1_score(y_train, y_train_pred, average='macro')
    
    # Evaluate on Test (Validation)
    y_test_pred = model.predict(X_test)
    test_acc = accuracy_score(y_test, y_test_pred)
    test_f1 = f1_score(y_test, y_test_pred, average='macro')
    
    tc = confusion_matrix(y_test, y_test_pred, labels=[0, 1])
    # tc is [[TN, FP], [FN, TP]] -> Road=0, Paddy=1
    # Road Acc = TN / (TN+FP) -> Actually Row 0 / Sum Row 0
    # Paddy Acc = TP / (FN+TP) -> Row 1 / Sum Row 1
    # This is Recall.
    
    recalls = recall_score(y_test, y_test_pred, average=None, labels=[0, 1])
    test_acc_road = recalls[0]
    test_acc_paddy = recalls[1] if len(recalls) > 1 else 0 # Should be 2 if stratify worked, but here group split
    
    params = model.named_steps['classifier'].get_params()
    
    # Identify Grid IDs in Test (informational)
    test_grids = groups.iloc[test_idx].unique()
    
    res = {
        'month': month,
        'delay': delay,
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'train_accuracy': train_acc,
        'train_f1': train_f1,
        'test_accuracy': test_acc,
        'test_f1': test_f1,
        'test_acc_road': test_acc_road,
        'test_acc_paddy': test_acc_paddy,
        'tn': tc[0, 0],
        'fp': tc[0, 1],
        'fn': tc[1, 0],
        'tp': tc[1, 1],
        'test_grids_count': len(test_grids)
    }
    results_list.append(res)
    
    # Save Model (Optional, maybe too many files)
    # joblib.dump(model, OUTPUT_DIR / f"rf_model_m{month}_d{delay}.joblib")
    
    # logger.info(f"  Train Acc: {train_acc:.3f}, Test Acc: {test_acc:.3f}")

def main():
    if not DATASET_PATH.exists():
        logger.error(f"Dataset not found: {DATASET_PATH}")
        return
        
    logger.info("Loading dataset...")
    df = pd.read_csv(DATASET_PATH)
    logger.info(f"Loaded {len(df)} rows.")
    
    # Filter valid columns
    # df.dropna(subset=['diff_vv'], inplace=True) # Already done in prep
    
    results = []
    
    for month in TARGET_MONTHS:
        logger.info(f"--- Processing Month {month} ---")
        month_df = df[df['month'] == month]
        
        if len(month_df) == 0:
            logger.warning(f"No data for Month {month}")
            continue
            
        for delay in DELAYS:
            delay_df = month_df[month_df['delay_bin'] == delay]
            
            if len(delay_df) < 50: # Minimum samples
                # logger.warning(f"Not enough data for M{month} D{delay} ({len(delay_df)})")
                continue
                
            train_and_eval(delay_df, month, delay, results)
            
    # Save Results
    if results:
        res_df = pd.DataFrame(results)
        result_csv = OUTPUT_DIR / "rf_accuracy_metrics.csv"
        res_df.to_csv(result_csv, index=False)
        logger.info(f"Saved metrics to {result_csv}")
        
        # Visualization
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=res_df, x='delay', y='test_accuracy', hue='month', marker='o', palette='tab10')
        plt.title(f"RF Classification Accuracy (Road vs Paddy) by Delay")
        plt.ylabel("Test Accuracy")
        plt.xlabel("Delay (h)")
        plt.grid(True)
        plt.ylim(0.4, 1.0)
        plt.savefig(OUTPUT_DIR / "plot_rf_accuracy_by_delay.png")
        logger.info("Saved Acc plot.")
        
        # Train vs Test Acc comparison
        plt.figure(figsize=(10, 6))
        # Melt
        df_melt = res_df.melt(id_vars=['month', 'delay'], value_vars=['train_accuracy', 'test_accuracy'], var_name='Type', value_name='Accuracy')
        sns.relplot(data=df_melt, x='delay', y='Accuracy', hue='month', style='Type', kind='line', markers=True, height=6, aspect=1.5)
        plt.title("RF Accuracy: Train vs Test")
        plt.grid()
        plt.savefig(OUTPUT_DIR / "plot_rf_train_vs_test_acc.png")
        
    else:
        logger.warning("No results generated.")

if __name__ == "__main__":
    main()
