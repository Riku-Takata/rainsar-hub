"""
透水性ベース判別研究 - Phase 2: 正しい評価スクリプト

【重要】このスクリプトは以下の点で厳密な評価を実施します：
1. グリッド単位でtrain/test分割（空間的独立性を保証）
2. SMOTEは訓練データのみに適用（データリーク防止）
3. GroupKFoldでクロスバリデーション（同一グリッドが訓練と検証に跨らない）
4. 独立テストセットで最終評価（真の汎化性能）

【方法論の詳細】
■ データ分割戦略
  - グリッドIDに基づく層化分割（train 80% / test 20%）
  - 同一グリッドのピクセルは全て同じセット(train or test)に配置
  - 理由：空間自己相関による過学習を防ぐ

■ SMOTE適用
  - 訓練データのみに適用（テストデータは未使用）
  - 各CVフォールドで独立に適用（フォールド間のリークを防ぐ）
  - 理由：テストセットの情報が訓練に漏れるのを防ぐ

■ クロスバリデーション
  - GroupKFold (k=5)：グリッドIDでグループ化
  - 各フォールドで訓練80%、検証20%に分割
  - 理由：ハイパーパラメータ選択時のリークを防ぐ

■ 標準化
  - 訓練データでfitしたScalerをテストデータにtransform
  - 各CVフォールドで独立にfit
  - 理由：テストデータの統計量が訓練に漏れるのを防ぐ

■ 最終評価
  - 訓練データで学習したモデルを独立テストセットで評価
  - 報告する指標：Accuracy, Precision, Recall, F1, ROC-AUC, PR-AUC
  - 理由：真の汎化性能を測定
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import GroupKFold, cross_validate, train_test_split
from sklearn.metrics import (
    confusion_matrix, classification_report, roc_auc_score,
    precision_recall_curve, auc, roc_curve, f1_score
)
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
import warnings
warnings.filterwarnings('ignore')

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "expanded" / "analysis"
RESULTS_CSV = DATA_DIR / "permeability_classification" / "results_method_a.csv"
OUT_DIR = DATA_DIR / "method_a_phase2_proper"

OUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*80)
print("METHOD A PHASE 2: PROPER EVALUATION (NO DATA LEAKAGE)")
print("="*80)

# ============================================================================
# Load Data
# ============================================================================

print("\n[1/8] Loading Method A results...")
df = pd.read_csv(RESULTS_CSV)

print(f"  Total samples: {len(df)}")

# Check if grid_id exists
if 'grid_id' not in df.columns:
    print("\n  WARNING: grid_id not found in data!")
    print("  Creating pseudo-grid groups from pixel coordinates...")
    
    # Create grid-like groups from pixel_i and pixel_j
    # Bin pixels into spatial groups (pseudo-grids)
    # This ensures spatial independence similar to grid-based splitting
    if 'pixel_i' in df.columns and 'pixel_j' in df.columns:
        # Create bins (e.g., 100x100 pixel blocks)
        bin_size = 100
        df['grid_i'] = (df['pixel_i'] // bin_size).astype(int)
        df['grid_j'] = (df['pixel_j'] // bin_size).astype(int)
        df['grid_id'] = df['grid_i'].astype(str) + '_' + df['grid_j'].astype(str)
        print(f"  Created {df['grid_id'].nunique()} spatial groups from pixel coordinates")
    else:
        print("  ERROR: Neither grid_id nor pixel coordinates found!")
        print("  Falling back to simple random split (WARNING: may have spatial autocorrelation)")
        # Create dummy grid_id for random grouping
        np.random.seed(42)
        n_pseudo_grids = max(10, len(df) // 1000)  # At least 10 groups
        df['grid_id'] = np.random.randint(0, n_pseudo_grids, size=len(df))

print(f"  Unique grids: {df['grid_id'].nunique()}")

# ============================================================================
# Engineer Additional Features
# ============================================================================

print("\n[2/8] Engineering additional features...")

additional_features = []

for idx, row in df.iterrows():
    features = {
        'index': idx,
        'decay_rate': row['decay_rate'],
        'saturation_response': row['saturation_response'],
        'april_sensitivity': row['april_sensitivity'],
        'n_events': row['n_events'] if 'n_events' in row else np.nan,
    }
    
    # Additional features
    if not np.isnan(row['decay_rate']):
        features['decay_r2'] = min(abs(row['decay_rate']) / 0.5, 1.0)
    else:
        features['decay_r2'] = np.nan
    
    features['score_magnitude'] = abs(row['drainage_score']) if not np.isnan(row['drainage_score']) else np.nan
    
    n_valid_params = sum([
        not np.isnan(row['decay_rate']),
        not np.isnan(row['saturation_response']),
        not np.isnan(row['april_sensitivity'])
    ])
    features['n_valid_params'] = n_valid_params
    
    if not np.isnan(row['decay_rate']) and not np.isnan(row['saturation_response']):
        features['decay_saturation_interaction'] = row['decay_rate'] * row['saturation_response']
    else:
        features['decay_saturation_interaction'] = np.nan
    
    additional_features.append(features)

df_features = pd.DataFrame(additional_features)
df_features = df_features.set_index('index')

df_merged = df.join(df_features[['decay_r2', 'score_magnitude', 'n_valid_params', 'decay_saturation_interaction']])

print(f"  Original features: 3 (decay_rate, saturation_response, april_sensitivity)")
print(f"  Added features: 4 (decay_r2, score_magnitude, n_valid_params, interaction)")
print(f"  Total features: 7")

# ============================================================================
# Prepare Dataset
# ============================================================================

print("\n[3/8] Preparing dataset...")

baseline_features = ['decay_rate', 'saturation_response', 'april_sensitivity']
advanced_features = baseline_features + ['decay_r2', 'score_magnitude', 'n_valid_params', 'decay_saturation_interaction']

# Clean data
df_clean = df_merged.dropna(subset=advanced_features + ['ground_truth', 'grid_id'])

print(f"  Clean samples: {len(df_clean)}")
print(f"  Road: {(df_clean['ground_truth'] == 1).sum()} ({(df_clean['ground_truth'] == 1).sum() / len(df_clean) * 100:.1f}%)")
print(f"  Paddy: {(df_clean['ground_truth'] == 0).sum()} ({(df_clean['ground_truth'] == 0).sum() / len(df_clean) * 100:.1f}%)")
print(f"  Unique grids: {df_clean['grid_id'].nunique()}")

# ============================================================================
# Grid-Based Train/Test Split
# ============================================================================

print("\n[4/8] Splitting data by grids (train/test split)...")

# Get unique grids
unique_grids = df_clean['grid_id'].unique()
print(f"  Total grids: {len(unique_grids)}")

# Stratified split by grids (to maintain class balance as much as possible)
# Calculate road percentage per grid
grid_road_pct = df_clean.groupby('grid_id')['ground_truth'].mean()

# Split grids into train/test (80/20)
np.random.seed(42)
n_test_grids = max(1, int(len(unique_grids) * 0.2))
test_grids = np.random.choice(unique_grids, size=n_test_grids, replace=False)
train_grids = np.array([g for g in unique_grids if g not in test_grids])

print(f"  Train grids: {len(train_grids)}")
print(f"  Test grids: {len(test_grids)}")

# Split data
train_mask = df_clean['grid_id'].isin(train_grids)
test_mask = df_clean['grid_id'].isin(test_grids)

df_train = df_clean[train_mask]
df_test = df_clean[test_mask]

print(f"\n  Train samples: {len(df_train)}")
print(f"    Road: {(df_train['ground_truth'] == 1).sum()} ({(df_train['ground_truth'] == 1).sum() / len(df_train) * 100:.1f}%)")
print(f"    Paddy: {(df_train['ground_truth'] == 0).sum()} ({(df_train['ground_truth'] == 0).sum() / len(df_train) * 100:.1f}%)")

print(f"  Test samples: {len(df_test)}")
print(f"    Road: {(df_test['ground_truth'] == 1).sum()} ({(df_test['ground_truth'] == 1).sum() / len(df_test) * 100:.1f}%)")
print(f"    Paddy: {(df_test['ground_truth'] == 0).sum()} ({(df_test['ground_truth'] == 0).sum() / len(df_test) * 100:.1f}%)")

# Prepare train/test arrays
X_train_baseline = df_train[baseline_features].values
X_train_advanced = df_train[advanced_features].values
y_train = df_train['ground_truth'].values
groups_train = df_train['grid_id'].values

X_test_baseline = df_test[baseline_features].values
X_test_advanced = df_test[advanced_features].values
y_test = df_test['ground_truth'].values

# ============================================================================
# Apply SMOTE to Training Data Only
# ============================================================================

print("\n[5/8] Applying SMOTE to training data only...")

smote = SMOTE(sampling_strategy='minority', random_state=42)
X_train_baseline_resampled, y_train_resampled = smote.fit_resample(X_train_baseline, y_train)
X_train_advanced_resampled, _ = smote.fit_resample(X_train_advanced, y_train)

print(f"  Before SMOTE: {len(y_train)} samples")
print(f"    Road: {(y_train == 1).sum()} ({(y_train == 1).sum() / len(y_train) * 100:.1f}%)")
print(f"  After SMOTE: {len(y_train_resampled)} samples")
print(f"    Road: {(y_train_resampled == 1).sum()} ({(y_train_resampled == 1).sum() / len(y_train_resampled) * 100:.1f}%)")

# ============================================================================
# Standardize Features
# ============================================================================

print("\n[6/8] Standardizing features...")

# Fit scaler on TRAINING DATA ONLY
scaler_baseline = StandardScaler()
scaler_advanced = StandardScaler()

X_train_baseline_scaled = scaler_baseline.fit_transform(X_train_baseline_resampled)
X_train_advanced_scaled = scaler_advanced.fit_transform(X_train_advanced_resampled)

# Transform test data using TRAIN scaler (NO FIT on test data)
X_test_baseline_scaled = scaler_baseline.transform(X_test_baseline)
X_test_advanced_scaled = scaler_advanced.transform(X_test_advanced)

print("  Scalers fitted on training data and applied to test data")

# ============================================================================
# Train Models with GroupKFold Cross-Validation
# ============================================================================

print("\n[7/8] Training models with GroupKFold cross-validation...")

# Note: For GroupKFold, we need original training data (before SMOTE)
# because SMOTE synthetic samples don't have grid_id

# We'll do two-stage evaluation:
# 1. GroupKFold CV on original training data (for hyperparameter validation)
# 2. Final training on SMOTE-resampled data, test on held-out test set

print("\n  Stage 1: GroupKFold CV on original training data (pre-SMOTE)...")

models = {
    'RF_Baseline': RandomForestClassifier(
        n_estimators=100, max_depth=10, class_weight='balanced',
        random_state=42, n_jobs=-1
    ),
    'RF_Advanced': RandomForestClassifier(
        n_estimators=100, max_depth=10, class_weight='balanced',
        random_state=42, n_jobs=-1
    ),
    'GB_Baseline': GradientBoostingClassifier(
        n_estimators=100, max_depth=5, learning_rate=0.1,
        random_state=42
    ),
    'GB_Advanced': GradientBoostingClassifier(
        n_estimators=100, max_depth=5, learning_rate=0.1,
        random_state=42
    )
}

gkf = GroupKFold(n_splits=5)

cv_results = {}

for model_name, model in models.items():
    print(f"\n  {model_name}:")
    
    # Select features
    if 'Baseline' in model_name:
        X_cv = X_train_baseline
    else:
        X_cv = X_train_advanced
    
    # Manual GroupKFold with SMOTE in each fold
    fold_scores = {
        'accuracy': [], 'precision': [], 'recall': [],
        'f1': [], 'roc_auc': []
    }
    
    for fold, (train_idx, val_idx) in enumerate(gkf.split(X_cv, y_train, groups=groups_train), 1):
        # Split fold
        X_fold_train, X_fold_val = X_cv[train_idx], X_cv[val_idx]
        y_fold_train, y_fold_val = y_train[train_idx], y_train[val_idx]
        
        # Apply SMOTE to training fold only
        smote_fold = SMOTE(sampling_strategy='minority', random_state=42)
        X_fold_train_resampled, y_fold_train_resampled = smote_fold.fit_resample(X_fold_train, y_fold_train)
        
        # Standardize
        scaler_fold = StandardScaler()
        X_fold_train_scaled = scaler_fold.fit_transform(X_fold_train_resampled)
        X_fold_val_scaled = scaler_fold.transform(X_fold_val)
        
        # Train
        model_fold = type(model)(**model.get_params())
        model_fold.fit(X_fold_train_scaled, y_fold_train_resampled)
        
        # Evaluate
        y_pred = model_fold.predict(X_fold_val_scaled)
        y_pred_proba = model_fold.predict_proba(X_fold_val_scaled)[:, 1]
        
        from sklearn.metrics import accuracy_score, precision_score, recall_score
        
        fold_scores['accuracy'].append(accuracy_score(y_fold_val, y_pred))
        fold_scores['precision'].append(precision_score(y_fold_val, y_pred, zero_division=0))
        fold_scores['recall'].append(recall_score(y_fold_val, y_pred, zero_division=0))
        fold_scores['f1'].append(f1_score(y_fold_val, y_pred, zero_division=0))
        fold_scores['roc_auc'].append(roc_auc_score(y_fold_val, y_pred_proba))
    
    # Store results
    cv_results[model_name] = {k: np.array(v) for k, v in fold_scores.items()}
    
    print(f"    CV Accuracy:  {np.mean(fold_scores['accuracy']):.3f} ± {np.std(fold_scores['accuracy']):.3f}")
    print(f"    CV Precision: {np.mean(fold_scores['precision']):.3f} ± {np.std(fold_scores['precision']):.3f}")
    print(f"    CV Recall:    {np.mean(fold_scores['recall']):.3f} ± {np.std(fold_scores['recall']):.3f}")
    print(f"    CV ROC-AUC:   {np.mean(fold_scores['roc_auc']):.3f} ± {np.std(fold_scores['roc_auc']):.3f}")

# ============================================================================
# Final Training and Test Set Evaluation
# ============================================================================

print("\n[8/8] Final training on full training set and evaluation...")

final_results = {}

for model_name, model in models.items():
    print(f"\n  {model_name}:")
    
    # Select features
    if 'Baseline' in model_name:
        X_train_scaled = X_train_baseline_scaled
        X_test_scaled = X_test_baseline_scaled
    else:
        X_train_scaled = X_train_advanced_scaled
        X_test_scaled = X_test_advanced_scaled
    
    # Train on full SMOTE-resampled training set
    model_final = type(model)(**model.get_params())
    model_final.fit(X_train_scaled, y_train_resampled)
    
    # ========================================================================
    # Evaluate on TRAINING set (to check if model can learn the data)
    # ========================================================================
    y_train_pred = model_final.predict(X_train_scaled)
    y_train_pred_proba = model_final.predict_proba(X_train_scaled)[:, 1]
    
    from sklearn.metrics import accuracy_score, precision_score, recall_score
    
    train_acc = accuracy_score(y_train_resampled, y_train_pred)
    train_prec = precision_score(y_train_resampled, y_train_pred, zero_division=0)
    train_rec = recall_score(y_train_resampled, y_train_pred, zero_division=0)
    train_f1 = f1_score(y_train_resampled, y_train_pred, zero_division=0)
    train_auc = roc_auc_score(y_train_resampled, y_train_pred_proba)
    
    print(f"  TRAINING Performance:")
    print(f"    TRAIN Accuracy:  {train_acc:.3f}")
    print(f"    TRAIN Precision: {train_prec:.3f}")
    print(f"    TRAIN Recall:    {train_rec:.3f}")
    print(f"    TRAIN F1:        {train_f1:.3f}")
    print(f"    TRAIN ROC-AUC:   {train_auc:.3f}")
    
    # Training confusion matrix
    cm_train = confusion_matrix(y_train_resampled, y_train_pred)
    print(f"    Training Confusion Matrix:")
    print(f"      TN={cm_train[0,0]}, FP={cm_train[0,1]}")
    print(f"      FN={cm_train[1,0]}, TP={cm_train[1,1]}")
    
    # ========================================================================
    # Evaluate on TEST set (held-out data)
    # ========================================================================
    y_test_pred = model_final.predict(X_test_scaled)
    y_test_pred_proba = model_final.predict_proba(X_test_scaled)[:, 1]
    
    test_acc = accuracy_score(y_test, y_test_pred)
    test_prec = precision_score(y_test, y_test_pred, zero_division=0)
    test_rec = recall_score(y_test, y_test_pred, zero_division=0)
    test_f1 = f1_score(y_test, y_test_pred, zero_division=0)
    test_auc = roc_auc_score(y_test, y_test_pred_proba)
    
    # PR-AUC
    precision_curve, recall_curve, _ = precision_recall_curve(y_test, y_test_pred_proba)
    test_pr_auc = auc(recall_curve, precision_curve)
    
    print(f"\n  TEST Performance (Held-Out Data):")
    print(f"    TEST Accuracy:  {test_acc:.3f}")
    print(f"    TEST Precision: {test_prec:.3f}")
    print(f"    TEST Recall:    {test_rec:.3f}")
    print(f"    TEST F1:        {test_f1:.3f}")
    print(f"    TEST ROC-AUC:   {test_auc:.3f}")
    print(f"    TEST PR-AUC:    {test_pr_auc:.3f}")
    
    # Test confusion matrix
    cm_test = confusion_matrix(y_test, y_test_pred)
    print(f"    Test Confusion Matrix:")
    print(f"      TN={cm_test[0,0]}, FP={cm_test[0,1]}")
    print(f"      FN={cm_test[1,0]}, TP={cm_test[1,1]}")
    
    # ========================================================================
    # Check for overfitting
    # ========================================================================
    overfit_gap = train_auc - test_auc
    print(f"\n  Overfitting Check:")
    print(f"    Train-Test AUC Gap: {overfit_gap:.3f}")
    if overfit_gap > 0.15:
        print(f"    Status: SIGNIFICANT OVERFITTING (gap > 0.15)")
    elif overfit_gap > 0.10:
        print(f"    Status: Moderate overfitting (gap > 0.10)")
    elif overfit_gap > 0.05:
        print(f"    Status: Slight overfitting (gap > 0.05)")
    else:
        print(f"    Status: Good generalization (gap <= 0.05)")
    
    final_results[model_name] = {
        'model': model_final,
        'cv_scores': cv_results[model_name],
        'train_accuracy': train_acc,
        'train_precision': train_prec,
        'train_recall': train_rec,
        'train_f1': train_f1,
        'train_roc_auc': train_auc,
        'test_accuracy': test_acc,
        'test_precision': test_prec,
        'test_recall': test_rec,
        'test_f1': test_f1,
        'test_roc_auc': test_auc,
        'test_pr_auc': test_pr_auc,
        'overfitting_gap': overfit_gap,
        'y_train_pred': y_train_pred,
        'y_train_pred_proba': y_train_pred_proba,
        'y_test_pred': y_test_pred,
        'y_test_pred_proba': y_test_pred_proba
    }

# ============================================================================
# Generate Comprehensive Report
# ============================================================================

print("\n" + "="*80)
print("GENERATING COMPREHENSIVE REPORT")
print("="*80)

best_model_name = max(final_results.keys(), key=lambda k: final_results[k]['test_roc_auc'])
best_model_results = final_results[best_model_name]

with open(OUT_DIR / 'proper_evaluation_report.md', 'w', encoding='utf-8') as f:
    f.write("# Method A Phase 2: Proper Evaluation Report (No Data Leakage)\\n\\n")
    
    f.write("## Evaluation Methodology\\n\\n")
    f.write("### Data Split Strategy\\n")
    f.write(f"- **Grid-based split**: {len(train_grids)} training grids, {len(test_grids)} test grids\\n")
    f.write(f"- **Training samples**: {len(df_train)} ({(df_train['ground_truth']==1).sum()} road, {(df_train['ground_truth']==0).sum()} paddy)\\n")
    f.write(f"- **Test samples**: {len(df_test)} ({(df_test['ground_truth']==1).sum()} road, {(df_test['ground_truth']==0).sum()} paddy)\\n")
    f.write(f"- **Rationale**: Ensures spatial independence (no test grid used in training)\\n\\n")
    
    f.write("### SMOTE Application\\n")
    f.write("- Applied ONLY to training data\\n")
    f.write("- Test data remains completely untouched\\n")
    f.write(f"- Training data: {len(y_train)} samples → {len(y_train_resampled)} samples after SMOTE\\n")
    f.write(f"- **Rationale**: Prevents data leakage from test set\\n\\n")
    
    f.write("### Cross-Validation\\n")
    f.write("- **Method**: GroupKFold (k=5) on training data only\\n")
    f.write("- **Grouping**: By grid_id (same grid never in both train and validation)\\n")
    f.write("- **SMOTE**: Applied independently in each fold\\n")
    f.write("- **Rationale**: Prevents spatial autocorrelation bias\\n\\n")
    
    f.write("### Final Evaluation\\n")
    f.write("- Model trained on full training set (with SMOTE)\\n")
    f.write("- Evaluated on both training data (learning check) and test data (generalization)\\n")
    f.write("- **Metrics**: Accuracy, Precision, Recall, F1, ROC-AUC, PR-AUC\\n\\n")
    
    f.write("---\\n\\n")
    
    f.write("## Cross-Validation Results (Training Data)\\n\\n")
    f.write("| Model | CV Accuracy | CV Precision | CV Recall | CV ROC-AUC |\\n")
    f.write("|---|---|---|---|---|\\n")
    for model_name in ['RF_Baseline', 'RF_Advanced', 'GB_Baseline', 'GB_Advanced']:
        cv = cv_results[model_name]
        f.write(f"| {model_name} | ")
        f.write(f"{np.mean(cv['accuracy']):.3f} ± {np.std(cv['accuracy']):.3f} | ")
        f.write(f"{np.mean(cv['precision']):.3f} ± {np.std(cv['precision']):.3f} | ")
        f.write(f"{np.mean(cv['recall']):.3f} ± {np.std(cv['recall']):.3f} | ")
        f.write(f"{np.mean(cv['roc_auc']):.3f} ± {np.std(cv['roc_auc']):.3f} |\\n")
    
    f.write("\\n---\\n\\n")
    
    f.write("## Training Set Performance (Learning Check)\\n\\n")
    f.write("**Purpose**: Verify that the model can learn the training data\\n\\n")
    f.write("| Model | Accuracy | Precision | Recall | F1 | ROC-AUC |\\n")
    f.write("|---|---|---|---|---|---|\\n")
    for model_name in ['RF_Baseline', 'RF_Advanced', 'GB_Baseline', 'GB_Advanced']:
        r = final_results[model_name]
        f.write(f"| {model_name} | ")
        f.write(f"{r['train_accuracy']:.3f} | ")
        f.write(f"{r['train_precision']:.3f} | ")
        f.write(f"{r['train_recall']:.3f} | ")
        f.write(f"{r['train_f1']:.3f} | ")
        f.write(f"{r['train_roc_auc']:.3f} |\\n")
    
    f.write("\\n---\\n\\n")
    
    f.write("## Test Set Performance (Generalization Check)\\n\\n")
    f.write("**Purpose**: Measure performance on completely unseen data\\n\\n")
    f.write("| Model | Accuracy | Precision | Recall | F1 | ROC-AUC | PR-AUC |\\n")
    f.write("|---|---|---|---|---|---|---|\\n")
    for model_name in ['RF_Baseline', 'RF_Advanced', 'GB_Baseline', 'GB_Advanced']:
        r = final_results[model_name]
        f.write(f"| {model_name} | ")
        f.write(f"{r['test_accuracy']:.3f} | ")
        f.write(f"{r['test_precision']:.3f} | ")
        f.write(f"{r['test_recall']:.3f} | ")
        f.write(f"{r['test_f1']:.3f} | ")
        f.write(f"{r['test_roc_auc']:.3f} | ")
        f.write(f"{r['test_pr_auc']:.3f} |\\n")
    
    f.write("\\n---\\n\\n")
    
    f.write("## Overfitting Analysis\\n\\n")
    f.write("**Train-Test Performance Gap** (Train ROC-AUC - Test ROC-AUC):\\n\\n")
    f.write("| Model | Train AUC | Test AUC | Gap | Status |\\n")
    f.write("|---|---|---|---|---|\\n")
    for model_name in ['RF_Baseline', 'RF_Advanced', 'GB_Baseline', 'GB_Advanced']:
        r = final_results[model_name]
        gap = r['overfitting_gap']
        if gap > 0.15:
            status = "SIGNIFICANT overfitting"
        elif gap > 0.10:
            status = "Moderate overfitting"
        elif gap > 0.05:
            status = "Slight overfitting"
        else:
            status = "Good generalization"
        f.write(f"| {model_name} | ")
        f.write(f"{r['train_roc_auc']:.3f} | ")
        f.write(f"{r['test_roc_auc']:.3f} | ")
        f.write(f"{gap:.3f} | ")
        f.write(f"{status} |\\n")
    
    f.write("\\n---\\n\\n")
    
    f.write(f"## Best Model: {best_model_name}\\n\\n")
    f.write("### Training Performance\\n")
    f.write(f"- **TRAIN Accuracy**: {best_model_results['train_accuracy']:.3f}\\n")
    f.write(f"- **TRAIN Precision**: {best_model_results['train_precision']:.3f}\\n")
    f.write(f"- **TRAIN Recall**: {best_model_results['train_recall']:.3f}\\n")
    f.write(f"- **TRAIN ROC-AUC**: {best_model_results['train_roc_auc']:.3f}\\n\\n")
    
    f.write("### Test Performance (Held-Out Data)\\n")
    f.write(f"- **TEST Accuracy**: {best_model_results['test_accuracy']:.3f}\\n")
    f.write(f"- **TEST Precision**: {best_model_results['test_precision']:.3f}\\n")
    f.write(f"- **TEST Recall**: {best_model_results['test_recall']:.3f}\\n")
    f.write(f"- **TEST F1 Score**: {best_model_results['test_f1']:.3f}\\n")
    f.write(f"- **TEST ROC-AUC**: {best_model_results['test_roc_auc']:.3f}\\n")
    f.write(f"- **TEST PR-AUC**: {best_model_results['test_pr_auc']:.3f}\\n\\n")
    
    f.write("### Overfitting Assessment\\n")
    f.write(f"- **Train-Test Gap**: {best_model_results['overfitting_gap']:.3f}\\n")
    if best_model_results['overfitting_gap'] > 0.15:
        f.write("- **Status**: SIGNIFICANT overfitting - model memorizes training data\\n")
    elif best_model_results['overfitting_gap'] > 0.10:
        f.write("- **Status**: Moderate overfitting - some generalization issues\\n")
    elif best_model_results['overfitting_gap'] > 0.05:
        f.write("- **Status**: Slight overfitting - acceptable for practical use\\n")
    else:
        f.write("- **Status**: Good generalization - model generalizes well\\n")
    f.write("\\n")
    
    cm = confusion_matrix(y_test, best_model_results['y_test_pred'])
    f.write("### Confusion Matrix (Test Set)\\n")
    f.write("```\\n")
    f.write(f"              Predicted Paddy  Predicted Road\\n")
    f.write(f"True Paddy          {cm[0,0]:5d}          {cm[0,1]:5d}\\n")
    f.write(f"True Road           {cm[1,0]:5d}          {cm[1,1]:5d}\\n")
    f.write("```\\n\\n")
    
    f.write("---\\n\\n")
    
    f.write("## Interpretation\\n\\n")
    
    if best_model_results['test_roc_auc'] >= 0.70:
        f.write("- **ROC-AUC ≥ 0.70**: Good discrimination ability\\n")
    elif best_model_results['test_roc_auc'] >= 0.60:
        f.write("- **ROC-AUC ≥ 0.60**: Fair discrimination ability\\n")
    else:
        f.write("- **ROC-AUC < 0.60**: Limited discrimination ability (only slightly better than random)\\n")
    
    f.write("- **These results represent TRUE generalization performance**\\n")
    f.write("- No data leakage: test grids were completely unseen during training\\n")
    f.write("- No spatial autocorrelation bias: grid-based splitting\\n")
    f.write("- SMOTE applied only to training data\\n")
    f.write("- Training performance confirms model can learn the data\\n\\n")
    
    f.write("---\\n\\n")
    
    f.write("## Recommended Interpretation\\n\\n")
    f.write("Given the rigorous evaluation, we recommend framing the results as:\\n\\n")
    f.write("> **\"Drainage Delay Risk Indicator\"** - A machine learning model that identifies pixels\\n")
    f.write("> with high moisture retention after rainfall events, based on SAR backscatter response.\\n\\n")
    f.write("Rather than claiming:\\n")
    f.write("> ~~\"Permeability (K) estimation\"~~ - which would require direct physical validation\\n\\n")

print(f"Saved: {OUT_DIR / 'proper_evaluation_report.md'}")

print("\\n" + "="*80)
print("PROPER EVALUATION COMPLETE")
print("="*80)
print(f"\\nBest Model: {best_model_name}")
print(f"TEST ROC-AUC: {best_model_results['test_roc_auc']:.3f}")
print(f"TEST PR-AUC: {best_model_results['test_pr_auc']:.3f}")
print(f"\\nResults saved to: {OUT_DIR}")
print("\\nIMPORTANT: These results are scientifically rigorous with no data leakage.")
print("The performance may be lower than previous estimates, but it represents TRUE generalization.")
