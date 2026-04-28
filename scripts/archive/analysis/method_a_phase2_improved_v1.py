"""
Method A Phase 2: 品質改善版（Week 1）

【改善内容】
1. イベント数フィルタ: ≥ 5（従来 ≥ 3）
2. 減衰率信頼性: R² ≥ 0.7
3. 降雨パターン多様性: 中程度雨・豪雨両方必須
4. 時期フィルタ:
   - 田んぼ: 3-4月のみ（乾田期、降雨応答が見える）
   - 道路: 3-11月（降雪期除外）
5. ラベル精緻化: OSM道路クラスでフィルタ（今後実装予定）

【期待効果】
- AUC: 0.559 → 0.650-0.680 (+16-22%)
- Precision: 2.2% → 12-18% (+450-720%)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import GroupKFold
from sklearn.metrics import (
    confusion_matrix, classification_report, roc_auc_score,
    precision_recall_curve, auc, roc_curve, f1_score, accuracy_score, 
    precision_score, recall_score
)
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from scipy.stats import linregress
import warnings
warnings.filterwarnings('ignore')

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "expanded" / "analysis"
RESULTS_CSV = DATA_DIR / "permeability_classification" / "results_method_a.csv"
OUT_DIR = DATA_DIR / "method_a_phase2_improved_v1"

OUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*80)
print("METHOD A PHASE 2: QUALITY IMPROVEMENT (WEEK 1)")
print("="*80)

# ============================================================================
# Load Original Data
# ============================================================================

print("\n[1/9] Loading original Method A results...")
df = pd.read_csv(RESULTS_CSV)

print(f"  Original samples: {len(df)}")

# Create pseudo-grid groups
if 'grid_id' not in df.columns:
    if 'pixel_i' in df.columns and 'pixel_j' in df.columns:
        bin_size = 100
        df['grid_i'] = (df['pixel_i'] // bin_size).astype(int)
        df['grid_j'] = (df['pixel_j'] // bin_size).astype(int)
        df['grid_id'] = df['grid_i'].astype(str) + '_' + df['grid_j'].astype(str)
        print(f"  Created {df['grid_id'].nunique()} spatial groups")

# ============================================================================
# Quality Filter 1: Minimum Events
# ============================================================================

print("\n[2/9] Applying quality filters...")

# Filter 1: Events ≥ 5
if 'n_events' in df.columns:
    df_filtered = df[df['n_events'] >= 5].copy()
    print(f"  Filter 1 (n_events ≥ 5): {len(df)} → {len(df_filtered)} samples")
else:
    print("  WARNING: n_events column not found, skipping filter 1")
    df_filtered = df.copy()

# ============================================================================
# Quality Filter 2: Decay Rate Reliability (R² ≥ 0.7)
# ============================================================================

# Note: R² is not in the original data, so we approximate it
# High |decay_rate| with low variance suggests high R²
# For now, we skip this filter and note it for future implementation

print(f"  Filter 2 (decay R² ≥ 0.7): SKIPPED (not in original data)")
print(f"    Recommendation: Re-run Method A with R² calculation")

# ============================================================================
# Quality Filter 3: Rainfall Pattern Diversity
# ============================================================================

# This requires event-level data which is not in the CSV
# Skip for now, note for future implementation

print(f"  Filter 3 (rainfall diversity): SKIPPED (requires event-level data)")

# ============================================================================
# Quality Filter 4: Seasonal Filter (CRITICAL)
# ============================================================================

# We need month information, but it's not in the current CSV
# Create a synthetic month filter based on available data patterns

print(f"\n  Filter 4 (Seasonal): SKIPPED (requires month information)")
print(f"    Recommendation: Re-run Method A with month tracking")
print(f"    Target: Paddy samples from Mar-Apr only")

# For demonstration, we'll continue with available filters

# ============================================================================
# Quality Filter 5: Remove NaN Features
# ============================================================================

baseline_features = ['decay_rate', 'saturation_response', 'april_sensitivity']
df_clean = df_filtered.dropna(subset=baseline_features + ['ground_truth', 'grid_id'])

print(f"  Filter 5 (Remove NaN): {len(df_filtered)} → {len(df_clean)} samples")

print(f"\nFinal filtered samples: {len(df_clean)}")
print(f"  Road: {(df_clean['ground_truth'] == 1).sum()} ({(df_clean['ground_truth'] == 1).sum() / len(df_clean) * 100:.1f}%)")
print(f"  Paddy: {(df_clean['ground_truth'] == 0).sum()} ({(df_clean['ground_truth'] == 0).sum() / len(df_clean) * 100:.1f}%)")
print(f"  Unique grids: {df_clean['grid_id'].nunique()}")

# ============================================================================
# Engineer Advanced Features
# ============================================================================

print("\n[3/9] Engineering advanced features...")

additional_features = []

for idx, row in df_clean.iterrows():
    features = {'index': idx}
    
    # Feature 4: decay_r2 (approximation)
    if not np.isnan(row['decay_rate']):
        features['decay_r2'] = min(abs(row['decay_rate']) / 0.5, 1.0)
    else:
        features['decay_r2'] = np.nan
    
    # Feature 5: score_magnitude
    features['score_magnitude'] = abs(row['drainage_score']) if not np.isnan(row['drainage_score']) else np.nan
    
    # Feature 6: n_valid_params
    features['n_valid_params'] = sum([
        not np.isnan(row['decay_rate']),
        not np.isnan(row['saturation_response']),
        not np.isnan(row['april_sensitivity'])
    ])
    
    # Feature 7: interaction
    if not np.isnan(row['decay_rate']) and not np.isnan(row['saturation_response']):
        features['decay_saturation_interaction'] = row['decay_rate'] * row['saturation_response']
    else:
        features['decay_saturation_interaction'] = np.nan
    
    additional_features.append(features)

df_features = pd.DataFrame(additional_features).set_index('index')
df_merged = df_clean.join(df_features[['decay_r2', 'score_magnitude', 'n_valid_params', 'decay_saturation_interaction']])

advanced_features = baseline_features + ['decay_r2', 'score_magnitude', 'n_valid_params', 'decay_saturation_interaction']

df_final = df_merged.dropna(subset=advanced_features + ['ground_truth', 'grid_id'])

print(f"  Samples after feature engineering: {len(df_final)}")

# ============================================================================
# Grid-Based Train/Test Split
# ============================================================================

print("\n[4/9] Grid-based train/test split...")

unique_grids = df_final['grid_id'].unique()
np.random.seed(42)
n_test_grids = max(1, int(len(unique_grids) * 0.2))
test_grids = np.random.choice(unique_grids, size=n_test_grids, replace=False)
train_grids = np.array([g for g in unique_grids if g not in test_grids])

train_mask = df_final['grid_id'].isin(train_grids)
test_mask = df_final['grid_id'].isin(test_grids)

df_train = df_final[train_mask]
df_test = df_final[test_mask]

print(f"  Train: {len(df_train)} samples, {len(train_grids)} grids")
print(f"    Road: {(df_train['ground_truth'] == 1).sum()}")
print(f"  Test: {len(df_test)} samples, {len(test_grids)} grids")
print(f"    Road: {(df_test['ground_truth'] == 1).sum()}")

X_train_baseline = df_train[baseline_features].values
X_train_advanced = df_train[advanced_features].values
y_train = df_train['ground_truth'].values
groups_train = df_train['grid_id'].values

X_test_baseline = df_test[baseline_features].values
X_test_advanced = df_test[advanced_features].values
y_test = df_test['ground_truth'].values

# ============================================================================
# SMOTE on Training Data Only
# ============================================================================

print("\n[5/9] Applying SMOTE to training data...")

smote = SMOTE(sampling_strategy='minority', random_state=42)
X_train_baseline_resampled, y_train_resampled = smote.fit_resample(X_train_baseline, y_train)
X_train_advanced_resampled, _ = smote.fit_resample(X_train_advanced, y_train)

print(f"  Before: {len(y_train)} → After: {len(y_train_resampled)}")

# ============================================================================
# Standardization
# ============================================================================

print("\n[6/9] Standardizing features...")

scaler_baseline = StandardScaler()
scaler_advanced = StandardScaler()

X_train_baseline_scaled = scaler_baseline.fit_transform(X_train_baseline_resampled)
X_train_advanced_scaled = scaler_advanced.fit_transform(X_train_advanced_resampled)

X_test_baseline_scaled = scaler_baseline.transform(X_test_baseline)
X_test_advanced_scaled = scaler_advanced.transform(X_test_advanced)

# ============================================================================
# GroupKFold Cross-Validation
# ============================================================================

print("\n[7/9] GroupKFold cross-validation...")

models = {
    'RF_Baseline': RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1),
    'RF_Advanced': RandomForestClassifier(n_estimators=100, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
}

gkf = GroupKFold(n_splits=5)
cv_results = {}

for model_name, model in models.items():
    print(f"\n  {model_name}:")
    
    X_cv = X_train_baseline if 'Baseline' in model_name else X_train_advanced
    
    fold_scores = {'accuracy': [], 'precision': [], 'recall': [], 'f1': [], 'roc_auc': []}
    
    for fold, (train_idx, val_idx) in enumerate(gkf.split(X_cv, y_train, groups=groups_train), 1):
        X_fold_train, X_fold_val = X_cv[train_idx], X_cv[val_idx]
        y_fold_train, y_fold_val = y_train[train_idx], y_train[val_idx]
        
        smote_fold = SMOTE(sampling_strategy='minority', random_state=42)
        X_fold_train_resampled, y_fold_train_resampled = smote_fold.fit_resample(X_fold_train, y_fold_train)
        
        scaler_fold = StandardScaler()
        X_fold_train_scaled = scaler_fold.fit_transform(X_fold_train_resampled)
        X_fold_val_scaled = scaler_fold.transform(X_fold_val)
        
        model_fold = type(model)(**model.get_params())
        model_fold.fit(X_fold_train_scaled, y_fold_train_resampled)
        
        y_pred = model_fold.predict(X_fold_val_scaled)
        y_pred_proba = model_fold.predict_proba(X_fold_val_scaled)[:, 1]
        
        fold_scores['accuracy'].append(accuracy_score(y_fold_val, y_pred))
        fold_scores['precision'].append(precision_score(y_fold_val, y_pred, zero_division=0))
        fold_scores['recall'].append(recall_score(y_fold_val, y_pred, zero_division=0))
        fold_scores['f1'].append(f1_score(y_fold_val, y_pred, zero_division=0))
        fold_scores['roc_auc'].append(roc_auc_score(y_fold_val, y_pred_proba))
    
    cv_results[model_name] = {k: np.array(v) for k, v in fold_scores.items()}
    
    print(f"    CV ROC-AUC: {np.mean(fold_scores['roc_auc']):.3f} ± {np.std(fold_scores['roc_auc']):.3f}")

# ============================================================================
# Final Training and Evaluation
# ============================================================================

print("\n[8/9] Final training and evaluation...")

final_results = {}

for model_name, model in models.items():
    print(f"\n  {model_name}:")
    
    X_train_scaled = X_train_baseline_scaled if 'Baseline' in model_name else X_train_advanced_scaled
    X_test_scaled = X_test_baseline_scaled if 'Baseline' in model_name else X_test_advanced_scaled
    
    model_final = type(model)(**model.get_params())
    model_final.fit(X_train_scaled, y_train_resampled)
    
    # Training performance
    y_train_pred = model_final.predict(X_train_scaled)
    y_train_pred_proba = model_final.predict_proba(X_train_scaled)[:, 1]
    
    train_auc = roc_auc_score(y_train_resampled, y_train_pred_proba)
    train_prec = precision_score(y_train_resampled, y_train_pred, zero_division=0)
    
    # Test performance
    y_test_pred = model_final.predict(X_test_scaled)
    y_test_pred_proba = model_final.predict_proba(X_test_scaled)[:, 1]
    
    test_auc = roc_auc_score(y_test, y_test_pred_proba)
    test_prec = precision_score(y_test, y_test_pred, zero_division=0)
    test_rec = recall_score(y_test, y_test_pred, zero_division=0)
    
    print(f"    TRAIN: AUC {train_auc:.3f}, Precision {train_prec:.3f}")
    print(f"    TEST:  AUC {test_auc:.3f}, Precision {test_prec:.3f}, Recall {test_rec:.3f}")
    print(f"    Overfit Gap: {train_auc - test_auc:.3f}")
    
    final_results[model_name] = {
        'train_auc': train_auc,
        'test_auc': test_auc,
        'test_precision': test_prec,
        'test_recall': test_rec,
        'gap': train_auc - test_auc
    }

# ============================================================================
# Generate Report
# ============================================================================

print("\n[9/9] Generating improvement report...")

best_model = max(final_results.keys(), key=lambda k: final_results[k]['test_auc'])

with open(OUT_DIR / 'improvement_report_v1.md', 'w', encoding='utf-8') as f:
    f.write("# Method A Phase 2: Week 1 Quality Improvement Report\n\n")
    
    f.write("## Applied Filters\n\n")
    f.write("1. **Events Filter**: n_events ≥ 5\n")
    f.write("2. **Decay R² Filter**: PENDING (requires re-run of Method A)\n")
    f.write("3. **Rainfall Diversity**: PENDING (requires event-level data)\n")
    f.write("4. **Seasonal Filter**: PENDING (requires month tracking)\n")
    f.write("5. **NaN Removal**: Applied\n\n")
    
    f.write("## Sample Statistics\n\n")
    f.write(f"- Original samples: {len(df)}\n")
    f.write(f"- After filtering: {len(df_final)}\n")
    f.write(f"- Reduction: {(1 - len(df_final)/len(df))*100:.1f}%\n\n")
    
    f.write("## Results Comparison\n\n")
    f.write("| Model | Test AUC | Test Precision | Overfit Gap | Status |\n")
    f.write("|---|---|---|---|---|\n")
    
    # Baseline from previous run
    f.write(f"| Original (no filter) | 0.559 | 0.022 | 0.358 | Baseline |\n")
    
    for model_name, results in final_results.items():
        status = "IMPROVED" if results['test_auc'] > 0.559 else "DECLINED"
        f.write(f"| {model_name} | {results['test_auc']:.3f} | {results['test_precision']:.3f} | {results['gap']:.3f} | {status} |\n")
    
    f.write("\n## Key Findings\n\n")
    
    improvement = final_results[best_model]['test_auc'] - 0.559
    if improvement > 0.05:
        f.write(f"- ✓ **Significant improvement**: +{improvement:.3f} AUC\n")
    elif improvement > 0.02:
        f.write(f"- ✓ **Moderate improvement**: +{improvement:.3f} AUC\n")
    elif improvement > 0:
        f.write(f"- ~ **Slight improvement**: +{improvement:.3f} AUC\n")
    else:
        f.write(f"- ✗ **No improvement**: {improvement:.3f} AUC\n")
    
    f.write("\n## Recommendations for Week 2\n\n")
    f.write("### Critical: Re-run Method A with Additional Tracking\n\n")
    f.write("The current CSV lacks critical information:\n\n")
    f.write("1. **Month information**: Required for seasonal filtering\n")
    f.write("2. **Decay R² values**: Required for reliability filtering\n")
    f.write("3. **Event-level rainfall data**: Required for diversity filtering\n\n")
    f.write("### Next Steps\n\n")
    f.write("1. Modify `compare_permeability_methods.py` to include:\n")
    f.write("   - Month column in results\n")
    f.write("   - R² value from linregress\n")
    f.write("   - Rainfall pattern flags\n")
    f.write("2. Re-run Method A data generation\n")
    f.write("3. Apply full quality filters (Week 1 complete)\n")
    f.write("4. Proceed to Week 2 (spatial features)\n")

print(f"\nSaved: {OUT_DIR / 'improvement_report_v1.md'}")

print("\n" + "="*80)
print("WEEK 1 IMPROVEMENT COMPLETE")
print("="*80)
print(f"\nBest Model: {best_model}")
print(f"Test AUC: {final_results[best_model]['test_auc']:.3f}")
print(f"Improvement: {final_results[best_model]['test_auc'] - 0.559:+.3f}")
print(f"\nWARNING: Full improvements require re-running Method A with additional data")
