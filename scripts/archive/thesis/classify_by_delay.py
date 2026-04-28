"""
Delay時間ごとの分類モデル構築
特徴量: 後方散乱強度差分, 降水量, 降雨継続時間
"""
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

# Paths
RESULT_DIR = Path(r'd:\sotsuron\rainsar-hub\data\result\vv')
BALANCED_DIR = RESULT_DIR / 'balanced'

# Load balanced data
df_paddy = pd.read_csv(BALANCED_DIR / 'paddy_balanced_with_rain.csv')
df_road = pd.read_csv(BALANCED_DIR / 'road_balanced_with_rain.csv')

# Load diff data for rainfall info and diff values
df_diff = pd.read_csv(RESULT_DIR / 'diff' / 'all_events_diff_vv.csv')

print('='*70)
print('Delay時間ごとの分類モデル構築')
print('特徴量: 後方散乱強度差分, 降水量, 降雨継続時間')
print('='*70)

# Extract delay hours
def get_delay(event_name):
    try:
        return float(event_name.split('_')[1].replace('h', ''))
    except:
        return None

df_paddy['delay_hours'] = df_paddy['event_name'].apply(get_delay)
df_road['delay_hours'] = df_road['event_name'].apply(get_delay)

# Create keys for merging
df_paddy['key'] = list(zip(df_paddy['grid_id'], df_paddy['event_name']))
df_road['key'] = list(zip(df_road['grid_id'], df_road['event_name']))
df_diff['key'] = list(zip(df_diff['grid_id'], df_diff['event_name']))

# Merge diff and rainfall data
diff_cols = ['key', 'paddy_diff_mean', 'road_diff_mean', 'total_precip_mm', 'duration_hours', 'max_intensity_mm_h']
df_diff_sub = df_diff[diff_cols].drop_duplicates(subset=['key'])

df_paddy = df_paddy.merge(df_diff_sub, on='key', how='left')
df_road = df_road.merge(df_diff_sub, on='key', how='left')

# Prepare features
# Paddy: diff = paddy_diff_mean, precip, duration, label=0
# Road: diff = road_diff_mean, precip, duration, label=1

df_paddy['diff_mean'] = df_paddy['paddy_diff_mean']
df_paddy['label'] = 0  # Paddy = 0

df_road['diff_mean'] = df_road['road_diff_mean']
df_road['label'] = 1   # Road = 1

# Combine
cols_to_use = ['grid_id', 'event_name', 'delay_hours', 'diff_mean', 'total_precip_mm', 'duration_hours', 'label']
df_paddy_features = df_paddy[cols_to_use + ['sample_size']].copy()
df_road_features = df_road[cols_to_use + ['road_after_count']].copy()
df_road_features = df_road_features.rename(columns={'road_after_count': 'sample_size'})

# Combine all data
df_all = pd.concat([df_paddy_features, df_road_features], ignore_index=True)

# Drop rows with missing features
df_all = df_all.dropna(subset=['diff_mean', 'total_precip_mm', 'duration_hours'])

print(f'\n総データ数: {len(df_all)}イベント')
print(f'  田んぼ: {len(df_all[df_all["label"]==0])}')
print(f'  道路:   {len(df_all[df_all["label"]==1])}')

# Split by grid_id to avoid data leakage (same grid in train and test)
unique_grids = df_all['grid_id'].unique()
train_grids, val_grids = train_test_split(unique_grids, test_size=0.2, random_state=42)

df_train = df_all[df_all['grid_id'].isin(train_grids)]
df_val = df_all[df_all['grid_id'].isin(val_grids)]

print(f'\n【データ分割】')
print(f'学習用グリッド: {len(train_grids)}')
print(f'検証用グリッド: {len(val_grids)}')
print(f'学習用イベント: {len(df_train)} (田:{len(df_train[df_train["label"]==0])}, 道:{len(df_train[df_train["label"]==1])})')
print(f'検証用イベント: {len(df_val)} (田:{len(df_val[df_val["label"]==0])}, 道:{len(df_val[df_val["label"]==1])})')

# Train models for each delay hour
print('\n' + '='*70)
print('【Delay時間ごとのモデル学習と評価】')
print('='*70)

feature_cols = ['diff_mean', 'total_precip_mm', 'duration_hours']
results = []

for delay in sorted(df_all['delay_hours'].unique()):
    # Filter by delay
    train_delay = df_train[df_train['delay_hours'] == delay]
    val_delay = df_val[df_val['delay_hours'] == delay]
    
    if len(train_delay) < 5 or len(val_delay) < 2:
        print(f'\nDelay {int(delay)}h: データ不足 (学習:{len(train_delay)}, 検証:{len(val_delay)})')
        continue
    
    X_train = train_delay[feature_cols]
    y_train = train_delay['label']
    X_val = val_delay[feature_cols]
    y_val = val_delay['label']
    
    # Train Random Forest
    model = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=5)
    model.fit(X_train, y_train)
    
    # Evaluate on training data
    y_train_pred = model.predict(X_train)
    train_acc = accuracy_score(y_train, y_train_pred)
    
    # Evaluate on validation data
    y_val_pred = model.predict(X_val)
    val_acc = accuracy_score(y_val, y_val_pred)
    val_f1 = f1_score(y_val, y_val_pred, average='weighted')
    
    # Feature importance
    importances = dict(zip(feature_cols, model.feature_importances_))
    
    result = {
        'delay': int(delay),
        'train_n': len(train_delay),
        'val_n': len(val_delay),
        'train_acc': train_acc,
        'val_acc': val_acc,
        'val_f1': val_f1,
        'imp_diff': importances['diff_mean'],
        'imp_precip': importances['total_precip_mm'],
        'imp_duration': importances['duration_hours']
    }
    results.append(result)
    
    print(f'\nDelay {int(delay)}h:')
    print(f'  データ数: 学習={len(train_delay)}, 検証={len(val_delay)}')
    print(f'  学習精度: {train_acc:.3f}')
    print(f'  検証精度: {val_acc:.3f} (F1: {val_f1:.3f})')
    print(f'  特徴量重要度: diff={importances["diff_mean"]:.3f}, precip={importances["total_precip_mm"]:.3f}, duration={importances["duration_hours"]:.3f}')

# Summary
print('\n' + '='*70)
print('【サマリー】')
print('='*70)
df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))

# Save results
df_results.to_csv(BALANCED_DIR / 'model_results_by_delay.csv', index=False)
print(f'\n結果を保存: model_results_by_delay.csv')

# Overall average
print(f'\n【全Delay平均】')
print(f'  学習精度平均: {df_results["train_acc"].mean():.3f}')
print(f'  検証精度平均: {df_results["val_acc"].mean():.3f}')
print(f'  検証F1平均:   {df_results["val_f1"].mean():.3f}')
