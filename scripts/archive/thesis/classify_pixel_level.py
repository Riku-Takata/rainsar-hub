"""
ピクセル単位での分類モデル構築
- 各イベントのdiff_pixel_values.csvから実際のピクセルデータを読み込み
- ダウンサンプリングを適用
- Delay時間ごとのモデルを学習
"""
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

# Paths
DATA_DIR = Path(r'd:\sotsuron\rainsar-hub\data')
RESULT_DIR = DATA_DIR / 'result' / 'vv'
DIFF_DIR = RESULT_DIR / 'diff'
BALANCED_DIR = RESULT_DIR / 'balanced'

print('='*70)
print('ピクセル単位での分類モデル構築')
print('='*70)

# Load balanced plans
df_paddy_plan = pd.read_csv(BALANCED_DIR / 'paddy_balanced_with_rain.csv')
df_road_plan = pd.read_csv(BALANCED_DIR / 'road_balanced_with_rain.csv')

# Load diff data for rainfall info
df_diff = pd.read_csv(DIFF_DIR / 'all_events_diff_vv.csv')
df_diff['key'] = list(zip(df_diff['grid_id'], df_diff['event_name']))

# Extract delay
def get_delay(event_name):
    try:
        return float(event_name.split('_')[1].replace('h', ''))
    except:
        return None

# Prepare rainfall lookup
rain_cols = ['key', 'total_precip_mm', 'duration_hours']
rain_lookup = df_diff[rain_cols].drop_duplicates(subset=['key']).set_index('key').to_dict('index')

# Function to load and sample pixels
def load_pixels(grid_id, event_name, land_type, sample_size=None):
    """Load pixel values from diff_pixel_values.csv"""
    event_path = DIFF_DIR / grid_id / event_name / 'diff_pixel_values.csv'
    
    if not event_path.exists():
        return None
    
    try:
        df = pd.read_csv(event_path)
        df = df[df['land_type'] == land_type]
        
        if len(df) == 0:
            return None
        
        # Sample if needed
        if sample_size and sample_size < len(df):
            df = df.sample(n=sample_size, random_state=42)
        
        return df['diff_db'].values
    except:
        return None

# Split grids for train/validation
all_grids = list(set(df_paddy_plan['grid_id'].unique()) | set(df_road_plan['grid_id'].unique()))
train_grids, val_grids = train_test_split(all_grids, test_size=0.2, random_state=42)

print(f'学習用グリッド: {len(train_grids)}')
print(f'検証用グリッド: {len(val_grids)}')

# Collect pixel data
print('\nピクセルデータを読み込み中...')

def collect_pixel_data(df_plan, land_type, grid_set, sample_col=None):
    """Collect pixel data from multiple events"""
    all_data = []
    
    for _, row in df_plan.iterrows():
        if row['grid_id'] not in grid_set:
            continue
        
        grid_id = row['grid_id']
        event_name = row['event_name']
        delay = get_delay(event_name)
        
        # Get sample size
        if sample_col and sample_col in row:
            sample_size = int(row[sample_col]) if pd.notna(row[sample_col]) else None
        else:
            sample_size = None
        
        # Load pixels
        pixels = load_pixels(grid_id, event_name, land_type, sample_size)
        
        if pixels is None or len(pixels) == 0:
            continue
        
        # Get rainfall data
        key = (grid_id, event_name)
        rain_data = rain_lookup.get(key, {})
        precip = rain_data.get('total_precip_mm', np.nan)
        duration = rain_data.get('duration_hours', np.nan)
        
        if pd.isna(precip) or pd.isna(duration):
            continue
        
        # Create records
        for diff_val in pixels:
            all_data.append({
                'grid_id': grid_id,
                'delay': delay,
                'diff': diff_val,
                'precip': precip,
                'duration': duration,
                'label': 0 if land_type == 'paddy' else 1
            })
    
    return pd.DataFrame(all_data)

# Collect training data
print('  田んぼ (学習)...')
train_paddy = collect_pixel_data(df_paddy_plan, 'paddy', set(train_grids), 'sample_size')
print(f'    {len(train_paddy)} ピクセル')

print('  道路 (学習)...')
train_road = collect_pixel_data(df_road_plan, 'road', set(train_grids))
print(f'    {len(train_road)} ピクセル')

# Collect validation data
print('  田んぼ (検証)...')
val_paddy = collect_pixel_data(df_paddy_plan, 'paddy', set(val_grids), 'sample_size')
print(f'    {len(val_paddy)} ピクセル')

print('  道路 (検証)...')
val_road = collect_pixel_data(df_road_plan, 'road', set(val_grids))
print(f'    {len(val_road)} ピクセル')

# Combine
df_train = pd.concat([train_paddy, train_road], ignore_index=True)
df_val = pd.concat([val_paddy, val_road], ignore_index=True)

print(f'\n総データ数:')
print(f'  学習: {len(df_train):,} ピクセル (田:{len(train_paddy):,}, 道:{len(train_road):,})')
print(f'  検証: {len(df_val):,} ピクセル (田:{len(val_paddy):,}, 道:{len(val_road):,})')

# Train models for each delay
print('\n' + '='*70)
print('【Delay時間ごとのモデル学習と評価】')
print('='*70)

feature_cols = ['diff', 'precip', 'duration']
results = []

for delay in sorted(df_train['delay'].dropna().unique()):
    train_delay = df_train[df_train['delay'] == delay]
    val_delay = df_val[df_val['delay'] == delay]
    
    if len(train_delay) < 100 or len(val_delay) < 50:
        print(f'\nDelay {int(delay)}h: データ不足 (学習:{len(train_delay)}, 検証:{len(val_delay)})')
        continue
    
    X_train = train_delay[feature_cols]
    y_train = train_delay['label']
    X_val = val_delay[feature_cols]
    y_val = val_delay['label']
    
    # Train Random Forest
    model = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=10, n_jobs=-1)
    model.fit(X_train, y_train)
    
    # Evaluate
    y_train_pred = model.predict(X_train)
    y_val_pred = model.predict(X_val)
    
    train_acc = accuracy_score(y_train, y_train_pred)
    val_acc = accuracy_score(y_val, y_val_pred)
    val_f1 = f1_score(y_val, y_val_pred, average='weighted')
    
    importances = dict(zip(feature_cols, model.feature_importances_))
    
    result = {
        'delay': int(delay),
        'train_n': len(train_delay),
        'val_n': len(val_delay),
        'train_acc': train_acc,
        'val_acc': val_acc,
        'val_f1': val_f1,
        'imp_diff': importances['diff'],
        'imp_precip': importances['precip'],
        'imp_duration': importances['duration']
    }
    results.append(result)
    
    print(f'\nDelay {int(delay)}h:')
    print(f'  データ数: 学習={len(train_delay):,}, 検証={len(val_delay):,}')
    print(f'  学習精度: {train_acc:.3f}')
    print(f'  検証精度: {val_acc:.3f} (F1: {val_f1:.3f})')
    print(f'  特徴量重要度: diff={importances["diff"]:.3f}, precip={importances["precip"]:.3f}, duration={importances["duration"]:.3f}')

# Summary
print('\n' + '='*70)
print('【サマリー】')
print('='*70)
df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))

df_results.to_csv(BALANCED_DIR / 'pixel_model_results_by_delay.csv', index=False)
print(f'\n結果を保存: pixel_model_results_by_delay.csv')

print(f'\n【全Delay平均】')
print(f'  学習精度平均: {df_results["train_acc"].mean():.3f}')
print(f'  検証精度平均: {df_results["val_acc"].mean():.3f}')
print(f'  検証F1平均:   {df_results["val_f1"].mean():.3f}')
