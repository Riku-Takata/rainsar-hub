"""
ピクセル単位の分類結果 - 混同行列を日本語で可視化
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
import warnings
warnings.filterwarnings('ignore')

# 日本語フォント設定
plt.rcParams['font.family'] = 'MS Gothic'

# Paths
DATA_DIR = Path(r'd:\sotsuron\rainsar-hub\data')
RESULT_DIR = DATA_DIR / 'result' / 'vv'
DIFF_DIR = RESULT_DIR / 'diff'
BALANCED_DIR = RESULT_DIR / 'balanced'
OUTPUT_DIR = BALANCED_DIR / 'visualizations'

print('ピクセル単位の混同行列を生成中...')

# Load data
df_paddy_plan = pd.read_csv(BALANCED_DIR / 'paddy_balanced_with_rain.csv')
df_road_plan = pd.read_csv(BALANCED_DIR / 'road_balanced_with_rain.csv')
df_diff = pd.read_csv(DIFF_DIR / 'all_events_diff_vv.csv')
df_diff['key'] = list(zip(df_diff['grid_id'], df_diff['event_name']))

def get_delay(event_name):
    try:
        return float(event_name.split('_')[1].replace('h', ''))
    except:
        return None

rain_cols = ['key', 'total_precip_mm', 'duration_hours']
rain_lookup = df_diff[rain_cols].drop_duplicates(subset=['key']).set_index('key').to_dict('index')

def load_pixels(grid_id, event_name, land_type, sample_size=None):
    event_path = DIFF_DIR / grid_id / event_name / 'diff_pixel_values.csv'
    if not event_path.exists():
        return None
    try:
        df = pd.read_csv(event_path)
        df = df[df['land_type'] == land_type]
        if len(df) == 0:
            return None
        if sample_size and sample_size < len(df):
            df = df.sample(n=sample_size, random_state=42)
        return df['diff_db'].values
    except:
        return None

# Split grids
all_grids = list(set(df_paddy_plan['grid_id'].unique()) | set(df_road_plan['grid_id'].unique()))
train_grids, val_grids = train_test_split(all_grids, test_size=0.2, random_state=42)

def collect_pixel_data(df_plan, land_type, grid_set, sample_col=None):
    all_data = []
    for _, row in df_plan.iterrows():
        if row['grid_id'] not in grid_set:
            continue
        grid_id = row['grid_id']
        event_name = row['event_name']
        delay = get_delay(event_name)
        
        if sample_col and sample_col in row:
            sample_size = int(row[sample_col]) if pd.notna(row[sample_col]) else None
        else:
            sample_size = None
        
        pixels = load_pixels(grid_id, event_name, land_type, sample_size)
        if pixels is None or len(pixels) == 0:
            continue
        
        key = (grid_id, event_name)
        rain_data = rain_lookup.get(key, {})
        precip = rain_data.get('total_precip_mm', np.nan)
        duration = rain_data.get('duration_hours', np.nan)
        
        if pd.isna(precip) or pd.isna(duration):
            continue
        
        for diff_val in pixels:
            all_data.append({
                'delay': delay,
                'diff': diff_val,
                'precip': precip,
                'duration': duration,
                'label': 0 if land_type == 'paddy' else 1
            })
    return pd.DataFrame(all_data)

print('  検証データ読み込み中...')
val_paddy = collect_pixel_data(df_paddy_plan, 'paddy', set(val_grids), 'sample_size')
val_road = collect_pixel_data(df_road_plan, 'road', set(val_grids))
df_val = pd.concat([val_paddy, val_road], ignore_index=True)

print('  学習データ読み込み中...')
train_paddy = collect_pixel_data(df_paddy_plan, 'paddy', set(train_grids), 'sample_size')
train_road = collect_pixel_data(df_road_plan, 'road', set(train_grids))
df_train = pd.concat([train_paddy, train_road], ignore_index=True)

print(f'  学習: {len(df_train):,} px, 検証: {len(df_val):,} px')

# Generate confusion matrices
print('  混同行列を生成中...')

fig, axes = plt.subplots(3, 4, figsize=(16, 12))
axes = axes.flatten()

feature_cols = ['diff', 'precip', 'duration']
delays = sorted(df_train['delay'].dropna().unique())

for i, delay in enumerate(delays):
    train_delay = df_train[df_train['delay'] == delay]
    val_delay = df_val[df_val['delay'] == delay]
    
    if len(train_delay) < 100 or len(val_delay) < 50:
        axes[i].text(0.5, 0.5, f'{int(delay)}h\nデータ不足', ha='center', va='center', fontsize=12)
        axes[i].set_xticks([])
        axes[i].set_yticks([])
        continue
    
    X_train = train_delay[feature_cols]
    y_train = train_delay['label']
    X_val = val_delay[feature_cols]
    y_val = val_delay['label']
    
    model = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=10, n_jobs=-1)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    
    cm = confusion_matrix(y_val, y_pred)
    
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[i],
                xticklabels=['田んぼ', '道路'], yticklabels=['田んぼ', '道路'])
    
    acc = (cm[0,0] + cm[1,1]) / cm.sum()
    axes[i].set_title(f'{int(delay)}時間経過\n(n={len(val_delay):,}, 精度={acc:.2f})', fontsize=11)
    axes[i].set_xlabel('予測')
    axes[i].set_ylabel('実際')

plt.suptitle('経過時間ごとの混同行列（ピクセル単位・検証データ）', fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'pixel_confusion_matrices_by_delay_jp.png', dpi=150)
plt.close()

print('完了！')
print('保存: pixel_confusion_matrices_by_delay_jp.png')
