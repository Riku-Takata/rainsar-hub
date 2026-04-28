"""
Delay時間ごとの分類結果をグラフィカルに表示
- 精度比較グラフ
- 混同行列
- 特徴量重要度
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

# Paths
RESULT_DIR = Path(r'd:\sotsuron\rainsar-hub\data\result\vv')
BALANCED_DIR = RESULT_DIR / 'balanced'

# Load results
df_results = pd.read_csv(BALANCED_DIR / 'model_results_by_delay.csv')

# Create output directory
OUTPUT_DIR = BALANCED_DIR / 'visualizations'
OUTPUT_DIR.mkdir(exist_ok=True)

# =====================
# 1. Accuracy by Delay
# =====================
fig, ax = plt.subplots(figsize=(12, 6))

x = df_results['delay']
width = 0.35

bars1 = ax.bar(x - width/2, df_results['train_acc'], width, label='Training', color='steelblue', alpha=0.8)
bars2 = ax.bar(x + width/2, df_results['val_acc'], width, label='Validation', color='coral', alpha=0.8)

ax.set_xlabel('Delay (hours)', fontsize=12)
ax.set_ylabel('Accuracy', fontsize=12)
ax.set_title('Classification Accuracy by Delay Time', fontsize=14)
ax.set_xticks(x)
ax.set_xticklabels([f'{int(d)}h' for d in x])
ax.legend()
ax.set_ylim(0, 1.1)
ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5, label='Random')

# Add value labels
for bar in bars2:
    height = bar.get_height()
    ax.annotate(f'{height:.2f}',
                xy=(bar.get_x() + bar.get_width()/2, height),
                xytext=(0, 3), textcoords="offset points",
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'accuracy_by_delay.png', dpi=150)
plt.close()

# =====================
# 2. Feature Importance Heatmap
# =====================
fig, ax = plt.subplots(figsize=(10, 8))

imp_data = df_results[['delay', 'imp_diff', 'imp_precip', 'imp_duration']].set_index('delay')
imp_data.columns = ['Backscatter Diff', 'Precipitation', 'Duration']

sns.heatmap(imp_data, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax, 
            vmin=0, vmax=0.6, cbar_kws={'label': 'Importance'})
ax.set_xlabel('Feature', fontsize=12)
ax.set_ylabel('Delay (hours)', fontsize=12)
ax.set_title('Feature Importance by Delay Time', fontsize=14)
ax.set_yticklabels([f'{int(d)}h' for d in imp_data.index], rotation=0)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'feature_importance_heatmap.png', dpi=150)
plt.close()

# =====================
# 3. Generate Confusion Matrices for each delay
# =====================
# Reload data and retrain to get confusion matrices
df_paddy = pd.read_csv(BALANCED_DIR / 'paddy_balanced_with_rain.csv')
df_road = pd.read_csv(BALANCED_DIR / 'road_balanced_with_rain.csv')
df_diff = pd.read_csv(RESULT_DIR / 'diff' / 'all_events_diff_vv.csv')

def get_delay(event_name):
    try:
        return float(event_name.split('_')[1].replace('h', ''))
    except:
        return None

df_paddy['delay_hours'] = df_paddy['event_name'].apply(get_delay)
df_road['delay_hours'] = df_road['event_name'].apply(get_delay)
df_paddy['key'] = list(zip(df_paddy['grid_id'], df_paddy['event_name']))
df_road['key'] = list(zip(df_road['grid_id'], df_road['event_name']))
df_diff['key'] = list(zip(df_diff['grid_id'], df_diff['event_name']))

diff_cols = ['key', 'paddy_diff_mean', 'road_diff_mean', 'total_precip_mm', 'duration_hours']
df_diff_sub = df_diff[diff_cols].drop_duplicates(subset=['key'])

df_paddy = df_paddy.merge(df_diff_sub, on='key', how='left')
df_road = df_road.merge(df_diff_sub, on='key', how='left')

df_paddy['diff_mean'] = df_paddy['paddy_diff_mean']
df_paddy['label'] = 0
df_road['diff_mean'] = df_road['road_diff_mean']
df_road['label'] = 1

cols_to_use = ['grid_id', 'event_name', 'delay_hours', 'diff_mean', 'total_precip_mm', 'duration_hours', 'label']
df_paddy_features = df_paddy[cols_to_use].copy()
df_road_features = df_road[cols_to_use].copy()

df_all = pd.concat([df_paddy_features, df_road_features], ignore_index=True)
df_all = df_all.dropna(subset=['diff_mean', 'total_precip_mm', 'duration_hours'])

unique_grids = df_all['grid_id'].unique()
train_grids, val_grids = train_test_split(unique_grids, test_size=0.2, random_state=42)

df_train = df_all[df_all['grid_id'].isin(train_grids)]
df_val = df_all[df_all['grid_id'].isin(val_grids)]

# Confusion matrix for each delay
fig, axes = plt.subplots(3, 4, figsize=(16, 12))
axes = axes.flatten()

feature_cols = ['diff_mean', 'total_precip_mm', 'duration_hours']
delays = sorted(df_all['delay_hours'].unique())

for i, delay in enumerate(delays):
    train_delay = df_train[df_train['delay_hours'] == delay]
    val_delay = df_val[df_val['delay_hours'] == delay]
    
    if len(train_delay) < 5 or len(val_delay) < 2:
        axes[i].set_visible(False)
        continue
    
    X_train = train_delay[feature_cols]
    y_train = train_delay['label']
    X_val = val_delay[feature_cols]
    y_val = val_delay['label']
    
    model = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=5)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    
    cm = confusion_matrix(y_val, y_pred)
    
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[i],
                xticklabels=['Paddy', 'Road'], yticklabels=['Paddy', 'Road'])
    axes[i].set_title(f'Delay {int(delay)}h (n={len(val_delay)})', fontsize=11)
    axes[i].set_xlabel('Predicted')
    axes[i].set_ylabel('Actual')

plt.suptitle('Confusion Matrices by Delay Time (Validation Set)', fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'confusion_matrices_by_delay.png', dpi=150)
plt.close()

# =====================
# 4. Summary Performance
# =====================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Accuracy and F1
ax1 = axes[0]
x = df_results['delay']
ax1.plot(x, df_results['val_acc'], 'o-', color='coral', label='Accuracy', markersize=8)
ax1.plot(x, df_results['val_f1'], 's--', color='green', label='F1 Score', markersize=8)
ax1.set_xlabel('Delay (hours)', fontsize=12)
ax1.set_ylabel('Score', fontsize=12)
ax1.set_title('Validation Performance by Delay', fontsize=14)
ax1.legend()
ax1.set_ylim(0, 1.05)
ax1.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5)
ax1.set_xticks(x)
ax1.grid(alpha=0.3)

# Data count
ax2 = axes[1]
ax2.bar(x, df_results['val_n'], color='steelblue', alpha=0.7)
ax2.set_xlabel('Delay (hours)', fontsize=12)
ax2.set_ylabel('Validation Samples', fontsize=12)
ax2.set_title('Validation Data Count by Delay', fontsize=14)
ax2.set_xticks(x)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'performance_summary.png', dpi=150)
plt.close()

print('可視化完了！保存先:', OUTPUT_DIR)
print('  - accuracy_by_delay.png')
print('  - feature_importance_heatmap.png')
print('  - confusion_matrices_by_delay.png')
print('  - performance_summary.png')
