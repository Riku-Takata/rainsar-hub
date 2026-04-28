"""
ピクセル単位の分類結果を日本語グラフで可視化
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
RESULT_DIR = Path(r'd:\sotsuron\rainsar-hub\data\result\vv')
BALANCED_DIR = RESULT_DIR / 'balanced'
OUTPUT_DIR = BALANCED_DIR / 'visualizations'
OUTPUT_DIR.mkdir(exist_ok=True)

# Load results
df_results = pd.read_csv(BALANCED_DIR / 'pixel_model_results_by_delay.csv')

# =====================
# 1. 精度比較グラフ
# =====================
fig, ax = plt.subplots(figsize=(12, 6))

x = df_results['delay']
width = 0.35

bars1 = ax.bar(x - width/2, df_results['train_acc'], width, label='学習', color='steelblue', alpha=0.8)
bars2 = ax.bar(x + width/2, df_results['val_acc'], width, label='検証', color='coral', alpha=0.8)

ax.set_xlabel('経過時間 (時間)', fontsize=12)
ax.set_ylabel('精度', fontsize=12)
ax.set_title('経過時間ごとの分類精度（ピクセル単位）', fontsize=14)
ax.set_xticks(x)
ax.set_xticklabels([f'{int(d)}h' for d in x])
ax.legend()
ax.set_ylim(0, 1.1)
ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5)

for bar in bars2:
    height = bar.get_height()
    ax.annotate(f'{height:.2f}',
                xy=(bar.get_x() + bar.get_width()/2, height),
                xytext=(0, 3), textcoords="offset points",
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'pixel_accuracy_by_delay_jp.png', dpi=150)
plt.close()

# =====================
# 2. 特徴量重要度ヒートマップ
# =====================
fig, ax = plt.subplots(figsize=(10, 8))

imp_data = df_results[['delay', 'imp_diff', 'imp_precip', 'imp_duration']].set_index('delay')
imp_data.columns = ['後方散乱差分', '降水量', '継続時間']

sns.heatmap(imp_data, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax, 
            vmin=0, vmax=0.8, cbar_kws={'label': '重要度'})
ax.set_xlabel('特徴量', fontsize=12)
ax.set_ylabel('経過時間 (時間)', fontsize=12)
ax.set_title('経過時間ごとの特徴量重要度（ピクセル単位）', fontsize=14)
ax.set_yticklabels([f'{int(d)}h' for d in imp_data.index], rotation=0)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'pixel_feature_importance_heatmap_jp.png', dpi=150)
plt.close()

# =====================
# 3. パフォーマンスサマリー
# =====================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Accuracy and F1
ax1 = axes[0]
x = df_results['delay']
ax1.plot(x, df_results['val_acc'], 'o-', color='coral', label='精度', markersize=8)
ax1.plot(x, df_results['val_f1'], 's--', color='green', label='F1スコア', markersize=8)
ax1.set_xlabel('経過時間 (時間)', fontsize=12)
ax1.set_ylabel('スコア', fontsize=12)
ax1.set_title('検証データでのパフォーマンス（ピクセル単位）', fontsize=14)
ax1.legend()
ax1.set_ylim(0, 1.05)
ax1.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5)
ax1.set_xticks(x)
ax1.grid(alpha=0.3)

# Data count (log scale due to large numbers)
ax2 = axes[1]
ax2.bar(x, df_results['val_n'] / 1000, color='steelblue', alpha=0.7)
ax2.set_xlabel('経過時間 (時間)', fontsize=12)
ax2.set_ylabel('検証サンプル数 (×1000)', fontsize=12)
ax2.set_title('経過時間ごとのデータ数（ピクセル単位）', fontsize=14)
ax2.set_xticks(x)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'pixel_performance_summary_jp.png', dpi=150)
plt.close()

# =====================
# 4. イベント単位 vs ピクセル単位 比較
# =====================
try:
    df_event = pd.read_csv(BALANCED_DIR / 'model_results_by_delay.csv')
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = df_results['delay']
    width = 0.35
    
    ax.bar(x - width/2, df_event['val_acc'], width, label='イベント単位', color='steelblue', alpha=0.8)
    ax.bar(x + width/2, df_results['val_acc'], width, label='ピクセル単位', color='coral', alpha=0.8)
    
    ax.set_xlabel('経過時間 (時間)', fontsize=12)
    ax.set_ylabel('検証精度', fontsize=12)
    ax.set_title('イベント単位 vs ピクセル単位の精度比較', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels([f'{int(d)}h' for d in x])
    ax.legend()
    ax.set_ylim(0, 1.1)
    ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'pixel_vs_event_comparison_jp.png', dpi=150)
    plt.close()
except:
    pass

print('ピクセル単位の日本語版可視化完了！')
print('保存先:', OUTPUT_DIR)
print('  - pixel_accuracy_by_delay_jp.png')
print('  - pixel_feature_importance_heatmap_jp.png')
print('  - pixel_performance_summary_jp.png')
print('  - pixel_vs_event_comparison_jp.png')
