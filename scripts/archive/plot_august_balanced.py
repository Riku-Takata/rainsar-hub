"""
Generate combined pixel count visualization for August
Shows both raw data and balanced data in one figure
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

matplotlib.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = r"D:\sotsuron\rainsar-hub"
balanced_csv = f"{BASE_DIR}/data/result/seasonal/rf_data/balanced_summary.csv"
stats_csv = f"{BASE_DIR}/data/result/seasonal/seasonal_stats_all.csv"
OUTPUT_DIR = f"{BASE_DIR}/data/result/Aug"

# Load balanced data
bal_df = pd.read_csv(balanced_csv)
aug_bal = bal_df[bal_df['month'] == 8].sort_values('delay_bin')

# Load raw data
raw_df = pd.read_csv(stats_csv)
raw_df['delay_bin'] = raw_df['delay_h'].astype(int)
aug_raw = raw_df[(raw_df['month'] == 8) & (raw_df['pol'] == 'vv')]
road_raw = aug_raw[aug_raw['type'] == 'road'].groupby('delay_bin')['clean_pixel_count'].sum()
paddy_raw = aug_raw[aug_raw['type'] == 'paddy'].groupby('delay_bin')['clean_pixel_count'].sum()

# Create figure with 2 subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# ============================================
# Left: Raw Data (before balancing)
# ============================================
delays = sorted(aug_bal['delay_bin'].unique())
x = np.arange(len(delays))
width = 0.35

# Get raw values aligned with delays
road_vals = [road_raw.get(d, 0) for d in delays]
paddy_vals = [paddy_raw.get(d, 0) for d in delays]

bars1 = ax1.bar(x - width/2, road_vals, width, label='道路', color='steelblue', alpha=0.7)
bars2 = ax1.bar(x + width/2, paddy_vals, width, label='田んぼ', color='forestgreen', alpha=0.7)

ax1.set_xlabel('経過時間 (h)', fontsize=12)
ax1.set_ylabel('ピクセル数', fontsize=12)
ax1.set_title('元データ（バランシング前）', fontsize=14)
ax1.set_xticks(x)
ax1.set_xticklabels(delays)
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# ============================================
# Right: Balanced Data (used for training)
# ============================================
bal_road = aug_bal['road_count'].values
bal_paddy = aug_bal['paddy_count'].values

bars3 = ax2.bar(x - width/2, bal_road, width, label='道路', color='steelblue')
bars4 = ax2.bar(x + width/2, bal_paddy, width, label='田んぼ', color='forestgreen')

ax2.set_xlabel('経過時間 (h)', fontsize=12)
ax2.set_ylabel('ピクセル数', fontsize=12)
ax2.set_title('学習・検証データ（バランシング後）', fontsize=14)
ax2.set_xticks(x)
ax2.set_xticklabels(delays)
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

# Add note
ax2.text(0.02, 0.98, "道路 = 田んぼ（1:1でダウンサンプリング）", 
         transform=ax2.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

plt.suptitle('8月 ピクセル数（元データ → バランシング後）', fontsize=16, y=1.02)
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/8月_学習検証ピクセル数_バランス.png", dpi=150, bbox_inches='tight')
print("Saved: 8月_学習検証ピクセル数_バランス.png")
