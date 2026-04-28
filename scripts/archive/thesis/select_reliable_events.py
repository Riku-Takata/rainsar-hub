"""
Method 1: Select most reliable events based on standard deviation
Lower std = more stable/reliable data
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')

print("=== 方法1: 標準偏差による信頼性フィルタ ===")
print()

# Filter events with both paddy and road data
df_valid = df[(df['paddy_after_count'].notna()) & (df['road_after_count'].notna())].copy()
print(f"両方のデータがあるイベント数: {len(df_valid)}")
print()

# Show current std distribution
print("【現在の標準偏差の分布】")
print(f"  田んぼ std - Mean: {df_valid['paddy_after_std'].mean():.2f}, Median: {df_valid['paddy_after_std'].median():.2f}")
print(f"  道路 std   - Mean: {df_valid['road_after_std'].mean():.2f}, Median: {df_valid['road_after_std'].median():.2f}")
print()

# Calculate percentiles
print("【標準偏差のパーセンタイル】")
for p in [25, 50, 75, 90]:
    paddy_pct = df_valid['paddy_after_std'].quantile(p/100)
    road_pct = df_valid['road_after_std'].quantile(p/100)
    print(f"  {p}%ile - 田んぼ: {paddy_pct:.2f}, 道路: {road_pct:.2f}")
print()

# Filter by std threshold (e.g., keep only events below median std)
# Use combined score: normalize both stds and sum
df_valid['paddy_std_normalized'] = (df_valid['paddy_after_std'] - df_valid['paddy_after_std'].min()) / (df_valid['paddy_after_std'].max() - df_valid['paddy_after_std'].min())
df_valid['road_std_normalized'] = (df_valid['road_after_std'] - df_valid['road_after_std'].min()) / (df_valid['road_after_std'].max() - df_valid['road_after_std'].min())
df_valid['combined_std_score'] = df_valid['paddy_std_normalized'] + df_valid['road_std_normalized']

# Sort by combined score (lower is better)
df_sorted = df_valid.sort_values('combined_std_score')

# Show top N% selection results
print("【異なる閾値でのフィルタ結果】")
for pct in [25, 50, 75]:
    n_keep = int(len(df_sorted) * pct / 100)
    selected = df_sorted.head(n_keep)
    
    paddy_pixels = int(selected['paddy_after_count'].sum())
    road_pixels = int(selected['road_after_count'].sum())
    grids = selected['grid_id'].nunique()
    
    print(f"\n上位{pct}%を選択 ({n_keep}イベント, {grids}グリッド):")
    print(f"  田んぼピクセル: {paddy_pixels:,}")
    print(f"  道路ピクセル: {road_pixels:,}")
    print(f"  比率 (田んぼ/道路): {paddy_pixels/road_pixels:.1f}倍")
    print(f"  田んぼstd平均: {selected['paddy_after_std'].mean():.2f}")
    print(f"  道路std平均: {selected['road_after_std'].mean():.2f}")

# Save sorted data for further analysis
output_path = r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\events_sorted_by_reliability.csv'
df_sorted.to_csv(output_path, index=False)
print(f"\n信頼性順にソートしたデータを保存: {output_path}")
