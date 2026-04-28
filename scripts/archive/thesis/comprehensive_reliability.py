"""
Comprehensive reliability analysis
- Original statistics for road and paddy
- Combined reliability score: high pixel count + low std
- Final reliable data pixel counts
"""
import pandas as pd
import numpy as np

df = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')

print("=" * 60)
print("統合信頼性分析: ピクセル数 + 標準偏差")
print("=" * 60)

# =====================
# 1. 元データの統計
# =====================
print("\n【1. 元データの後方散乱強度統計 (全イベント)】")
print("-" * 40)

df_paddy = df[df['paddy_after_count'].notna()]
df_road = df[df['road_after_count'].notna()]

print(f"\n■ 田んぼ (有効イベント: {len(df_paddy)})")
print(f"  総ピクセル数: {int(df_paddy['paddy_after_count'].sum()):,}")
print(f"  後方散乱強度 Mean (平均の平均): {df_paddy['paddy_after_mean'].mean():.2f} dB")
print(f"  後方散乱強度 Median (中央値の平均): {df_paddy['paddy_after_median'].mean():.2f} dB")
print(f"  標準偏差 平均: {df_paddy['paddy_after_std'].mean():.2f}")
print(f"  1イベントあたり平均ピクセル: {int(df_paddy['paddy_after_count'].mean()):,}")

print(f"\n■ 道路 (有効イベント: {len(df_road)})")
print(f"  総ピクセル数: {int(df_road['road_after_count'].sum()):,}")
print(f"  後方散乱強度 Mean (平均の平均): {df_road['road_after_mean'].mean():.2f} dB")
print(f"  後方散乱強度 Median (中央値の平均): {df_road['road_after_median'].mean():.2f} dB")
print(f"  標準偏差 平均: {df_road['road_after_std'].mean():.2f}")
print(f"  1イベントあたり平均ピクセル: {int(df_road['road_after_count'].mean()):,}")

# =====================
# 2. 信頼性スコア計算
# =====================
print("\n" + "=" * 60)
print("【2. 信頼性スコア計算方法】")
print("-" * 40)
print("信頼性 = (ピクセル数が多い) AND (標準偏差が小さい)")
print("スコア = normalized(count) - normalized(std)")
print("  → スコアが高いほど信頼性が高い")

# Both data available
df_both = df[(df['paddy_after_count'].notna()) & (df['road_after_count'].notna())].copy()
print(f"\n両方のデータがあるイベント: {len(df_both)}")

# Normalize count (0-1, higher is better)
df_both['paddy_count_norm'] = (df_both['paddy_after_count'] - df_both['paddy_after_count'].min()) / (df_both['paddy_after_count'].max() - df_both['paddy_after_count'].min())
df_both['road_count_norm'] = (df_both['road_after_count'] - df_both['road_after_count'].min()) / (df_both['road_after_count'].max() - df_both['road_after_count'].min())

# Normalize std (0-1, lower is better, so we invert)
df_both['paddy_std_norm'] = 1 - (df_both['paddy_after_std'] - df_both['paddy_after_std'].min()) / (df_both['paddy_after_std'].max() - df_both['paddy_after_std'].min())
df_both['road_std_norm'] = 1 - (df_both['road_after_std'] - df_both['road_after_std'].min()) / (df_both['road_after_std'].max() - df_both['road_after_std'].min())

# Combined reliability score (average of count and std scores)
df_both['paddy_reliability'] = (df_both['paddy_count_norm'] + df_both['paddy_std_norm']) / 2
df_both['road_reliability'] = (df_both['road_count_norm'] + df_both['road_std_norm']) / 2
df_both['combined_reliability'] = (df_both['paddy_reliability'] + df_both['road_reliability']) / 2

# Sort by combined reliability
df_sorted = df_both.sort_values('combined_reliability', ascending=False)

# =====================
# 3. 信頼性フィルタ結果
# =====================
print("\n" + "=" * 60)
print("【3. 信頼性フィルタ結果】")
print("-" * 40)

for pct in [25, 50, 75, 100]:
    n = int(len(df_sorted) * pct / 100)
    sel = df_sorted.head(n)
    
    paddy_pixels = int(sel['paddy_after_count'].sum())
    road_pixels = int(sel['road_after_count'].sum())
    grids = sel['grid_id'].nunique()
    
    print(f"\n■ 上位{pct}%選択 ({n}イベント, {grids}グリッド)")
    print(f"  田んぼ: {paddy_pixels:,} ピクセル (std平均: {sel['paddy_after_std'].mean():.2f}, mean: {sel['paddy_after_mean'].mean():.2f} dB)")
    print(f"  道路:   {road_pixels:,} ピクセル (std平均: {sel['road_after_std'].mean():.2f}, mean: {sel['road_after_mean'].mean():.2f} dB)")
    print(f"  比率:   田んぼ/道路 = {paddy_pixels/road_pixels:.1f}倍")

# =====================
# 4. 推奨選択結果
# =====================
print("\n" + "=" * 60)
print("【4. 推奨: 上位50%を選択した場合】")
print("-" * 40)

sel_50 = df_sorted.head(int(len(df_sorted) * 0.5))
print(f"イベント数: {len(sel_50)}")
print(f"グリッド数: {sel_50['grid_id'].nunique()}")
print(f"\n田んぼ:")
print(f"  ピクセル数: {int(sel_50['paddy_after_count'].sum()):,}")
print(f"  平均ピクセル/イベント: {int(sel_50['paddy_after_count'].mean()):,}")
print(f"  後方散乱強度 Mean: {sel_50['paddy_after_mean'].mean():.2f} dB")
print(f"  標準偏差 平均: {sel_50['paddy_after_std'].mean():.2f}")
print(f"\n道路:")
print(f"  ピクセル数: {int(sel_50['road_after_count'].sum()):,}")
print(f"  平均ピクセル/イベント: {int(sel_50['road_after_count'].mean()):,}")
print(f"  後方散乱強度 Mean: {sel_50['road_after_mean'].mean():.2f} dB")
print(f"  標準偏差 平均: {sel_50['road_after_std'].mean():.2f}")

# Save
output_path = r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\events_reliability_scored.csv'
df_sorted.to_csv(output_path, index=False)
print(f"\n信頼性スコア付きデータを保存: {output_path}")
