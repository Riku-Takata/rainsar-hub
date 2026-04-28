"""
Combined reliability analysis showing both criteria
"""
import pandas as pd

df = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\events_reliability_scored.csv')

print("=" * 60)
print("両方の信頼性を満たすピクセル数の確認")
print("=" * 60)
print()
print("信頼性スコア = (ピクセル数スコア + 標準偏差スコア) / 2")
print("  - ピクセル数スコア: 0-1 (多いほど高い)")
print("  - 標準偏差スコア: 0-1 (小さいほど高い)")
print()

# Show score breakdown for top 50%
sel = df.head(int(len(df) * 0.5))

print("【上位50%選択時のスコア内訳】")
print("-" * 40)

print(f"\n■ 田んぼ ({len(sel)}イベント)")
print(f"  総ピクセル数: {int(sel['paddy_after_count'].sum()):,}")
print(f"  ピクセル数スコア平均: {sel['paddy_count_norm'].mean():.3f}")
print(f"  標準偏差スコア平均: {sel['paddy_std_norm'].mean():.3f}")
print(f"  → 統合スコア平均: {sel['paddy_reliability'].mean():.3f}")

print(f"\n■ 道路 ({len(sel)}イベント)")
print(f"  総ピクセル数: {int(sel['road_after_count'].sum()):,}")
print(f"  ピクセル数スコア平均: {sel['road_count_norm'].mean():.3f}")
print(f"  標準偏差スコア平均: {sel['road_std_norm'].mean():.3f}")
print(f"  → 統合スコア平均: {sel['road_reliability'].mean():.3f}")

# Compare with full data
print("\n" + "=" * 60)
print("【全データ vs 信頼性フィルタ後の比較】")
print("-" * 40)

print("\n            | 全データ      | 上位50%       | 減少率")
print("-" * 55)
full_paddy = int(df['paddy_after_count'].sum())
full_road = int(df['road_after_count'].sum())
sel_paddy = int(sel['paddy_after_count'].sum())
sel_road = int(sel['road_after_count'].sum())

print(f"田んぼピクセル | {full_paddy:>12,} | {sel_paddy:>12,} | {100*(1-sel_paddy/full_paddy):>5.1f}%")
print(f"道路ピクセル   | {full_road:>12,} | {sel_road:>12,} | {100*(1-sel_road/full_road):>5.1f}%")
print(f"比率(田/道)   | {full_paddy/full_road:>12.1f}倍 | {sel_paddy/sel_road:>12.1f}倍 |")

# Additional: show threshold approach
print("\n" + "=" * 60)
print("【参考: 閾値ベースのフィルタ】")
print("-" * 40)
print("条件: 田んぼstd < 3.0 AND 道路std < 4.0 AND 道路ピクセル >= 100")

filtered = df[(df['paddy_after_std'] < 3.0) & 
              (df['road_after_std'] < 4.0) & 
              (df['road_after_count'] >= 100)]

print(f"\nフィルタ後イベント数: {len(filtered)}")
print(f"フィルタ後グリッド数: {filtered['grid_id'].nunique()}")
print(f"田んぼピクセル: {int(filtered['paddy_after_count'].sum()):,}")
print(f"道路ピクセル: {int(filtered['road_after_count'].sum()):,}")
