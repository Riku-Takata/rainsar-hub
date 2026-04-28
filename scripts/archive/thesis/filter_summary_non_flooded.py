"""
段階的な信頼性フィルタの適用とデータ減少の可視化
田んぼと道路を独立して分析（同一イベントである必要なし）
田んぼ: 灌水期(5-7月)を除外
道路: 全期間を対象
"""
import pandas as pd
import numpy as np

df_original = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')

print("=" * 70)
print("信頼性フィルタ基準と段階的データ減少")
print("（田んぼ・道路を独立して分析）")
print("=" * 70)

print("""
【信頼性フィルタの基準】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. ピクセル数スコア (0-1): 高いほど良い
2. 標準偏差スコア (0-1): 低いほど良い → 反転して高いほど良い
3. 統合スコア = (ピクセル数スコア + 標準偏差スコア) / 2

※ 田んぼと道路は独立して信頼性フィルタを適用
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

# =====================
# 田んぼの分析（非灌水期のみ）
# =====================
print("\n" + "=" * 70)
print("【田んぼ】非灌水期(8-4月)のみ")
print("=" * 70)

df_paddy = df_original[~df_original['month'].isin([5, 6, 7])].copy()
df_paddy = df_paddy[df_paddy['paddy_after_count'].notna()]

# Calculate reliability score for paddy
df_paddy['count_norm'] = (df_paddy['paddy_after_count'] - df_paddy['paddy_after_count'].min()) / (df_paddy['paddy_after_count'].max() - df_paddy['paddy_after_count'].min())
df_paddy['std_norm'] = 1 - (df_paddy['paddy_after_std'] - df_paddy['paddy_after_std'].min()) / (df_paddy['paddy_after_std'].max() - df_paddy['paddy_after_std'].min())
df_paddy['reliability'] = (df_paddy['count_norm'] + df_paddy['std_norm']) / 2
df_paddy = df_paddy.sort_values('reliability', ascending=False)

paddy_original = df_paddy['paddy_after_count'].sum()

print(f"\n元データ: {len(df_paddy)}イベント, {df_paddy['grid_id'].nunique()}グリッド, {int(paddy_original):,}ピクセル")
print()

print("【段階的フィルタ】")
print("-" * 50)
for pct in [100, 75, 50, 25]:
    n = int(len(df_paddy) * pct / 100)
    sel = df_paddy.head(n)
    pixels = int(sel['paddy_after_count'].sum())
    reduction = (1 - pixels / paddy_original) * 100
    print(f"上位{pct:>3}%: {n:>5}イベント, {sel['grid_id'].nunique():>3}グリッド, {pixels:>12,}ピクセル ({-reduction:>+6.1f}%)")

# =====================
# 道路の分析（全期間）
# =====================
print("\n" + "=" * 70)
print("【道路】全期間")
print("=" * 70)

df_road = df_original[df_original['road_after_count'].notna()].copy()

# Calculate reliability score for road
df_road['count_norm'] = (df_road['road_after_count'] - df_road['road_after_count'].min()) / (df_road['road_after_count'].max() - df_road['road_after_count'].min())
df_road['std_norm'] = 1 - (df_road['road_after_std'] - df_road['road_after_std'].min()) / (df_road['road_after_std'].max() - df_road['road_after_std'].min())
df_road['reliability'] = (df_road['count_norm'] + df_road['std_norm']) / 2
df_road = df_road.sort_values('reliability', ascending=False)

road_original = df_road['road_after_count'].sum()

print(f"\n元データ: {len(df_road)}イベント, {df_road['grid_id'].nunique()}グリッド, {int(road_original):,}ピクセル")
print()

print("【段階的フィルタ】")
print("-" * 50)
for pct in [100, 75, 50, 25]:
    n = int(len(df_road) * pct / 100)
    sel = df_road.head(n)
    pixels = int(sel['road_after_count'].sum())
    reduction = (1 - pixels / road_original) * 100
    print(f"上位{pct:>3}%: {n:>5}イベント, {sel['grid_id'].nunique():>3}グリッド, {pixels:>12,}ピクセル ({-reduction:>+6.1f}%)")

# =====================
# サマリー
# =====================
print("\n" + "=" * 70)
print("【サマリー比較表】")
print("=" * 70)
print()
print(f"{'選択率':<10} {'田んぼ':>18} {'道路':>18} {'比率':>8}")
print("-" * 60)

for pct in [100, 75, 50, 25]:
    n_paddy = int(len(df_paddy) * pct / 100)
    n_road = int(len(df_road) * pct / 100)
    
    paddy_px = int(df_paddy.head(n_paddy)['paddy_after_count'].sum())
    road_px = int(df_road.head(n_road)['road_after_count'].sum())
    ratio = paddy_px / road_px
    
    print(f"上位{pct:>3}%   {paddy_px:>15,}px {road_px:>15,}px  {ratio:>6.1f}倍")

print("-" * 60)

# Save filtered data
df_paddy.to_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\paddy_reliability_scored.csv', index=False)
df_road.to_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\road_reliability_scored.csv', index=False)
print("\n信頼性スコア付きデータを保存しました")
