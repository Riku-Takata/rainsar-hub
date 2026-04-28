"""
ダウンサンプリング: 田んぼと道路のピクセル数を揃える
信頼性フィルタ適用後のデータを使用
"""
import pandas as pd
import numpy as np
from pathlib import Path

# Load reliability-scored data
df_paddy = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\paddy_reliability_scored.csv')
df_road = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\road_reliability_scored.csv')

print("=" * 70)
print("ダウンサンプリング: 田んぼと道路のピクセル数を揃える")
print("=" * 70)

# Show options for different filter levels
print("\n【各選択率でのダウンサンプリング結果】")
print("-" * 70)

results = []

for pct in [100, 75, 50, 25]:
    n_paddy = int(len(df_paddy) * pct / 100)
    n_road = int(len(df_road) * pct / 100)
    
    sel_paddy = df_paddy.head(n_paddy)
    sel_road = df_road.head(n_road)
    
    paddy_px = int(sel_paddy['paddy_after_count'].sum())
    road_px = int(sel_road['road_after_count'].sum())
    
    # Downsampling strategy: match to the smaller count (road)
    # We'll downsample paddy to match road pixel count
    target_px = road_px
    
    # Calculate how much to sample from paddy
    # Simple approach: sample fraction of events to get approximately target pixels
    sample_ratio = target_px / paddy_px
    
    results.append({
        'pct': pct,
        'paddy_events': n_paddy,
        'road_events': n_road,
        'paddy_px': paddy_px,
        'road_px': road_px,
        'ratio': paddy_px / road_px,
        'sample_ratio': sample_ratio
    })
    
    print(f"\n■ 信頼性上位{pct}%")
    print(f"  田んぼ: {n_paddy}イベント, {paddy_px:,}ピクセル")
    print(f"  道路:   {n_road}イベント, {road_px:,}ピクセル")
    print(f"  比率:   {paddy_px/road_px:.1f}倍")
    print(f"  → ダウンサンプリング率: {sample_ratio*100:.1f}% (田んぼを道路に合わせる)")

# Recommend 25% selection
print("\n" + "=" * 70)
print("【信頼性上位25%でのダウンサンプリング】")
print("=" * 70)

# Use 25% filtered data
pct = 25
n_paddy = int(len(df_paddy) * pct / 100)
n_road = int(len(df_road) * pct / 100)

sel_paddy = df_paddy.head(n_paddy).copy()
sel_road = df_road.head(n_road).copy()

paddy_px_total = int(sel_paddy['paddy_after_count'].sum())
road_px_total = int(sel_road['road_after_count'].sum())

print(f"\nフィルタ後:")
print(f"  田んぼ: {n_paddy}イベント, {paddy_px_total:,}ピクセル")
print(f"  道路:   {n_road}イベント, {road_px_total:,}ピクセル")

# Method: Random sampling of paddy pixels to match road
target_pixels = road_px_total

# For each paddy event, we'll sample a fraction of pixels
# Store the sample size for each event
np.random.seed(42)  # For reproducibility

sel_paddy['sample_size'] = (sel_paddy['paddy_after_count'] * (target_pixels / paddy_px_total)).astype(int)
sampled_paddy_total = sel_paddy['sample_size'].sum()

print(f"\nダウンサンプリング後:")
print(f"  田んぼ: {n_paddy}イベント, {sampled_paddy_total:,}ピクセル (サンプル)")
print(f"  道路:   {n_road}イベント, {road_px_total:,}ピクセル")
print(f"  比率:   {sampled_paddy_total/road_px_total:.2f}倍 (≒1:1)")

# Save the sampling plan
output_dir = Path(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced')
output_dir.mkdir(parents=True, exist_ok=True)

sel_paddy.to_csv(output_dir / 'paddy_balanced_plan.csv', index=False)
sel_road.to_csv(output_dir / 'road_balanced_plan.csv', index=False)

print(f"\nサンプリング計画を保存:")
print(f"  {output_dir / 'paddy_balanced_plan.csv'}")
print(f"  {output_dir / 'road_balanced_plan.csv'}")

# Summary table
print("\n" + "=" * 70)
print("【最終データセット】")
print("=" * 70)
print(f"\n{'データタイプ':<15} {'イベント数':>10} {'ピクセル数':>15}")
print("-" * 45)
print(f"{'田んぼ (サンプル)':<15} {n_paddy:>10} {sampled_paddy_total:>15,}")
print(f"{'道路':<15} {n_road:>10} {road_px_total:>15,}")
print("-" * 45)
print(f"{'合計':<15} {'-':>10} {sampled_paddy_total + road_px_total:>15,}")
