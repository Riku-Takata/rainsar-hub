"""
段階的な信頼性フィルタの適用とデータ減少の可視化
"""
import pandas as pd

df_original = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')

print("=" * 70)
print("信頼性フィルタ基準と段階的データ減少")
print("=" * 70)

print("""
【信頼性フィルタの基準】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. ピクセル数スコア (0-1)
   - 各イベントのピクセル数を正規化
   - 高いほど良い（データ量が多い）

2. 標準偏差スコア (0-1)  
   - 各イベントの後方散乱強度の標準偏差を正規化
   - 低いほど良い（データのばらつきが少ない）→ 反転して高いほど良い

3. 統合信頼性スコア
   - スコア = (ピクセル数スコア + 標準偏差スコア) / 2
   - 田んぼと道路それぞれで計算し、平均を取る

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

# Step-by-step filtering
results = []

# Step 0: Original data
results.append({
    "ステップ": "0. 元データ（全イベント）",
    "イベント数": len(df_original),
    "グリッド数": df_original['grid_id'].nunique(),
    "田んぼピクセル": df_original['paddy_after_count'].sum(),
    "道路ピクセル": df_original['road_after_count'].sum()
})

# Step 1: Events with both paddy and road data
df_step1 = df_original[(df_original['paddy_after_count'].notna()) & 
                        (df_original['road_after_count'].notna())]
results.append({
    "ステップ": "1. 田んぼ・道路両方あり",
    "イベント数": len(df_step1),
    "グリッド数": df_step1['grid_id'].nunique(),
    "田んぼピクセル": df_step1['paddy_after_count'].sum(),
    "道路ピクセル": df_step1['road_after_count'].sum()
})

# Load reliability scored data
df_scored = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\events_reliability_scored.csv')

# Step 2-5: Top N% by reliability score
for pct in [75, 50, 25]:
    n = int(len(df_scored) * pct / 100)
    sel = df_scored.head(n)
    results.append({
        "ステップ": f"2. 信頼性上位{pct}%",
        "イベント数": len(sel),
        "グリッド数": sel['grid_id'].nunique(),
        "田んぼピクセル": sel['paddy_after_count'].sum(),
        "道路ピクセル": sel['road_after_count'].sum()
    })

# Create table
df_results = pd.DataFrame(results)
df_results['田んぼピクセル'] = df_results['田んぼピクセル'].astype(int)
df_results['道路ピクセル'] = df_results['道路ピクセル'].astype(int)

# Calculate reduction rates
df_results['田んぼ減少率'] = ""
df_results['道路減少率'] = ""
original_paddy = df_results.loc[0, '田んぼピクセル']
original_road = df_results.loc[0, '道路ピクセル']

for i in range(1, len(df_results)):
    paddy_rate = (1 - df_results.loc[i, '田んぼピクセル'] / original_paddy) * 100
    road_rate = (1 - df_results.loc[i, '道路ピクセル'] / original_road) * 100
    df_results.loc[i, '田んぼ減少率'] = f"-{paddy_rate:.1f}%"
    df_results.loc[i, '道路減少率'] = f"-{road_rate:.1f}%"

print("【段階的データ減少表】")
print("-" * 70)
print()

# Format numbers with commas
for idx, row in df_results.iterrows():
    print(f"━━━ {row['ステップ']} ━━━")
    print(f"  イベント数:     {row['イベント数']:>6}")
    print(f"  グリッド数:     {row['グリッド数']:>6}")
    print(f"  田んぼピクセル: {row['田んぼピクセル']:>15,} {row['田んぼ減少率']:>8}")
    print(f"  道路ピクセル:   {row['道路ピクセル']:>15,} {row['道路減少率']:>8}")
    print()

# Summary table
print("=" * 70)
print("【サマリー表】")
print("-" * 70)
print(f"{'ステップ':<25} {'イベント':>8} {'グリッド':>8} {'田んぼpx':>15} {'道路px':>12}")
print("-" * 70)
for idx, row in df_results.iterrows():
    step = row['ステップ'].split('. ')[1] if '. ' in row['ステップ'] else row['ステップ']
    print(f"{step:<25} {row['イベント数']:>8} {row['グリッド数']:>8} {row['田んぼピクセル']:>15,} {row['道路ピクセル']:>12,}")
print("-" * 70)
