"""Road-only reliability analysis"""
import pandas as pd

df = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')
df_road = df[df['road_after_count'].notna()].copy()
df_road = df_road.sort_values('road_after_std')

print('=== 道路のみで信頼性フィルタ ===')
print(f'有効イベント数: {len(df_road)}')
print()

print('【道路 標準偏差のパーセンタイル】')
for p in [25, 50, 75, 90]:
    print(f'  {p}%ile: {df_road["road_after_std"].quantile(p/100):.2f}')
print()

for pct in [25, 50, 75]:
    n = int(len(df_road) * pct / 100)
    sel = df_road.head(n)
    print(f'上位{pct}% ({n}イベント, {sel["grid_id"].nunique()}グリッド):')
    print(f'  道路ピクセル: {int(sel["road_after_count"].sum()):,}')
    print(f'  道路std平均: {sel["road_after_std"].mean():.2f}')
    print()
