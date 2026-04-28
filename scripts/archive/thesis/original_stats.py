"""Original data statistics for paddy and road"""
import pandas as pd

df = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')

# Paddy (non-flooded: exclude May-July)
df_paddy = df[~df['month'].isin([5, 6, 7])]
df_paddy = df_paddy[df_paddy['paddy_after_count'].notna()]

# Road (all periods)  
df_road = df[df['road_after_count'].notna()]

print('='*60)
print('【田んぼ】非灌水期(8-4月)のみ')
print('='*60)
print(f'イベント数: {len(df_paddy)}')
print(f'グリッド数: {df_paddy["grid_id"].nunique()}')
print()
print('■ ピクセル数統計')
print(f'  総ピクセル: {int(df_paddy["paddy_after_count"].sum()):,}')
print(f'  平均/イベント: {df_paddy["paddy_after_count"].mean():,.0f}')
print(f'  中央値/イベント: {df_paddy["paddy_after_count"].median():,.0f}')
print(f'  最小/イベント: {int(df_paddy["paddy_after_count"].min()):,}')
print(f'  最大/イベント: {int(df_paddy["paddy_after_count"].max()):,}')
print()
print('■ 後方散乱強度統計 (dB)')
print(f'  Mean平均: {df_paddy["paddy_after_mean"].mean():.2f}')
print(f'  Mean中央値: {df_paddy["paddy_after_mean"].median():.2f}')
print(f'  Mean最小: {df_paddy["paddy_after_mean"].min():.2f}')
print(f'  Mean最大: {df_paddy["paddy_after_mean"].max():.2f}')
print(f'  Std平均: {df_paddy["paddy_after_std"].mean():.2f}')

print()
print('='*60)
print('【道路】全期間')
print('='*60)
print(f'イベント数: {len(df_road)}')
print(f'グリッド数: {df_road["grid_id"].nunique()}')
print()
print('■ ピクセル数統計')
print(f'  総ピクセル: {int(df_road["road_after_count"].sum()):,}')
print(f'  平均/イベント: {df_road["road_after_count"].mean():,.0f}')
print(f'  中央値/イベント: {df_road["road_after_count"].median():,.0f}')
print(f'  最小/イベント: {int(df_road["road_after_count"].min()):,}')
print(f'  最大/イベント: {int(df_road["road_after_count"].max()):,}')
print()
print('■ 後方散乱強度統計 (dB)')
print(f'  Mean平均: {df_road["road_after_mean"].mean():.2f}')
print(f'  Mean中央値: {df_road["road_after_mean"].median():.2f}')
print(f'  Mean最小: {df_road["road_after_mean"].min():.2f}')
print(f'  Mean最大: {df_road["road_after_mean"].max():.2f}')
print(f'  Std平均: {df_road["road_after_std"].mean():.2f}')
