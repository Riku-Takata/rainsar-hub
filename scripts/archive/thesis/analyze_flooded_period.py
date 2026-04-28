"""Analyze paddy data excluding flooded period (May-August)"""
import pandas as pd

df = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\sigma\all_events_detailed_vv.csv')

print('=== 田んぼデータ: 灌水期(5-8月)除外 ===')
print()

all_events = df[df['paddy_after_count'].notna()]
flooded = all_events[all_events['month'].isin([5, 6, 7])]
non_flooded = all_events[~all_events['month'].isin([5, 6, 7])]

print('【全期間】')
print(f'  イベント数: {len(all_events)}')
print(f'  グリッド数: {all_events["grid_id"].nunique()}')
print(f'  ピクセル合計: {int(all_events["paddy_after_count"].sum()):,}')
print()

print('【灌水期(5-7月)】')
print(f'  イベント数: {len(flooded)}')
print(f'  グリッド数: {flooded["grid_id"].nunique()}')
print(f'  ピクセル合計: {int(flooded["paddy_after_count"].sum()):,}')
print()

print('【非灌水期(8-4月)】')
print(f'  イベント数: {len(non_flooded)}')
print(f'  グリッド数: {non_flooded["grid_id"].nunique()}')
print(f'  ピクセル合計: {int(non_flooded["paddy_after_count"].sum()):,}')
print()

# 月別内訳
print('【月別イベント数】')
monthly = all_events.groupby('month').size()
for m, cnt in monthly.items():
    flooded_mark = "★" if m in [5, 6, 7] else ""
    print(f'  {int(m)}月: {cnt} {flooded_mark}')
