"""Rainfall statistics for target events"""
import pandas as pd

# Load diff data which has rainfall info
df_diff = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\diff\all_events_diff_vv.csv')

# Load balanced data event names
df_paddy = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\paddy_balanced_plan.csv')
df_road = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\road_balanced_plan.csv')

print('='*60)
print('対象降雨イベントの降水量・継続時間')
print('='*60)

# Merge to get rainfall info
paddy_events = set(zip(df_paddy['grid_id'], df_paddy['event_name']))
road_events = set(zip(df_road['grid_id'], df_road['event_name']))

# Filter diff data
df_diff['key'] = list(zip(df_diff['grid_id'], df_diff['event_name']))
df_paddy_rain = df_diff[df_diff['key'].isin(paddy_events)]
df_road_rain = df_diff[df_diff['key'].isin(road_events)]

print('\n【田んぼ対象イベントの降雨統計】')
print(f'イベント数: {len(df_paddy_rain)}')
if 'total_precip_mm' in df_paddy_rain.columns:
    valid = df_paddy_rain['total_precip_mm'].dropna()
    print(f'データありイベント: {len(valid)}')
    print(f'総降水量 平均: {valid.mean():.1f} mm')
    print(f'総降水量 中央値: {valid.median():.1f} mm')
    print(f'総降水量 範囲: {valid.min():.1f} - {valid.max():.1f} mm')
if 'duration_hours' in df_paddy_rain.columns:
    valid = df_paddy_rain['duration_hours'].dropna()
    print(f'継続時間 平均: {valid.mean():.1f} 時間')
    print(f'継続時間 中央値: {valid.median():.1f} 時間')
    print(f'継続時間 範囲: {valid.min():.1f} - {valid.max():.1f} 時間')

print('\n【道路対象イベントの降雨統計】')
print(f'イベント数: {len(df_road_rain)}')
if 'total_precip_mm' in df_road_rain.columns:
    valid = df_road_rain['total_precip_mm'].dropna()
    print(f'データありイベント: {len(valid)}')
    print(f'総降水量 平均: {valid.mean():.1f} mm')
    print(f'総降水量 中央値: {valid.median():.1f} mm')
    print(f'総降水量 範囲: {valid.min():.1f} - {valid.max():.1f} mm')
if 'duration_hours' in df_road_rain.columns:
    valid = df_road_rain['duration_hours'].dropna()
    print(f'継続時間 平均: {valid.mean():.1f} 時間')
    print(f'継続時間 中央値: {valid.median():.1f} 時間')
    print(f'継続時間 範囲: {valid.min():.1f} - {valid.max():.1f} 時間')
