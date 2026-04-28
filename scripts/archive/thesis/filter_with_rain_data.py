"""
Filter balanced data to include only events with DB rainfall info
Then recalculate downsampling
"""
import pandas as pd
import numpy as np

# Load diff data (has rainfall info)
df_diff = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\diff\all_events_diff_vv.csv')

# Load balanced data
df_paddy = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\paddy_balanced_plan.csv')
df_road = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\road_balanced_plan.csv')

print('='*60)
print('DBに降雨データがあるイベントのみにフィルタ')
print('='*60)

# Create keys for matching
df_diff['key'] = list(zip(df_diff['grid_id'], df_diff['event_name']))
df_paddy['key'] = list(zip(df_paddy['grid_id'], df_paddy['event_name']))
df_road['key'] = list(zip(df_road['grid_id'], df_road['event_name']))

# Get events with valid rainfall data
valid_rain_events = set(df_diff[df_diff['total_precip_mm'].notna()]['key'])

# Filter
df_paddy_valid = df_paddy[df_paddy['key'].isin(valid_rain_events)].copy()
df_road_valid = df_road[df_road['key'].isin(valid_rain_events)].copy()

print(f'\n【フィルタ前】')
print(f'  田んぼ: {len(df_paddy)}イベント')
print(f'  道路:   {len(df_road)}イベント')

print(f'\n【フィルタ後 (DB降雨データあり)】')
print(f'  田んぼ: {len(df_paddy_valid)}イベント')
print(f'  道路:   {len(df_road_valid)}イベント')

# Recalculate pixel counts
paddy_px_original = df_paddy_valid['paddy_after_count'].sum()
road_px = df_road_valid['road_after_count'].sum()

print(f'\n【ピクセル数】')
print(f'  田んぼ (元): {int(paddy_px_original):,}')
print(f'  道路:        {int(road_px):,}')
print(f'  比率:        {paddy_px_original/road_px:.1f}倍')

# Recalculate downsampling
target_px = road_px
df_paddy_valid['sample_size'] = (df_paddy_valid['paddy_after_count'] * (target_px / paddy_px_original)).astype(int)
sampled_paddy = df_paddy_valid['sample_size'].sum()

print(f'\n【ダウンサンプリング後】')
print(f'  田んぼ (サンプル): {int(sampled_paddy):,}')
print(f'  道路:              {int(road_px):,}')
print(f'  比率:              {sampled_paddy/road_px:.2f}倍')
print(f'  合計:              {int(sampled_paddy + road_px):,}')

# Save
df_paddy_valid.to_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\paddy_balanced_with_rain.csv', index=False)
df_road_valid.to_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\road_balanced_with_rain.csv', index=False)

print(f'\n保存完了:')
print(f'  paddy_balanced_with_rain.csv')
print(f'  road_balanced_with_rain.csv')

# Summary by delay
print('\n' + '='*60)
print('【Delay時間ごとの分布】')
print('='*60)

def get_delay(event_name):
    try:
        return float(event_name.split('_')[1].replace('h', ''))
    except:
        return None

df_paddy_valid['delay_hours'] = df_paddy_valid['event_name'].apply(get_delay)
df_road_valid['delay_hours'] = df_road_valid['event_name'].apply(get_delay)

print('\n田んぼ:')
print(df_paddy_valid.groupby('delay_hours')['sample_size'].agg(['count', 'sum']).to_string())

print('\n道路:')
print(df_road_valid.groupby('delay_hours')['road_after_count'].agg(['count', 'sum']).to_string())
