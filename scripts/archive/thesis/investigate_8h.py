"""Investigate 8h delay data issue"""
import pandas as pd
from sklearn.model_selection import train_test_split

df_paddy = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\paddy_balanced_with_rain.csv')
df_road = pd.read_csv(r'd:\sotsuron\rainsar-hub\data\result\vv\balanced\road_balanced_with_rain.csv')

def get_delay(event_name):
    try:
        return float(event_name.split('_')[1].replace('h', ''))
    except:
        return None

df_paddy['delay'] = df_paddy['event_name'].apply(get_delay)
df_road['delay'] = df_road['event_name'].apply(get_delay)

print('=== 8時間経過のデータ詳細 ===')
print()

paddy_8h = df_paddy[df_paddy['delay'] == 8]
road_8h = df_road[df_road['delay'] == 8]

print('【田んぼ 8h】')
print(f'  イベント数: {len(paddy_8h)}')
print(f'  グリッドID: {list(paddy_8h["grid_id"].unique())}')
print()

print('【道路 8h】')
print(f'  イベント数: {len(road_8h)}')
print(f'  グリッドID: {list(road_8h["grid_id"].unique())}')
print()

# Check train/val split
all_grids = list(set(df_paddy['grid_id'].unique()) | set(df_road['grid_id'].unique()))
train_grids, val_grids = train_test_split(all_grids, test_size=0.2, random_state=42)

paddy_8h_train = paddy_8h[paddy_8h['grid_id'].isin(train_grids)]
paddy_8h_val = paddy_8h[paddy_8h['grid_id'].isin(val_grids)]
road_8h_train = road_8h[road_8h['grid_id'].isin(train_grids)]
road_8h_val = road_8h[road_8h['grid_id'].isin(val_grids)]

print('【8h データ分割】')
print(f'  田んぼ 学習: {len(paddy_8h_train)} イベント')
print(f'  田んぼ 検証: {len(paddy_8h_val)} イベント')
print(f'  道路 学習: {len(road_8h_train)} イベント')
print(f'  道路 検証: {len(road_8h_val)} イベント')
print()

print('【問題の分析】')
if len(paddy_8h_val) == 0:
    print('  → 田んぼの8hデータが検証セットに含まれていない！')
if len(road_8h_val) == 0:
    print('  → 道路の8hデータが検証セットに含まれていない！')
    
# Check which class is missing in validation
print()
print('【検証データのラベル分布】')
print(f'  田んぼ (label=0): {len(paddy_8h_val)} 件')
print(f'  道路 (label=1): {len(road_8h_val)} 件')

if len(paddy_8h_val) == 0 or len(road_8h_val) == 0:
    print()
    print('原因: 検証データに片方のクラスしか存在しないため、')
    print('      すべてのサンプルを誤分類しても精度0%になる')
