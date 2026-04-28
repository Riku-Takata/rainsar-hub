import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.family'] = 'MS Gothic'

df = pd.read_csv(r'D:\sotsuron\rainsar-hub\data\result\seasonal\rf_data\rf_dataset_balanced.csv')
aug = df[df['month'] == 8]

# Get unique events
events = aug.groupby('event_id').agg({
    'total_rain': 'first',
    'duration': 'first',
    'delay_bin': 'first'
}).reset_index()

print('=== 8月 降雨イベント統計 ===')
print(f'イベント数: {len(events)}')
print()
print('【降水量 (mm)】')
print(f'  平均: {events["total_rain"].mean():.1f}')
print(f'  中央値: {events["total_rain"].median():.1f}')
print(f'  最小: {events["total_rain"].min():.1f}')
print(f'  最大: {events["total_rain"].max():.1f}')
print()
print('【降雨継続時間 (h)】')
print(f'  平均: {events["duration"].mean():.1f}')
print(f'  中央値: {events["duration"].median():.1f}')
print(f'  最小: {events["duration"].min():.1f}')
print(f'  最大: {events["duration"].max():.1f}')

# Save detailed list
events_out = events.copy()
events_out.columns = ['イベントID', '総降水量(mm)', '継続時間(h)', '経過時間(h)']
events_out.to_csv(r'D:\sotsuron\rainsar-hub\data\result\Aug\8月_降雨イベント一覧.csv', index=False, encoding='utf-8-sig')
print()
print('詳細を 8月_降雨イベント一覧.csv に保存しました')

# Create visualization
fig, axes = plt.subplots(1, 2, figsize=(12, 5))

# Rainfall histogram
axes[0].hist(events['total_rain'], bins=20, color='steelblue', edgecolor='black', alpha=0.7)
axes[0].set_xlabel('総降水量 (mm)', fontsize=12)
axes[0].set_ylabel('イベント数', fontsize=12)
axes[0].set_title('8月 降水量分布', fontsize=14)
axes[0].axvline(events['total_rain'].mean(), color='red', linestyle='--', label=f'平均: {events["total_rain"].mean():.1f} mm')
axes[0].axvline(events['total_rain'].median(), color='orange', linestyle='--', label=f'中央値: {events["total_rain"].median():.1f} mm')
axes[0].legend()

# Duration histogram
axes[1].hist(events['duration'], bins=20, color='forestgreen', edgecolor='black', alpha=0.7)
axes[1].set_xlabel('継続時間 (h)', fontsize=12)
axes[1].set_ylabel('イベント数', fontsize=12)
axes[1].set_title('8月 降雨継続時間分布', fontsize=14)
axes[1].axvline(events['duration'].mean(), color='red', linestyle='--', label=f'平均: {events["duration"].mean():.1f} h')
axes[1].axvline(events['duration'].median(), color='orange', linestyle='--', label=f'中央値: {events["duration"].median():.1f} h')
axes[1].legend()

plt.tight_layout()
plt.savefig(r'D:\sotsuron\rainsar-hub\data\result\Aug\8月_降雨特性.png', dpi=150)
print('グラフを 8月_降雨特性.png に保存しました')
