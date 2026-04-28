"""
Visualize rainfall patterns after event end for ALL target months (April, August, September, October)
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

matplotlib.rcParams['font.family'] = 'MS Gothic'

GSMAP_DIR = Path(r"D:\sotsuron\products\binary-rain-data")
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "rain_continuity"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / "backend" / ".env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"
engine = create_engine(DATABASE_URL)

def grid_id_to_latlon(grid_id):
    lat = float(grid_id[1:6]) / 100
    lon = float(grid_id[7:12]) / 100
    return lat, lon

def get_gsmap_rainfall(lat, lon, timestamp):
    lat_rounded = round(lat * 10) / 10
    lon_rounded = round(lon * 10) / 10
    
    csv_path = GSMAP_DIR / f"{timestamp.year}" / f"{timestamp.month:02d}" / f"{timestamp.day:02d}" / f"gsmap_hourly_{timestamp.year}{timestamp.month:02d}{timestamp.day:02d}_{timestamp.hour:02d}00.csv"
    
    if not csv_path.exists():
        return None
    
    try:
        df = pd.read_csv(csv_path)
        match = df[
            (abs(df['Latitude'] - lat_rounded) < 0.06) & 
            (abs(df['Longitude'] - lon_rounded) < 0.06)
        ]
        if not match.empty:
            return match.iloc[0]['Gauge-calibrated-Rain-Rate']
    except:
        pass
    return None

def analyze_month(month):
    month_names = {4: '4月', 8: '8月', 9: '9月', 10: '10月'}
    print(f"--- Analyzing Month {month} ({month_names[month]}) ---")
    
    query = text("""
        SELECT DISTINCT s.grid_id, s.event_end_ts_utc
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND MONTH(s.event_end_ts_utc) = :month
        AND s.delay_h BETWEEN 0 AND 12
        AND s.before_scene_id IS NOT NULL
        AND g.max_gauge_mm_h >= 10.0
    """)
    
    with engine.connect() as conn:
        events = pd.read_sql(query, conn, params={"month": month})
    
    print(f"Processing {len(events)} events...")
    
    if len(events) == 0:
        print("No events found.")
        return

    all_timelines = []
    
    for idx, row in events.iterrows():
        lat, lon = grid_id_to_latlon(row['grid_id'])
        event_end = row['event_end_ts_utc']
        
        if idx % 50 == 0:
            print(f"  Processed {idx}/{len(events)}...")
        
        for hour_offset in range(-3, 13):
            ts = event_end + timedelta(hours=hour_offset)
            rain = get_gsmap_rainfall(lat, lon, ts)
            if rain is not None:
                all_timelines.append({
                    'hour_offset': hour_offset,
                    'rain_mm_h': rain
                })
    
    if not all_timelines:
        print("No rainfall data retrieved.")
        return

    timeline_df = pd.DataFrame(all_timelines)
    
    stats = timeline_df.groupby('hour_offset')['rain_mm_h'].agg([
        'count', 'mean', 'median', 'std'
    ])
    
    stats['prob_rain_gt_1'] = timeline_df[timeline_df['rain_mm_h'] >= 1.0].groupby('hour_offset')['rain_mm_h'].count() / stats['count']
    stats = stats.fillna(0)
    
    # Plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # 1. Intensity
    ax1.bar(stats.index, stats['mean'], 
           yerr=stats['std']/np.sqrt(stats['count']),
           color=['steelblue' if h <= 0 else 'orange' for h in stats.index],
           edgecolor='black', alpha=0.7, capsize=3)
    ax1.axvline(x=0, color='red', linestyle='--', label='イベント終了時刻')
    ax1.axhline(y=1.0, color='gray', linestyle=':', label='1.0 mm/h')
    
    ax1.set_xlabel('イベント終了からの経過時間 (h)', fontsize=12)
    ax1.set_ylabel('平均降雨強度 (mm/h)', fontsize=12)
    ax1.set_title(f'{month_names[month]} (n={len(events)}) 終了前後の降雨強度推移', fontsize=14)
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    mean_0h = stats.loc[0, 'mean'] if 0 in stats.index else 0
    ax1.annotate(f'{mean_0h:.1f} mm/h', xy=(0, mean_0h), xytext=(0.5, mean_0h+2),
                 fontsize=10, arrowprops=dict(arrowstyle='->'))

    # 2. Probability
    ax2.plot(stats.index, stats['prob_rain_gt_1'] * 100, 'o-', color='purple', linewidth=2)
    ax2.set_xlabel('イベント終了からの経過時間 (h)', fontsize=12)
    ax2.set_ylabel('降雨継続確率 (1mm/h以上) [%]', fontsize=12)
    ax2.set_ylim(0, 100)
    ax2.grid(True, alpha=0.3)
    
    for x, y in zip(stats.index, stats['prob_rain_gt_1'] * 100):
        ax2.text(x, y+5, f"{y:.0f}%", ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / f"{month}月_降雨継続分析.png", dpi=150)
    print(f"Saved plot for Month {month}")
    
    stats.reset_index().to_csv(OUTPUT_DIR / f"{month}月_降雨継続統計.csv", index=False, encoding='utf-8-sig')

def main():
    months = [4, 8, 9, 10]
    for m in months:
        analyze_month(m)

if __name__ == "__main__":
    main()
