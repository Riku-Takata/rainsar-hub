"""
Visualize rainfall patterns after event end for August events
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
OUTPUT_DIR = BASE_DIR / "data" / "result" / "Aug"

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
    return None

def main():
    # Get August events
    query = text("""
        SELECT DISTINCT s.grid_id, s.event_end_ts_utc, g.max_gauge_mm_h
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND MONTH(s.event_end_ts_utc) = 8
        AND s.delay_h BETWEEN 0 AND 12
        AND s.before_scene_id IS NOT NULL
        AND g.max_gauge_mm_h >= 10.0
    """)
    
    with engine.connect() as conn:
        events = pd.read_sql(query, conn)
    
    print(f"Processing ALL {len(events)} events (this may take a few minutes)...")
    
    # Collect rainfall timelines
    all_timelines = []
    
    # Cache for CSVs to speed up (Lazy loading map: path -> df)
    # Actually, simplistic approach is fine for ~120 events -> ~1200 reads.
    
    for idx, row in events.iterrows():
        lat, lon = grid_id_to_latlon(row['grid_id'])
        event_end = row['event_end_ts_utc']
        
        if idx % 10 == 0:
            print(f"  Processed {idx}/{len(events)}...")
        
        for hour_offset in range(-3, 7):
            ts = event_end + timedelta(hours=hour_offset)
            rain = get_gsmap_rainfall(lat, lon, ts)
            if rain is not None:
                all_timelines.append({
                    'event_id': f"{row['grid_id']}_{event_end}",
                    'hour_offset': hour_offset,
                    'rain_mm_h': rain
                })
    
    timeline_df = pd.DataFrame(all_timelines)
    
    # Calculate detailed stats
    stats = timeline_df.groupby('hour_offset')['rain_mm_h'].agg([
        'count', 'mean', 'median', 'std', 'min', 'max'
    ])
    
    # Calculate probabilities
    stats['prob_rain_gt_0'] = timeline_df[timeline_df['rain_mm_h'] > 0].groupby('hour_offset')['rain_mm_h'].count() / stats['count']
    stats['prob_rain_gt_1'] = timeline_df[timeline_df['rain_mm_h'] >= 1.0].groupby('hour_offset')['rain_mm_h'].count() / stats['count']
    stats = stats.fillna(0)
    
    # Plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Subplot 1: Intensity
    ax1.bar(stats.index, stats['mean'], 
           yerr=stats['std']/np.sqrt(stats['count']),
           color=['steelblue' if h <= 0 else 'orange' for h in stats.index],
           edgecolor='black', alpha=0.7, capsize=3)
    
    ax1.axvline(x=0, color='red', linestyle='--', linewidth=2, label='イベント終了時刻')
    ax1.axhline(y=1.0, color='gray', linestyle=':', linewidth=1, label='閾値 1.0 mm/h')
    
    ax1.set_xlabel('イベント終了からの経過時間 (h)', fontsize=12)
    ax1.set_ylabel('平均降雨強度 (mm/h)', fontsize=12)
    ax1.set_title(f'8月 全イベント(n={len(events)}) 終了前後の降雨強度推移', fontsize=14)
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # Annotations
    mean_0h = stats.loc[0, 'mean']
    ax1.annotate(f'{mean_0h:.1f} mm/h', xy=(0, mean_0h), xytext=(0.5, mean_0h+2),
                 fontsize=10, arrowprops=dict(arrowstyle='->'))
                 
    # Subplot 2: Probability of Rain > 1.0mm/h
    ax2.plot(stats.index, stats['prob_rain_gt_1'] * 100, 'o-', color='purple', linewidth=2)
    ax2.set_xlabel('イベント終了からの経過時間 (h)', fontsize=12)
    ax2.set_ylabel('降雨継続確率 (1mm/h以上) [%]', fontsize=12)
    ax2.set_title('降雨が継続しているイベントの割合', fontsize=14)
    ax2.set_ylim(0, 100)
    ax2.grid(True, alpha=0.3)
    
    for x, y in zip(stats.index, stats['prob_rain_gt_1'] * 100):
        ax2.text(x, y+3, f"{y:.1f}%", ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_全イベント終了後降雨推移.png", dpi=150)
    print("Saved: 8月_全イベント終了後降雨推移.png")
    
    # Save CSV
    stats.reset_index().to_csv(OUTPUT_DIR / "8月_全イベント終了後統計.csv", index=False, encoding='utf-8-sig')
    print("Saved: 8月_全イベント終了後統計.csv")

if __name__ == "__main__":
    main()
