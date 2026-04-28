"""
Visualize rainfall patterns for ALL target months (Apr, Aug, Sep, Oct)
Optimized version: Batch processing to minimize CSV reads
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
from collections import defaultdict

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

def analyze_month_optimized(month):
    month_names = {4: '4月', 8: '8月', 9: '9月', 10: '10月'}
    print(f"--- Analyzing Month {month} ({month_names[month]}) [Optimized] ---")
    
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
    
    print(f"Total events: {len(events)}")
    if len(events) == 0:
        return

    # 1. Prepare list of all required (timestamp, lat, lon) lookups
    # Map: timestamp -> list of (event_idx, hour_offset, lat, lon)
    tasks_by_time = defaultdict(list)
    
    for idx, row in events.iterrows():
        lat, lon = grid_id_to_latlon(row['grid_id'])
        event_end = row['event_end_ts_utc']
        
        for hour_offset in range(-3, 13):
            ts = event_end + timedelta(hours=hour_offset)
            # Normalize to hour (remove minutes/seconds just in case, though usually 00:00)
            ts_hour = ts.replace(minute=0, second=0, microsecond=0)
            tasks_by_time[ts_hour].append((idx, hour_offset, lat, lon))
            
    print(f"Unique timestamps to read: {len(tasks_by_time)}")
    
    # 2. Process by timestamp (read each CSV once)
    results = [] # list of dict(event_idx, hour_offset, rain)
    
    processed_count = 0
    total_timestamps = len(tasks_by_time)
    
    sorted_timestamps = sorted(tasks_by_time.keys())
    
    for ts in sorted_timestamps:
        pk_list = tasks_by_time[ts]
        processed_count += 1
        
        if processed_count % 50 == 0:
            print(f"  Reading CSVs: {processed_count}/{total_timestamps}...")
            
        csv_path = GSMAP_DIR / f"{ts.year}" / f"{ts.month:02d}" / f"{ts.day:02d}" / f"gsmap_hourly_{ts.year}{ts.month:02d}{ts.day:02d}_{ts.hour:02d}00.csv"
        
        if not csv_path.exists():
            continue
            
        try:
            df = pd.read_csv(csv_path)
            # Optimization: Filter DF to relevant bounds first? No, simple lookup is ok for now.
            # Or broadcast lookup.
            
            # Using numpy for fast lookup
            lats = df['Latitude'].values
            lons = df['Longitude'].values
            rains = df['Gauge-calibrated-Rain-Rate'].values
            
            for (evt_idx, offset, target_lat, target_lon) in pk_list:
                # Find match (approx)
                # target_lat is typically .x5, target_lon .x5
                # The tolerance 0.06 is good.
                
                mask = (np.abs(lats - round(target_lat, 1)) < 0.06) & \
                       (np.abs(lons - round(target_lon, 1)) < 0.06)
                
                if np.any(mask):
                    val = rains[mask][0]
                    # if val is nan, treat as None or nan
                    if not np.isnan(val):
                        results.append({
                            'event_idx': evt_idx,
                            'hour_offset': offset,
                            'rain_mm_h': val
                        })
        except Exception as e:
            # print(f"Error reading {csv_path}: {e}")
            pass

    # 3. Aggregate results
    print(f"Aggregation phase... ({len(results)} data points)")
    if not results:
        print("No valid rainfall data found.")
        return

    timeline_df = pd.DataFrame(results)
    
    stats = timeline_df.groupby('hour_offset')['rain_mm_h'].agg([
        'count', 'mean', 'median', 'std'
    ])
    
    # Probability > 1.0mm/h
    # Need to be careful with denominator. 'count' is available data points.
    # Assuming 'count' roughly equals total events if no missing csvs.
    # But better to use len(events) as denominator? 
    # GSMap might have missing data. Let's use 'count' (valid data points).
    
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
    ax2.set_title('降雨が継続しているイベントの割合', fontsize=14)
    ax2.set_ylim(0, 100)
    ax2.grid(True, alpha=0.3)
    
    # Add labels specifically for 0h, 3h, 9h
    for x in [-3, 0, 3, 6, 9, 12]:
        if x in stats.index:
            y = stats.loc[x, 'prob_rain_gt_1'] * 100
            ax2.text(x, y+5, f"{y:.0f}%", ha='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / f"{month}月_全Delay降雨状況.png", dpi=150)
    print(f"Saved plot: {month}月_全Delay降雨状況.png")
    
    stats.reset_index().to_csv(OUTPUT_DIR / f"{month}月_全Delay降雨統計.csv", index=False, encoding='utf-8-sig')


def main():
    months = [4, 8, 9, 10]
    for m in months:
        analyze_month_optimized(m)

if __name__ == "__main__":
    main()
