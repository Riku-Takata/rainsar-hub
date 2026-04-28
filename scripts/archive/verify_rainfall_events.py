"""
Verify rainfall event definition against raw GSMap data
Check if light rain continues after event end
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json

# Setup
GSMAP_DIR = Path(r"D:\sotsuron\products\binary-rain-data")
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
OUTPUT_DIR = BASE_DIR / "data" / "result" / "Aug"

# DB
load_dotenv(BASE_DIR / "backend" / ".env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"
engine = create_engine(DATABASE_URL)

def grid_id_to_latlon(grid_id):
    """Convert grid_id like N03535E13625 to center lat/lon"""
    lat_str = grid_id[1:6]
    lon_str = grid_id[7:12]
    lat = float(lat_str) / 100
    lon = float(lon_str) / 100
    return lat, lon

def get_gsmap_rainfall(lat, lon, timestamp):
    """Get rainfall rate for a specific location and time from GSMap CSV"""
    # Round to 0.1 degree grid
    lat_rounded = round(lat * 10) / 10
    lon_rounded = round(lon * 10) / 10
    
    # Build file path
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    hour = timestamp.hour
    
    csv_path = GSMAP_DIR / f"{year}" / f"{month:02d}" / f"{day:02d}" / f"gsmap_hourly_{year}{month:02d}{day:02d}_{hour:02d}00.csv"
    
    if not csv_path.exists():
        return None
    
    try:
        df = pd.read_csv(csv_path)
        # Find matching row (increase tolerance to 0.06 for 0.1 degree grid)
        match = df[
            (abs(df['Latitude'] - lat_rounded) < 0.06) & 
            (abs(df['Longitude'] - lon_rounded) < 0.06)
        ]
        if not match.empty:
            return match.iloc[0]['Gauge-calibrated-Rain-Rate']
    except:
        pass
    return None

def check_event_rainfall_timeline(grid_id, event_end_ts, hours_before=6, hours_after=6):
    """Get rainfall timeline around an event end time"""
    lat, lon = grid_id_to_latlon(grid_id)
    
    timeline = []
    start_time = event_end_ts - timedelta(hours=hours_before)
    end_time = event_end_ts + timedelta(hours=hours_after)
    
    current = start_time
    while current <= end_time:
        rain = get_gsmap_rainfall(lat, lon, current)
        timeline.append({
            'timestamp': current,
            'hours_from_end': (current - event_end_ts).total_seconds() / 3600,
            'rain_mm_h': rain
        })
        current += timedelta(hours=1)
    
    return timeline

def main():
    # Get August events used in analysis
    query = text("""
        SELECT DISTINCT s.grid_id, s.event_end_ts_utc, g.max_gauge_mm_h
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND MONTH(s.event_end_ts_utc) = 8
        AND s.delay_h BETWEEN 0 AND 12
        AND s.before_scene_id IS NOT NULL
        AND g.max_gauge_mm_h >= 10.0
        ORDER BY s.event_end_ts_utc
        LIMIT 10
    """)
    
    with engine.connect() as conn:
        events = pd.read_sql(query, conn)
    
    print(f"Checking {len(events)} August events...")
    print()
    
    results = []
    
    for idx, row in events.iterrows():
        grid_id = row['grid_id']
        event_end = row['event_end_ts_utc']
        
        print(f"Event: {grid_id} @ {event_end}")
        
        timeline = check_event_rainfall_timeline(grid_id, event_end, hours_before=3, hours_after=6)
        
        # Analyze post-event rainfall
        post_event = [t for t in timeline if t['hours_from_end'] > 0 and t['rain_mm_h'] is not None]
        
        if post_event:
            max_post_rain = max([t['rain_mm_h'] for t in post_event])
            any_rain = any([t['rain_mm_h'] > 0 for t in post_event])
            light_rain = any([0 < t['rain_mm_h'] < 10 for t in post_event])
            
            print(f"  イベント終了後 (1-6h後):")
            for t in timeline:
                marker = "<< EVENT END" if t['hours_from_end'] == 0 else ""
                rain_str = f"{t['rain_mm_h']:.1f}" if t['rain_mm_h'] is not None else "N/A"
                print(f"    {t['hours_from_end']:+.0f}h: {rain_str} mm/h {marker}")
            
            results.append({
                'grid_id': grid_id,
                'event_end': event_end,
                'max_post_rain': max_post_rain,
                'has_any_post_rain': any_rain,
                'has_light_rain_post': light_rain
            })
        print()
    
    # Summary
    if results:
        res_df = pd.DataFrame(results)
        print("=== サマリー ===")
        print(f"チェックしたイベント数: {len(res_df)}")
        print(f"終了後に何らかの雨があったイベント: {res_df['has_any_post_rain'].sum()}")
        print(f"終了後に10mm/h未満の雨があったイベント: {res_df['has_light_rain_post'].sum()}")
        
        res_df.to_csv(OUTPUT_DIR / "8月_イベント終了後降雨確認.csv", index=False, encoding='utf-8-sig')
        print(f"\n詳細を保存しました")

if __name__ == "__main__":
    main()
