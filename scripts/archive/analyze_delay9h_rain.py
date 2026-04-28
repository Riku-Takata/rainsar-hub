"""
Analyze rainfall condition specifically for Delay 9h events
to understand the accuracy spike.
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

def main():
    # FOCUS ON DELAY 9h EVENTS IN AUGUST
    print("Fetching Delay 9h events for August...")
    query = text("""
        SELECT DISTINCT s.grid_id, s.event_end_ts_utc, s.delay_h
        FROM s1_pairs s
        WHERE s.source = 'cdse_nationwide_search'
        AND MONTH(s.event_end_ts_utc) = 8
        AND FLOOR(s.delay_h) = 9
        AND s.before_scene_id IS NOT NULL
    """)
    
    with engine.connect() as conn:
        events = pd.read_sql(query, conn)
    
    print(f"Found {len(events)} events for Delay 9h.")
    
    results = []
    
    for idx, row in events.iterrows():
        lat_str = row['grid_id'][1:6]
        lon_str = row['grid_id'][7:12]
        lat = float(lat_str) / 100
        lon = float(lon_str) / 100
        
        event_end = row['event_end_ts_utc']
        # Check rain at Delay 9h (Event End + 9h)
        target_ts = event_end + timedelta(hours=9)
        
        rain = get_gsmap_rainfall(lat, lon, target_ts)
        
        results.append({
            'grid_id': row['grid_id'],
            'event_end': event_end,
            'target_ts': target_ts,
            'rain_mm_h': rain if rain is not None else -1
        })
    
    df = pd.DataFrame(results)
    
    # Filter valid
    valid_df = df[df['rain_mm_h'] >= 0]
    
    # Statistics
    rain_count = valid_df[valid_df['rain_mm_h'] > 0]['rain_mm_h'].count()
    total_count = len(valid_df)
    mean_rain = valid_df['rain_mm_h'].mean()
    max_rain = valid_df['rain_mm_h'].max()
    
    print("\n=== Delay 9h 時点の降雨状況 ===")
    print(f"有効データ数: {total_count}")
    print(f"雨が降っているイベント数: {rain_count} ({rain_count/total_count*100:.1f}%)")
    print(f"平均降雨強度: {mean_rain:.2f} mm/h")
    print(f"最大降雨強度: {max_rain:.2f} mm/h")
    
    # Plot histogram
    plt.figure(figsize=(10, 6))
    plt.hist(valid_df['rain_mm_h'], bins=20, color='purple', edgecolor='black', alpha=0.7)
    plt.title('Delay 9h 時点での降雨強度分布', fontsize=14)
    plt.xlabel('降雨強度 (mm/h)', fontsize=12)
    plt.ylabel('イベント数', fontsize=12)
    plt.axvline(x=0, color='red', linestyle='--', label='0 mm/h')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_Delay9h_降雨状況.png", dpi=150)
    print("Saved: 8月_Delay9h_降雨状況.png")
    
    valid_df.to_csv(OUTPUT_DIR / "8月_Delay9h_詳細データ.csv", index=False)

if __name__ == "__main__":
    main()
