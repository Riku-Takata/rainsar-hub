"""
Compare Rainfall Event Duration (hit_hours) between April and August
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

matplotlib.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = r"D:\sotsuron\rainsar-hub"
OUTPUT_DIR = f"{BASE_DIR}/data/result/seasonal/rain_continuity"

load_dotenv(f"{BASE_DIR}/backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"
engine = create_engine(DATABASE_URL)

def main():
    months = [4, 8]
    
    event_data = []
    
    for month in months:
        query = text("""
            SELECT DISTINCT s.grid_id, s.event_end_ts_utc, 
                   g.hit_hours as duration
            FROM s1_pairs s
            JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
            WHERE s.source = 'cdse_nationwide_search'
            AND MONTH(s.event_end_ts_utc) = :month
            AND s.delay_h BETWEEN 0 AND 12
            AND s.before_scene_id IS NOT NULL
            AND g.max_gauge_mm_h >= 10.0
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"month": month})
            df['month'] = month
            event_data.append(df)
            
    all_df = pd.concat(event_data, ignore_index=True)
    
    # Statistics
    stats = all_df.groupby('month')['duration'].agg(['count', 'mean', 'median', 'std', 'min', 'max']).reset_index()
    print("\n=== 降雨継続時間 (hours) 統計 ===")
    print(stats.to_string(index=False))
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    data_4 = all_df[all_df['month'] == 4]['duration']
    data_8 = all_df[all_df['month'] == 8]['duration']
    
    # Histogram
    bins = range(0, int(all_df['duration'].max()) + 2)
    
    ax.hist(data_4, bins=bins, alpha=0.6, label='4月', density=True, color='skyblue', edgecolor='blue')
    ax.hist(data_8, bins=bins, alpha=0.6, label='8月', density=True, color='orange', edgecolor='red')
    
    ax.set_xlabel('降雨継続時間 (h)', fontsize=12)
    ax.set_ylabel('密度 (頻度)', fontsize=12)
    ax.set_title('降雨継続時間の季節比較 (4月 vs 8月)', fontsize=14)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    # Add mean lines
    ax.axvline(data_4.mean(), color='blue', linestyle='--', linewidth=2, label=f'4月平均: {data_4.mean():.1f}h')
    ax.axvline(data_8.mean(), color='red', linestyle='--', linewidth=2, label=f'8月平均: {data_8.mean():.1f}h')
    
    # Add median lines
    # ax.axvline(data_4.median(), color='blue', linestyle=':', label=f'4月中央値: {data_4.median():.1f}h')
    # ax.axvline(data_8.median(), color='red', linestyle=':', label=f'8月中央値: {data_8.median():.1f}h')

    ax.legend()
    ax.set_xlim(0, 24) # Focus on main range (some might be longer but rare)

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/4月8月_継続時間比較.png", dpi=150)
    print(f"Saved: {OUTPUT_DIR}/4月8月_継続時間比較.png")
    
    # Save CSV
    all_df.to_csv(f"{OUTPUT_DIR}/4月8月_継続時間詳細.csv", index=False)

if __name__ == "__main__":
    main()
