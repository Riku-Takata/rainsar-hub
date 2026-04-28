import pandas as pd
from sqlalchemy import create_engine, text
import json
import os
from dotenv import load_dotenv

load_dotenv(r'D:\sotsuron\rainsar-hub\backend\.env')
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"
engine = create_engine(DATABASE_URL)

# Get sample August event with rainfall_data
query = text("""
    SELECT g.grid_id, g.start_ts_utc, g.end_ts_utc, g.max_gauge_mm_h, g.threshold_mm_h, g.rainfall_data
    FROM gsmap_events g
    WHERE MONTH(g.end_ts_utc) = 8
    AND g.max_gauge_mm_h >= 10.0
    LIMIT 3
""")

with engine.connect() as conn:
    result = pd.read_sql(query, conn)

print("=== サンプルイベント (降雨終了時の時系列確認) ===")
for i, row in result.iterrows():
    print(f"\nイベント {i+1}: {row['grid_id']}")
    print(f"  開始: {row['start_ts_utc']}")
    print(f"  終了: {row['end_ts_utc']}")
    print(f"  閾値: {row['threshold_mm_h']} mm/h")
    print(f"  最大強度: {row['max_gauge_mm_h']} mm/h")
    
    if row['rainfall_data']:
        data = json.loads(row['rainfall_data'])
        print(f"  時系列データ点数: {len(data)}")
        print("  終了前後の降雨強度 (最後の5時間分):")
        for entry in data[-5:]:
            print(f"    {entry}")
