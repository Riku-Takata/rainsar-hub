"""
Check potential events (>10mm/h) in ALL grids vs Thesis Grids.
To estimate data gain if we expand the grid list.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json

BASE_DIR = r"D:\sotsuron\rainsar-hub"
load_dotenv(f"{BASE_DIR}/backend/.env")

DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

engine = create_engine(DATABASE_URL)

def main():
    # Load current thesis grids
    json_path = f"{BASE_DIR}/data/thesis_grids_final_filtered.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # Assuming list of grid IDs or objects
        # Check structure first
        # Based on previous context, it's likely a list of objects
        
        # Actually, let's just inspect what we load.
        # Often it's [{'grid_id': '...'}, ...] or just list of strings.
        pass
    
    # We can handle json loading robustly
    try:
        current_grids = [item['grid_id'] if isinstance(item, dict) else item for item in data]
    except:
        current_grids = []
        print("Warning: Could not parse JSON grids correctly. Assuming empty for now.")
    
    current_grids_str = "', '".join(current_grids)
    
    # Query: Total Events vs Current Thesis Events
    query = text("""
        SELECT 
            MONTH(s.event_end_ts_utc) as month,
            FLOOR(s.delay_h) as delay_bin,
            COUNT(DISTINCT CONCAT(s.grid_id, '_', g.start_ts_utc)) as total_events_all_grids,
            COUNT(DISTINCT CASE WHEN s.grid_id IN :grids THEN CONCAT(s.grid_id, '_', g.start_ts_utc) END) as current_thesis_events
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        AND g.max_gauge_mm_h >= 10.0
        GROUP BY month, delay_bin
        ORDER BY month, delay_bin
    """)
    
    print(f"Loaded {len(current_grids)} thesis grids.")
    print("Querying database for potential grid expansion...")
    
    with engine.connect() as conn:
        # Pass list properly using bindparam if needed, but simple string formatting or parameter binding works
        # SQLAlchemy explicit IN clause handling is safer with tuple
        df = pd.read_sql(query, conn, params={"grids": tuple(current_grids)})
        
    df['potential_gain'] = df['total_events_all_grids'] - df['current_thesis_events']
    df['gain_ratio'] = df['total_events_all_grids'] / df['current_thesis_events'].replace(0, 1)
    
    print(df.to_string(index=False))
    
    # Check critical gaps
    print("\n--- Analysis of Critical Gaps (Potential Gain) ---")
    gaps = [(4, 3), (8, 3), (10, 7)]
    for m, d in gaps:
        row = df[(df['month'] == m) & (df['delay_bin'] == d)]
        if not row.empty:
            curr = row.iloc[0]['current_thesis_events']
            total = row.iloc[0]['total_events_all_grids']
            print(f"M{m} D{d}h: Current={curr} -> Total={total} (Gain: +{total-curr})")

if __name__ == "__main__":
    main()
