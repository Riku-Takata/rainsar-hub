"""
Check for Grids that have masks (in data/expanded/masks) but are NOT in thesis_grids.
Calculate potential event gain from these unused grids.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json
from pathlib import Path

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
load_dotenv(BASE_DIR / "backend/.env")

DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

engine = create_engine(DATABASE_URL)

def main():
    # 1. Get Existing Masks
    masks_dir = BASE_DIR / "data/expanded/masks"
    if not masks_dir.exists():
        print("Masks dir not found.")
        return
        
    mask_grids = {d.name for d in masks_dir.iterdir() if d.is_dir()}
    print(f"Total Grids with Masks: {len(mask_grids)}")
    
    # 2. Get Thesis Grids
    json_path = BASE_DIR / "data/thesis_grids_final_filtered.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    thesis_grids = {item['grid_id'] if isinstance(item, dict) else item for item in data}
    print(f"Thesis Grids: {len(thesis_grids)}")
    
    # 3. Find Unused
    unused_grids = list(mask_grids - thesis_grids)
    print(f"Unused Grids (Ready to use): {len(unused_grids)}")
    
    if not unused_grids:
        print("No unused grids found. Need to fetch new OSM data.")
        return

    # 4. Check potential gain from Unused Grids
    query = text("""
        SELECT 
            MONTH(s.event_end_ts_utc) as month,
            FLOOR(s.delay_h) as delay_bin,
            COUNT(DISTINCT CONCAT(s.grid_id, '_', g.start_ts_utc)) as unused_gain
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        AND g.max_gauge_mm_h >= 10.0
        AND s.grid_id IN :grids
        GROUP BY month, delay_bin
    """)
    
    print("\nQuerying DB for unused grids gain...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": tuple(unused_grids)})
        
    # Pivot for display
    print(df.pivot(index='delay_bin', columns='month', values='unused_gain').fillna(0))
    
    print("\n--- Specific Gaps gain ---")
    gaps = [(4, 3), (8, 3), (10, 7)]
    for m, d in gaps:
        val = df[(df['month'] == m) & (df['delay_bin'] == d)]['unused_gain'].sum()
        print(f"M{m} D{d}h: +{int(val)} events")

if __name__ == "__main__":
    main()
