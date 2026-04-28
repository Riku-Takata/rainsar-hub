"""
Check if Satellite Data (TIFFs) exists for the Expansion Candidate Grids/Events.
Cross-reference DB events with local file system.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json
from pathlib import Path

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"
load_dotenv(BASE_DIR / "backend/.env")

DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

engine = create_engine(DATABASE_URL)

def main():
    # 1. Load Candidate Grids
    candidates_csv = BASE_DIR / "data/analysis/suggested_grids_quality.csv"
    if not candidates_csv.exists():
        print("Candidates CSV not found.")
        return
    candidates = pd.read_csv(candidates_csv)['grid_id'].tolist()
    print(f"Checking {len(candidates)} candidate grids...")

    # 2. Query Target Events
    query = text("""
        SELECT 
            s.grid_id,
            s.event_end_ts_utc
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        AND g.max_gauge_mm_h >= 10.0
        AND s.grid_id IN :grids
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": tuple(candidates)})
        
    print(f"Total Target Events in DB: {len(df)}")
    
    # 3. Check Local Files
    exists_count = 0
    missing_count = 0
    
    # We need to match date format. 
    # Usually directories are named like "20220815_..." or just matched by date part?
    # Let's inspect one existing directory structure if possible.
    # Standard format often used: YearMonthDay (YYYYMMDD) or ISO.
    
    for _, row in df.iterrows():
        grid_id = row['grid_id']
        ts = str(row['event_end_ts_utc'])
        # ts format: '2022-08-15 10:00:00'
        # Date part: '20220815'
        date_str = ts.split(" ")[0].replace("-", "")
        
        # Search for folder containing this date in grid directory
        grid_dir = SAMPLES_DIR / grid_id
        if not grid_dir.exists():
            missing_count += 1
            # print(f"Missing Grid Dir: {grid_id}")
            continue
            
        # Check subdirs
        # The fetch script usually names them by event range or just unique ID?
        # Often it includes dates.
        found = False
        for d in grid_dir.iterdir():
            if d.is_dir() and date_str in d.name:
                # Check for TIFF
                if (d / "after_vv.tif").exists():
                    found = True
                    break
        
        if found:
            exists_count += 1
        else:
            missing_count += 1
            # print(f"Missing File: {grid_id} / {date_str}")
            
    print(f"\nVerification Results:")
    print(f"  Existing (Ready): {exists_count}")
    print(f"  Missing (Need Download): {missing_count}")
    print(f"  Availability Rate: {exists_count / len(df) * 100:.1f}%")

if __name__ == "__main__":
    main()
