"""
Check Potential Event Counts with lower rainfall thresholds.
To see if we can fill the data gaps by relaxing criteria.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

BASE_DIR = r"D:\sotsuron\rainsar-hub"
load_dotenv(f"{BASE_DIR}/backend/.env")

DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

engine = create_engine(DATABASE_URL)

def main():
    # Load target grids to filter by the same thesis scope
    # (If we expand grids, that's a different strategy)
    # Actually, let's look at ALL S1 pairs first, then filter by Thesis Grids if needed.
    # But user asked to fill gaps for *this* study, so better keep grid constraint?
    # Let's check generally first.
    
    query = text("""
        SELECT 
            MONTH(s.event_end_ts_utc) as month,
            FLOOR(s.delay_h) as delay_bin,
            COUNT(CASE WHEN g.max_gauge_mm_h >= 10 THEN 1 END) as count_10mm,
            COUNT(CASE WHEN g.max_gauge_mm_h >= 5 THEN 1 END) as count_5mm,
            COUNT(CASE WHEN g.max_gauge_mm_h >= 1 THEN 1 END) as count_1mm
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        GROUP BY month, delay_bin
        ORDER BY month, delay_bin
    """)
    
    print("Querying database for potential events...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    print(df.to_string(index=False))
    
    # Analyze specific gaps
    print("\n--- Analysis of Critical Gaps ---")
    gaps = [
        (4, 3), (4, 9), 
        (8, 3), (8, 9),
        (10, 7)
    ]
    
    for m, d in gaps:
        row = df[(df['month'] == m) & (df['delay'] == d)]
        if not row.empty:
            c10 = row.iloc[0]['count_10mm']
            c5 = row.iloc[0]['count_5mm']
            c1 = row.iloc[0]['count_1mm']
            print(f"M{m} D{d}h: Current(>10mm)={c10} -> if >5mm={c5} (+{c5-c10}), if >1mm={c1} (+{c1-c10})")
        else:
            print(f"M{m} D{d}h: No data even with relax?")

if __name__ == "__main__":
    main()
