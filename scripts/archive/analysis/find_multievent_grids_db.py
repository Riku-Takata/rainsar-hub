import os
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

# Load env from backend
env_path = r"D:\sotsuron\rainsar-hub\backend\.env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1" # Force localhost
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Connecting to {DATABASE_URL} ...")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Query: Get all events with reasonable delay
        # We want to find grids that have events separated by at least 1 day.
        # But first let's just dump all events for grids with count >= 2.
        
        # This query gets GridID and StartTime for all events
        query = text("""
            SELECT grid_id, start_ts_utc as rain_date
            FROM gsmap_events
            WHERE max_gauge_mm_h >= 2.0
        """)
        
        df = pd.read_sql(query, conn)
        print(f"Total Events Fetched: {len(df)}")
        
        if len(df) == 0:
            print("No events found.")
            exit()

        df['rain_date'] = pd.to_datetime(df['rain_date'])
        
        # Group by Grid
        grouped = df.groupby('grid_id')
        
        candidates = []
        for grid, g in grouped:
            # Check distinct dates (day level)
            dates = g['rain_date'].dt.date.unique()
            if len(dates) >= 2:
                # Calculate span
                span = (dates.max() - dates.min()).days
                if span >= 1:
                    candidates.append({
                        'GridID': grid,
                        'EventCount': len(dates),
                        'SpanDays': span,
                        'Dates': sorted([str(d) for d in dates])
                    })
        
        # Sort by EventCount
        candidates.sort(key=lambda x: x['EventCount'], reverse=True)
        
        # Save to CSV
        out_df = pd.DataFrame(candidates)
        out_csv = r"D:\sotsuron\rainsar-hub\data_vv\analysis\nationwide_candidates.csv"
        out_df.to_csv(out_csv, index=False)
        print(f"Saved {len(candidates)} candidates to {out_csv}")
        
        print("\nTop 10 Grids:")
        for c in candidates[:10]:
            print(f"Grid: {c['GridID']}, Events: {c['EventCount']}, Span: {c['SpanDays']} days")

except Exception as e:
    print(f"Error: {e}")
