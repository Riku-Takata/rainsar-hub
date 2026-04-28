
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(BASE_DIR))

# Load Env
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def main():
    engine = create_engine(DATABASE_URL)
    
    # 1. Get All August Events (Thesis + Expansion)
    # Re-use logic from count_august_distribution.py to get target grids
    expansion_csv = BASE_DIR / "data/analysis/suggested_grids_quality.csv"
    if expansion_csv.exists():
        expansion_grids = pd.read_csv(expansion_csv)['grid_id'].tolist()
    else:
        expansion_grids = []

    thesis_json = BASE_DIR / "data/thesis_grids_final_filtered.json"
    thesis_grids = []
    if thesis_json.exists():
        import json
        with open(thesis_json, 'r') as f:
            data = json.load(f)
            if data and isinstance(data[0], dict):
                thesis_grids = [d['grid_id'] for d in data if 'grid_id' in d]
    
    all_target_grids = list(set(expansion_grids + thesis_grids))
    
    # Query S1 Events
    query_s1 = text("""
        SELECT grid_id, delay_h, event_end_ts_utc
        FROM s1_pairs
        WHERE grid_id IN :grids
        AND MONTH(event_end_ts_utc) = 8
        AND delay_h BETWEEN 0 AND 12
    """)
    
    with engine.connect() as conn:
        df_s1 = pd.read_sql(query_s1, conn, params={"grids": tuple(all_target_grids)})
        
    print(f"Total August S1 Events: {len(df_s1)}")
    
    # 2. Check linkage with GSMaP Events
    # Valid if a record exists in gsmap_events with same grid_id and DATE(end_ts)
    # This mimics analyze_diff.py logic
    
    # Get distinct (grid_id, date) tuples from S1
    df_s1['date_str'] = pd.to_datetime(df_s1['event_end_ts_utc']).dt.strftime('%Y-%m-%d')
    unique_keys = df_s1[['grid_id', 'date_str']].drop_duplicates()
    
    print(f"Checking linkage for {len(unique_keys)} unique grid-date pairs...")
    
    # Load all relevant GSMaP events for efficiency
    query_gsmap = text("""
        SELECT grid_id, DATE_FORMAT(end_ts_utc, '%Y-%m-%d') as date_str, sum_gauge_mm_h
        FROM gsmap_events
        WHERE grid_id IN :grids
        AND MONTH(end_ts_utc) = 8
    """)
    
    with engine.connect() as conn:
        df_gsmap = pd.read_sql(query_gsmap, conn, params={"grids": tuple(all_target_grids)})
    
    # Join
    # Match S1 (grid, date) with GSMaP (grid, date)
    # We want to know which S1 events have a match
    
    # Create combined key
    df_s1['key'] = df_s1['grid_id'] + '_' + df_s1['date_str']
    df_gsmap['key'] = df_gsmap['grid_id'] + '_' + df_gsmap['date_str']
    
    valid_keys = set(df_gsmap['key'])
    
    df_s1['has_rain_data'] = df_s1['key'].isin(valid_keys)
    
    # Identify Source
    df_s1['Source'] = df_s1['grid_id'].apply(lambda x: 'Expansion' if x in expansion_grids and x not in thesis_grids else ('Thesis' if x in thesis_grids else 'shared'))
    
    # Filtered Stats
    df_valid = df_s1[df_s1['has_rain_data']]
    
    print(f"Events with Rain Data: {len(df_valid)} / {len(df_s1)} ({len(df_valid)/len(df_s1)*100:.1f}%)")
    
    # Distribution of VALID events
    bins = list(range(0, 13))
    labels = [f"{i}-{i+1}h" for i in range(0, 12)]
    df_valid['Delay_Bin'] = pd.cut(df_valid['delay_h'], bins=bins, labels=labels, include_lowest=True)
    
    summary = df_valid.pivot_table(index='Delay_Bin', columns='Source', values='grid_id', aggfunc='count', fill_value=0)
    summary['Total'] = summary.sum(axis=1)
    
    print("\n--- Linked Data Distribution (1h bins) ---")
    print(summary)

if __name__ == "__main__":
    main()
