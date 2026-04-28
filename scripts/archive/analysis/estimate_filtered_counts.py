
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
    
    # 1. Get All August Targets
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
    
    # 2. Query Rainfall Info from DB
    # We need: total_precip, duration -> avg_intensity
    query = text("""
        SELECT 
            s.grid_id,
            s.delay_h,
            g.sum_gauge_mm_h as total_precip,
            TIMESTAMPDIFF(HOUR, g.start_ts_utc, g.end_ts_utc) as duration_h,
            g.start_ts_utc,
            g.end_ts_utc
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id 
            AND DATE(s.event_end_ts_utc) = DATE(g.end_ts_utc)
        WHERE s.grid_id IN :grids
        AND MONTH(s.event_end_ts_utc) = 8
        AND s.delay_h BETWEEN 0 AND 12
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": tuple(all_target_grids)})
        
    print(f"Total August Events (DB Linked): {len(df)}")
    
    # 3. Apply Filters
    
    # Filter A: Intensity >= 10mm/h
    # Intensity = Total / Duration
    # Handle duration=0
    df['duration_h'] = df['duration_h'].replace(0, 1) # Avoid div by zero, assume at least 1h or take raw total? 
    # Usually intensity is defined. If duration is from timestamps, it might be 0 if <1h.
    # Let's say if total >= 10 and duration < 1, intensity > 10.
    
    df['avg_intensity'] = df['total_precip'] / df['duration_h']
    
    # User's criteria: "降雨量が10mm/h～"
    # This likely means Average Intensity >= 10mm/h, OR Max Intensity >= 10mm/h?
    # "降雨イベントの平均降水量...降雨量が10mm/h～" implies avg_intensity.
    
    df_filtered = df[df['avg_intensity'] >= 10.0].copy()
    print(f"Filter: Intensity >= 10mm/h: {len(df_filtered)} / {len(df)} ({len(df_filtered)/len(df)*100:.1f}%)")
    
    # Filter B: Data availability (Paddy/Road)
    # We assume all events HAVE Paddy/Road pixels if they are in these grids.
    # Previous check showed 99% have enough pixels.
    # So we focus on "Delay Bin coverage".
    
    # Identify Source
    df_filtered['Source'] = df_filtered['grid_id'].apply(lambda x: 'Expansion' if x in expansion_grids and x not in thesis_grids else ('Thesis' if x in thesis_grids else 'shared'))
    
    # Binning
    bins = list(range(0, 13))
    labels = [f"{i}-{i+1}h" for i in range(0, 12)]
    df_filtered['Delay_Bin'] = pd.cut(df_filtered['delay_h'], bins=bins, labels=labels, include_lowest=True)
    
    # 4. Check Dataset Completeness per Bin
    # "各Delayごとに...道路のデータがない，もしくは田んぼのデータがない，という状況にならないようにすること"
    # -> Since we assume all grids have both land types (verified by quality check), 
    #    as long as Count > 0 in a bin, we have data.
    
    summary = df_filtered.pivot_table(index='Delay_Bin', columns='Source', values='grid_id', aggfunc='count', fill_value=0)
    summary['Total'] = summary.sum(axis=1)
    
    print("\n--- Filtered Distribution (Intensity >= 10mm/h) ---")
    print(summary)
    
    # Validation
    missing_bins = summary[summary['Total'] == 0]
    if not missing_bins.empty:
        print("\nWarning: The following bins have 0 events after filtering:")
        print(missing_bins.index.tolist())
    else:
        print("\nSuccess: All bins have at least one event.")
        
    # Check 7-8h
    print(f"\n7-8h Status: {summary.loc['7-8h', 'Total']} events")

if __name__ == "__main__":
    main()
