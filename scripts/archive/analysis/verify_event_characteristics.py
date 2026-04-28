
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(BASE_DIR))
import scripts.thesis.common as common

# Load Env
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def main():
    engine = create_engine(DATABASE_URL)
    
    # 1. Load Diff Stats (CSV)
    # This contains the computed Sigma0 Diff for events processed so far (both Thesis and Expansion)
    # Note: If preprocessing is still running, this might be partial.
    # We check VV polarization as representative.
    diff_csv = BASE_DIR / "data/result/vv/diff/diff_stats_vv.csv"
    
    if not diff_csv.exists():
        print("Error: Diff stats CSV not found. Run analyze_diff.py first.")
        # But we can still analyze Rainfall characteristics from DB for all targets.
        df_diff = pd.DataFrame()
    else:
        df_diff = pd.read_csv(diff_csv)
        print(f"Loaded {len(df_diff)} processed diff events.")

    # 2. Get All August Targets (Thesis + Expansion)
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
    
    # 3. Query Rainfall Metrics from DB for ALL August Targets
    query = text("""
        SELECT 
            s.grid_id,
            s.delay_h,
            g.sum_gauge_mm_h as total_precip,
            g.max_gauge_mm_h as max_precip,
            TIMESTAMPDIFF(HOUR, g.start_ts_utc, g.end_ts_utc) as duration_h,
            DATE_FORMAT(s.event_end_ts_utc, '%Y%m%d') as event_date,
            s.event_end_ts_utc
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id 
            AND DATE(s.event_end_ts_utc) = DATE(g.end_ts_utc)
        WHERE s.grid_id IN :grids
        AND MONTH(s.event_end_ts_utc) = 8
        AND s.delay_h BETWEEN 0 AND 12
    """)
    
    with engine.connect() as conn:
        df_rain = pd.read_sql(query, conn, params={"grids": tuple(all_target_grids)})

    # Calculate Average Rainfall (Total / Duration) ? Or just Total?
    # User asked for "Average Rainfall" (average intensity?) or "Mean of Total Rainfall"?
    # Usually "Average Rainfall" means intensity (mm/h).
    df_rain['avg_intensity'] = df_rain['total_precip'] / df_rain['duration_h'].replace(0, 1)

    # 4. Merge with Diff Data
    # Diff data key: grid_id, event_name. event_name usually "delay_Xh_YYYYMMDD"
    if not df_diff.empty:
        # Create matching key in Rain DF
        # delay formatted as 1 decimal?
        df_rain['delay_str'] = df_rain['delay_h'].apply(lambda x: f"{x:.1f}")
        df_rain['key'] = df_rain['grid_id'] + "_" + df_rain['delay_str'] + "_" + df_rain['event_date']
        
        # Diff DF key
        # event_name: delay_10.0h_20240801 -> we need to extract delay and date to be sure, or just string match
        # Let's try to reconstruct the key from event_name in df_diff if possible
        # df_diff['event_name'] is literally the folder name.
        # usually "delay_{h}h_{date}"
        
        def parse_key(row):
             # event_name: delay_10.0h_20240801
             parts = row['event_name'].split('_')
             if len(parts) >= 3:
                 d = parts[1].replace('h', '')
                 date = parts[2]
                 return f"{row['grid_id']}_{d}_{date}"
             return None
             
        df_diff['key'] = df_diff.apply(parse_key, axis=1)
        
        # Merge
        # Inner join to see stats for processed events
        df_merged = pd.merge(df_rain, df_diff, left_on='key', right_on='key', how='inner', suffixes=('', '_diff'))
        
        print("\n=== Data Characteristics (Processed Events) ===")
        print(f"Count: {len(df_merged)}")
        
        columns_to_stats = [
            'paddy_diff_mean', 'road_diff_mean', 
            'total_precip', 'duration_h', 'avg_intensity'
        ]
        stats = df_merged[columns_to_stats].describe().T[['count', 'mean', 'std', 'min', '50%', 'max']]
        print(stats)
        
        # Save detailed report
        df_merged.to_csv(BASE_DIR / "data/analysis/august_data_characteristics.csv", index=False)
        print(f"\nSaved detailed list to data/analysis/august_data_characteristics.csv")

    else:
        print("\nDiff data not available yet. Showing Rainfall characteristics only.")
        columns_to_stats = ['total_precip', 'duration_h', 'avg_intensity']
        stats = df_rain[columns_to_stats].describe().T[['count', 'mean', 'std', 'min', '50%', 'max']]
        print(stats)

if __name__ == "__main__":
    main()
