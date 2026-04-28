import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env from backend
env_path = r"D:\sotsuron\rainsar-hub\backend\.env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1" 
DB_PORT = os.getenv("DB_PORT_HOST", "3307") 
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Modified to process Selected Best Grids
top_csv = r"D:\sotsuron\rainsar-hub\data_vv\analysis\best_50_grids.csv"
if not os.path.exists(top_csv):
    print("Top grids file not found.")
    exit()

df_top = pd.read_csv(top_csv)
target_grids = df_top['grid_id'].tolist() 

print(f"Generating detailed report for {len(target_grids)} selected grids...")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # 1. Skip finding grids, use loaded list
        valid_grids = target_grids

        # 2. Get Details
        query = text("""
            SELECT 
                grid_id, 
                delay_h, 
                after_start_ts_utc,
                after_platform,
                after_pass_direction,
                after_relative_orbit,
                after_scene_id,
                before_start_ts_utc,
                before_scene_id,
                event_start_ts_utc,
                event_end_ts_utc,
                max_gauge_mm_h,
                hit_hours
            FROM s1_pairs
            WHERE source = 'cdse_nationwide_search'
            AND delay_h >= 1.0 AND delay_h <= 12.0
            AND max_gauge_mm_h >= 10.0
            AND grid_id IN :grids
            AND before_scene_id IS NOT NULL
            ORDER BY grid_id, after_start_ts_utc
        """)
        
        # Use chunked reading if massive, but 14k rows is fine for pandas
        df_events = pd.read_sql(query, conn, params={"grids": valid_grids})
        
        # Output Markdown Report
        out_md = r"D:\sotsuron\rainsar-hub\data_vv\analysis\grid_event_details_best_31.md"
        with open(out_md, "w", encoding="utf-8") as f:
            f.write("# Grid Event Details (Selected Best 31 / Diverse & Heavy)\n\n")
            f.write(f"Total Grids: {len(valid_grids)}\n")
            f.write(f"Total Pairs: {len(df_events)}\n\n")
            
            # Group iterate
            for grid, g_df in df_events.groupby('grid_id'):
                f.write(f"## Grid: {grid} (Count: {len(g_df)})\n")
                
                # Table
                f.write("| S1 After (UTC) | S1 Before (UTC) | Plat/Orbit | Rain End (UTC) | Delay (h) | Max Rain (mm/h) | After ID | Before ID |\n")
                f.write("|---|---|---|---|---|---|---|---|\n")
                
                for _, row in g_df.iterrows():
                    s1_info = f"{row['after_platform']} {row['after_pass_direction']} {row['after_relative_orbit']}"
                    f.write(f"| {row['after_start_ts_utc']} | {row['before_start_ts_utc']} | {s1_info} | {row['event_end_ts_utc']} | {row['delay_h']:.2f} | {row['max_gauge_mm_h']:.1f} | {row['after_scene_id']} | {row['before_scene_id']} |\n")
                
                f.write("\n")
                
        print(f"Saved detailed report to: {out_md}")

except Exception as e:
    print(f"Error: {e}")
