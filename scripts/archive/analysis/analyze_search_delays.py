import os
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Load env from backend
env_path = r"D:\sotsuron\rainsar-hub\backend\.env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1" 
# Use mapped port for host access
DB_PORT = os.getenv("DB_PORT_HOST", "3307") 
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Connecting to {DATABASE_URL} ...")

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Query: Get delay_h for nationwide search results
        query = text("""
            SELECT 
                grid_id, 
                delay_h, 
                after_start_ts_utc as s1_time_utc,
                event_start_ts_utc as rain_start_utc,
                event_end_ts_utc as rain_end_utc,
                max_gauge_mm_h as max_rain,
                hit_hours as rain_duration_h
            FROM s1_pairs
            WHERE source = 'cdse_nationwide_search'
        """)
        
        df = pd.read_sql(query, conn)
        print(f"Total Pairs Found so far: {len(df)}")
        
        if len(df) == 0:
            print("No pairs found yet. The search might still be processing initial checks or found no matches.")
            exit()

        print("\n=== Delay Statistics (Hours) ===")
        print(df['delay_h'].describe())
        
        # Plot
        plt.figure(figsize=(10, 6))
        sns.histplot(df['delay_h'], bins=50, kde=True)
        plt.title(f"Distribution of Post-Rain Delays (n={len(df)})")
        plt.xlabel("Delay (Hours)")
        plt.ylabel("Count")
        plt.grid(True, alpha=0.3)
        
        out_png = r"D:\sotsuron\rainsar-hub\data_vv\analysis\nationwide_delay_distribution.png"
        plt.savefig(out_png)
        print(f"\nSaved histogram to: {out_png}")
        
        # Save CSV
        out_csv = r"D:\sotsuron\rainsar-hub\data_vv\analysis\nationwide_search_results.csv"
        df.to_csv(out_csv, index=False)
        print(f"Saved full list to: {out_csv}")
        
        df.to_csv(out_csv, index=False)
        print(f"Saved full list to: {out_csv}")
        
        # Grid Analysis
        print("\n=== Top Grids by Pair Count ===")
        grid_counts = df['grid_id'].value_counts().reset_index()
        grid_counts.columns = ['grid_id', 'pair_count']
        
        # Add stats per grid
        grid_stats = df.groupby('grid_id').agg(
            min_delay=('delay_h', 'min'),
            max_delay=('delay_h', 'max'),
            avg_rain=('max_rain', 'mean'),
            first_event=('rain_start_utc', 'min'),
            last_event=('rain_start_utc', 'max')
        ).reset_index()
        
        top_grids = pd.merge(grid_counts, grid_stats, on='grid_id')
        
        top_csv = r"D:\sotsuron\rainsar-hub\data_vv\analysis\nationwide_top_grids.csv"
        top_grids.head(50).to_csv(top_csv, index=False)
        print(f"Saved top 50 grids to: {top_csv}")
        
        # === 1-12h Specific Analysis (Heavy Rain Only) ===
        print("\n=== Top Grids (1-12h Window, >= 10mm/h) ===")
        # Filter Delay 1-12h AND Max Rain >= 10.0
        df_win = df[(df['delay_h'] >= 1.0) & (df['delay_h'] <= 12.0) & (df['max_rain'] >= 10.0)]
        print(f"Pairs within 1-12h (Heavy Rain): {len(df_win)}")
        
        if len(df_win) > 0:
            grid_counts_win = df_win['grid_id'].value_counts().reset_index()
            grid_counts_win.columns = ['grid_id', 'pair_count']
            
            grid_stats_win = df_win.groupby('grid_id').agg(
                min_delay=('delay_h', 'min'),
                max_delay=('delay_h', 'max'),
                avg_rain=('max_rain', 'mean'),
            ).reset_index()
            
            top_grids_win = pd.merge(grid_counts_win, grid_stats_win, on='grid_id')
            
            top_csv_win = r"D:\sotsuron\rainsar-hub\data_vv\analysis\nationwide_top_grids_1-12h_heavy.csv"
            top_grids_win.head(50).to_csv(top_csv_win, index=False)
            print(f"Saved top 50 grids (1-12h Heavy) to: {top_csv_win}")
            print(top_grids_win.head(10))

except Exception as e:
    print(f"Error: {e}")
