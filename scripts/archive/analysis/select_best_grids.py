import os
import pandas as pd
import numpy as np
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

def main():
    print("Connecting to DB...")
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Get all valid pairs (Heavy Rain >= 10mm, Delay 1-12h, Nationwide, Paired)
        query = text("""
            SELECT 
                grid_id, 
                delay_h, 
                max_gauge_mm_h
            FROM s1_pairs
            WHERE source = 'cdse_nationwide_search'
            AND delay_h >= 1.0 AND delay_h <= 12.0
            AND max_gauge_mm_h >= 10.0
            AND before_scene_id IS NOT NULL
        """)
        df = pd.read_sql(query, conn)
    
    print(f"Total valid pairs loaded: {len(df)}")
    
    # Group by Grid
    results = []
    
    for grid, g_df in df.groupby('grid_id'):
        count = len(g_df)
        
        # Criteria 1: Count >= 8
        if count < 8:
            continue
            
        delays = g_df['delay_h'].values
        
        # Criteria 2: Has Delay < 2.0h
        if np.min(delays) >= 2.0:
            continue
            
        # Criteria 3: Diversity (Unique Integer Hours)
        # 1.0-1.9 -> 1, 9.0-9.9 -> 9
        unique_hours = len(set(delays.astype(int)))
        
        # Criteria 4: Rain Intensity
        avg_rain = g_df['max_gauge_mm_h'].mean()
        
        results.append({
            'grid_id': grid,
            'count': count,
            'min_delay': np.min(delays),
            'max_delay': np.max(delays),
            'diversity_score': unique_hours, # Higher is better
            'avg_rain': avg_rain,
            'std_delay': np.std(delays)
        })
        
    results_df = pd.DataFrame(results)
    print(f"Grids passing basic filters: {len(results_df)}")
    
    if len(results_df) == 0:
        print("No grids met reliability criteria.")
        return

    # Ranking Logic
    # 1. Sort by Diversity Score (descending)
    # 2. Then by Count (descending)
    # 3. Then by Avg Rain (descending)
    
    results_df = results_df.sort_values(
        by=['diversity_score', 'count', 'avg_rain'], 
        ascending=[False, False, False]
    )
    
    top_50 = results_df.head(50)
    
    out_csv = r"D:\sotsuron\rainsar-hub\data_vv\analysis\best_50_grids.csv"
    top_50.to_csv(out_csv, index=False)
    print(f"Saved Top 50 to: {out_csv}")
    
    print("\n=== Top 10 Selected Grids ===")
    print(top_50[['grid_id', 'count', 'diversity_score', 'min_delay', 'avg_rain']].to_string(index=False))

if __name__ == "__main__":
    main()
