"""
Check Rain Intensity Stats for Expansion Candidates vs Current Grids.
Ensure the new events have comparable rain intensity distribution.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib

matplotlib.rcParams['font.family'] = 'MS Gothic'
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
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
    
    # 2. Load Current Thesis Grids
    json_path = BASE_DIR / "data/thesis_grids_final_filtered.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    current_grids = [item['grid_id'] if isinstance(item, dict) else item for item in data]
    
    print(f"Candidates: {len(candidates)}, Current: {len(current_grids)}")
    
    # 3. Query Rain Intensity
    query = text("""
        SELECT 
            s.grid_id,
            g.max_gauge_mm_h
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        AND g.max_gauge_mm_h >= 10.0
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    # Label groups
    df['Type'] = 'Other'
    df.loc[df['grid_id'].isin(candidates), 'Type'] = 'Candidates'
    df.loc[df['grid_id'].isin(current_grids), 'Type'] = 'Current'
    
    # Filter
    df_filtered = df[df['Type'].isin(['Candidates', 'Current'])]
    
    # Stats
    stats = df_filtered.groupby('Type')['max_gauge_mm_h'].describe()
    print("\nRain Intensity Stats (mm/h):")
    print(stats)
    
    # Plot
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df_filtered, x='max_gauge_mm_h', hue='Type', element='step', stat='density', common_norm=False, bins=30)
    plt.title('降雨強度分布の比較 (Current vs Candidates)')
    plt.xlabel('Max Gauge Rain (mm/h)')
    plt.xlim(10, 100) # Cut off extreme tails for visibility
    plt.grid(True, alpha=0.3)
    
    out_png = BASE_DIR / "data/analysis/expansion_rain_dist_comparison.png"
    plt.savefig(out_png)
    print(f"Saved plot: {out_png}")

if __name__ == "__main__":
    main()
