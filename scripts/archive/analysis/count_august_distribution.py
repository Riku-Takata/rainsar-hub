
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
    
    # Load Expansion Grids
    expansion_csv = BASE_DIR / "data/analysis/suggested_grids_quality.csv"
    if expansion_csv.exists():
        expansion_grids = pd.read_csv(expansion_csv)['grid_id'].tolist()
    else:
        expansion_grids = []
        print("Warning: Expansion grids CSV not found.")

    # Thesis Grids (Original) - Assuming we can get them from a file or query
    # Let's load from the filtered json if available
    thesis_json = BASE_DIR / "data/thesis_grids_final_filtered.json"
    if thesis_json.exists():
        import json
        with open(thesis_json, 'r') as f:
            data = json.load(f)
            # Check if list of strings or list of dicts
            if data and isinstance(data[0], dict):
                thesis_grids = [d['grid_id'] for d in data if 'grid_id' in d]
            elif data and isinstance(data[0], str):
                thesis_grids = data
            else:
                thesis_grids = []
    else:
        thesis_grids = []
        print("Warning: Thesis grids JSON not found.")

    all_target_grids = list(set(expansion_grids + thesis_grids))
    
    print(f" Thesis Grids: {len(thesis_grids)}")
    print(f" Expansion Grids: {len(expansion_grids)}")
    print(f" Total Unique Targets: {len(all_target_grids)}")

    query = text("""
        SELECT grid_id, delay_h
        FROM s1_pairs
        WHERE grid_id IN :grids
        AND MONTH(event_end_ts_utc) = 8
        AND delay_h BETWEEN 0 AND 12
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": tuple(all_target_grids)})
        
    print(f"\nTotal August Events (0-12h): {len(df)}")
    
    # Identify Source
    df['Source'] = df['grid_id'].apply(lambda x: 'Expansion' if x in expansion_grids and x not in thesis_grids else ('Thesis' if x in thesis_grids else 'shared'))
    
    # Binning Delay (1-hour intervals)
    bins = list(range(0, 13))
    labels = [f"{i}-{i+1}h" for i in range(0, 12)]
    df['Delay_Bin'] = pd.cut(df['delay_h'], bins=bins, labels=labels, include_lowest=True)
    
    summary = df.pivot_table(index='Delay_Bin', columns='Source', values='grid_id', aggfunc='count', fill_value=0)
    summary['Total'] = summary.sum(axis=1)
    
    print("\n--- August Data Distribution by Delay ---")
    print(summary)
    
    # Save
    summary.to_csv(BASE_DIR / "data/analysis/august_distribution_summary.csv")

if __name__ == "__main__":
    main()
