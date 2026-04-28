import sys
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup
BACKEND_DIR = Path(__file__).resolve().parents[2] / "backend"
sys.path.append(str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

GRID_CSV = Path(r"D:\sotsuron\rainsar-hub\data_vv\analysis\nationwide\best_50_grids_v2.csv")

def main():
    if not GRID_CSV.exists():
        print("Grid CSV missing.")
        return

    df_grids = pd.read_csv(GRID_CSV)
    grids = df_grids['grid_id'].tolist()
    print(f"Analyzing {len(grids)} grids from best_50_grids_v2.csv")
    
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        query = text("""
            SELECT grid_id, after_scene_id, before_scene_id, delay_h
            FROM s1_pairs
            WHERE source = 'cdse_nationwide_search'
            AND delay_h <= 12.0
            AND grid_id IN :grids
        """)
        df = pd.read_sql(query, conn, params={"grids": grids})
        
    print(f"Total Pairs Found: {len(df)}")
    
    unique_after = df['after_scene_id'].nunique()
    unique_before = df['before_scene_id'].nunique()
    
    all_scenes = set(df['after_scene_id'].dropna()).union(set(df['before_scene_id'].dropna()))
    print(f"Unique After Scenes: {unique_after}")
    print(f"Unique Before Scenes: {unique_before}")
    print(f"Total Unique Scenes (All): {len(all_scenes)}")
    
    # Check IW_GRDH filter impact
    iw_grdh = {s for s in all_scenes if "IW_GRDH" in s}
    print(f"Scenes matching 'IW_GRDH': {len(iw_grdh)}")
    
    non_iw = all_scenes - iw_grdh
    if non_iw:
        print(f"WARNING: {len(non_iw)} scenes do NOT match 'IW_GRDH'!")
        print("Examples:", list(non_iw)[:5])
    
    # Check spatial clustering
    # How many grids does the top scene cover?
    scene_counts = pd.concat([df['after_scene_id'], df['before_scene_id']]).value_counts()
    print("\nTop 5 Scenes by Frequency (Grid-Event Reuse):")
    print(scene_counts.head(5))
    
    print("\nInterpretation:")
    print(f"Average reuse per scene: {len(df)*2 / len(all_scenes):.2f} references/scene")
    
if __name__ == "__main__":
    main()
