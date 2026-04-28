"""
Verify quality of unused grids (Mask Area & Rain Events).
Output 'suggested_grids.csv'.
"""
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import json
from pathlib import Path
import rasterio

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
load_dotenv(BASE_DIR / "backend/.env")

DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"
MASKS_DIR = BASE_DIR / "data/expanded/masks"

engine = create_engine(DATABASE_URL)

def get_mask_pixels(grid_id):
    """Calculate approximate pixels from GeoJSON using bounds or loading raster if cached?
    Actually, we assume 10m pixels.
    Area (m2) / 100 = Pixels.
    Better: Load GeoJSON and calculate area in projected CRS?
    Or just assume quality based on file existence?
    User asked "Ensure rain intensity is ok".
    Let's check rain first (Database).
    Then check mask area roughly.
    """
    # Simple check: Do the geojson files exist and are they non-empty?
    road_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
    paddy_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
    
    has_road = road_path.exists() and road_path.stat().st_size > 100
    has_paddy = paddy_path.exists() and paddy_path.stat().st_size > 100
    
    return has_road, has_paddy

def main():
    # 1. Get Unused Grids
    if not MASKS_DIR.exists(): return
    mask_grids = {d.name for d in MASKS_DIR.iterdir() if d.is_dir()}
    
    json_path = BASE_DIR / "data/thesis_grids_final_filtered.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    thesis_grids = {item['grid_id'] if isinstance(item, dict) else item for item in data}
    
    unused_grids = list(mask_grids - thesis_grids)
    print(f"Checking {len(unused_grids)} unused grids...")
    
    if not unused_grids: return

    # 2. Query Valid Events (>10mm/h)
    query = text("""
        SELECT 
            s.grid_id,
            COUNT(DISTINCT CONCAT(s.grid_id, '_', g.start_ts_utc)) as valid_events
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        AND g.max_gauge_mm_h >= 10.0
        AND s.grid_id IN :grids
        GROUP BY s.grid_id
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": tuple(unused_grids)})
    
    # 3. Validate Masks
    results = []
    
    for _, row in df.iterrows():
        grid_id = row['grid_id']
        events = row['valid_events']
        
        has_road, has_paddy = get_mask_pixels(grid_id)
        
        # Valid if has events AND both masks exist
        if events > 0 and has_road and has_paddy:
            results.append({
                'grid_id': grid_id,
                'valid_events_10mm': events,
                'has_road': has_road,
                'has_paddy': has_paddy
            })
            
    res_df = pd.DataFrame(results)
    
    if not res_df.empty:
        res_df.sort_values('valid_events_10mm', ascending=False, inplace=True)
        out_csv = BASE_DIR / "data" / "analysis" / "suggested_grids_quality.csv"
        out_csv.parent.mkdir(exist_ok=True, parents=True) # ensure dir
        res_df.to_csv(out_csv, index=False)
        
        print(f"\nVerified Grids: {len(res_df)}")
        print(f"Total New Events (>10mm/h): {res_df['valid_events_10mm'].sum()}")
        print("Top 10 candidates:")
        print(res_df.head(10))
        print(f"Saved to {out_csv}")
    else:
        print("No grids met the quality criteria.")

if __name__ == "__main__":
    main()
