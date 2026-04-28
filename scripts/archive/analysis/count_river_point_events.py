
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import os
import json
from pathlib import Path
from shapely.geometry import box
import sys

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))
BACKEND_DIR = SCRIPTS_DIR.parent / "backend"
sys.path.append(str(BACKEND_DIR))

from common_utils import setup_logger, HUB_DIR
from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env")

logger = setup_logger("count_river_point_events")

# DB Configuration
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

engine = create_engine(DATABASE_URL)

def load_target_grids_gdf(json_path: Path):
    if not json_path.exists():
        logger.error(f"Target Grids JSON not found: {json_path}")
        return None
        
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    rows = []
    for item in data:
        # standard GSMaP grid is 0.1 degree
        lat = item['lat']
        lon = item['lon']
        grid_id = item['grid_id']
        # box(minx, miny, maxx, maxy)
        grid_poly = box(lon - 0.05, lat - 0.05, lon + 0.05, lat + 0.05)
        rows.append({'grid_id': grid_id, 'geometry': grid_poly})
        
    return gpd.GeoDataFrame(rows, crs="EPSG:4326")

def main():
    RIVER_POINTS_FILE = HUB_DIR / "mask-data" / "river_points.geojson"
    GRID_JSON = HUB_DIR / "data" / "thesis_grids_final_filtered.json"
    OUTPUT_CSV = HUB_DIR / "data" / "analysis" / "river_point_events_detail.csv"

    if not RIVER_POINTS_FILE.exists():
        logger.error(f"River points file not found: {RIVER_POINTS_FILE}")
        return

    # 1. Load Data
    logger.info("Loading River Points...")
    river_gdf = gpd.read_file(RIVER_POINTS_FILE)
    logger.info(f"Loaded {len(river_gdf)} river point features.")
    
    logger.info("Loading Target Grids...")
    grids_gdf = load_target_grids_gdf(GRID_JSON)
    if grids_gdf is None:
        return
        
    # 2. Match Points to Grids
    logger.info("Matching River Points to Grids (Spatial Join)...")
    # river_gdf usually contains (Multi)Polygons, grids_gdf contains Polygons
    # We want to find which grid contains the river point polygon.
    # 'intersects' is safe.
    joined = gpd.sjoin(river_gdf, grids_gdf, how="inner", predicate="intersects")
    
    logger.info(f"Matches found: {len(joined)}")
    
    if joined.empty:
        logger.warning("No river points matched to target grids.")
        return

    # Get unique affected grids
    target_grid_ids = joined['grid_id'].unique().tolist()
    logger.info(f"Unique Grids identified: {len(target_grid_ids)}")
    
    # 3. Query DB for Events
    logger.info("Querying Database for Rainfall Events...")
    
    query = text("""
        SELECT DISTINCT
            s.id as pair_id,
            s.grid_id,
            g.start_ts_utc as event_start,
            MONTH(s.event_end_ts_utc) as month,
            s.delay_h,
            g.max_gauge_mm_h as rain_mm_h
        FROM s1_pairs s
        JOIN gsmap_events g ON s.grid_id = g.grid_id AND s.event_start_ts_utc = g.start_ts_utc
        WHERE s.source = 'cdse_nationwide_search'
        AND s.before_scene_id IS NOT NULL
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        AND g.max_gauge_mm_h >= 10.0
        AND s.grid_id IN :grids
        ORDER BY s.grid_id, g.start_ts_utc
    """)
    
    try:
        with engine.connect() as conn:
            events_df = pd.read_sql(query, conn, params={"grids": tuple(target_grid_ids)})
    except Exception as e:
        logger.error(f"DB Query failed: {e}")
        return

    if events_df.empty:
        logger.warning("No events found for the matched grids.")
        return

    # 4. Merge Results
    # We join distinct events to the river points. 
    # Note: One point might have multiple events if it's in a grid with events.
    # joined has ['id', 'grid_id', ...]. events_df has ['grid_id', 'month', 'delay_h', ...]
    
    result_df = pd.merge(joined[['id', 'grid_id']], events_df, on='grid_id', how='inner')
    
    # 5. Output Details
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(OUTPUT_CSV, index=False)
    logger.info(f"Saved detailed results to {OUTPUT_CSV}")

    # 6. Analyze Distributions
    print("\n=== River Point Rainfall Event Analysis ===")
    print(f"Total Features Matched: {len(joined)}")
    print(f"Total Events Found: {len(result_df)}")
    
    # Seasonal Distribution
    print("\n--- Seasonal Distribution (Count) ---")
    seasonal_stats = result_df['month'].value_counts().sort_index()
    print(seasonal_stats.to_string())
    
    # Delay Distribution
    print("\n--- Delay Distribution (Hours) ---")
    delay_stats = result_df['delay_h'].describe(percentiles=[0.25, 0.5, 0.75])
    print(delay_stats.to_string())
    
    # Detailed List (Top 20)
    print("\n--- Detailed Events (First 20) ---")
    print(result_df[['id', 'grid_id', 'month', 'delay_h', 'rain_mm_h']].head(20).to_string(index=False))

if __name__ == "__main__":
    main()
