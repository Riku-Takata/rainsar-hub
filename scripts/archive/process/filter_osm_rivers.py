
import geopandas as gpd
from shapely.geometry import box
from shapely.ops import unary_union
import json
from pathlib import Path
import sys

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))
BACKEND_DIR = SCRIPTS_DIR.parent / "backend"
sys.path.append(str(BACKEND_DIR))

from common_utils import setup_logger, HUB_DIR

logger = setup_logger("filter_osm_rivers")

# Paths
OSM_RIVERS_FILE = HUB_DIR / "mask-data" / "osm_rivers" / "osm_rivers_japan.geojson"
GRID_JSON = HUB_DIR / "data" / "thesis_grids_final_filtered.json"
OUTPUT_FILE = HUB_DIR / "mask-data" / "osm_rivers" / "osm_rivers_target_grids.geojson"

def load_target_grids(json_path: Path):
    if not json_path.exists():
        logger.error(f"Target Grids JSON not found: {json_path}")
        return None
        
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    grids = []
    for item in data:
        # standard GSMaP grid is 0.1 degree
        lat = item['lat']
        lon = item['lon']
        # box(minx, miny, maxx, maxy)
        grid_poly = box(lon - 0.05, lat - 0.05, lon + 0.05, lat + 0.05)
        grids.append(grid_poly)
        
    if not grids:
        return None
    
    logger.info(f"Loaded {len(grids)} target grids.")
    return unary_union(grids)

def main():
    if not OSM_RIVERS_FILE.exists():
        logger.error(f"OSM River file not found: {OSM_RIVERS_FILE}")
        return

    logger.info("Loading OSM Rivers...")
    gdf = gpd.read_file(OSM_RIVERS_FILE)
    logger.info(f"Loaded {len(gdf)} rivers.")
    
    logger.info(f"Loading Target Grids from {GRID_JSON}...")
    target_grids_union = load_target_grids(GRID_JSON)
    if target_grids_union is None:
        return

    logger.info("Filtering rivers by target grids (Spatial Intersection)...")
    
    # Filter: Keep river if it intersects ANY target grid
    filtered_gdf = gdf[gdf.geometry.intersects(target_grids_union)].copy()
    
    # Deduplicate by OSM ID just in case (though geometry filter on unique rows shouldn't produce duplicates, 
    # but good to be safe if input had duplicates)
    before_count = len(filtered_gdf)
    filtered_gdf = filtered_gdf.drop_duplicates(subset=['osm_id'])
    
    if filtered_gdf.empty:
        logger.warning("No rivers intersect with the target grids.")
        return
        
    logger.info(f"Filtered Rivers: {len(gdf)} -> {len(filtered_gdf)}")
    
    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    filtered_gdf.to_file(OUTPUT_FILE, driver='GeoJSON')
    logger.info(f"Saved filtered rivers to: {OUTPUT_FILE}")
    
    # Print Summary
    print("\n--- Selected Rivers ---")
    summary = filtered_gdf[['name', 'osm_id', 'area_km2']].sort_values('area_km2', ascending=False)
    print(summary.to_string(index=False))

if __name__ == "__main__":
    main()
