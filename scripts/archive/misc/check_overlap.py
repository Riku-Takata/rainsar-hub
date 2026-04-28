import rasterio
import sys
import geopandas as gpd
from pathlib import Path
from shapely.geometry import box

# Config
GRID_ID = "N03145E13095"
HUB_DIR = Path(r"d:\sotsuron\rainsar-hub")
sys.path.append(str(Path(__file__).parent.parent))
from common_utils import S1_SAMPLES_DIR, HUB_DIR

FUDE_DIR = HUB_DIR / "mask-data" / "fude_polygons" / GRID_ID
OSM_DIR = HUB_DIR / "mask-data" / "osm_roads"

def check_overlap(grid_id):
    # 1. Get Source Raster
    grid_dir = S1_SAMPLES_DIR / grid_id
    procs = list(grid_dir.glob("*_proc.tif"))
    if not procs:
        print("No proc tif found.")
        return
    src_path = procs[0]
    
    with rasterio.open(src_path) as src:
        r_bounds = src.bounds
        r_crs = src.crs
        r_box = box(r_bounds.left, r_bounds.bottom, r_bounds.right, r_bounds.top)
        print(f"Raster: {src_path.name}")
        print(f"  CRS: {r_crs}")
        print(f"  Bounds: {r_bounds}")
        
        # 2. Get Fude
        fude_path = FUDE_DIR / f"{grid_id}_paddy_buff.geojson"
        if fude_path.exists():
            gdf = gpd.read_file(fude_path)
            if gdf.crs != r_crs:
                gdf = gdf.to_crs(r_crs)
            
            f_bounds = gdf.total_bounds
            f_box = box(f_bounds[0], f_bounds[1], f_bounds[2], f_bounds[3])
            
            print(f"Fude: {fude_path.name}")
            print(f"  Bounds: {f_bounds}")
            print(f"  Intersects Raster? {r_box.intersects(f_box)}")
            print(f"  Intersection Area: {r_box.intersection(f_box).area}")
            
            # Check actual containment
            overlap = gdf.clip(r_box)
            print(f"  Polygons colliding with raster: {len(overlap)}")
        else:
            print(f"Fude not found: {fude_path}")

        # 3. Get OSM
        osm_path = OSM_DIR / f"{grid_id}_roads.geojson"
        if osm_path.exists():
            gdf = gpd.read_file(osm_path)
            if gdf.crs != r_crs:
                gdf = gdf.to_crs(r_crs)
            
            o_bounds = gdf.total_bounds
            o_box = box(o_bounds[0], o_bounds[1], o_bounds[2], o_bounds[3])
            
            print(f"OSM: {osm_path.name}")
            print(f"  Bounds: {o_bounds}")
            print(f"  Intersects Raster? {r_box.intersects(o_box)}")
            
            overlap = gdf.clip(r_box)
            print(f"  Lines colliding with raster: {len(overlap)}")
        else:
            print(f"OSM not found: {osm_path}")

if __name__ == "__main__":
    check_overlap(GRID_ID)
