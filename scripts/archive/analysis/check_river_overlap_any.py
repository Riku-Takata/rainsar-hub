import rasterio
import geopandas as gpd
from pathlib import Path
from shapely.geometry import box
import pandas as pd

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
RIVER_FILES = [
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson"),
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320061.geojson")
]

def main():
    print("Checking River Overlap with ANY grid...")
    
    # 1. Load River GDFs
    river_gdfs = []
    for p in RIVER_FILES:
        if p.exists():
            print(f"Loading {p.name}...")
            gdf = gpd.read_file(p)
            river_gdfs.append(gdf)
            
    if not river_gdfs:
        print("No river files loaded.")
        return
        
    # 2. Iterate Grids
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    print(f"Checking {len(grid_dirs)} grids...")
    
    intersect_count = 0
    
    for i, grid_dir in enumerate(grid_dirs):
        # Just need one S1 to get bounds (they are all same grid size usually)
        # But bounds depend on grid location.
        event_dir = next(grid_dir.iterdir(), None)
        if not event_dir: continue
        
        s1_path = event_dir / "after_vv.tif"
        if not s1_path.exists(): continue
        
        try:
            with rasterio.open(s1_path) as src:
                b = src.bounds
                grid_box = box(b.left, b.bottom, b.right, b.top)
                
            # Check intersection
            hit = False
            for gdf in river_gdfs:
                # Use sindex for speed
                possible_matches_index = list(gdf.sindex.intersection(grid_box.bounds))
                if possible_matches_index:
                    possible_matches = gdf.iloc[possible_matches_index]
                    if not possible_matches[possible_matches.intersects(grid_box)].empty:
                        hit = True
                        break
            
            if hit:
                intersect_count += 1
                if intersect_count % 10 == 0:
                    print(f"  Found {intersect_count} intersections so far (Last: {grid_dir.name})")
                    
        except Exception as e:
            pass
            
        if (i+1) % 100 == 0:
            print(f"Checked {i+1} grids...")
            
    print(f"\nTotal Grids Intersecting Rivers: {intersect_count} / {len(grid_dirs)}")

if __name__ == "__main__":
    main()
