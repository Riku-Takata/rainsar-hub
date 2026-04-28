import os
from pathlib import Path

MASK_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks")

def main():
    print("Checking mask coverage...")
    
    total_grids = 0
    grids_with_tif = 0
    grids_with_geojson_only = 0
    empty_grids = 0
    
    for grid_dir in MASK_DIR.iterdir():
        if not grid_dir.is_dir(): continue
        
        total_grids += 1
        
        tifs = list(grid_dir.glob("*.tif"))
        geojsons = list(grid_dir.glob("*.geojson"))
        
        if tifs:
            grids_with_tif += 1
        elif geojsons:
            grids_with_geojson_only += 1
        else:
            empty_grids += 1
            
    print(f"Total Mask Directories: {total_grids}")
    print(f"Grids with TIF masks: {grids_with_tif}")
    print(f"Grids with GeoJSON only: {grids_with_geojson_only}")
    print(f"Empty/Other: {empty_grids}")

if __name__ == "__main__":
    main()
