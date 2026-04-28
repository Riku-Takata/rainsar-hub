import rasterio
from rasterio import features
import geopandas as gpd
from pathlib import Path
import numpy as np
from shapely.geometry import box

# Specific Test Case
GRID_ID = "N03385E13065"
EVENT_DIR_NAME = "delay_0.3h_20190827"
S1_PATH = Path(rf"D:\sotsuron\rainsar-hub\data\expanded\samples\{GRID_ID}\{EVENT_DIR_NAME}\after_vv.tif")
RIVER_PATH = Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson")

def main():
    print(f"Debugging River Intersection for {GRID_ID}...")
    
    # 1. Load S1 Properties
    if not S1_PATH.exists():
        print("S1 path does not exist.")
        return
        
    with rasterio.open(S1_PATH) as src:
        bounds = src.bounds
        transform = src.transform
        shape = src.shape
        crs = src.crs
        print(f"S1 Bounds: {bounds}")
        print(f"S1 CRS: {crs}")
        print(f"S1 Shape: {shape}")
        
        s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        print(f"S1 Box: {s1_box}")

    # 2. Load River
    if not RIVER_PATH.exists():
        print("River path does not exist.")
        return
        
    gdf = gpd.read_file(RIVER_PATH)
    print(f"River CRS: {gdf.crs}")
    print(f"River Bounds: {gdf.total_bounds}")
    
    # 3. Check Intersection (Geopandas)
    print("\nChecking intersection with S1 box...")
    intersects = gdf[gdf.intersects(s1_box)]
    print(f"Intersecting features found: {len(intersects)}")
    
    if len(intersects) > 0:
        print("First intersecting feature bounds:", intersects.iloc[0].geometry.bounds)
        
        # 4. Try Rasterize
        print("\nRasterizing...")
        mask = features.rasterize(
            shapes=intersects.geometry,
            out_shape=shape,
            transform=transform,
            fill=0,
            default_value=1,
            dtype=rasterio.uint8
        )
        
        total_pixels = np.sum(mask)
        print(f"Total River Pixels in Mask: {total_pixels}")
        
        if total_pixels == 0:
            print("Rasterization result is empty! Checking transform vs geometry...")
            # Maybe coords are flipped? But CRS matches.
            pass
    else:
        print("No intersection found by geopandas.")

if __name__ == "__main__":
    main()
