
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
from pathlib import Path
import numpy as np

# Specific Grid and Event from previous debug logs
GRID_ID = "N03595E13765" # Trying one from the potential overlap list
# Or N03595E14005
GRID_ID = "N03595E14005"

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"
RIVER_PATHS = [
    BASE_DIR / "mask-data/river_polygon_2320001.geojson",
    BASE_DIR / "mask-data/river_polygon_2320061.geojson"
]

def debug():
    # Load River
    gdfs = []
    for p in RIVER_PATHS:
        if p.exists():
            gdfs.append(gpd.read_file(p))
    
    if not gdfs:
        print("No river files.")
        return
        
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    print(f"River CRS: {gdf.crs}")

    # Find the grid directory
    grid_dir = SAMPLES_DIR / GRID_ID
    if not grid_dir.exists():
        print(f"Grid dir not found: {grid_dir}")
        return

    # Find the first TIF
    tif_path = None
    for evt in grid_dir.iterdir():
        cand = evt / "before_vv.tif"
        if cand.exists():
            tif_path = cand
            break
            
    if not tif_path:
        print("No TIF found in grid.")
        return
        
    print(f"Testing on {tif_path}")
    
    with rasterio.open(tif_path) as src:
        print(f"TIF CRS: {src.crs}, Shape: {src.shape}")
        
        # Reproject River
        gdf_proj = gdf.to_crs(src.crs)
        
        # Test Invert=True
        mask_inv_true = geometry_mask(gdf_proj.geometry, out_shape=src.shape, transform=src.transform, invert=True, all_touched=True)
        sum_true = np.sum(mask_inv_true)
        print(f"Invert=True (True count): {sum_true} / {src.shape[0]*src.shape[1]}")
        
        # Test Invert=False
        mask_inv_false = geometry_mask(gdf_proj.geometry, out_shape=src.shape, transform=src.transform, invert=False, all_touched=True)
        sum_false = np.sum(mask_inv_false)
        print(f"Invert=False (True count): {sum_false} / {src.shape[0]*src.shape[1]}")
        
        if sum_true < sum_false:
            print("Conclusion: invert=True selects the minority (River).")
        else:
            print("Conclusion: invert=False selects the minority (River).")

import pandas as pd
if __name__ == "__main__":
    debug()
