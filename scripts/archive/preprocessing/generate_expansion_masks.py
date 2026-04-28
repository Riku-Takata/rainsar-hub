
import os
import sys
import numpy as np
import geopandas as gpd
import rasterio
import rasterio.features
from pathlib import Path
from shapely.geometry import box, mapping
import warnings

warnings.filterwarnings('ignore')

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
MASKS_DIR = BASE_DIR / "data/expanded/masks"
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"

def get_reference_tif(grid_id):
    """Find a reference tif for grid dimensions/crs."""
    grid_sample_dir = SAMPLES_DIR / grid_id
    if not grid_sample_dir.exists():
        return None
        
    # Look into event directories
    for event_dir in grid_sample_dir.iterdir():
        if event_dir.is_dir():
            tifs = list(event_dir.glob("*_vv.tif"))
            if tifs:
                return tifs[0]
    return None

def rasterize_geojson(geojson_path, ref_tif_path, out_tif_path, burn_value=1):
    if out_tif_path.exists():
        print(f"  Skipping {out_tif_path.name} (Exists)")
        return

    try:
        gdf = gpd.read_file(geojson_path)
    except Exception as e:
        print(f"  Error reading {geojson_path.name}: {e}")
        return
        
    if gdf.empty:
        print(f"  Empty GeoJSON: {geojson_path.name}")
        return

    with rasterio.open(ref_tif_path) as src:
        # Reproject if needed
        if gdf.crs is None: gdf.set_crs("EPSG:4326", inplace=True)
        if gdf.crs != src.crs:
            gdf = gdf.to_crs(src.crs)
            
        # Clip
        bounds = src.bounds
        clip_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        gdf_clipped = gpd.clip(gdf, clip_box)
        
        if gdf_clipped.empty:
            print(f"  No features inside grid: {geojson_path.name}")
            return
            
        out_shape = src.shape
        transform = src.transform
        meta = src.meta.copy()
        meta.update({
            "driver": "GTiff",
            "count": 1,
            "dtype": "uint8",
            "nodata": 0,
            "compress": "lzw"
        })
        
        # Rasterize
        # all_touched=True is better for thin features like roads
        mask = rasterio.features.rasterize(
            ((geom, burn_value) for geom in gdf_clipped.geometry),
            out_shape=out_shape,
            transform=transform,
            fill=0,
            all_touched=True,
            dtype=rasterio.uint8
        )
        
        if np.sum(mask) == 0:
            print(f"  Rasterized mask empty: {out_tif_path.name}")
            return

        with rasterio.open(out_tif_path, "w", **meta) as dst:
            dst.write(mask, 1)
            
        print(f"  Generated: {out_tif_path.name}")

def main():
    print("Generating masks for Expansion Grids...")
    
    grids = [d for d in MASKS_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(grids)} mask directories.")
    
    for idx, grid_dir in enumerate(grids):
        grid_id = grid_dir.name
        
        # Inputs
        road_json = grid_dir / f"{grid_id}_motorway.geojson"
        paddy_json = grid_dir / f"{grid_id}_paddy.geojson"
        
        # Outputs
        road_tif = grid_dir / f"{grid_id}_road_mask.tif"
        paddy_tif = grid_dir / f"{grid_id}_paddy_mask.tif"
        
        if road_tif.exists() and paddy_tif.exists():
            continue
            
        # Reference
        ref_tif = get_reference_tif(grid_id)
        if not ref_tif:
            # print(f"No reference TIF found for {grid_id}, skipping.")
            continue
            
        if idx % 50 == 0:
            print(f"Processing {idx}/{len(grids)}: {grid_id}")
            
        if road_json.exists():
            rasterize_geojson(road_json, ref_tif, road_tif)
            
        if paddy_json.exists():
            rasterize_geojson(paddy_json, ref_tif, paddy_tif)

if __name__ == "__main__":
    main()
