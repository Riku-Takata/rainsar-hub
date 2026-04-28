import os
import sys
import re
import math
import numpy as np
import geopandas as gpd
import rasterio
import rasterio.features
import rasterio.mask
from rasterio.warp import reproject, Resampling
from shapely.geometry import mapping, box, shape
from pathlib import Path

# Setup sys.path to find common_utils in parent directory
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

from common_utils import setup_logger, decode_grid_id, S1_SAMPLES_DIR, TARGET_GRIDS, HUB_DIR, DATA_DIR

logger = setup_logger("mask_processing")

# Settings
BASE_DIR = Path(__file__).resolve().parent.parent
# OSM Roads Directory
OSM_ROADS_DIR = HUB_DIR / "mask-data" / "osm_roads"
# Fude Polygons Directory
FUDE_POLYGONS_DIR = HUB_DIR / "mask-data" / "fude_polygons"
# Export Directory for Masks
EXPORT_MASK_DIR = DATA_DIR / "masks"

JAXA_DATA_DIR = Path(r"D:\sotsuron") / "jaxa-data"

def create_highway_mask(tif_path):
    # Derive Grid ID from parent folder
    grid_id = tif_path.parent.name
    
    # Path to OSM GeoJSON (LineStrings)
    geojson_path = OSM_ROADS_DIR / f"{grid_id}_roads.geojson"
    if not geojson_path.exists():
        # logger.warning(f"  OSM Road file not found: {geojson_path}")
        return

    out_path = tif_path.with_name(tif_path.stem + "_highway_mask.tif")

    try:
        with rasterio.open(tif_path) as src:
            raster_crs = src.crs
            out_meta = src.meta.copy()
            out_meta.update({"driver": "GTiff", "dtype": "float32", "nodata": np.nan, "compress": "lzw"})

            # Read GeoJSON
            try:
                gdf = gpd.read_file(geojson_path)
            except Exception as e:
                # logger.warning(f"Failed to read geojson: {e}")
                return

            if gdf.empty: return
            
            # Reproject if needed
            if gdf.crs is None: gdf.set_crs("EPSG:4326", inplace=True) # Assumed WGS84
            if gdf.crs != raster_crs:
                gdf = gdf.to_crs(raster_crs)
                
            # Clipping to bounds optimization
            bounds = src.bounds
            img_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            gdf_clipped = gpd.clip(gdf, img_box)
            if gdf_clipped.empty: return
            
            # Create Shapes
            shapes = [mapping(geom) for geom in gdf_clipped.geometry]

            # Use all_touched=True for lines to ensure pixels crossed by lines are included
            out_image, out_transform = rasterio.mask.mask(src, shapes, invert=False, crop=False, nodata=np.nan, filled=True, all_touched=True)
            
            if np.sum(~np.isnan(out_image)) > 0:
                with rasterio.open(out_path, "w", **out_meta) as dst:
                    dst.write(out_image)
                
                # Export to Data Directory (Data/Masks)
                export_folder = EXPORT_MASK_DIR / grid_id
                export_folder.mkdir(parents=True, exist_ok=True)
                export_path = export_folder / out_path.name

                with rasterio.open(export_path, "w", **out_meta) as dst:
                    dst.write(out_image)

                logger.info(f"  [OK] Saved highway mask (OSM Motorway): {out_path.name}")

    except Exception as e:
        logger.error(f"  Error processing {tif_path.name}: {e}")

def create_paddy_mask(tif_path):
    grid_id = tif_path.parent.name
    
    # Path to Fude GeoJSON (Buffered Polygons)
    geojson_path = FUDE_POLYGONS_DIR / grid_id / f"{grid_id}_paddy_buff.geojson"
    if not geojson_path.exists():
        # logger.warning(f"  Fude Paddy file not found: {geojson_path}")
        return

    out_path = tif_path.with_name(tif_path.stem + "_paddy_mask.tif")

    try:
        with rasterio.open(tif_path) as src:
            raster_crs = src.crs
            out_meta = src.meta.copy()
            out_meta.update({"driver": "GTiff", "dtype": "float32", "nodata": np.nan, "compress": "lzw"})

            # Read GeoJSON
            try:
                gdf = gpd.read_file(geojson_path)
            except Exception as e:
                # logger.warning(f"Failed to read geojson: {e}")
                return

            if gdf.empty: return
            
            # Reproject if needed
            if gdf.crs is None: gdf.set_crs("EPSG:4326", inplace=True) # Assumed WGS84
            if gdf.crs != raster_crs:
                gdf = gdf.to_crs(raster_crs)
                
            # Clipping to bounds optimization
            bounds = src.bounds
            img_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            gdf_clipped = gpd.clip(gdf, img_box)
            if gdf_clipped.empty: return
            
            # Create Shapes
            shapes = [mapping(geom) for geom in gdf_clipped.geometry]

            # Rasterize Polygons (all_touched=True to include boundary pixels)
            out_image, out_transform = rasterio.mask.mask(src, shapes, invert=False, crop=False, nodata=np.nan, filled=True, all_touched=True)
            
            if np.sum(~np.isnan(out_image)) > 0:
                # Save to S1_SAMPLES_DIR (Original)
                with rasterio.open(out_path, "w", **out_meta) as dst:
                    dst.write(out_image)
                
                # Export to Data Directory (Data/Masks)
                export_folder = EXPORT_MASK_DIR / grid_id
                export_folder.mkdir(parents=True, exist_ok=True)
                export_path = export_folder / out_path.name
                
                with rasterio.open(export_path, "w", **out_meta) as dst:
                    dst.write(out_image)
                    
                logger.info(f"  [OK] Saved & Exported paddy mask (Fude): {out_path.name}")

    except Exception as e:
        logger.error(f"  Error processing {tif_path.name}: {e}")

def main(target_grids=None):
    logger.info("Starting Mask Generation (OSM Motorway & Fude Paddy)...")
    
    if target_grids is None:
        target_grids = TARGET_GRIDS
    
    # Check Inputs
    if not OSM_ROADS_DIR.exists():
        logger.warning(f"OSM Roads dir not found at {OSM_ROADS_DIR}. Run fetch_osm_roads.py first.")
    if not FUDE_POLYGONS_DIR.exists():
        logger.warning(f"Fude Polygons dir not found at {FUDE_POLYGONS_DIR}. Run fetch_fude_polygons.py first.")

    for grid_id in target_grids:
        logger.info(f"Processing Grid: {grid_id}")
        grid_dir = S1_SAMPLES_DIR / grid_id
        if not grid_dir.exists():
            logger.warning(f"Grid directory not found: {grid_dir}")
            continue
            
        tif_files = list(grid_dir.glob("*_proc.tif"))
        
        for tif_path in tif_files:
            if "_mask" in tif_path.name or "_road" in tif_path.name or "_paddy" in tif_path.name:
                continue
            
            create_highway_mask(tif_path)
            create_paddy_mask(tif_path)

if __name__ == "__main__":
    main()
