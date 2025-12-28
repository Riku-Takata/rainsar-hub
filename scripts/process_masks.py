import os
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
from common_utils import setup_logger, decode_grid_id, S1_SAMPLES_DIR, TARGET_GRIDS

logger = setup_logger("mask_processing")

# Settings
BASE_DIR = Path(r"D:\sotsuron")
ROAD_POLYGON_DIR = BASE_DIR / "road-polygon"
JAXA_DATA_DIR = BASE_DIR / "jaxa-data"

ROAD_BUFFER_METER = 5.0
PADDY_BUFFER_METER = -5.0
PADDY_CATEGORY_ID = 3

def get_year_from_filename(filename):
    match = re.search(r"(20\d{2})", filename)
    if match: return int(match.group(1))
    return None

def find_available_shapefiles(root_dir):
    if not root_dir.exists(): return {}
    shapefiles = {}
    candidates = list(root_dir.glob("**/*HighwaySection.shp"))
    
    for shp_path in candidates:
        if "UTF-8" in str(shp_path): continue
        match = re.search(r"N06-(\d{2})", shp_path.name)
        if match:
            yy = int(match.group(1))
            year = 2000 + yy
            if year in shapefiles:
                if "Shift-JIS" in str(shp_path) and "Shift-JIS" not in str(shapefiles[year]):
                     shapefiles[year] = shp_path
            else:
                shapefiles[year] = shp_path
            continue
        match_full = re.search(r"N06-(20\d{2})", shp_path.name)
        if match_full:
            year = int(match_full.group(1))
            if year in shapefiles:
                if "Shift-JIS" in str(shp_path) and "Shift-JIS" not in str(shapefiles[year]):
                     shapefiles[year] = shp_path
            else:
                shapefiles[year] = shp_path
    return shapefiles

def get_best_match_shapefile(target_year, available_files):
    if not available_files: return None, None
    years = np.array(list(available_files.keys()))
    idx = (np.abs(years - target_year)).argmin()
    nearest_year = years[idx]
    return available_files[nearest_year], nearest_year

def get_jaxa_lulc_path(year, lat, lon):
    lat_int = math.floor(lat)
    lon_int = math.floor(lon)
    filename = f"LC_N{lat_int:02d}E{lon_int:03d}.tif"
    
    if year >= 2022: version_dir = "2024JPN_v25.04"
    elif year >= 2020: version_dir = "2020JPN_v25.04"
    else: version_dir = "2018-2020JPN_v21.11_10m"
        
    path = JAXA_DATA_DIR / version_dir / filename
    if not path.exists():
        fallback_path = JAXA_DATA_DIR / "2018-2020JPN_v21.11_10m" / filename
        if fallback_path.exists(): return fallback_path
    return path

def create_highway_mask(tif_path, available_shps):
    img_year = get_year_from_filename(tif_path.name)
    if img_year is None: return 

    shp_path, map_year = get_best_match_shapefile(img_year, available_shps)
    if shp_path is None: return

    out_path = tif_path.with_name(tif_path.stem + "_highway_mask.tif")
    # if out_path.exists(): return

    try:
        with rasterio.open(tif_path) as src:
            raster_crs = src.crs
            out_meta = src.meta.copy()
            out_meta.update({"driver": "GTiff", "dtype": "float32", "nodata": np.nan, "compress": "lzw"})

            try:
                bounds = src.bounds
                img_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                gdf = gpd.read_file(shp_path, encoding='cp932')
                if gdf.crs is None: gdf.set_crs("EPSG:6668", inplace=True)
                if gdf.crs != raster_crs: gdf = gdf.to_crs(raster_crs)
                gdf_clipped = gpd.clip(gdf, img_box)
                if gdf_clipped.empty: return
            except Exception as e:
                logger.warning(f"  Shapefile read error: {e}")
                return

            utm_crs = gdf_clipped.estimate_utm_crs()
            gdf_utm = gdf_clipped.to_crs(utm_crs)
            buffered_utm = gdf_utm.geometry.buffer(ROAD_BUFFER_METER)
            roads_buffer = buffered_utm.to_crs(raster_crs)
            shapes = [mapping(geom) for geom in roads_buffer]

            out_image, out_transform = rasterio.mask.mask(src, shapes, invert=False, crop=False, nodata=np.nan, filled=True)
            
            if np.sum(~np.isnan(out_image)) > 0:
                with rasterio.open(out_path, "w", **out_meta) as dst:
                    dst.write(out_image)
                logger.info(f"  [OK] Saved highway mask: {out_path.name}")

    except Exception as e:
        logger.error(f"  Error processing {tif_path.name}: {e}")

def create_paddy_mask(tif_path):
    grid_id = tif_path.parent.name
    coords = decode_grid_id(grid_id)
    if not coords: return
    lat, lon = coords
    
    img_year = get_year_from_filename(tif_path.name)
    if img_year is None: return

    jaxa_path = get_jaxa_lulc_path(img_year, lat, lon)
    if not jaxa_path.exists(): return

    out_path = tif_path.with_name(tif_path.stem + "_paddy_mask.tif")
    # if out_path.exists(): return

    try:
        with rasterio.open(tif_path) as src:
            meta = src.meta.copy()
            height, width = src.shape
            transform = src.transform
            crs = src.crs
            
            jaxa_reprojected = np.zeros((height, width), dtype=rasterio.uint8)
            with rasterio.open(jaxa_path) as jaxa_src:
                reproject(
                    source=rasterio.band(jaxa_src, 1),
                    destination=jaxa_reprojected,
                    src_transform=jaxa_src.transform,
                    src_crs=jaxa_src.crs,
                    dst_transform=transform,
                    dst_crs=crs,
                    resampling=Resampling.nearest
                )

            paddy_binary = (jaxa_reprojected == PADDY_CATEGORY_ID).astype('uint8')
            shapes_gen = rasterio.features.shapes(paddy_binary, transform=transform)
            polygons = [shape(geom) for geom, val in shapes_gen if val == 1]
            
            if not polygons: return

            gdf = gpd.GeoDataFrame({'geometry': polygons}, crs=crs)
            utm_crs = gdf.estimate_utm_crs()
            gdf_utm = gdf.to_crs(utm_crs)
            gdf_utm['geometry'] = gdf_utm.geometry.buffer(PADDY_BUFFER_METER, resolution=2)
            gdf_utm = gdf_utm[~gdf_utm.is_empty]
            
            if gdf_utm.empty: return

            gdf_shrunk = gdf_utm.to_crs(crs)
            mask_shapes = [geom for geom in gdf_shrunk.geometry]

            meta.update({"driver": "GTiff", "dtype": "float32", "nodata": np.nan, "compress": "lzw"})
            out_image, out_transform = rasterio.mask.mask(src, mask_shapes, crop=False, invert=False, nodata=np.nan)
            
            if np.sum(~np.isnan(out_image)) > 0:
                with rasterio.open(out_path, "w", **meta) as dest:
                    dest.write(out_image)
                logger.info(f"  [OK] Saved paddy mask: {out_path.name}")

    except Exception as e:
        logger.error(f"  Error processing {tif_path.name}: {e}")

def main(target_grids=None):
    logger.info("Starting Mask Generation...")
    
    if target_grids is None:
        target_grids = TARGET_GRIDS
    
    # Highway Setup
    available_shps = find_available_shapefiles(ROAD_POLYGON_DIR)
    if not available_shps: logger.warning("No Highway Shapefiles found.")
    
    # JAXA Setup
    if not JAXA_DATA_DIR.exists(): logger.warning("JAXA Data dir not found.")

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
            
            if available_shps:
                create_highway_mask(tif_path, available_shps)
            
            if JAXA_DATA_DIR.exists():
                create_paddy_mask(tif_path)

if __name__ == "__main__":
    main()
