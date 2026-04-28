import os
import sys
import logging
import json
import glob
from pathlib import Path
import numpy as np
import rasterio
from rasterio.mask import mask
import pandas as pd
import mysql.connector

# Setup Logger
def setup_logger(name, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        if log_file:
            fh = logging.FileHandler(log_file, encoding='utf-8')
            fh.setFormatter(formatter)
            logger.addHandler(fh)
    return logger

logger = setup_logger("thesis_common")

# Path Definitions
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATA_DIR = BASE_DIR / "data"
EXPANDED_DIR = DATA_DIR / "expanded"
SAMPLES_DIR = EXPANDED_DIR / "samples"
MASKS_DIR = EXPANDED_DIR / "masks"
RESULT_DIR = DATA_DIR / "result"

# DB Config
DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'root',
    'password': 'root',
    'database': 'rainsar_hub'
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"DB Connection failed: {e}")
        return None

def get_grid_ids():
    """Returns a list of available grid IDs in SAMPLES_DIR."""
    if not SAMPLES_DIR.exists():
        logger.error(f"Samples directory not found: {SAMPLES_DIR}")
        return []
    return [d.name for d in SAMPLES_DIR.iterdir() if d.is_dir()]

def get_events(grid_id):
    """Returns a list of event directory names for a specific grid."""
    grid_dir = SAMPLES_DIR / grid_id
    if not grid_dir.exists():
        return []
    return [d.name for d in grid_dir.iterdir() if d.is_dir() and d.name.startswith("delay_")]

def load_raster(tif_path):
    """
    Loads a GeoTIFF file and returns data (band 1) and the source profile.
    Returns (data, profile) or (None, None) if failed.
    """
    if not tif_path.exists():
        logger.warning(f"File not found: {tif_path}")
        return None, None
    
    try:
        with rasterio.open(tif_path) as src:
            data = src.read(1)
            profile = src.profile
            # Handle nodata
            if src.nodata is not None:
                data[data == src.nodata] = np.nan
            return data, profile, src # Return src context handling if needed, but src is closed here
    except Exception as e:
        logger.error(f"Error reading {tif_path}: {e}")
        return None, None, None

def load_raster_masked(tif_path, mask_shapes):
    """
    Loads a GeoTIFF and applies the given mask shapes (GeoJSON geometry list).
    Returns valid pixel values as a 1D numpy array.
    """
    if not tif_path.exists():
        return None
        
    if not mask_shapes:
        return None

    try:
        with rasterio.open(tif_path) as src:
            # Check for overlap
            # Simple check: mask transforms? 
            # rasterio.mask handles cropping.
            
            try:
                out_image, out_transform = mask(src, mask_shapes, crop=True, nodata=np.nan)
                data = out_image[0] # Band 1
                
                # Flatten and remove NaNs
                valid_pixels = data[~np.isnan(data)]
                return valid_pixels
            except ValueError as ve:
                # Often happens if shapes do not overlap raster
                # logger.debug(f"Masking failed (likely no overlap) for {tif_path.name}: {ve}")
                return None
    except Exception as e:
        logger.error(f"Error masking {tif_path}: {e}")
        return None

def get_mask_shapes(grid_id, mask_type="paddy"):
    """
    Loads GeoJSON shapes for a specific grid and mask type.
    mask_type: 'paddy', 'road' (auto-detects motorway/trunk)
    Returns a list of geometries.
    """
    mask_dir = MASKS_DIR / grid_id
    shapes = []
    
    if mask_type == "paddy":
        paddy_path = mask_dir / f"{grid_id}_paddy.geojson"
        if paddy_path.exists():
            shapes.extend(_read_geojson_geometries(paddy_path))
    elif mask_type == "road":
        # Load both motorway and trunk if available
        for road_type in ["motorway", "trunk"]:
            road_path = mask_dir / f"{grid_id}_{road_type}.geojson"
            if road_path.exists():
                shapes.extend(_read_geojson_geometries(road_path))
    
    return shapes

def _read_geojson_geometries(geojson_path):
    shapes = []
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if "features" in data:
            for feature in data["features"]:
                if "geometry" in feature:
                    shapes.append(feature["geometry"])
    except Exception as e:
        logger.error(f"Error reading GeoJSON {geojson_path}: {e}")
    return shapes

def filter_outliers(data, method="iqr", factor=1.5):
    """
    Filters outliers from a 1D numpy array.
    method: 'iqr' or 'sigma'
    """
    if data is None or len(data) == 0:
        return np.array([])
    
    if method == "iqr":
        q1 = np.percentile(data, 25)
        q3 = np.percentile(data, 75)
        iqr = q3 - q1
        lower_bound = q1 - (factor * iqr)
        upper_bound = q3 + (factor * iqr)
        return data[(data >= lower_bound) & (data <= upper_bound)]
    elif method == "sigma":
        mean = np.mean(data)
        std = np.std(data)
        lower_bound = mean - (factor * std)
        upper_bound = mean + (factor * std)
        return data[(data >= lower_bound) & (data <= upper_bound)]
    return data

def db_to_linear(db_vals):
    return 10.0 ** (db_vals / 10.0)

def linear_to_db(linear_vals):
    # Avoid log(0) or log(negative)
    with np.errstate(divide='ignore', invalid='ignore'):
        return 10.0 * np.log10(linear_vals)
