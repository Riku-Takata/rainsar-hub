"""
Calculate Sigma0 and Difference metrics for preprocessed expansion grids.
Iterates over data/expanded/samples, applies Paddy/Road masks, and computes statistics.
"""
import os
import sys
import logging
import warnings
from pathlib import Path
import pandas as pd
import numpy as np
import rasterio
import rasterio.mask
import geopandas as gpd
from shapely.geometry import box

# Warnings check
warnings.filterwarnings('ignore')

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"
MASKS_DIR = BASE_DIR / "data/expanded/masks"
OUTPUT_CSV = BASE_DIR / "data/analysis/expansion_sigma_diff.csv"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MetricsCalc")

def calculate_stats(tif_path, mask_gdf):
    """
    Calculate mean value of TIF pixels within mask geometry.
    """
    try:
        with rasterio.open(tif_path) as src:
            # Check overlap
            tif_bounds = box(*src.bounds)
            # Reproject mask to TIF CRS if needed (usually EPSG:4326 for both)
            if mask_gdf.crs != src.crs:
                mask_gdf = mask_gdf.to_crs(src.crs)
            
            # Filter geometries that intersect with TIF
            valid_geoms = mask_gdf[mask_gdf.intersects(tif_bounds)].geometry
            
            if valid_geoms.is_empty.all():
                return np.nan, 0

            # Mask
            out_image, out_transform = rasterio.mask.mask(src, valid_geoms, crop=True, nodata=np.nan)
            data = out_image[0] # Band 1
            
            # Remove NaNs and nodata
            valid_pixels = data[~np.isnan(data)]
            
            if valid_pixels.size == 0:
                return np.nan, 0
                
            return np.mean(valid_pixels), valid_pixels.size
            
    except Exception as e:
        logger.warning(f"Error processing {tif_path.name}: {e}")
        return np.nan, 0

def main():
    if not SAMPLES_DIR.exists():
        logger.error("Samples directory not found.")
        return

    # Find all event directories
    # Structure: SAMPLES_DIR / grid_id / delay_... / *.tif
    results = []
    
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    logger.info(f"Found {len(grids)} grids in samples directory.")
    
    count = 0
    for grid_dir in grids:
        grid_id = grid_dir.name
        
        # Load Masks once per grid
        paddy_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
        road_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson" # Try motorway first
        
        paddy_gdf = None
        road_gdf = None
        
        if paddy_path.exists():
            try:
                paddy_gdf = gpd.read_file(paddy_path)
            except: pass
            
        if not road_path.exists():
             road_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson" # Fallback
             
        if road_path.exists():
            try:
                road_gdf = gpd.read_file(road_path)
            except: pass
            
        if paddy_gdf is None and road_gdf is None:
            logger.warning(f"No masks found for {grid_id}, skipping.")
            continue

        events = [d for d in grid_dir.iterdir() if d.is_dir()]
        
        for event_dir in events:
            event_name = event_dir.name # delay_10.0h_20240801
            
            # Parse event info roughly
            parts = event_name.split('_')
            try:
                delay = float(parts[1].replace('h', ''))
                date = parts[2]
            except:
                delay = -1
                date = "unknown"

            # Process Polarizations
            for pol in ["vv", "vh"]:
                after_tif = event_dir / f"after_{pol}.tif"
                before_tif = event_dir / f"before_{pol}.tif"
                
                if not after_tif.exists() or not before_tif.exists():
                    continue
                
                # --- Paddy ---
                if paddy_gdf is not None:
                    sigma_after, n_after = calculate_stats(after_tif, paddy_gdf)
                    sigma_before, n_before = calculate_stats(before_tif, paddy_gdf)
                    
                    if not np.isnan(sigma_after) and not np.isnan(sigma_before):
                        diff = sigma_after - sigma_before
                        results.append({
                            'grid_id': grid_id,
                            'event_name': event_name,
                            'date': date,
                            'delay_h': delay,
                            'pol': pol.upper(),
                            'feature': 'paddy',
                            'sigma_after': sigma_after,
                            'sigma_before': sigma_before,
                            'diff': diff,
                            'pixel_count': n_after
                        })

                # --- Road ---
                if road_gdf is not None:
                    sigma_after, n_after = calculate_stats(after_tif, road_gdf)
                    sigma_before, n_before = calculate_stats(before_tif, road_gdf)
                    
                    if not np.isnan(sigma_after) and not np.isnan(sigma_before):
                        diff = sigma_after - sigma_before
                        results.append({
                            'grid_id': grid_id,
                            'event_name': event_name,
                            'date': date,
                            'delay_h': delay,
                            'pol': pol.upper(),
                            'feature': 'road',
                            'sigma_after': sigma_after,
                            'sigma_before': sigma_before,
                            'diff': diff,
                            'pixel_count': n_after
                        })
        
        count += 1
        if count % 10 == 0:
            logger.info(f"Processed {count}/{len(grids)} grids...")
            if results:
                df = pd.DataFrame(results)
                # Append to CSV
                header = not OUTPUT_CSV.exists()
                df.to_csv(OUTPUT_CSV, mode='a', header=header, index=False)
                results = [] # Clear buffer

    if results:
        df = pd.DataFrame(results)
        header = not OUTPUT_CSV.exists()
        df.to_csv(OUTPUT_CSV, mode='a', header=header, index=False)
        logger.info(f"Saved remaining metrics to {OUTPUT_CSV}")
    
    if OUTPUT_CSV.exists():
        df_all = pd.read_csv(OUTPUT_CSV)
        logger.info(f"Total records processed: {len(df_all)}")
        logger.info("\n--- Summary ---")
        print(df_all.groupby(['feature', 'pol'])[['sigma_after', 'diff']].mean())
    else:
        logger.warning("No metrics computed.")

if __name__ == "__main__":
    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink() # Start fresh
    main()
