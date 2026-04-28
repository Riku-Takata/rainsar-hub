import os
import sys
import re
import math
import logging
import zipfile
import tempfile
import shutil
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
import rasterio.mask
import fiona
from shapely.geometry import shape, mapping, box, Point
from pathlib import Path

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
SAMPLES_DIR = BASE_DIR / "data_vv" / "samples"
FUDE_DIR = Path("D:/sotsuron/fude-polygon")
OSM_DIR = BASE_DIR / "mask-data" / "osm_roads" # Hypothethical, check later
MULTI_EVENT_CSV = BASE_DIR / "data_vv" / "analysis" / "best_31_grids_extracted.csv"
OUTPUT_MASK_DIR = BASE_DIR / "data_vv" / "masks"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("opt_masks")

def decode_grid_id(grid_id):
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m: return None, None
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S': lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W': lon = -lon
    return lat, lon

def get_reference_tif(grid_id):
    # Find a storage tif or event tif
    storage = SAMPLES_DIR / grid_id / "storage"
    tifs = list(storage.glob("*_proc.tif"))
    if tifs: return tifs[0]
    
    # Fallback to event folders
    gdir = SAMPLES_DIR / grid_id
    if not gdir.exists():
        return None
        
    for d in gdir.iterdir():
        if d.is_dir():
            tifs = list(d.glob("*.tif"))
            if tifs: return tifs[0]
    return None

def create_road_mask_for_grid(grid_id, ref_tif):
    # Check output existence
    out_dir = OUTPUT_MASK_DIR / grid_id
    out_name = out_dir / f"{grid_id}_road_mask.tif"
    if out_name.exists():
        logger.info(f"Skipping Road Mask for {grid_id} (Exists)")
        return

    geojson_path = OSM_DIR / f"{grid_id}_roads.geojson"
    if not geojson_path.exists():
        # logger.warning(f"OSM data not found for {grid_id}")
        return

    try:
        with rasterio.open(ref_tif) as src:
            # Read GeoJSON
            try:
                gdf = gpd.read_file(geojson_path)
            except Exception as e:
                logger.warning(f"Failed to read OSM {grid_id}: {e}")
                return
            
            if gdf.empty: return

            # Ensure CRS matches
            if gdf.crs is None: gdf.set_crs("EPSG:4326", inplace=True)
            if gdf.crs != src.crs:
                gdf = gdf.to_crs(src.crs)

            # Clip
            bounds = src.bounds
            g_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            clipped = gpd.clip(gdf, g_box)
            if clipped.empty: return

            # Rasterize
            out_shape = src.shape
            transform = src.transform
            
            # all_touched=True for lines is critical
            mask = rasterio.features.rasterize(
                ((geom, 1) for geom in clipped.geometry),
                out_shape=out_shape,
                transform=transform,
                fill=0,
                all_touched=True,
                dtype=rasterio.uint8
            )
            
            if np.sum(mask) == 0: return

            # Save
            out_dir = OUTPUT_MASK_DIR / grid_id
            out_dir.mkdir(parents=True, exist_ok=True)
            out_name = out_dir / f"{grid_id}_road_mask.tif"
            
            meta = src.meta.copy()
            meta.update({"count": 1, "dtype": "uint8", "nodata": 0, "compress": "lzw"})
            
            with rasterio.open(out_name, "w", **meta) as dst:
                dst.write(mask, 1)
                
            logger.info(f"[ROAD] Saved {out_name.name}")

    except Exception as e:
        logger.error(f"Error creating road mask for {grid_id}: {e}")

def process_fude_zip(zip_path, target_grids_df):
    """
    1. Scan contents for .json/.geojson.
    2. Peek FIRST file to determine approximate location (Prefecture Check).
    3. If Zip is relevant, iterate ALL files inside.
    """
    logger.info(f"Checking {zip_path.name}...")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            names = z.namelist()
            # Fude data uses .json for GeoJSON in recent releases
            spatial_files = [n for n in names if n.lower().endswith('.geojson') or n.lower().endswith('.json')]
            if not spatial_files:
                logger.warning(f"No spatial file found in {zip_path.name}")
                return

            # --- Step 1: Zip-level Proximity Check (using the first file) ---
            first_file = spatial_files[0]
            abs_zip = str(zip_path.resolve()).replace("\\", "/")
            
            # vsi path for the first file
            vsi_first = f"/vsizip/{abs_zip}/{first_file}"
            
            sample_lat, sample_lon = None, None
            try:
                with fiona.open(vsi_first) as src:
                    try:
                        first = next(iter(src))
                        geom = shape(first['geometry'])
                        centroid = geom.centroid
                        sample_lon, sample_lat = centroid.x, centroid.y
                    except StopIteration:
                        pass
            except Exception as e:
                logger.warning(f"Failed to peek {first_file}: {e}")

            if sample_lat is None:
                logger.warning("Could not determine location from first file. Skipping zip.")
                return

            # Filter Grids near this sample point (Prefecture Level Check)
            # Threshold: 1.5 degrees (~150km) to cover large prefectures
            nearby_grids_ids = []
            for _, row in target_grids_df.iterrows():
                dist = math.sqrt((row['lat'] - sample_lat)**2 + (row['lon'] - sample_lon)**2)
                if dist < 1.5: 
                    nearby_grids_ids.append(row['grid_id'])
            
            if not nearby_grids_ids:
                logger.info(f"  -> Skipped (No target grids near {sample_lat:.2f}, {sample_lon:.2f})")
                return
            
            logger.info(f"  -> Zip is relevant for {len(nearby_grids_ids)} grids. Processing {len(spatial_files)} files...")

            # --- Step 2: Process ALL files in the Zip ---
            for sp_file in spatial_files:
                vsi_path = f"/vsizip/{abs_zip}/{sp_file}"
                
                # Check bounds of this specific municipality file before loading full DF?
                # fiona can give bounds.
                # (Optimization: Skip municipalities far from target grids)
                file_relevant = False
                file_target_grids = []
                
                try:
                    with fiona.open(vsi_path) as src:
                        # src.bounds = (minx, miny, maxx, maxy)
                        if src.bounds:
                            minx, miny, maxx, maxy = src.bounds
                            
                            # Expand bounds slightly
                            b_box = box(minx, miny, maxx, maxy)
                            
                            # Intersection check with Target Grids Points?
                            # Use bbox check against Grid Centers +- 0.05 deg (approx grid size)
                            for gid in nearby_grids_ids:
                                row = target_grids_df[target_grids_df['grid_id'] == gid].iloc[0]
                                g_p = Point(row['lon'], row['lat'])
                                # Rough check: distance from point to box < threshold
                                # or point inside box?
                                # Grids are small (10km). Municipality can be larger.
                                # Simple: If box is within 0.2 deg of grid center.
                                if b_box.distance(g_p) < 0.1: 
                                    file_relevant = True
                                    file_target_grids.append(gid)
                except Exception as e:
                    logger.warning(f"Error checking bounds of {sp_file}: {e}")
                    continue

                if not file_relevant:
                    continue

                # Load Data (Force fiona engine for vsizip stability)
                try:
                    gdf = gpd.read_file(vsi_path, engine="fiona")
                except Exception as e:
                    logger.warning(f"Failed to read {sp_file}: {e}")
                    continue

                if gdf.empty: continue

                # Detect Paddy (LandType 100)
                # Columns vary. 'LandType', 'LAND_TYPE', 'CHI_CD', etc.
                # MAFF 2021: often 'land_type' (int)
                
                target_col = None
                for c in gdf.columns:
                    if c.lower() in ['landtype', 'land_type', 'chi_cd', 'mokuteki', 'chimmoku']:
                        target_col = c
                        break
                
                filtered_gdf = gdf
                if target_col:
                    # try to filter 100 or '100'
                    # Cast to int if possible
                    try:
                        filtered_gdf = gdf[gdf[target_col].astype(str) == '100']
                    except:
                        pass
                
                if filtered_gdf.empty: continue

                # Buffer -5m Properly (Project -> Buffer -> Unproject)
                try:
                    # Estimate UTM CRS based on the first geometry
                    utm_crs = filtered_gdf.estimate_utm_crs()
                    # Project, Buffer -5m, Reproject back
                    buffered = filtered_gdf.to_crs(utm_crs).buffer(-5)
                    # Convert GeoSeries back to GeoDataFrame (geometry column)
                    filtered_gdf = filtered_gdf.copy()
                    filtered_gdf['geometry'] = buffered.to_crs(filtered_gdf.crs)
                except Exception as e:
                    # Fallback if estimation fails (rare)
                    filtered_gdf['geometry'] = filtered_gdf.geometry.buffer(-0.000045)
                
                filtered_gdf = filtered_gdf[~filtered_gdf.is_empty]
                if filtered_gdf.empty: continue

                # Rasterize for relevant grids
                for gid in file_target_grids:
                    ref_tif = get_reference_tif(gid)
                    if not ref_tif: continue
                    
                    try:
                        with rasterio.open(ref_tif) as src:
                            bounds = src.bounds
                            g_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                            
                            # Ensure CRS matches
                            if filtered_gdf.crs != src.crs:
                                filtered_gdf = filtered_gdf.to_crs(src.crs)

                            clipped = gpd.clip(filtered_gdf, g_box)
                            if clipped.empty: continue
                            
                            out_shape = src.shape
                            transform = src.transform
                            
                            out_dir = OUTPUT_MASK_DIR / gid
                            out_dir.mkdir(parents=True, exist_ok=True)
                            out_name = out_dir / f"{gid}_paddy_mask.tif"
                            
                            existing_mask = None
                            if out_name.exists():
                                with rasterio.open(out_name) as existing:
                                    existing_mask = existing.read(1)
                            else:
                                existing_mask = np.zeros(out_shape, dtype=rasterio.uint8)

                            # Rasterize 
                            # Note: all_touched=True for masks is usually good for small features
                            new_mask = rasterio.features.rasterize(
                                ((geom, 1) for geom in clipped.geometry),
                                out_shape=out_shape,
                                transform=transform,
                                fill=0,
                                all_touched=True,
                                dtype=rasterio.uint8
                            )
                            
                            # Union
                            final_mask = np.maximum(existing_mask, new_mask)
                            
                            meta = src.meta.copy()
                            meta.update({"count": 1, "dtype": "uint8", "nodata": 0, "compress": "lzw"})
                            
                            with rasterio.open(out_name, "w", **meta) as dst:
                                dst.write(final_mask, 1)

                    except Exception as e:
                        # logger.error(f"Rasterize error {gid}: {e}")
                        pass

    except Exception as e:
        logger.error(f"Error processing {zip_path.name}: {e}")

def main():
    if not MULTI_EVENT_CSV.exists():
        logger.error("Multi event CSV not found.")
        return
        
    df = pd.read_csv(MULTI_EVENT_CSV)
    
    # Use all grids in this file (already filtered)
    targets = df.copy()
    logger.info(f"Target Grids (Original Top List): {len(targets)}")
    
    # Decode Lat/Lon
    coords = targets['grid_id'].apply(decode_grid_id)
    targets['lat'] = [c[0] for c in coords]
    targets['lon'] = [c[1] for c in coords]
    targets = targets.dropna(subset=['lat', 'lon'])
    
    # Filter out grids that already have a Paddy Mask
    # Since Paddy processing is additive, we assume existence means "done" for this batch.
    incomplete_mask = []
    for _, row in targets.iterrows():
        gid = row['grid_id']
        piddy_path = OUTPUT_MASK_DIR / gid / f"{gid}_paddy_mask.tif"
        if piddy_path.exists():
            incomplete_mask.append(False)
        else:
            incomplete_mask.append(True)
            
    skipped_count = len(targets) - sum(incomplete_mask)
    if skipped_count > 0:
        logger.info(f"Skipping {skipped_count} grids that already have Paddy Masks.")
        
    targets = targets[incomplete_mask].copy()
    if targets.empty:
        logger.info("All target grids have Paddy Masks. Nothing to process for Fude.")
        # We still might run Road Masks if those are missing, but let's assume filtering applies to Fude loop
    
    # Process Roads for all targets first
    logger.info("Generating Road Masks (OSM)...")
    for _, row in targets.iterrows():
        gid = row['grid_id']
        ref_tif = get_reference_tif(gid)
        if ref_tif:
            create_road_mask_for_grid(gid, ref_tif)
        else:
            logger.warning(f"No reference tif for {gid}, skipping road mask.")

    # Iterate Fude Zips
    zip_files = list(FUDE_DIR.glob("*.zip"))
    logger.info(f"Found {len(zip_files)} Fude Zip files to scan.")
    
    for zf in zip_files:
        process_fude_zip(zf, targets)

    logger.info("Done.")

if __name__ == "__main__":
    main()
