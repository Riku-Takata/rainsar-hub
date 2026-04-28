
import os
import sys
import numpy as np
import pandas as pd
import rasterio
import rasterio.features
import rasterio.mask
import geopandas as gpd
from pathlib import Path
from shapely.geometry import mapping
import warnings

warnings.filterwarnings('ignore')

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
MASKS_DIR = BASE_DIR / "data/expanded/masks"
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"

def get_geojson_path(grid_id, mask_type):
    # Try multiple naming conventions
    candidates = [
        MASKS_DIR / grid_id / f"{grid_id}_{mask_type}.geojson",
        MASKS_DIR / grid_id / f"{mask_type}.geojson"
    ]
    for c in candidates:
        if c.exists(): return c
    return None

def read_tif(path):
    try:
        with rasterio.open(path) as src:
            return src.read(1), src.nodata, src.meta, src
    except:
        return None, None, None, None

def main():
    print("Extracting pixels from Expansion Grids (Direct GeoJSON Mode)...")
    
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(grids)} grids to process.")
    
    processed_events = 0
    total_pixels = 0
    
    for idx, grid_dir in enumerate(grids):
        grid_id = grid_dir.name
        
        # Load GeoJSONs once per grid (lazy load geometries)
        road_json = get_geojson_path(grid_id, "motorway")
        paddy_json = get_geojson_path(grid_id, "paddy")
        
        if not road_json and not paddy_json:
            # Skip if no masks at all
            continue
            
        road_geoms = None
        paddy_geoms = None
        
        try:
            if road_json:
                gdf_r = gpd.read_file(road_json)
                if not gdf_r.empty:
                    if gdf_r.crs is None: gdf_r.set_crs("EPSG:4326", inplace=True)
                    road_geoms = gdf_r
                    
            if paddy_json:
                gdf_p = gpd.read_file(paddy_json)
                if not gdf_p.empty:
                    if gdf_p.crs is None: gdf_p.set_crs("EPSG:4326", inplace=True)
                    paddy_geoms = gdf_p
        except Exception as e:
            print(f"Error loading GeoJSON for {grid_id}: {e}")
            continue

        if road_geoms is None and paddy_geoms is None:
            continue

        # Iterate Events
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            
            # Skip if output exists (optional, keeping overwrite for now or skipping)
            out_csv = event_dir / "diff_pixel_values.csv"
            if out_csv.exists(): 
               pass # Overwrite or Skip? Let's overwrite to be sure.
            
            after_path = event_dir / "after_vv.tif"
            before_path = event_dir / "before_vv.tif"
            
            if not (after_path.exists() and before_path.exists()):
                continue
                
            # Open TIFs
            try:
                msg_prefix = f"[{grid_id}/{event_dir.name}]"
                
                # We need to open src to reproject/rasterize
                # Using 'after' as reference
                with rasterio.open(after_path) as src_a:
                    val_after = src_a.read(1)
                    meta_a = src_a.meta
                    
                    with rasterio.open(before_path) as src_b:
                        val_before = src_b.read(1)
                        
                        # Align dimensions if needed
                        if val_after.shape != val_before.shape:
                            # print(f"{msg_prefix} Shape mismatch: {val_after.shape} vs {val_before.shape}. Cropping.")
                            min_h = min(val_after.shape[0], val_before.shape[0])
                            min_w = min(val_after.shape[1], val_before.shape[1])
                            val_after = val_after[:min_h, :min_w]
                            val_before = val_before[:min_h, :min_w]
                        
                        # Basic Valid Mask (Not Zero)
                        valid_mask = (val_after > 0) & (val_before > 0)
                        
                        # Calc DB
                        after_db = 10 * np.log10(np.maximum(val_after, 1e-10))
                        before_db = 10 * np.log10(np.maximum(val_before, 1e-10))
                        diff_db = after_db - before_db
                        
                        data_rows = []
                        
                        # Process ROAD
                        if road_geoms is not None:
                            try:
                                # Ensure CRS
                                curr_roads = road_geoms
                                if curr_roads.crs != src_a.crs:
                                    curr_roads = curr_roads.to_crs(src_a.crs)
                                
                                # Rasterize to mask
                                # Using rasterize usually faster than mask() for many features
                                shapes = ((geom, 1) for geom in curr_roads.geometry)
                                road_mask = rasterio.features.rasterize(
                                    shapes,
                                    out_shape=val_after.shape, # Match cropped shape? No, src shape.
                                    # Wait, if we cropped image, we must rasterize to full then crop
                                    # Or rasterize to cropped shape? 
                                    # transform handles coordinates.
                                    transform=src_a.transform,
                                    fill=0,
                                    all_touched=True,
                                    dtype=rasterio.uint8
                                )
                                
                                # Apply crop if needed
                                if val_after.shape != src_a.shape:
                                    road_mask = road_mask[:val_after.shape[0], :val_after.shape[1]]
                                
                                # Extract
                                r_locs = (road_mask == 1) & valid_mask
                                if np.any(r_locs):
                                    vals = diff_db[r_locs]
                                    for v in vals:
                                        data_rows.append({'land_type': 'road', 'diff_db': v})
                                        
                            except Exception as e:
                                # print(f"{msg_prefix} Road error: {e}")
                                pass

                        # Process PADDY
                        if paddy_geoms is not None:
                            try:
                                curr_paddy = paddy_geoms
                                if curr_paddy.crs != src_a.crs:
                                    curr_paddy = curr_paddy.to_crs(src_a.crs)
                                    
                                shapes = ((geom, 1) for geom in curr_paddy.geometry)
                                paddy_mask = rasterio.features.rasterize(
                                    shapes,
                                    out_shape=src_a.shape,
                                    transform=src_a.transform,
                                    fill=0,
                                    all_touched=True,
                                    dtype=rasterio.uint8
                                )
                                
                                if val_after.shape != src_a.shape:
                                    paddy_mask = paddy_mask[:val_after.shape[0], :val_after.shape[1]]

                                p_locs = (paddy_mask == 1) & valid_mask
                                if np.any(p_locs):
                                    vals = diff_db[p_locs]
                                    for v in vals:
                                        data_rows.append({'land_type': 'paddy', 'diff_db': v})
                                        
                            except Exception as e:
                                pass
                                
                        # Save
                        if data_rows:
                            df = pd.DataFrame(data_rows)
                            df.to_csv(out_csv, index=False)
                            total_pixels += len(df)
                            processed_events += 1

            except Exception as e:
                # print(f"Error processing {event_dir.name}: {e}")
                pass
                
        if idx % 50 == 0:
            print(f"Processed {idx} grids... (Events: {processed_events}, Pixels: {total_pixels})")
            
    print(f"Done. Processed {processed_events} events, {total_pixels} total pixels extracted.")

if __name__ == "__main__":
    main()
