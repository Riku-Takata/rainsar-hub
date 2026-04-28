import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import rasterio
from rasterio.mask import mask
from rasterio.features import geometry_mask
import geopandas as gpd
import warnings
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# --- CONFIG ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "result" / "River_vs_Road_Oct"
MASKS_DIR = DATA_DIR / "expanded" / "masks"
SAMPLES_DIR = DATA_DIR / "expanded" / "samples"

# Corrected River Paths provided by the user
RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data" / "river_polygon_2320001.geojson",
    BASE_DIR / "mask-data" / "river_polygon_2320061.geojson"
]

# Font setup
import matplotlib as mpl
font_list = ['Meiryo', 'MS Gothic', 'Yu Gothic', 'sans-serif']
mpl.rcParams['font.family'] = font_list

def load_data():
    """Load data for October Events (0-5h), grouped by Event."""
    # Load River Polygons
    river_gdf_list = []
    print("Loading River GeoJSONs...")
    for p in RIVER_GEOJSON_PATHS:
        print(f"Checking {p}...")
        if p.exists():
            river_gdf_list.append(gpd.read_file(p))
            print("  Loaded.")
        else:
            print("  Not found.")
            
    if not river_gdf_list:
        print("River GeoJSONs not found.")
        return {}
        
    gdf_river = pd.concat(river_gdf_list, ignore_index=True)
    
    # Event Data Storage: { event_id: {'river': df, 'road': df} }
    events_data = {}
    
    # Scan grids
    print(f"Scanning samples in {SAMPLES_DIR}...")
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    count = 0
    
    for grid_dir in grids:
        grid_id = grid_dir.name
        
        # Load Road Mask
        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists():
            road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
            
        gdf_road = None
        if road_mask_path.exists():
            try:
                gdf_road = gpd.read_file(road_mask_path)
            except: pass
            
        # Scan Events in Grid
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            event_name = event_dir.name
            
            # Parse: delay_{dh}_{YYYYMMDD}
            try:
                parts = event_name.split('_')
                if len(parts) < 3: continue
                
                date_str = parts[2]
                month = int(date_str[4:6])
                
                if month != 10: continue # October only
                
                delay_h = float(parts[1].replace('h', ''))
                if delay_h > 5: continue # 0-5h
                
            except: continue
            
            event_id = f"{grid_id}_{date_str}_{int(delay_h)}h"
            
            paths = {
                'vv_be': event_dir / "before_vv.tif",
                'vv_af': event_dir / "after_vv.tif",
                'vh_be': event_dir / "before_vh.tif",
                'vh_af': event_dir / "after_vh.tif"
            }
            
            if not all(p.exists() for p in paths.values()): continue
            
            try:
                with rasterio.open(paths['vv_af']) as src:
                    # 1. Coordinate check for River
                    if gdf_river.crs != src.crs:
                        gdf_curr_river = gdf_river.to_crs(src.crs)
                    else:
                        gdf_curr_river = gdf_river
                        
                    # Check Bounds
                    r_bounds = gdf_curr_river.total_bounds
                    s_bounds = src.bounds
                    
                    has_river = False
                    if not (r_bounds[0] > s_bounds.right or r_bounds[2] < s_bounds.left or 
                            r_bounds[1] > s_bounds.top or r_bounds[3] < s_bounds.bottom):
                        has_river = True

                    # Generate River Mask
                    river_mask_bool = np.zeros(src.shape, dtype=bool)
                    if has_river:
                        river_mask_bool = geometry_mask(gdf_curr_river.geometry, out_shape=src.shape, 
                                                        transform=src.transform, invert=True, all_touched=True)
                    
                    # Generate Road Mask
                    road_mask_bool = np.zeros(src.shape, dtype=bool)
                    if gdf_road is not None:
                        if gdf_road.crs != src.crs:
                            gdf_curr_road = gdf_road.to_crs(src.crs)
                        else:
                            gdf_curr_road = gdf_road
                        road_mask_bool = geometry_mask(gdf_curr_road.geometry, out_shape=src.shape,
                                                       transform=src.transform, invert=True, all_touched=False)

                    if not np.any(river_mask_bool) and not np.any(road_mask_bool):
                        continue

                    # Read and Crop
                    def read_band(p):
                        with rasterio.open(p) as s:
                            return s.read(1)

                    vv_af_raw = read_band(paths['vv_af'])
                    vh_af_raw = read_band(paths['vh_af'])
                    vv_be_raw = read_band(paths['vv_be'])
                    vh_be_raw = read_band(paths['vh_be'])
                    
                    min_h = min(vv_af_raw.shape[0], vh_af_raw.shape[0])
                    min_w = min(vv_af_raw.shape[1], vh_af_raw.shape[1])
                    
                    vv_af = vv_af_raw[:min_h, :min_w]
                    vh_af = vh_af_raw[:min_h, :min_w]
                    
                    river_mask_bool = river_mask_bool[:min_h, :min_w]
                    road_mask_bool = road_mask_bool[:min_h, :min_w]
                    
                    LINEAR_THRESHOLD = 2.0
                    
                    # River
                    rv_dat = None
                    if np.any(river_mask_bool):
                        subset = (vv_af < LINEAR_THRESHOLD) & river_mask_bool
                        if np.any(subset):
                            rv_dat = pd.DataFrame({'vv': vv_af[subset], 'vh': vh_af[subset]})
                    
                    # Road
                    rd_dat = None
                    if np.any(road_mask_bool):
                        subset = (vv_af < LINEAR_THRESHOLD) & road_mask_bool
                        if np.any(subset):
                            rd_dat = pd.DataFrame({'vv': vv_af[subset], 'vh': vh_af[subset]})
                            
                    if (rv_dat is not None and not rv_dat.empty) or \
                       (rd_dat is not None and not rd_dat.empty):
                        
                        events_data[event_id] = {
                            'river': rv_dat if rv_dat is not None else pd.DataFrame(columns=['vv', 'vh']),
                            'road': rd_dat if rd_dat is not None else pd.DataFrame(columns=['vv', 'vh'])
                        }
                        
            except: continue
                
        count += 1
        if count % 20 == 0:
            print(f"Scanned {count} grids...")

    return events_data

def plot_variance(events_data):
    if not events_data:
        print("No event data found.")
        return

    print(f"Plotting for {len(events_data)} events...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    axes = axes.flatten()
    
    plot_cfg = [
        ('River VV', 'river', 'vv'),
        ('River VH', 'river', 'vh'),
        ('Road VV', 'road', 'vv'),
        ('Road VH', 'road', 'vh')
    ]
    
    for i, (title, land_type, pol) in enumerate(plot_cfg):
        ax = axes[i]
        ax.set_title(f"{title} Intensity Distribution per Event")
        ax.set_xlabel("Linear Intensity")
        ax.set_ylabel("Density")
        ax.set_xlim(0, 0.4) 
        
        has_data = False
        for eid, dat in events_data.items():
            df = dat[land_type]
            if df.empty or len(df) < 5: continue
            
            vals = df[pol].values
            try:
                sns.kdeplot(vals, ax=ax, linewidth=0.8, alpha=0.3, color='black', warn_singular=False)
                has_data = True
            except: pass
            
        if not has_data:
            ax.text(0.5, 0.5, "No Data", ha='center')
            
    plt.tight_layout()
    save_path = OUTPUT_DIR / "event_variance_oct.png"
    plt.savefig(save_path)
    print(f"Saved: {save_path}")

def main():
    events_data = load_data()
    plot_variance(events_data)

if __name__ == "__main__":
    main()
