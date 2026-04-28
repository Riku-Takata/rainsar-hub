import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import rasterio
from rasterio.features import geometry_mask
import geopandas as gpd
import warnings
import os

warnings.filterwarnings('ignore')

# --- CONFIG ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATA_DIR = BASE_DIR / "data"
SAMPLES_DIR = DATA_DIR / "expanded" / "samples"
MASKS_DIR = DATA_DIR / "expanded" / "masks"
OUTPUT_DIR = DATA_DIR / "result" / "distribution_analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# River Paths
RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data" / "river_polygon_2320001.geojson",
    BASE_DIR / "mask-data" / "river_polygon_2320061.geojson"
]

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def load_diff_data():
    """Extract diff pixels for Road, Paddy, and River across all events."""
    # 1. Load River Polygons
    river_gdf_list = []
    for p in RIVER_GEOJSON_PATHS:
        if p.exists():
            river_gdf_list.append(gpd.read_file(p))
    if not river_gdf_list:
        print("Error: River GeoJSONs not found.")
        return pd.DataFrame()
    gdf_river = pd.concat(river_gdf_list, ignore_index=True)

    all_data = []
    
    # 2. Iterate through all grids and events
    print(f"Scanning samples in {SAMPLES_DIR}...")
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    
    total_grids = len(grids)
    for i, grid_dir in enumerate(grids):
        grid_id = grid_dir.name
        
        # Load Masks for this grid
        # Road
        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists():
            road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
        gdf_road = None
        if road_mask_path.exists():
            try: gdf_road = gpd.read_file(road_mask_path)
            except: pass
            
        # Paddy
        paddy_mask_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
        gdf_paddy = None
        if paddy_mask_path.exists():
            try: gdf_paddy = gpd.read_file(paddy_mask_path)
            except: pass

        # Scan Events
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            
            try:
                parts = event_dir.name.split('_')
                if len(parts) < 2: continue
                delay_h = float(parts[1].replace('h', ''))
                delay_int = int(delay_h)
                if delay_int > 5: continue # Focus on 0-5h
            except: continue

            paths = {
                'vv_be': event_dir / "before_vv.tif",
                'vv_af': event_dir / "after_vv.tif",
                'vh_be': event_dir / "before_vh.tif",
                'vh_af': event_dir / "after_vh.tif"
            }
            if not all(p.exists() for p in paths.values()): continue

            try:
                with rasterio.open(paths['vv_af']) as src:
                    # Read all bands
                    vv_af = src.read(1)
                    vv_be = rasterio.open(paths['vv_be']).read(1)
                    vh_af = rasterio.open(paths['vh_af']).read(1)
                    vh_be = rasterio.open(paths['vh_be']).read(1)
                    
                    # Common shape crop
                    min_h = min(vv_af.shape[0], vv_be.shape[0], vh_af.shape[0], vh_be.shape[0])
                    min_w = min(vv_af.shape[1], vv_be.shape[1], vh_af.shape[1], vh_be.shape[1])
                    vv_af = vv_af[:min_h, :min_w]
                    vv_be = vv_be[:min_h, :min_w]
                    vh_af = vh_af[:min_h, :min_w]
                    vh_be = vh_be[:min_h, :min_w]

                    # Calculate Diff
                    vv_diff = vv_af - vv_be
                    vh_diff = vh_af - vh_be

                    # Generate Masks
                    # River
                    if gdf_river.crs != src.crs:
                        gdf_curr_river = gdf_river.to_crs(src.crs)
                    else:
                        gdf_curr_river = gdf_river
                    river_mask = geometry_mask(gdf_curr_river.geometry, out_shape=(min_h, min_w), 
                                               transform=src.transform, invert=True, all_touched=True)
                    
                    # Road
                    road_mask = np.zeros((min_h, min_w), dtype=bool)
                    if gdf_road is not None:
                        if gdf_road.crs != src.crs:
                            gdf_curr_road = gdf_road.to_crs(src.crs)
                        else:
                            gdf_curr_road = gdf_road
                        road_mask = geometry_mask(gdf_curr_road.geometry, out_shape=(min_h, min_w),
                                                  transform=src.transform, invert=True, all_touched=False)

                    # Paddy
                    paddy_mask = np.zeros((min_h, min_w), dtype=bool)
                    if gdf_paddy is not None:
                        if gdf_paddy.crs != src.crs:
                            gdf_curr_paddy = gdf_paddy.to_crs(src.crs)
                        else:
                            gdf_curr_paddy = gdf_paddy
                        paddy_mask = geometry_mask(gdf_curr_paddy.geometry, out_shape=(min_h, min_w),
                                                   transform=src.transform, invert=True, all_touched=False)

                    # --- Extract and Filter ---
                    LINEAR_THRESHOLD = 2.0
                    valid_pixels = (vv_af < LINEAR_THRESHOLD) & (vh_af < LINEAR_THRESHOLD) & \
                                   (vv_be < LINEAR_THRESHOLD) & (vh_be < LINEAR_THRESHOLD) & \
                                   (vv_af > 0) & (vv_be > 0)

                    for label, mask_arr in [('Road', road_mask), ('Paddy', paddy_mask), ('River', river_mask)]:
                        m = mask_arr & valid_pixels
                        if np.any(m):
                            vv_diff_vals = vv_diff[m]
                            vh_diff_vals = vh_diff[m]
                            
                            # Sample if too many
                            if len(vv_diff_vals) > 500:
                                idx = np.random.choice(len(vv_diff_vals), 500, replace=False)
                                vv_diff_vals = vv_diff_vals[idx]
                                vh_diff_vals = vh_diff_vals[idx]
                                
                            temp_df = pd.DataFrame({
                                'vv_diff': vv_diff_vals,
                                'vh_diff': vh_diff_vals,
                                'delay': delay_int,
                                'class': label
                            })
                            all_data.append(temp_df)

            except: continue
        
        if (i+1) % 50 == 0:
            print(f"Processed {i+1}/{total_grids} grids...")

    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

def plot_boxplots(df):
    if df.empty:
        print("No data to plot.")
        return

    classes = ['Road', 'Paddy', 'River']
    jp_classes = {'Road': '道路', 'Paddy': '水田', 'River': '河川周辺'}
    
    for cls in classes:
        cls_df = df[df['class'] == cls]
        if cls_df.empty: continue
        
        # Melt for seaborn (VV vs VH)
        df_melt = cls_df.melt(id_vars=['delay'], value_vars=['vv_diff', 'vh_diff'], 
                              var_name='Polarization', value_name='Intensity Diff')
        
        plt.figure(figsize=(12, 8))
        sns.boxplot(data=df_melt, x='delay', y='Intensity Diff', hue='Polarization', 
                    palette='Set2', showfliers=False)
        
        plt.title(f"{jp_classes[cls]}における後方散乱強度差分 (After - Before) の推移", fontsize=16)
        plt.xlabel("降雨後経過時間 (Delay) [h]", fontsize=14)
        plt.ylabel("強度差分 (Linear Intensity Diff)", fontsize=14)
        plt.grid(True, axis='y', alpha=0.3)
        plt.axhline(0, color='red', linestyle='--', alpha=0.5)
        
        save_path = OUTPUT_DIR / f"diff_boxplot_{cls}.png"
        plt.savefig(save_path)
        plt.close()
        print(f"Saved: {save_path}")

def main():
    df = load_diff_data()
    if not df.empty:
        plot_boxplots(df)
        print(f"Analysis completed. Results in {OUTPUT_DIR}")
    else:
        print("No pixel data extracted.")

if __name__ == "__main__":
    main()
