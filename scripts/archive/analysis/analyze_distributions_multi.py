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

def load_all_data():
    """Extract pixels for Road, Paddy, and River across all events."""
    # 1. Load River Polygons once
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
            
            # Parse delay
            try:
                parts = event_dir.name.split('_')
                if len(parts) < 2: continue
                delay_h = float(parts[1].replace('h', ''))
                delay_int = int(delay_h)
                if delay_int > 5: continue # Focus on 0-5h for cleaner plot
            except: continue

            paths = {
                'vv': event_dir / "after_vv.tif",
                'vh': event_dir / "after_vh.tif"
            }
            if not all(p.exists() for p in paths.values()): continue

            try:
                with rasterio.open(paths['vv']) as src:
                    vv_af = src.read(1)
                    vh_af = rasterio.open(paths['vh']).read(1)
                    
                    # Common shape crop
                    min_h = min(vv_af.shape[0], vh_af.shape[0])
                    min_w = min(vv_af.shape[1], vh_af.shape[1])
                    vv_af = vv_af[:min_h, :min_w]
                    vh_af = vh_af[:min_h, :min_w]

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
                    valid_pixels = (vv_af < LINEAR_THRESHOLD) & (vh_af < LINEAR_THRESHOLD) & (vv_af > 0)

                    # Extract by class
                    for label, mask_arr in [('Road', road_mask), ('Paddy', paddy_mask), ('River', river_mask)]:
                        m = mask_arr & valid_pixels
                        if np.any(m):
                            # Sample some pixels if too many to keep memory low (e.g. max 500 per event-class)
                            vv_vals = vv_af[m]
                            vh_vals = vh_af[m]
                            
                            if len(vv_vals) > 500:
                                idx = np.random.choice(len(vv_vals), 500, replace=False)
                                vv_vals = vv_vals[idx]
                                vh_vals = vh_vals[idx]
                                
                            temp_df = pd.DataFrame({
                                'vv': vv_vals,
                                'vh': vh_vals,
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

def plot_distributions(df):
    if df.empty:
        print("No data to plot.")
        return

    print("Generating plots...")
    delays = sorted(df['delay'].unique())
    classes = ['Road', 'Paddy', 'River']
    colors = {'Road': 'orange', 'Paddy': 'green', 'River': 'blue'}
    
    # 1. VV Distribution Grid
    fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharex=True)
    fig.suptitle("後方散乱強度 (VV After) のクラス別分布推移 (全期間)", fontsize=20)
    axes = axes.flatten()
    
    for i, d in enumerate(range(6)):
        ax = axes[i]
        subset = df[df['delay'] == d]
        if subset.empty:
            ax.set_title(f"Delay {d}h: データなし")
            continue
            
        for cls in classes:
            cls_df = subset[subset['class'] == cls]
            if not cls_df.empty:
                sns.kdeplot(data=cls_df, x='vv', label=cls, color=colors[cls], ax=ax, fill=True, alpha=0.2)
        
        ax.set_title(f"Delay {d}h")
        ax.set_xlim(0, 0.4)
        ax.set_xlabel("VV Intensity (Linear)")
        if i == 0: ax.legend()

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(OUTPUT_DIR / "vv_distribution_multi_class.png")
    plt.close()

    # 2. VH Distribution Grid
    fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharex=True)
    fig.suptitle("後方散乱強度 (VH After) のクラス別分布推移 (全期間)", fontsize=20)
    axes = axes.flatten()
    
    for i, d in enumerate(range(6)):
        ax = axes[i]
        subset = df[df['delay'] == d]
        if subset.empty:
            ax.set_title(f"Delay {d}h: データなし")
            continue
            
        for cls in classes:
            cls_df = subset[subset['class'] == cls]
            if not cls_df.empty:
                sns.kdeplot(data=cls_df, x='vh', label=cls, color=colors[cls], ax=ax, fill=True, alpha=0.2)
        
        ax.set_title(f"Delay {d}h")
        ax.set_xlim(0, 0.1)
        ax.set_xlabel("VH Intensity (Linear)")
        if i == 0: ax.legend()

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(OUTPUT_DIR / "vh_distribution_multi_class.png")
    plt.close()

def main():
    df = load_all_data()
    if not df.empty:
        df.to_csv(OUTPUT_DIR / "extracted_pixels_all.csv", index=False)
        plot_distributions(df)
        print(f"Results saved to {OUTPUT_DIR}")
    else:
        print("No pixel data extracted.")

if __name__ == "__main__":
    main()
