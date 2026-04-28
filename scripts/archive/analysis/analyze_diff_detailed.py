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

def load_data():
    """Extract pixels for all classes across all months/delays."""
    # Load River Polygons
    river_gdf_list = []
    for p in RIVER_GEOJSON_PATHS:
        if p.exists():
            river_gdf_list.append(gpd.read_file(p))
    gdf_river = pd.concat(river_gdf_list, ignore_index=True)

    all_data = []
    print(f"Scanning samples in {SAMPLES_DIR}...")
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    
    total_grids = len(grids)
    for i, grid_dir in enumerate(grids):
        grid_id = grid_dir.name
        
        # Load Masks
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

        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            try:
                parts = event_dir.name.split('_')
                delay_int = int(float(parts[1].replace('h', '')))
                if delay_int > 5: continue
            except: continue

            paths = {
                'vv_af': event_dir / "after_vv.tif", 'vv_be': event_dir / "before_vv.tif",
                'vh_af': event_dir / "after_vh.tif", 'vh_be': event_dir / "before_vh.tif"
            }
            if not all(p.exists() for p in paths.values()): continue

            try:
                with rasterio.open(paths['vv_af']) as src:
                    vv_af = src.read(1); vv_be = rasterio.open(paths['vv_be']).read(1)
                    vh_af = rasterio.open(paths['vh_af']).read(1); vh_be = rasterio.open(paths['vh_be']).read(1)
                    
                    min_h = min(vv_af.shape[0], vv_be.shape[0], vh_af.shape[0], vh_be.shape[0])
                    min_w = min(vv_af.shape[1], vv_be.shape[1], vh_af.shape[1], vh_be.shape[1])
                    vv_af = vv_af[:min_h, :min_w]; vv_be = vv_be[:min_h, :min_w]
                    vh_af = vh_af[:min_h, :min_w]; vh_be = vh_be[:min_h, :min_w]
                    
                    vv_diff = vv_af - vv_be; vh_diff = vh_af - vh_be

                    # Masks
                    river_mask = geometry_mask(gdf_river.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=True)
                    road_mask = geometry_mask(gdf_road.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=False) if gdf_road is not None else np.zeros((min_h, min_w), dtype=bool)
                    paddy_mask = geometry_mask(gdf_paddy.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=False) if gdf_paddy is not None else np.zeros((min_h, min_w), dtype=bool)

                    valid = (vv_af < 2.0) & (vv_be < 2.0) & (vv_af > 0) & (vv_be > 0)
                    
                    for label, mask_arr in [('Road', road_mask), ('Paddy', paddy_mask), ('River', river_mask)]:
                        m = mask_arr & valid
                        if np.any(m):
                            vv_d = vv_diff[m]; vh_d = vh_diff[m]
                            if len(vv_d) > 300: # Slightly smaller sample to speed up KDE
                                idx = np.random.choice(len(vv_d), 300, replace=False)
                                vv_d = vv_d[idx]; vh_d = vh_d[idx]
                            all_data.append(pd.DataFrame({'vv_diff': vv_d, 'vh_diff': vh_d, 'delay': delay_int, 'class': label}))
            except: continue
        if (i+1) % 100 == 0: print(f"Processed {i+1}/{total_grids} grids...")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def process_results(df):
    if df.empty: return
    
    # 1. Statistics
    stats = df.groupby(['delay', 'class']).agg({
        'vv_diff': ['mean', 'median', 'std', 'count'],
        'vh_diff': ['mean', 'median', 'std']
    }).reset_index()
    stats.columns = ['Delay', 'Class', 'VV_Mean', 'VV_Median', 'VV_Std', 'Count', 'VH_Mean', 'VH_Median', 'VH_Std']
    stats.to_csv(OUTPUT_DIR / "diff_statistics_summary.csv", index=False)
    print("Saved statistics CSV.")

    # 2. Plots (2x3 Panel for VV and VH)
    colors = {'Road': 'orange', 'Paddy': 'green', 'River': 'blue'}
    classes = ['Road', 'Paddy', 'River']
    
    for pol in ['vv_diff', 'vh_diff']:
        fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharex=True)
        fig.suptitle(f"後方散乱強度差分 ({pol.upper()}) の分布推移 (全期間)", fontsize=20)
        axes = axes.flatten()
        
        for d in range(6):
            ax = axes[d]
            subset = df[df['delay'] == d]
            if subset.empty: continue
            
            for cls in classes:
                cls_df = subset[subset['class'] == cls]
                if not cls_df.empty:
                    sns.kdeplot(data=cls_df, x=pol, label=cls, color=colors[cls], ax=ax, fill=True, alpha=0.2)
            
            ax.set_title(f"Delay {d}h")
            ax.set_xlim(-0.15, 0.15) if 'vv' in pol else ax.set_xlim(-0.05, 0.05)
            ax.axvline(0, color='red', linestyle='--', alpha=0.5)
            if d == 0: ax.legend()
        
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(OUTPUT_DIR / f"diff_dist_{pol}_panel.png")
        plt.close()

    # Individual Plots (One per Class x Pol) as per user suggestion "line up horizontally or one by one"
    # Actually, the panel is better for "lining up". I'll also save individual delay plots for clarity.
    for d in range(6):
        fig, ax = plt.subplots(1, 2, figsize=(16, 6))
        subset = df[df['delay'] == d]
        if subset.empty: continue
        
        for idx, pol in enumerate(['vv_diff', 'vh_diff']):
            for cls in classes:
                cls_df = subset[subset['class'] == cls]
                if not cls_df.empty:
                    sns.kdeplot(data=cls_df, x=pol, label=cls, color=colors[cls], ax=ax[idx], fill=True, alpha=0.2)
            ax[idx].set_title(f"Delay {d}h - {pol.upper()}")
            ax[idx].axvline(0, color='red', linestyle='--', alpha=0.5)
            ax[idx].set_xlim(-0.15, 0.15) if 'vv' in pol else ax[idx].set_xlim(-0.05, 0.05)
            ax[idx].legend()

        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / f"diff_dist_delay_{d}h_combined.png")
        plt.close()

def main():
    df = load_data()
    if not df.empty:
        process_results(df)
        print(f"Results in {OUTPUT_DIR}")

if __name__ == "__main__": main()
