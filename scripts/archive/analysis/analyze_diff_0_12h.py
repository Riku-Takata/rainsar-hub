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
OUTPUT_DIR = DATA_DIR / "result" / "distribution_analysis_0_12h"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# River Paths
RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data" / "river_polygon_2320001.geojson",
    BASE_DIR / "mask-data" / "river_polygon_2320061.geojson"
]

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def load_data_0_12h():
    """Extract pixels for all classes across all months for delays 0-12h."""
    # Load River Polygons
    river_gdf_list = []
    for p in RIVER_GEOJSON_PATHS:
        if p.exists():
            river_gdf_list.append(gpd.read_file(p))
    gdf_river = pd.concat(river_gdf_list, ignore_index=True)

    all_data = []
    print(f"Scanning a subset of samples in {SAMPLES_DIR} for delays 0-12h...")
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    grids = grids[:50] # Subset for speed
    
    for i, grid_dir in enumerate(grids):
        grid_id = grid_dir.name
        
        # Load Masks
        # Road
        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists(): road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
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
                if delay_int > 12: continue
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
                    vv_af, vv_be = vv_af[:min_h, :min_w], vv_be[:min_h, :min_w]
                    vh_af, vh_be = vh_af[:min_h, :min_w], vh_be[:min_h, :min_w]
                    
                    vv_diff = vv_af - vv_be; vh_diff = vh_af - vh_be

                    # Masks
                    river_m = geometry_mask(gdf_river.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=True)
                    road_m = geometry_mask(gdf_road.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=False) if gdf_road is not None else np.zeros((min_h, min_w), dtype=bool)
                    paddy_m = geometry_mask(gdf_paddy.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=False) if gdf_paddy is not None else np.zeros((min_h, min_w), dtype=bool)

                    # Outlier filter
                    valid = (vv_af < 2.0) & (vv_be < 2.0) & (vv_af > 0) & (vv_be > 0)
                    
                    for label, mask_arr in [('Road', road_m), ('Paddy', paddy_m), ('River', river_m)]:
                        m = mask_arr & valid
                        if np.any(m):
                            vvd, vhd = vv_diff[m], vh_diff[m]
                            # Cap samples
                            if len(vvd) > 200:
                                idx = np.random.choice(len(vvd), 200, replace=False)
                                vvd, vhd = vvd[idx], vhd[idx]
                            all_data.append(pd.DataFrame({'vv_diff': vvd, 'vh_diff': vhd, 'delay': delay_int, 'class': label}))
            except: continue
        if (i+1) % 100 == 0:
            print(f"Processed {i+1}/{len(grids)} grids...")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def generate_visualizations(df):
    if df.empty: return
    
    # 1. Statistics
    stats = df.groupby(['delay', 'class']).agg({
        'vv_diff': ['mean', 'median', 'std', 'count'],
        'vh_diff': ['mean', 'median', 'std']
    }).reset_index()
    stats.columns = ['Delay', 'Class', 'VV_Mean', 'VV_Median', 'VV_Std', 'Count', 'VH_Mean', 'VH_Median', 'VH_Std']
    stats.to_csv(OUTPUT_DIR / "diff_stats_0_12h.csv", index=False)
    
    # 2. Boxplots (Consolidated per Class)
    classes = {'Road': '道路', 'Paddy': '水田', 'River': '河川周辺'}
    for cls_key, cls_name in classes.items():
        subset = df[df['class'] == cls_key]
        if subset.empty: continue
        
        melted = subset.melt(id_vars=['delay'], value_vars=['vv_diff', 'vh_diff'], var_name='Pol', value_name='Diff')
        
        plt.figure(figsize=(14, 7))
        sns.boxplot(data=melted, x='delay', y='Diff', hue='Pol', palette='Set2', showfliers=False)
        plt.title(f"{cls_name}における後方散乱強度差分 (0-12h) の推移", fontsize=16)
        plt.xlabel("Delay [h]", fontsize=12)
        plt.ylabel("Intensity Diff (Linear)", fontsize=12)
        plt.axhline(0, color='red', linestyle='--', alpha=0.5)
        plt.grid(True, axis='y', alpha=0.3)
        plt.savefig(OUTPUT_DIR / f"boxplot_0_12h_{cls_key}.png")
        plt.close()

    # 3. Histograms (Consolidated per Polarization)
    # Using facet grid for all delays in one image
    for pol in ['vv_diff', 'vh_diff']:
        g = sns.FacetGrid(df, col="delay", col_wrap=4, hue="class", palette={'Road': 'orange', 'Paddy': 'green', 'River': 'blue'}, 
                          height=3, aspect=1.2, sharey=False)
        g.map(sns.histplot, pol, bins=30, element="step", alpha=0.3)
        g.add_legend()
        g.set_axis_labels(f"{pol.upper()} Diff", "Count")
        
        # Limit x axis for focus
        for ax in g.axes.flat:
            ax.set_xlim(-0.15, 0.15) if 'vv' in pol else ax.set_xlim(-0.05, 0.05)
            ax.axvline(0, color='red', linestyle='--', alpha=0.5)
            
        plt.subplots_adjust(top=0.9)
        g.fig.suptitle(f"{pol.upper()} 強度差分分布の推移 (0-12h)", fontsize=18)
        plt.savefig(OUTPUT_DIR / f"histogram_0_12h_{pol}.png")
        plt.close()

def main():
    df = load_data_0_12h()
    if not df.empty:
        # Save raw pixels for 0-12h subset
        df.to_csv(OUTPUT_DIR / "extracted_diff_pixels_0_12h.csv", index=False)
        generate_visualizations(df)
        print(f"Analysis completed. Results in {OUTPUT_DIR}")
    else:
        print("No data extracted.")

if __name__ == "__main__": main()
