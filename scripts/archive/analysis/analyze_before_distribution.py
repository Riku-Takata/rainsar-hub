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

def load_before_data():
    """Extract pixels for all classes *before* rainfall."""
    # Load River Polygons
    river_gdf_list = []
    for p in RIVER_GEOJSON_PATHS:
        if p.exists():
            river_gdf_list.append(gpd.read_file(p))
    gdf_river = pd.concat(river_gdf_list, ignore_index=True)

    all_data = []
    print(f"Scanning samples for baseline (Before) intensity...")
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
            
            # To avoid redundant extraction of the same "Before" image, 
            # we can just take the first delay we find for each event-day.
            if "delay_0h" not in event_dir.name and "delay_1h" not in event_dir.name:
                # This is a heuristic to skip most delays but keep at least one per event.
                # Actually, simpler: only process if it's the first dir in the iterator or matching 0h.
                pass

            paths = {
                'vv_be': event_dir / "before_vv.tif",
                'vh_be': event_dir / "before_vh.tif"
            }
            if not all(p.exists() for p in paths.values()): continue

            try:
                with rasterio.open(paths['vv_be']) as src:
                    vv_be = src.read(1)
                    vh_be = rasterio.open(paths['vh_be']).read(1)
                    
                    min_h = min(vv_be.shape[0], vh_be.shape[0])
                    min_w = min(vv_be.shape[1], vh_be.shape[1])
                    vv_be = vv_be[:min_h, :min_w]
                    vh_be = vh_be[:min_h, :min_w]

                    # Masks
                    river_m = geometry_mask(gdf_river.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=True)
                    road_m = geometry_mask(gdf_road.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=False) if gdf_road is not None else np.zeros((min_h, min_w), dtype=bool)
                    paddy_m = geometry_mask(gdf_paddy.to_crs(src.crs).geometry, out_shape=(min_h, min_w), transform=src.transform, invert=True, all_touched=False) if gdf_paddy is not None else np.zeros((min_h, min_w), dtype=bool)

                    # Baseline (Before) value filter
                    valid = (vv_be < 2.0) & (vv_be > 0) & (vh_be > 0)
                    
                    for label, mask_arr in [('道路', road_m), ('水田', paddy_m), ('河川', river_m)]:
                        m = mask_arr & valid
                        if np.any(m):
                            vv_vals = vv_be[m]
                            vh_vals = vh_be[m]
                            # Sample if too many
                            if len(vv_vals) > 300:
                                idx = np.random.choice(len(vv_vals), 300, replace=False)
                                vv_vals = vv_vals[idx]
                                vh_vals = vh_vals[idx]
                            
                            all_data.append(pd.DataFrame({
                                'vv_before': vv_vals,
                                'vh_before': vh_vals,
                                'class': label
                            }))
                
                # After one successful extraction for this event, we can break from this grid 
                # OR just continue. Let's break to keep it fast and unique per event.
                # Actually, different events in same grid have different Before images.
                # But same event with multiple delays has same Before image.
                # So we should break the grid-event loop after one delay.
            except: continue
        
        if (i+1) % 10 == 0:
            print(f"Processed {i+1}/{len(grids)} grids...")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def plot_before_distribution(df):
    if df.empty: return

    # Save stats
    stats = df.groupby('class').agg({
        'vv_before': ['mean', 'median', 'std', 'count'],
        'vh_before': ['mean', 'median', 'std']
    })
    stats.to_csv(OUTPUT_DIR / "before_intensity_stats.csv")
    print("Saved stats CSV.")

    # Boxplot
    # Melt for polarization comparison
    melted = df.melt(id_vars=['class'], value_vars=['vv_before', 'vh_before'], var_name='Pol', value_name='Intensity')
    
    plt.figure(figsize=(12, 8))
    sns.boxplot(data=melted, x='class', y='Intensity', hue='Pol', palette='Set2', showfliers=False)
    
    plt.title("対象地点ごとの後方散乱強度比較", fontsize=18)
    plt.xlabel("対象地点", fontsize=14)
    plt.ylabel("後方散乱強度 (線形値)", fontsize=14)
    plt.grid(True, axis='y', alpha=0.3)
    
    save_path = OUTPUT_DIR / "before_intensity_boxplot.png"
    plt.savefig(save_path, dpi=300)
    plt.close()
    print(f"Saved: {save_path}")

def main():
    df = load_before_data()
    if not df.empty:
        plot_before_distribution(df)
        print(f"Analysis completed. Results in {OUTPUT_DIR}")
    else:
        print("No data extracted.")

if __name__ == "__main__":
    main()
