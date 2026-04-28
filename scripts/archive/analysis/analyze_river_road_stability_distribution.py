import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import rasterio
from rasterio.features import geometry_mask, rasterize
from shapely.geometry import box
import geopandas as gpd
import warnings
import os

warnings.filterwarnings('ignore')

# --- CONFIG ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
S1_PAIRS_CSV = BASE_DIR / "data" / "analysis" / "s1_pairs.csv"
PIXEL_COUNTS_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "stability_analysis"
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data/river_polygon_2320001.geojson",
    BASE_DIR / "mask-data/river_polygon_2320061.geojson"
]

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def to_linear(arr):
    # Mask out extreme nodata values before calculating mean
    valid_mask = (arr > -100) & (arr < 100)
    if not np.any(valid_mask): return arr
    mean_val = np.mean(arr[valid_mask])
    if mean_val < -1:
        return np.power(10, arr / 10.0)
    return arr

def get_unique_october_befores():
    """Identify pairs of different 'Before' scenes within the same October for each grid."""
    df_pairs = pd.read_csv(S1_PAIRS_CSV)
    df_pixels = pd.read_csv(PIXEL_COUNTS_CSV)
    
    df_pairs['date_str'] = pd.to_datetime(df_pairs['event_start_ts_utc']).dt.strftime('%Y%m%d')
    df_pairs['month'] = pd.to_datetime(df_pairs['event_start_ts_utc']).dt.month
    oct_pairs = df_pairs[df_pairs['month'] == 10]
    
    analysis_links = []
    for grid_id, group in oct_pairs.groupby('grid_id'):
        group = group.sort_values('event_start_ts_utc')
        unique_befores = group.drop_duplicates(subset=['before_scene_id'])
        if len(unique_befores) < 2: continue
            
        for i in range(len(unique_befores) - 1):
            s1 = unique_befores.iloc[i]
            s2 = unique_befores.iloc[i+1]
            
            def find_dir(grid, datestr):
                match = df_pixels[(df_pixels['grid_id'] == grid) & (df_pixels['event_dir'].str.contains(datestr))]
                if not match.empty: return match.iloc[0]['event_dir']
                return None

            dir1 = find_dir(grid_id, s1['date_str'])
            dir2 = find_dir(grid_id, s2['date_str'])
            
            if dir1 and dir2:
                analysis_links.append({
                    'grid_id': grid_id, 'dir_old': dir1, 'dir_new': dir2
                })
    return analysis_links

def load_mask(mask_path, shape, transform):
    if not mask_path.exists(): return None
    if mask_path.suffix == '.tif':
        with rasterio.open(mask_path) as src:
            data = src.read(1); return data[:shape[0], :shape[1]] > 0 if data.shape != shape else data > 0
    else:
        gdf = gpd.read_file(mask_path)
        return geometry_mask(gdf.geometry, shape, transform, invert=True) if not gdf.empty else None

def analyze_stability(links):
    all_results = []
    print(f"Starting stability analysis for {len(links)} scene pairs (Road vs River)...")
    
    river_gdfs = [gpd.read_file(p) for p in RIVER_GEOJSON_PATHS if p.exists()]
    
    for i, link in enumerate(links):
        grid_id = link['grid_id']
        p_old = SAMPLES_DIR / grid_id / link['dir_old'] / "before_vv.tif"
        p_new = SAMPLES_DIR / grid_id / link['dir_new'] / "before_vv.tif"
        ph_old = SAMPLES_DIR / grid_id / link['dir_old'] / "before_vh.tif"
        ph_new = SAMPLES_DIR / grid_id / link['dir_new'] / "before_vh.tif"

        if not (p_old.exists() and p_new.exists() and ph_old.exists() and ph_new.exists()): continue

        try:
            with rasterio.open(p_old) as src:
                vv_old = to_linear(src.read(1))
                vv_new = to_linear(rasterio.open(p_new).read(1))
                vh_old = to_linear(rasterio.open(ph_old).read(1))
                vh_new = to_linear(rasterio.open(ph_new).read(1))
                
                h, w = vv_old.shape
                # Resize check
                min_h = min(h, vv_new.shape[0]); min_w = min(w, vv_new.shape[1])
                vv_old = vv_old[:min_h, :min_w]; vv_new = vv_new[:min_h, :min_w]
                vh_old = vh_old[:min_h, :min_w]; vh_new = vh_new[:min_h, :min_w]
                
                trans = src.transform; bounds = src.bounds
                
                vv_diff = vv_new - vv_old
                vh_diff = vh_new - vh_old
                
                # Valid mask
                valid = (~np.isnan(vv_old)) & (~np.isnan(vv_new)) & (vv_old > 1e-9) & (vv_new > 1e-9) & (vv_old < 2.0) & (vv_new < 2.0)
                
                # --- Road Mask ---
                local_mask_dir = MASKS_DIR / grid_id
                road_m = load_mask(local_mask_dir / f"{grid_id}_road_mask.tif", (min_h, min_w), trans)
                if road_m is None: road_m = load_mask(local_mask_dir / f"{grid_id}_motorway.geojson", (min_h, min_w), trans)
                if road_m is None: road_m = load_mask(local_mask_dir / f"{grid_id}_road.geojson", (min_h, min_w), trans)
                
                if road_m is not None:
                    m = road_m & valid
                    if np.any(m):
                        all_results.append(pd.DataFrame({
                            'vv_diff': vv_diff[m], 'vh_diff': vh_diff[m], 'class': '道路'
                        }).sample(min(np.sum(m), 100)))

                # --- River Mask ---
                s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                river_shapes = []
                for river_gdf in river_gdfs:
                    idx = list(river_gdf.sindex.intersection(s1_box.bounds))
                    if idx:
                        matches = river_gdf.iloc[idx]
                        matches = matches[matches.intersects(s1_box)]
                        if not matches.empty: river_shapes.extend(matches.geometry)
                
                if river_shapes:
                    river_m = rasterize(river_shapes, out_shape=(min_h, min_w), transform=trans, fill=0, default_value=1, dtype=rasterio.uint8) > 0
                    m = river_m & valid
                    if np.any(m):
                        all_results.append(pd.DataFrame({
                            'vv_diff': vv_diff[m], 'vh_diff': vh_diff[m], 'class': '河川'
                        }).sample(min(np.sum(m), 100)))

        except Exception: continue
        if (i+1) % 50 == 0: print(f"Processed {i+1}/{len(links)} pairs...")

    if not all_results: return pd.DataFrame()
    return pd.concat(all_results, ignore_index=True)

def plot_stability(df):
    if df.empty:
        print("No data to plot.")
        return

    # Balance for plotting visibility
    g = df.groupby('class')
    min_count = g.size().min()
    if min_count > 0:
        df = g.apply(lambda x: x.sample(min_count)).reset_index(drop=True)
        print(f"Balanced plotting data to {min_count} samples per class.")

    plt.figure(figsize=(14, 12))
    
    # 1. VV Diff Boxplot
    plt.subplot(2, 2, 1)
    sns.boxplot(data=df, x='class', y='vv_diff', showfliers=False, palette='Set2')
    plt.axhline(0, color='black', linestyle='--', alpha=0.5)
    plt.title("VV偏波 差分分布 (無降雨時)", fontsize=14)
    plt.ylabel("強度差分 (Newer - Older)", fontsize=12)
    plt.grid(True, axis='y', alpha=0.3)

    # 2. VV Diff KDE
    plt.subplot(2, 2, 2)
    for label in df['class'].unique():
        sns.kdeplot(df[df['class'] == label]['vv_diff'], label=label, fill=True, alpha=0.3)
    plt.axvline(0, color='black', linestyle='--')
    plt.title("VV偏波 差分密度", fontsize=14)
    plt.xlabel("強度差分", fontsize=12)
    plt.xlim(-0.5, 0.5)
    plt.legend()

    # 3. VH Diff Boxplot
    plt.subplot(2, 2, 3)
    sns.boxplot(data=df, x='class', y='vh_diff', showfliers=False, palette='Set2')
    plt.axhline(0, color='black', linestyle='--', alpha=0.5)
    plt.title("VH偏波 差分分布 (無降雨時)", fontsize=14)
    plt.ylabel("強度差分 (Newer - Older)", fontsize=12)
    plt.grid(True, axis='y', alpha=0.3)

    # 4. VH Diff KDE
    plt.subplot(2, 2, 4)
    for label in df['class'].unique():
        sns.kdeplot(df[df['class'] == label]['vh_diff'], label=label, fill=True, alpha=0.3)
    plt.axvline(0, color='black', linestyle='--')
    plt.title("VH偏波 差分密度", fontsize=14)
    plt.xlabel("強度差分", fontsize=12)
    plt.xlim(-0.5, 0.5)
    plt.legend()

    plt.tight_layout()
    plt.suptitle("河川 vs 道路: 降雨前データ間の時系列変化 (Stability Analysis)", fontsize=18, y=1.02)
    plt.savefig(OUTPUT_DIR / "river_road_stability_distribution.png", dpi=300, bbox_inches='tight')
    plt.savefig(ARTIFACT_DIR / "river_road_stability_distribution.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated plot: {OUTPUT_DIR / 'river_road_stability_distribution.png'}")

if __name__ == "__main__":
    links = get_unique_october_befores()
    if links:
        results_df = analyze_stability(links)
        plot_stability(results_df)
    else:
        print("No suitable Before-Before pairs found.")
