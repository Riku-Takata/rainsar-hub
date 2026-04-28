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
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
S1_PAIRS_CSV = BASE_DIR / "data" / "analysis" / "s1_pairs.csv"
PIXEL_COUNTS_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "stability_analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def to_linear(arr):
    # If values are significantly negative, assume dB and convert to Linear
    mask = (arr != 0)
    if np.any(mask):
        mean_val = np.mean(arr[mask])
        if mean_val < -1:
            return np.power(10, arr / 10.0)
    return arr

def get_unique_october_befores():
    """Identify pairs of different 'Before' scenes within the same October for each grid."""
    df_pairs = pd.read_csv(S1_PAIRS_CSV)
    df_pixels = pd.read_csv(PIXEL_COUNTS_CSV)
    
    # Merge to get event_dir info
    # Both have grid_id. 
    # s1_pairs has event_start_ts_utc.
    # df_pixels has month, delay_int, event_dir...
    
    # Simplify s1_pairs to join
    df_pairs['date_str'] = pd.to_datetime(df_pairs['event_start_ts_utc']).dt.strftime('%Y%m%d')
    df_pairs['month'] = pd.to_datetime(df_pairs['event_start_ts_utc']).dt.month
    
    # Filter for October
    oct_pairs = df_pairs[df_pairs['month'] == 10]
    
    # Group by grid and find unique before_scene_id
    analysis_links = []
    
    for grid_id, group in oct_pairs.groupby('grid_id'):
        # Sort by date
        group = group.sort_values('event_start_ts_utc')
        
        # Unique before scenes in chronological order
        unique_befores = group.drop_duplicates(subset=['before_scene_id'])
        
        if len(unique_befores) < 2:
            continue
            
        for i in range(len(unique_befores) - 1):
            s1 = unique_befores.iloc[i]
            s2 = unique_befores.iloc[i+1]
            
            # Find an event_dir for each scene from df_pixels
            # Note: Multiple event_dirs might have the same before_scene_id. 
            # We just need any one folder that contains this before_scene_id file.
            
            def find_dir(grid, datestr):
                # Match grid and date in df_pixels
                match = df_pixels[(df_pixels['grid_id'] == grid) & (df_pixels['event_dir'].str.contains(datestr))]
                if not match.empty:
                    return match.iloc[0]['event_dir']
                return None

            dir1 = find_dir(grid_id, s1['date_str'])
            dir2 = find_dir(grid_id, s2['date_str'])
            
            if dir1 and dir2:
                analysis_links.append({
                    'grid_id': grid_id,
                    'dir_old': dir1,
                    'dir_new': dir2,
                    'scene_old': s1['before_scene_id'],
                    'scene_new': s2['before_scene_id']
                })
                
    return analysis_links

def analyze_stability(links):
    all_results = []
    print(f"Starting stability analysis for {len(links)} scene pairs...")
    
    for i, link in enumerate(links):
        grid_id = link['grid_id']
        p_old = SAMPLES_DIR / grid_id / link['dir_old'] / "before_vv.tif"
        p_new = SAMPLES_DIR / grid_id / link['dir_new'] / "before_vv.tif"
        
        ph_old = SAMPLES_DIR / grid_id / link['dir_old'] / "before_vh.tif"
        ph_new = SAMPLES_DIR / grid_id / link['dir_new'] / "before_vh.tif"

        if not (p_old.exists() and p_new.exists() and ph_old.exists() and ph_new.exists()):
            continue

        # Load Masks
        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists(): road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
        paddy_mask_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
        
        try:
            with rasterio.open(p_old) as src:
                vv_old = to_linear(src.read(1))
                vv_new = to_linear(rasterio.open(p_new).read(1))
                vh_old = to_linear(rasterio.open(ph_old).read(1))
                vh_new = to_linear(rasterio.open(ph_new).read(1))
                
                # Resizing
                h, w = vv_old.shape
                vv_new = vv_new[:h, :w]
                vh_old = vh_old[:h, :w]
                vh_new = vh_new[:h, :w]
                
                # Difference (Linear Diff)
                vv_diff = vv_new - vv_old
                vh_diff = vh_new - vh_old
                
                # Valid mask (Standard Linear range)
                valid = (vv_old > 0) & (vv_new > 0) & (vv_old < 2.0) & (vv_new < 2.0)
                
                # Road Mask
                if road_mask_path.exists():
                    gdf_road = gpd.read_file(road_mask_path)
                    road_m = geometry_mask(gdf_road.to_crs(src.crs).geometry, out_shape=(h, w), transform=src.transform, invert=True)
                    m = road_m & valid
                    if np.any(m):
                        all_results.append(pd.DataFrame({
                            'vv_diff': vv_diff[m], 'vh_diff': vh_diff[m], 'class': 'Road'
                        }).sample(min(np.sum(m), 100))) # Smaller sample per event for speed/balance
                
                # Paddy Mask
                if paddy_mask_path.exists():
                    gdf_paddy = gpd.read_file(paddy_mask_path)
                    paddy_m = geometry_mask(gdf_paddy.to_crs(src.crs).geometry, out_shape=(h, w), transform=src.transform, invert=True)
                    m = paddy_m & valid
                    if np.any(m):
                        all_results.append(pd.DataFrame({
                            'vv_diff': vv_diff[m], 'vh_diff': vh_diff[m], 'class': 'Paddy'
                        }).sample(min(np.sum(m), 100)))
                        
        except Exception as e:
            # print(f"Error processing {grid_id}: {e}")
            continue
            
        if (i+1) % 50 == 0:
            print(f"Processed {i+1}/{len(links)} pairs...")

    if not all_results:
        return pd.DataFrame()
    return pd.concat(all_results, ignore_index=True)

def plot_stability(df):
    if df.empty:
        print("No data to plot.")
        return

    # Statistics
    stats = df.groupby('class').agg({
        'vv_diff': ['mean', 'std', 'count'],
        'vh_diff': ['mean', 'std']
    })
    stats.to_csv(OUTPUT_DIR / "before_stability_stats.csv")
    print("Saved stats CSV.")

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
    plt.suptitle("10月における降雨前データ間の時系列変化 (Stability Analysis)", fontsize=18, y=1.02)
    plt.savefig(OUTPUT_DIR / "before_stability_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Generated plot: {OUTPUT_DIR / 'before_stability_analysis.png'}")

if __name__ == "__main__":
    links = get_unique_october_befores()
    if links:
        # Limit to reasonable number for testing/speed if needed, but Oct data is manageable
        results_df = analyze_stability(links)
        plot_stability(results_df)
    else:
        print("No suitable Before-Before pairs found in October.")
