import pandas as pd
import numpy as np
from pathlib import Path
import rasterio
from rasterio.features import geometry_mask, rasterize
from shapely.geometry import box
import geopandas as gpd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import accuracy_score, confusion_matrix
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import os

warnings.filterwarnings('ignore')

# --- CONFIG ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
PIXEL_COUNTS_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "stability_analysis"
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data/river_polygon_2320001.geojson",
    BASE_DIR / "mask-data/river_polygon_2320061.geojson"
]

# Font setup
plt.rcParams['font.family'] = ['MS Gothic', 'Meiryo', 'sans-serif']

def to_linear(arr):
    # Mask out extreme nodata values before calculating mean
    valid_mask = (arr > -100) & (arr < 100)
    if not np.any(valid_mask): return arr
    mean_val = np.mean(arr[valid_mask])
    if mean_val < -1:
        return np.power(10, arr / 10.0)
    return arr

def get_extensive_october_pairs():
    """Use all 713 October events to create Before-Before pairs."""
    df_pixels = pd.read_csv(PIXEL_COUNTS_CSV)
    oct_events = df_pixels[df_pixels['month'] == 10]
    
    def extract_date(s):
        import re
        m = re.search(r'(\d{8})', s)
        return m.group(1) if m else s

    oct_events['date'] = oct_events['event_dir'].apply(extract_date)
    analysis_links = []
    
    for grid_id, group in oct_events.groupby('grid_id'):
        unique_dirs = group.sort_values('date').drop_duplicates(subset=['event_dir'])
        if len(unique_dirs) < 2: continue
            
        for i in range(len(unique_dirs) - 1):
            dir1 = unique_dirs.iloc[i]['event_dir']
            dir2 = unique_dirs.iloc[i+1]['event_dir']
            analysis_links.append({'grid_id': grid_id, 'dir_1': dir1, 'dir_2': dir2})
                
    return analysis_links

def load_mask(mask_path, shape, transform):
    if not mask_path.exists(): return None
    if mask_path.suffix == '.tif':
        with rasterio.open(mask_path) as src:
            data = src.read(1); return data[:shape[0], :shape[1]] > 0 if data.shape != shape else data > 0
    else:
        gdf = gpd.read_file(mask_path)
        return geometry_mask(gdf.geometry, shape, transform, invert=True) if not gdf.empty else None

def collect_data(links, river_gdfs):
    X_road, X_river = [], []
    print(f"Collecting pixels from {len(links)} pairs...")
    
    for i, link in enumerate(links):
        grid_id = link['grid_id']
        d1, d2 = SAMPLES_DIR / grid_id / link['dir_1'], SAMPLES_DIR / grid_id / link['dir_2']
        p1_vv, p2_vv = d1 / "before_vv.tif", d2 / "before_vv.tif"
        p1_vh, p2_vh = d1 / "before_vh.tif", d2 / "before_vh.tif"

        if not all(p.exists() for p in [p1_vv, p2_vv, p1_vh, p2_vh]): continue

        try:
            with rasterio.open(p1_vv) as src:
                vv1 = src.read(1); vv2 = rasterio.open(p2_vv).read(1)
                vh1 = rasterio.open(p1_vh).read(1); vh2 = rasterio.open(p2_vh).read(1)
                vv1 = to_linear(vv1); vv2 = to_linear(vv2)
                vh1 = to_linear(vh1); vh2 = to_linear(vh2)
                
                h, w = vv1.shape
                # Crop to min shape
                min_h = min(h, vv2.shape[0]); min_w = min(w, vv2.shape[1])
                vv1 = vv1[:min_h, :min_w]; vv2 = vv2[:min_h, :min_w]
                vh1 = vh1[:min_h, :min_w]; vh2 = vh2[:min_h, :min_w]
                
                trans = src.transform; bounds = src.bounds
                
                stack = np.stack([vv2, vh2, vv2 - vv1, vh2 - vh1], axis=-1)
                valid = (~np.isnan(vv1)) & (~np.isnan(vv2)) & (vv1 > 1e-9) & (vv2 > 1e-9)
                
                # --- Road Mask ---
                local_mask_dir = MASKS_DIR / grid_id
                road_m = load_mask(local_mask_dir / f"{grid_id}_road_mask.tif", (min_h, min_w), trans)
                if road_m is None: road_m = load_mask(local_mask_dir / f"{grid_id}_motorway.geojson", (min_h, min_w), trans)
                if road_m is None: road_m = load_mask(local_mask_dir / f"{grid_id}_road.geojson", (min_h, min_w), trans)
                
                if road_m is not None:
                     m = road_m & valid
                     if np.any(m): X_road.append(stack[m])

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
                    if np.any(m): X_river.append(stack[m])
                        
        except Exception: continue
        if (i+1) % 100 == 0: print(f"  Processed {i+1} pairs...")

    if not X_road and not X_river: return None, None, 0, 0
    
    # Fair Sampling
    TARGET = 5000
    MAX_PER = 500
    
    def fair_sample(pix_list):
        if not pix_list: return np.empty((0, 4))
        counts = [len(p) for p in pix_list]
        low, high = 0, MAX_PER
        limit = 0
        for _ in range(20):
            mid = (low + high) / 2
            if sum(min(c, mid) for c in counts) < TARGET: limit = mid; low = mid
            else: limit = mid; high = mid
        limit = int(np.ceil(limit))
        
        sampled = []
        for p in pix_list:
            n = len(p)
            sel = min(n, limit)
            if sel > 0: sampled.append(p[np.random.choice(n, sel, replace=False)])
        return np.vstack(sampled) if sampled else np.empty((0, 4))

    Xr_final = fair_sample(X_road)
    Xrv_final = fair_sample(X_river)
    
    if len(Xr_final) > TARGET: Xr_final = Xr_final[np.random.choice(len(Xr_final), TARGET, replace=False)]
    if len(Xrv_final) > TARGET: Xrv_final = Xrv_final[np.random.choice(len(Xrv_final), TARGET, replace=False)]
    
    n_road = len(Xr_final)
    n_river = len(Xrv_final)
    print(f"Final Samples (after initial sampling): Road={n_road}, River={n_river}")

    # Balance them to the smaller of the two (Align scales)
    n_min = min(n_road, n_river)
    if n_min < 50: # Changed from '== 0' to '< 50' for more robust check
        print(f"Not enough balanced samples (min_samples={n_min}). Returning None.")
        return None, None, n_road, n_river
    
    print(f"Balancing samples to scale: {n_min} per class (Total {n_min*2})")
    
    X = np.vstack([Xr_final[:n_min], Xrv_final[:n_min]])
    y = np.concatenate([np.zeros(n_min), np.ones(n_min)])
    
    return X, y, n_road, n_river

def run():
    print("=== River vs Road Before-Before Stability Analysis ===")
    links = get_extensive_october_pairs()
    river_gdfs = [gpd.read_file(p) for p in RIVER_GEOJSON_PATHS if p.exists()]
    
    X, y, nr, nrv = collect_data(links, river_gdfs)
    if X is None:
        print("Not enough data.")
        return

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    total_cm = np.zeros((2, 2))
    accs = []
    
    for tr, te in skf.split(X, y):
        clf = RandomForestClassifier(n_estimators=100, n_jobs=-1, random_state=42)
        clf.fit(X[tr], y[tr])
        pred = clf.predict(X[te])
        accs.append(accuracy_score(y[te], pred))
        total_cm += confusion_matrix(y[te], pred, labels=[0, 1])
        
    avg_acc = np.mean(accs)
    print(f"Average Stability Accuracy: {avg_acc:.3f}")

    # Plot
    plt.figure(figsize=(8, 6))
    cm_df = pd.DataFrame(total_cm, index=['道路 (降雨前)', '河川 (降雨前)'], columns=['予測：道路', '予測：河川'])
    sns.heatmap(cm_df, annot=True, fmt='.0f', cmap='Blues', annot_kws={"size": 14})
    plt.title(f"河川 vs 道路: 降雨前データ間の安定性解析\n精度: {avg_acc:.3f}", fontsize=15)
    plt.xlabel('予測クラス', fontsize=12); plt.ylabel('正解クラス', fontsize=12)
    
    out_path = OUTPUT_DIR / "stability_river_road_cm.png"
    art_path = ARTIFACT_DIR / "stability_river_road_cm.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.savefig(art_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved CM to {art_path}")

if __name__ == "__main__":
    run()
