import pandas as pd
import numpy as np
from pathlib import Path
import rasterio
from rasterio.features import geometry_mask
import geopandas as gpd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
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
S1_PAIRS_CSV = BASE_DIR / "data" / "analysis" / "s1_pairs.csv"
PIXEL_COUNTS_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "stability_analysis"
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Font setup
plt.rcParams['font.family'] = ['MS Gothic', 'Meiryo', 'sans-serif']

def to_linear(arr):
    # If values are significantly negative, assume dB and convert to Linear
    # Standard threshold: if mean < -1, it's likely dB
    mask = (arr != 0)
    if np.any(mask):
        mean_val = np.mean(arr[mask])
        if mean_val < -1:
            return np.power(10, arr / 10.0)
    return arr

def get_extensive_october_pairs():
    """Use all 713 October events to create Before-Before pairs."""
    df_pixels = pd.read_csv(PIXEL_COUNTS_CSV)
    oct_events = df_pixels[df_pixels['month'] == 10]
    
    # Sort events within each grid chronologically (date is in event_dir name)
    # event_dir format: delay_X.Xh_YYYYMMDD
    def extract_date(s):
        import re
        m = re.search(r'(\d{8})', s)
        return m.group(1) if m else s

    oct_events['date'] = oct_events['event_dir'].apply(extract_date)
    
    analysis_links = []
    
    for grid_id, group in oct_events.groupby('grid_id'):
        # Sort by date and unique event directories
        unique_dirs = group.sort_values('date').drop_duplicates(subset=['event_dir'])
        
        if len(unique_dirs) < 2:
            continue
            
        for i in range(len(unique_dirs) - 1):
            dir1 = unique_dirs.iloc[i]['event_dir']
            dir2 = unique_dirs.iloc[i+1]['event_dir']
            
            analysis_links.append({
                'grid_id': grid_id,
                'dir_1': dir1,
                'dir_2': dir2
            })
                
    return analysis_links

def collect_data(links):
    X_road, X_paddy = [], []
    print(f"Collecting pixels from {len(links)} Before-Before pairs...")
    
    for i, link in enumerate(links):
        grid_id = link['grid_id']
        p1_vv = SAMPLES_DIR / grid_id / link['dir_1'] / "before_vv.tif"
        p2_vv = SAMPLES_DIR / grid_id / link['dir_2'] / "before_vv.tif"
        p1_vh = SAMPLES_DIR / grid_id / link['dir_1'] / "before_vh.tif"
        p2_vh = SAMPLES_DIR / grid_id / link['dir_2'] / "before_vh.tif"

        if not all(p.exists() for p in [p1_vv, p2_vv, p1_vh, p2_vh]): continue

        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists(): road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
        paddy_mask_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
        
        try:
            with rasterio.open(p1_vv) as src:
                vv1 = src.read(1); vv2 = rasterio.open(p2_vv).read(1)
                vh1 = rasterio.open(p1_vh).read(1); vh2 = rasterio.open(p2_vh).read(1)
                vv1 = to_linear(vv1); vv2 = to_linear(vv2)
                vh1 = to_linear(vh1); vh2 = to_linear(vh2)
                
                h, w = vv1.shape
                vv2 = vv2[:h, :w]; vh1 = vh1[:h, :w]; vh2 = vh2[:h, :w]
                
                stack = np.stack([vv2, vh2, vv2 - vv1, vh2 - vh1], axis=-1)
                valid = (vv1 > 0) & (vv2 > 0) & (vv1 < 2.0) & (vv2 < 2.0)
                
                for label, m_path in [(0, road_mask_path), (1, paddy_mask_path)]:
                    if m_path.exists():
                        gdf = gpd.read_file(m_path)
                        if gdf.empty: continue
                        mask = geometry_mask(gdf.to_crs(src.crs).geometry, (h, w), src.transform, invert=True)
                        m = mask & valid
                        if np.any(m):
                            pix = stack[m]
                            if label == 0: X_road.append(pix)
                            else: X_paddy.append(pix)
        except: continue
        if (i+1) % 100 == 0: print(f"Processed {i+1} pairs...")

    if not X_road and not X_paddy: return None, None, 0, 0
    
    X_road_all = np.vstack(X_road) if X_road else np.empty((0, 4))
    X_paddy_all = np.vstack(X_paddy) if X_paddy else np.empty((0, 4))
    
    n_road = len(X_road_all)
    n_paddy = len(X_paddy_all)
    
    print(f"\nTotal pixels found: Road={n_road}, Paddy={n_paddy}")
    
    # --- Robust Fair Sampling Logic ---
    TARGET_TOTAL = 5000
    MAX_PER_EVENT = 500
    
    def fair_sample(pix_list, target):
        if not pix_list: return np.empty((0, 4))
        avail_counts = [len(p) for p in pix_list]
        n_ev = len(pix_list)
        
        # Iteratively find a limit L per event that reaches total target
        low, high = 0, MAX_PER_EVENT
        limit = 0
        for _ in range(20):
            mid = (low + high) / 2
            if sum(min(c, mid) for c in avail_counts) < target:
                limit = mid
                low = mid
            else:
                limit = mid
                high = mid
        
        limit_int = int(np.ceil(limit))
        sampled = []
        for p in pix_list:
            n = len(p)
            sel = min(n, limit_int)
            if sel > 0:
                idx = np.random.choice(n, sel, replace=False)
                sampled.append(p[idx])
        
        if not sampled: return np.empty((0, 4))
        res = np.vstack(sampled)
        if len(res) > target:
            idx = np.random.permutation(len(res))
            res = res[idx[:target]]
        return res

    X_road_final = fair_sample(X_road, TARGET_TOTAL)
    X_paddy_final = fair_sample(X_paddy, TARGET_TOTAL)
    
    print(f"Final sampled dataset: Road={len(X_road_final)}, Paddy={len(X_paddy_final)}")
    
    # Balance them to the smaller of the two (should both be 5000 if data exists)
    n_min = min(len(X_road_final), len(X_paddy_final))
    if n_min == 0: return None, None, n_road, n_paddy
    
    X = np.vstack([X_road_final[:n_min], X_paddy_final[:n_min]])
    y = np.concatenate([np.zeros(n_min), np.ones(n_min)])
    
    return X, y, n_road, n_paddy

def run():
    links = get_extensive_october_pairs()
    print(f"Total pairs identified: {len(links)}")
    
    X, y, n_road, n_paddy = collect_data(links)
    if X is None or len(X) == 0:
        print("No data collected or one class is empty.")
        return
    
    print(f"Balanced dataset size (downsampled to smaller class): {len(X)}")
    
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    total_cm = np.zeros((2, 2))
    accuracies = []
    
    for train_idx, test_idx in skf.split(X, y):
        X_tr, X_te = X[train_idx], X[test_idx]
        y_tr, y_te = y[train_idx], y[test_idx]
        
        clf = RandomForestClassifier(n_estimators=100, max_depth=10, n_jobs=-1, random_state=42)
        clf.fit(X_tr, y_tr)
        y_pr = clf.predict(X_te)
        
        accuracies.append(accuracy_score(y_te, y_pr))
        total_cm += confusion_matrix(y_te, y_pr, labels=[0, 1])
        
    avg_acc = np.mean(accuracies)
    print(f"Average Accuracy: {avg_acc:.3f}")
    
    # Visualize Matrix
    cm_df = pd.DataFrame(total_cm, index=['実際の道路', '実際の水田'], columns=['予測：道路', '予測：水田'])
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm_df, annot=True, fmt='.0f', cmap='Blues', annot_kws={"size": 14})
    plt.title(f"降雨前のデータによる混同行列 精度: {avg_acc:.3f}", fontsize=15)
    plt.ylabel('正解クラス', fontsize=12)
    plt.xlabel('予測クラス', fontsize=12)
    
    # Save to project data directory
    project_plot_path = OUTPUT_DIR / "stability_rf_confusion_matrix.png"
    plt.savefig(project_plot_path, dpi=300, bbox_inches='tight')
    
    # Save to artifact directory
    artifact_plot_path = ARTIFACT_DIR / "stability_rf_confusion_matrix.png"
    plt.savefig(artifact_plot_path, dpi=300, bbox_inches='tight')
    
    plt.close()
    print(f"Saved confusion matrix to:\n - {project_plot_path}\n - {artifact_plot_path}")

if __name__ == "__main__":
    run()
