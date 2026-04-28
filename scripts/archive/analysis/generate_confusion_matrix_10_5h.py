import pandas as pd
import numpy as np
from pathlib import Path
import rasterio
from rasterio import features
import geopandas as gpd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, accuracy_score
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

warnings.filterwarnings('ignore')

# --- CONFIG ---
DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
MASK_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks")
PIXEL_COUNTS_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\monthly_delay_pixel_counts_detailed.csv")
S1_PAIRS_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\s1_pairs.csv")
GSMAP_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\gsmap_events.csv")
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")

# Font setup
plt.rcParams['font.family'] = ['MS Gothic', 'Meiryo', 'sans-serif']

LINEAR_THRESHOLD = 2.0
TARGET_R = 5000
TARGET_P = 5000
MAX_PER_EVENT = 500

def to_linear(arr):
    # Check if dB-like (mean < -1)
    mask = (arr != 0)
    if np.any(mask):
        mean_val = np.mean(arr[mask])
        if mean_val < -1:
            return np.power(10, arr / 10.0)
    return arr

def load_data_and_sample(row, n_road_target, n_paddy_target, rain_info):
    grid_id = row['grid_id']
    evt_dir_name = row['event_dir']
    
    evt_dir = DATA_DIR / grid_id / evt_dir_name
    p_vv_after = evt_dir / "after_vv.tif"
    p_vv_before = evt_dir / "before_vv.tif"
    p_vh_after = evt_dir / "after_vh.tif"
    p_vh_before = evt_dir / "before_vh.tif"
    
    if not p_vv_after.exists(): return None, None
    
    mask_road_json = MASK_DIR / grid_id / f"{grid_id}_motorway.geojson"
    if not mask_road_json.exists(): mask_road_json = MASK_DIR / grid_id / f"{grid_id}_road.geojson"
    mask_paddy_json = MASK_DIR / grid_id / f"{grid_id}_paddy.geojson"
    
    try:
        with rasterio.open(p_vv_after) as src:
            vv_after = to_linear(src.read(1)); vv_before = to_linear(rasterio.open(p_vv_before).read(1))
            vh_after = to_linear(rasterio.open(p_vh_after).read(1)); vh_before = to_linear(rasterio.open(p_vh_before).read(1))
            h, w = vv_after.shape
            transform = src.transform
            
            vv_diff = vv_after - vv_before
            vh_diff = vh_after - vh_before
            
            valid_mask = (vv_after > 0) & (vv_before > 0) & (vv_after < LINEAR_THRESHOLD)
            
            # Features
            feat_stack = np.stack([
                vv_after, vh_after, vv_diff, vh_diff,
                np.full((h, w), rain_info['rain_mean']),
                np.full((h, w), rain_info['rain_duration'])
            ], axis=-1)
            
            X_ev, y_ev = [], []
            
            # Masks
            for label, m_path in [(0, mask_road_json), (1, mask_paddy_json)]:
                if not m_path.exists(): continue
                gdf = gpd.read_file(m_path)
                m_arr = features.rasterize(gdf.geometry, out_shape=(h, w), transform=transform, fill=0, default_value=1)
                m_final = (m_arr > 0) & valid_mask
                
                if np.any(m_final):
                    pix = feat_stack[m_final]
                    n_avail = len(pix)
                    n_sel = min(n_avail, n_road_target if label == 0 else n_paddy_target)
                    idx = np.random.choice(n_avail, n_sel, replace=False)
                    X_ev.append(pix[idx])
                    y_ev.append(np.full(n_sel, label))
            
            if X_ev:
                return np.vstack(X_ev), np.concatenate(y_ev)
    except: pass
    return None, None

def main():
    print("Generating Confusion Matrix for October 5h Delay...")
    df_counts = pd.read_csv(PIXEL_COUNTS_CSV)
    subset = df_counts[(df_counts['month'] == 10) & (df_counts['delay_int'] == 5)]
    
    # Load Rainfall
    s1_df = pd.read_csv(S1_PAIRS_CSV)
    gsmap_df = pd.read_csv(GSMAP_CSV)
    s1_df['event_start_ts_utc'] = pd.to_datetime(s1_df['event_start_ts_utc'])
    gsmap_df['start_ts_utc'] = pd.to_datetime(gsmap_df['start_ts_utc'])
    merged = pd.merge(s1_df, gsmap_df, left_on=['grid_id', 'event_start_ts_utc'], right_on=['grid_id', 'start_ts_utc'])
    
    rain_map = {}
    for _, r in merged.iterrows():
        dur = max(1.0, (pd.to_datetime(r['end_ts_utc']) - pd.to_datetime(r['start_ts_utc'])).total_seconds() / 3600.0)
        rain_map[(r['grid_id'], r['delay_h'])] = {'rain_mean': r['sum_gauge_mm_h'] / dur, 'rain_duration': dur}

    # Fair limit logic
    def find_limit(counts, target):
        low, high = 0, MAX_PER_EVENT
        L = 0
        for _ in range(20):
            mid = (low + high) / 2
            if sum(min(c, mid) for c in counts) < target: L = mid; low = mid
            else: L = mid; high = mid
        return int(np.ceil(L))

    r_limit = find_limit(subset['road_pixels'], TARGET_R)
    p_limit = find_limit(subset['paddy_pixels'], TARGET_P)
    
    X_list, y_list = [], []
    for _, row in subset.iterrows():
        key = (row['grid_id'], row['delay_float'])
        r_info = rain_map.get(key, {'rain_mean': 0.0, 'rain_duration': 0.0})
        X_ev, y_ev = load_data_and_sample(row, r_limit, p_limit, r_info)
        if X_ev is not None:
            X_list.append(X_ev); y_list.append(y_ev)
            
    if not X_list: return
    X_all = np.vstack(X_list); y_all = np.concatenate(y_list)
    
    # Balance
    idx_r = np.where(y_all == 0)[0]; idx_p = np.where(y_all == 1)[0]
    np.random.shuffle(idx_r); np.random.shuffle(idx_p)
    limit = min(TARGET_R, len(idx_r), len(idx_p))
    
    X_final = np.vstack([X_all[idx_r[:limit]], X_all[idx_p[:limit]]])
    y_final = np.concatenate([np.zeros(limit), np.ones(limit)])
    
    # Train/Test Split (8:2)
    X_tr, X_te, y_tr, y_te = train_test_split(X_final, y_final, test_size=0.2, random_state=42, stratify=y_final)
    
    clf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    clf.fit(X_tr, y_tr)
    y_pred = clf.predict(X_te)
    acc = accuracy_score(y_te, y_pred)
    cm = confusion_matrix(y_te, y_pred)
    
    # Plot
    plt.figure(figsize=(8, 6))
    cm_df = pd.DataFrame(cm, index=['Road', 'Paddy'], columns=['Pred Road', 'Pred Paddy'])
    sns.heatmap(cm_df, annot=True, fmt='d', cmap='Blues', annot_kws={"size": 16})
    plt.title(f"Confusion Matrix: October 5h Delay (Acc: {acc:.3f})", fontsize=15)
    plt.xlabel("Predicted Class", fontsize=12)
    plt.ylabel("Actual Class", fontsize=12)
    
    save_path = ARTIFACT_DIR / "conf_matrix_10_5h.png"
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"Saved Matrix to {save_path}")

if __name__ == "__main__":
    main()
