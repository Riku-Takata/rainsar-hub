import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask, rasterize
from shapely.geometry import box
import warnings
import os
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# --- Configuration ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
RIVER_GEOJSON_PATHS = [
    BASE_DIR / "mask-data/river_polygon_2320001.geojson",
    BASE_DIR / "mask-data/river_polygon_2320061.geojson"
]
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"
MASKS_DIR = BASE_DIR / "data/expanded/masks"
PIXEL_COUNTS_CSV = BASE_DIR / "data/analysis/aug_oct_pixel_counts_with_river_detailed.csv"
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")
OUTPUT_DIR = BASE_DIR / "data/result/River_vs_Road_Aug"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Database
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

# RF Config
TARGET_TOTAL = 150
MAX_PER_EVENT = 150 
LINEAR_THRESHOLD = 5.0 

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

def get_rainfall_map():
    engine = create_engine(DATABASE_URL)
    query = text("SELECT grid_id, DATE_FORMAT(end_ts_utc, '%Y-%m-%d') as date_str, sum_gauge_mm_h, TIMESTAMPDIFF(HOUR, start_ts_utc, end_ts_utc) as duration_h FROM gsmap_events")
    try:
        with engine.connect() as conn: return pd.read_sql(query, conn)
    except: return pd.DataFrame()

def find_fair_limit(counts, target, max_limit):
    if not counts: return 0
    low, high = 0, max_limit
    L = 0
    for _ in range(20):
        mid = (low + high) / 2
        if sum(min(c, mid) for c in counts) < target: L = mid; low = mid
        else: L = mid; high = mid
    return int(np.ceil(L))

def load_mask(mask_path, shape, transform):
    if not mask_path.exists(): return None
    if mask_path.suffix == '.tif':
        with rasterio.open(mask_path) as src:
            data = src.read(1); return data[:shape[0], :shape[1]] > 0 if data.shape != shape else data > 0
    else: # GeoJSON
        gdf = gpd.read_file(mask_path)
        if gdf.empty: return None
        return geometry_mask(gdf.geometry, shape, transform, invert=True)

def extract_features_for_event(event_dir, grid_id, rain_mean, rain_dur, RIVER_GDFS):
    paths = {'vv_af': event_dir / "after_vv.tif", 'vv_be': event_dir / "before_vv.tif", 'vh_af': event_dir / "after_vh.tif", 'vh_be': event_dir / "before_vh.tif"}
    if not all(p.exists() for p in paths.values()): return None, None, 0, 0

    try:
        with rasterio.open(paths['vv_af']) as src:
            vv_af = to_linear(src.read(1)); vv_be = to_linear(rasterio.open(paths['vv_be']).read(1))
            vh_af = to_linear(rasterio.open(paths['vh_af']).read(1)); vh_be = to_linear(rasterio.open(paths['vh_be']).read(1))
            
            # Auto-crop to intersection to filter shape mismatches
            h = min(vv_af.shape[0], vv_be.shape[0], vh_af.shape[0], vh_be.shape[0])
            w = min(vv_af.shape[1], vv_be.shape[1], vh_af.shape[1], vh_be.shape[1])
            
            vv_af = vv_af[:h, :w]; vv_be = vv_be[:h, :w]
            vh_af = vh_af[:h, :w]; vh_be = vh_be[:h, :w]
            
            trans = src.transform; bounds = src.bounds
            
            # Valid mask: Relaxed to allow almost all valid datatypes
            # Only filter out NaN and Nodatas (if any)
            # S1 usually is float32. 
            valid = (~np.isnan(vv_af)) & (~np.isnan(vv_be))
            
            stack = np.stack([vv_af, vh_af, vv_af - vv_be, vh_af - vh_be, np.full((h, w), rain_mean), np.full((h, w), rain_dur)], axis=-1)
            
            # --- RIVER EXTRACTION LOGIC FROM COUNT SCRIPT ---
            s1_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            river_shapes = []
            
            # Checking spatial intersection
            for river_gdf in RIVER_GDFS:
                # Check intersection with cached sindex
                possible_idx = list(river_gdf.sindex.intersection(s1_box.bounds))
                if possible_idx:
                    matches = river_gdf.iloc[possible_idx]
                    matches = matches[matches.intersects(s1_box)]
                    if not matches.empty:
                        river_shapes.extend(matches.geometry)
            
            if river_shapes:
                river_m = rasterize(river_shapes, out_shape=(h, w), transform=trans, fill=0, default_value=1, dtype=rasterio.uint8) > 0
            else:
                river_m = np.zeros((h, w), dtype=bool)

            # Road mask
            local_mask_dir = MASKS_DIR / grid_id
            road_m = load_mask(local_mask_dir / f"{grid_id}_road_mask.tif", (h, w), trans)
            if road_m is None: road_m = load_mask(local_mask_dir / f"{grid_id}_motorway.geojson", (h, w), trans)
            if road_m is None: road_m = load_mask(local_mask_dir / f"{grid_id}_road.geojson", (h, w), trans)

            rv_pix = stack[river_m & valid]
            rd_pix = stack[road_m & valid] if road_m is not None else np.empty((0, 6))
            return rv_pix, rd_pix, len(rv_pix), len(rd_pix)
    except Exception as e:
        # print(f"Error: {e}") 
        return None, None, 0, 0

def main():
    print("=== August River vs Road Analysis (Ensuring 100 Samples) ===")
    RIVER_GDFS = [gpd.read_file(p) for p in RIVER_GEOJSON_PATHS if p.exists()]
    df_rain = get_rainfall_map()
    df_counts = pd.read_csv(PIXEL_COUNTS_CSV)
    subset_all = df_counts[df_counts['month'] == 8].copy()
    
    # Filter delays up to 5h as requested
    delays = sorted([d for d in subset_all['delay_int'].unique() if d <= 5])
    all_metrics, cms_dict = [], {}

    # Optimized rain lookup
    df_rain['date_str'] = df_rain['date_str'].astype(str)
    # Remove duplicates if any
    df_rain = df_rain.drop_duplicates(subset=['grid_id', 'date_str'])
    rain_map = df_rain.set_index(['grid_id', 'date_str'])[['sum_gauge_mm_h', 'duration_h']].to_dict('index')

    for d in delays:
        subset = subset_all[subset_all['delay_int'] == d]
        print(f"\nDelay {d}h: processing {len(subset)} events...")
        ev_river, ev_road, c_river, c_road = [], [], [], []
        
        for _, row in subset.iterrows():
            grid_id = row['grid_id']
            # Event dir format: delay_XH_YYYYMMDD
            date_part = row['event_dir'].split('_')[2] 
            date_str_fmt = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]}"
            
            # Direct Dict Lookup
            r_info = rain_map.get((grid_id, date_str_fmt), {'sum_gauge_mm_h': 0.0, 'duration_h': 0.0})
            rain_mean = r_info['sum_gauge_mm_h'] / max(1.0, r_info['duration_h'])
            rain_dur = r_info['duration_h']
            
            rv, rd, nr, nrd = extract_features_for_event(SAMPLES_DIR / grid_id / row['event_dir'], grid_id, rain_mean, rain_dur, RIVER_GDFS)
            if rv is not None and (len(rv) > 0 or len(rd) > 0):
                ev_river.append(rv); ev_road.append(rd); c_river.append(nr); c_road.append(nrd)
        
        if not ev_river: continue
        limit_r = find_fair_limit(c_river, TARGET_TOTAL, MAX_PER_EVENT)
        limit_rd = find_fair_limit(c_road, TARGET_TOTAL, MAX_PER_EVENT)
        print(f"  Available (Total valid): River={sum(c_river)}, Road={sum(c_road)}")
        print(f"  Fair Limits: River={limit_r}, Road={limit_rd}")
        
        X_river = np.vstack([rv[np.random.choice(len(rv), min(len(rv), limit_r), replace=False)] for rv in ev_river if len(rv) > 0])
        X_road = np.vstack([rd[np.random.choice(len(rd), min(len(rd), limit_rd), replace=False)] for rd in ev_road if len(rd) > 0])
        
        n_min = min(len(X_river), len(X_road), TARGET_TOTAL)
        if n_min < 10: continue 
        
        X, y = np.vstack([X_river[:n_min], X_road[:n_min]]), np.concatenate([np.zeros(n_min), np.ones(n_min)])
        skf = StratifiedKFold(n_splits=min(5, n_min//4), shuffle=True, random_state=42)
        rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        acc_list, cm_list = [], []
        for tr, te in skf.split(X, y):
            rf.fit(X[tr], y[tr])
            pred = rf.predict(X[te])
            acc_list.append(accuracy_score(y[te], pred))
            cm_list.append(confusion_matrix(y[te], pred, labels=[0, 1]))
            
        avg_acc = np.mean(acc_list)
        all_metrics.append({'delay': d, 'accuracy': avg_acc, 'n_samples': n_min})
        cms_dict[d] = np.sum(cm_list, axis=0)
        print(f"  Final Set: {n_min}*2 samples, Accuracy: {avg_acc:.3f}")

    # Plot Trend
    # Plot Trend
    df_res = pd.DataFrame(all_metrics)
    plt.figure(figsize=(10, 6)); sns.lineplot(data=df_res, x='delay', y='accuracy', marker='o')
    plt.title("Random Forest 分類精度の推移", fontsize=16)
    plt.xlabel("降雨後経過時間 [時間後]", fontsize=14); plt.ylabel("精度", fontsize=14) 
    plt.grid(True)
    plt.ylim(0.70, 1.00)
    plt.axhline(0.751, color='red', linestyle='--', linewidth=1.5, label='Baseline (0.751)')
    plt.legend()
    
    # Add annotations
    for i, row in df_res.iterrows():
        plt.text(row['delay'], row['accuracy'] + 0.002, f"{row['accuracy']:.3f}", 
                 fontsize=11, ha='center', va='bottom', color='black', fontweight='bold')

    plt.savefig(OUTPUT_DIR / "accuracy_trend_aug_river.png", dpi=300); plt.savefig(ARTIFACT_DIR / "accuracy_trend_aug_river.png", dpi=300); plt.close()

    # Consolidated Confusion Matrices
    n_plots = len(cms_dict); cols = 3; rows = (n_plots + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(cols*5, rows*4)); axes = axes.flatten()
    for i, (d, cm) in enumerate(sorted(cms_dict.items())):
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[i], xticklabels=['河川', '道路'], yticklabels=['河川', '道路'], annot_kws={"size": 12})
        axes[i].set_title(f"{d}時間後 (精度: {df_res[df_res['delay']==d]['accuracy'].iloc[0]:.3f})", fontsize=14)
        axes[i].set_xlabel('予測クラス', fontsize=12); axes[i].set_ylabel('正解クラス', fontsize=12)
    for j in range(i+1, len(axes)): axes[j].axis('off')
    plt.tight_layout(); plt.savefig(OUTPUT_DIR / "confusion_matrices_aug_river.png", dpi=300); plt.savefig(ARTIFACT_DIR / "confusion_matrices_aug_river.png", dpi=300); plt.close()
    
    df_res.to_csv(OUTPUT_DIR / "metrics_aug_refined.csv", index=False)
    print("\nAnalysis Completed Successfully.")

if __name__ == "__main__":
    main()
