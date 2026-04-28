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
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score
import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
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
PIXEL_COUNTS_CSV = BASE_DIR / "data/analysis/monthly_delay_pixel_counts_detailed.csv"
OUTPUT_DIR = BASE_DIR / "data/result/River_vs_Road_Oct"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Database
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

# RF Config
TARGET_TOTAL = 5000
MAX_PER_EVENT = 500
LINEAR_THRESHOLD = 2.0

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def to_linear(arr):
    mask = (arr != 0)
    if np.any(mask):
        mean_val = np.mean(arr[mask])
        if mean_val < -1:
            return np.power(10, arr / 10.0)
    return arr

def get_rainfall_map():
    engine = create_engine(DATABASE_URL)
    query = text("""
        SELECT grid_id, DATE_FORMAT(end_ts_utc, '%Y-%m-%d') as date_str, 
               sum_gauge_mm_h, TIMESTAMPDIFF(HOUR, start_ts_utc, end_ts_utc) as duration_h
        FROM gsmap_events
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except:
        print("Database connection failed. Working with fallback rainfall.")
        return pd.DataFrame()

def find_fair_limit(counts, target, max_limit):
    if not counts: return 0
    low, high = 0, max_limit
    L = 0
    for _ in range(20):
        mid = (low + high) / 2
        if sum(min(c, mid) for c in counts) < target:
            L = mid
            low = mid
        else:
            L = mid
            high = mid
    return int(np.ceil(L))

def extract_features_for_event(event_dir, grid_id, date_str, df_rain, gdf_river, road_gdf):
    # Rainfall
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    rain_row = df_rain[(df_rain['grid_id'] == grid_id) & (df_rain['date_str'] == formatted_date)]
    if not rain_row.empty:
        rain_total = rain_row.iloc[0]['sum_gauge_mm_h']
        rain_dur = max(1.0, rain_row.iloc[0]['duration_h'])
        rain_mean = rain_total / rain_dur
    else:
        rain_mean, rain_dur = 0.0, 0.0

    # TIF Paths
    paths = {
        'vv_be': event_dir / "before_vv.tif",
        'vv_af': event_dir / "after_vv.tif",
        'vh_be': event_dir / "before_vh.tif",
        'vh_af': event_dir / "after_vh.tif"
    }
    if not all(p.exists() for p in paths.values()): return None, None, None, None

    try:
        with rasterio.open(paths['vv_af']) as src:
            vv_af = to_linear(src.read(1)); vv_be = to_linear(rasterio.open(paths['vv_be']).read(1))
            vh_af = to_linear(rasterio.open(paths['vh_af']).read(1)); vh_be = to_linear(rasterio.open(paths['vh_be']).read(1))
            
            h, w = vv_af.shape
            vv_be = vv_be[:h, :w]; vh_af = vh_af[:h, :w]; vh_be = vh_be[:h, :w]
            
            vv_diff = vv_af - vv_be
            vh_diff = vh_af - vh_be
            
            valid = (vv_af > 0) & (vv_be > 0) & (vv_af < LINEAR_THRESHOLD) & (vv_be < LINEAR_THRESHOLD)
            stack = np.stack([vv_af, vh_af, vv_diff, vh_diff, np.full((h, w), rain_mean), np.full((h, w), rain_dur)], axis=-1)
            
            # River Mask
            river_m = geometry_mask(gdf_river.to_crs(src.crs).geometry, (h, w), src.transform, invert=True)
            river_pixels = stack[river_m & valid]
            
            # Road Mask
            road_pixels = np.empty((0, 6))
            if road_gdf is not None:
                road_m = geometry_mask(road_gdf.to_crs(src.crs).geometry, (h, w), src.transform, invert=True)
                road_pixels = stack[road_m & valid]
                
            return river_pixels, road_pixels, len(river_pixels), len(road_pixels)
    except:
        return None, None, None, None

def main():
    print("=== Refined River vs Road Analysis (October) ===")
    
    # Load River Polygons
    gdfs = [gpd.read_file(p) for p in RIVER_GEOJSON_PATHS if p.exists()]
    if not gdfs:
        print("Error: No river files found."); return
    gdf_river = pd.concat(gdfs, ignore_index=True)
    
    df_rain = get_rainfall_map()
    df_counts = pd.read_csv(PIXEL_COUNTS_CSV)
    subset_all = df_counts[df_counts['month'] == 10]
    
    delays = sorted(subset_all['delay_int'].unique())
    all_metrics = []
    
    for d in delays:
        subset = subset_all[subset_all['delay_int'] == d]
        print(f"\nDelay {d}h: processing {len(subset)} events...")
        
        event_data_river = []
        event_data_road = []
        counts_river = []
        counts_road = []
        
        for _, row in subset.iterrows():
            grid_id = row['grid_id']
            date_str = row['event_dir'].split('_')[2]
            event_dir = SAMPLES_DIR / grid_id / row['event_dir']
            
            # Road mask
            road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
            if not road_mask_path.exists(): road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
            road_gdf = gpd.read_file(road_mask_path) if road_mask_path.exists() else None
            
            rv, rd, nr, n_rd = extract_features_for_event(event_dir, grid_id, date_str, df_rain, gdf_river, road_gdf)
            if rv is not None:
                event_data_river.append(rv); event_data_road.append(rd)
                counts_river.append(nr); counts_road.append(n_rd)
        
        if not event_data_river: continue
        
        limit_r = find_fair_limit(counts_river, TARGET_TOTAL, MAX_PER_EVENT)
        limit_rd = find_fair_limit(counts_road, TARGET_TOTAL, MAX_PER_EVENT)
        print(f"  Fair Limits: River={limit_r}, Road={limit_rd}")
        
        # Sampling
        X_river = []
        for rv in event_data_river:
            n = len(rv)
            if n > 0:
                sel = min(n, limit_r)
                X_river.append(rv[np.random.choice(n, sel, replace=False)])
        
        X_road = []
        for rd in event_data_road:
            n = len(rd)
            if n > 0:
                sel = min(n, limit_rd)
                X_road.append(rd[np.random.choice(n, sel, replace=False)])
        
        if not X_river or not X_road: continue
        X_river = np.vstack(X_river); X_road = np.vstack(X_road)
        
        n_min = min(len(X_river), len(X_road), TARGET_TOTAL)
        if n_min < 50: print("  Too few samples."); continue
        
        print(f"  Training with {n_min}*2 samples...")
        X = np.vstack([X_river[:n_min], X_road[:n_min]])
        y = np.concatenate([np.zeros(n_min), np.ones(n_min)])
        
        # CV
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        acc_list = []
        cms = []
        
        for tr, te in skf.split(X, y):
            rf.fit(X[tr], y[tr])
            pred = rf.predict(X[te])
            acc_list.append(accuracy_score(y[te], pred))
            cms.append(confusion_matrix(y[te], pred))
            
        avg_acc = np.mean(acc_list)
        total_cm = np.sum(cms, axis=0)
        tn, fp, fn, tp = total_cm.ravel()
        
        # Plot CM
        plt.figure(figsize=(6, 5))
        sns.heatmap(total_cm, annot=True, fmt='d', cmap='Blues', xticklabels=['River', 'Road'], yticklabels=['River', 'Road'])
        plt.title(f"River vs Road (Oct {d}h)\nAcc: {avg_acc:.3f}")
        plt.savefig(OUTPUT_DIR / f"cm_river_road_m10_d{d}h.png", dpi=300)
        plt.close()
        
        all_metrics.append({'delay': d, 'accuracy': avg_acc, 'n_samples': n_min, 'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp})
        
    pd.DataFrame(all_metrics).to_csv(OUTPUT_DIR / "metrics_refined_river.csv", index=False)
    print("\nAnalysis Completed Successfully.")

if __name__ == "__main__":
    main()
