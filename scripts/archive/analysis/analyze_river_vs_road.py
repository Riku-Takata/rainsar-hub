
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.features import geometry_mask
import warnings
import os
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# Japanese font
plt.rcParams['font.family'] = 'MS Gothic'

# Config
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
RIVER_GEOJSON = BASE_DIR / "mask-data/river_points.geojson"
SAMPLES_DIR = BASE_DIR / "data/expanded/samples"
MASKS_DIR = BASE_DIR / "data/expanded/masks"
OUTPUT_DIR = BASE_DIR / "data/result/River_vs_Road_AllData"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

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
        return pd.DataFrame()

def run_analysis():
    print("Initializing River vs Road Analysis (ALL DATA)...")
    
    # 1. Load River Polygons
    gdf_river = gpd.read_file(RIVER_GEOJSON)
    print(f"Loaded {len(gdf_river)} river features.")
    
    # Rainfall Data
    df_rain = get_rainfall_map()
    
    river_pixels = []
    road_pixels = []
    
    # 2. Iterate Grids in Samples
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    print(f"Scanning {len(grids)} grids in Expanded Dataset...")
    
    for grid_dir in grids:
        grid_id = grid_dir.name
        
        # Load Road Mask
        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists():
            road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
            
        gdf_road = None
        if road_mask_path.exists():
            try:
                gdf_road = gpd.read_file(road_mask_path)
            except: pass
            
        # Check River Intersection using one sample
        sample_tif = None
        for evt in grid_dir.iterdir():
            if (evt / "before_vv.tif").exists():
                sample_tif = evt / "before_vv.tif"
                break
        
        if not sample_tif: continue
        
        has_river = False
        with rasterio.open(sample_tif) as src:
            if gdf_river.crs != src.crs:
                gdf_river_proj = gdf_river.to_crs(src.crs)
            else:
                gdf_river_proj = gdf_river
            
            r_minx, r_miny, r_maxx, r_maxy = gdf_river_proj.total_bounds
            s_left, s_bottom, s_right, s_top = src.bounds
            
            if not (r_minx > s_right or r_maxx < s_left or r_miny > s_top or r_maxy < s_bottom):
                 try:
                     out_img, _ = mask(src, gdf_river_proj.geometry, crop=True, nodata=np.nan)
                     if not np.isnan(out_img).all():
                         has_river = True
                 except: pass

        if not has_river:
            continue
            
        print(f"  Grid {grid_id}: Intersects River. Processing events...")
        
        river_mask_geom = gdf_river_proj.geometry
        
        road_mask_geom = None
        if gdf_road is not None:
             if gdf_road.crs != src.crs:
                 gdf_road = gdf_road.to_crs(src.crs)
             road_mask_geom = gdf_road.geometry
        
        # Process Events
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            event_name = event_dir.name
            
            # Format: delay_{dh}_{YYYYMMDD}
            try:
                parts = event_name.split('_')
                if len(parts) < 3: continue
                date_str = parts[2]
                # month = int(date_str[4:6])
                # Filter removed: Processing ALL months
                
                delay_h = float(parts[1].replace('h', ''))
                
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                rain_row = df_rain[(df_rain['grid_id'] == grid_id) & (df_rain['date_str'] == formatted_date)]
                
                if not rain_row.empty:
                    total_precip = rain_row.iloc[0]['sum_gauge_mm_h']
                    duration = rain_row.iloc[0]['duration_h']
                else:
                    if '2025' in date_str or '2024' in date_str: 
                        total_precip = 20.0 
                        duration = 5.0
                    else:
                        continue 

            except: continue
            
            before_p = event_dir / "before_vv.tif"
            after_p = event_dir / "after_vv.tif"
            
            if not before_p.exists() or not after_p.exists(): continue
            
            try:
                with rasterio.open(before_p) as src_b, rasterio.open(after_p) as src_a:
                    b_data = src_b.read(1)
                    a_data = src_a.read(1)
                    
                    diff_data = a_data - b_data
                    
                    river_bool = geometry_mask(river_mask_geom, out_shape=src_b.shape, transform=src_b.transform, invert=True, all_touched=True)
                    r_vals = diff_data[river_bool]
                    r_vals = r_vals[~np.isnan(r_vals)]
                    
                    if len(r_vals) > 0:
                        df_r = pd.DataFrame({'diff_db': r_vals})
                        df_r['total_precip'] = total_precip
                        df_r['duration'] = duration
                        df_r['delay_h'] = delay_h
                        df_r['label'] = 0 # River
                        river_pixels.append(df_r)
                        
                    if road_mask_geom is not None:
                         road_bool = geometry_mask(road_mask_geom, out_shape=src_b.shape, transform=src_b.transform, invert=True, all_touched=False)
                         rd_vals = diff_data[road_bool]
                         rd_vals = rd_vals[~np.isnan(rd_vals)]
                         
                         if len(rd_vals) > 0:
                            if len(rd_vals) > 1000:
                                rd_vals = np.random.choice(rd_vals, 1000, replace=False)
                                
                            df_rd = pd.DataFrame({'diff_db': rd_vals})
                            df_rd['total_precip'] = total_precip
                            df_rd['duration'] = duration
                            df_rd['delay_h'] = delay_h
                            df_rd['label'] = 1 # Road
                            road_pixels.append(df_rd)
                            
            except Exception:
                pass
                
    # Combine
    if not river_pixels:
        print("No River pixels found.")
        return
        
    df_river = pd.concat(river_pixels, ignore_index=True)
    df_river['delay_int'] = df_river['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    
    if not road_pixels:
        print("No Road pixels found.")
        return
        
    df_road = pd.concat(road_pixels, ignore_index=True)
    df_road['delay_int'] = df_road['delay_h'].apply(lambda x: int(x) if x < 12 else 11)

    print(f"Total River Pixels: {len(df_river)}")
    print(f"Total Road Pixels: {len(df_road)}")
    
    # Analyze per Delay
    unique_delays = sorted(df_river['delay_int'].unique())
    print(f"Delays found: {unique_delays}")
    
    results = []
    cm_data = []
    
    for d in unique_delays:
        print(f"\n--- Analyzing Delay {d}h ---")
        
        r_part = df_river[df_river['delay_int'] == d]
        rd_part = df_road[df_road['delay_int'] == d]
        
        if len(r_part) < 10 or len(rd_part) < 10:
            print(f"  Skipping (Insufficient Data: River={len(r_part)}, Road={len(rd_part)})")
            continue
            
        min_len = min(len(r_part), len(rd_part))
        print(f"  Balancing to {min_len} pixels.")
        
        df_bal = pd.concat([
            r_part.sample(n=min_len, random_state=42),
            rd_part.sample(n=min_len, random_state=42)
        ]).sample(frac=1, random_state=42)
        
        X = df_bal[['diff_db', 'total_precip', 'duration']]
        y = df_bal['label']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        
        model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred, labels=[0, 1])
        
        tn, fp, fn, tp = cm.ravel()
        acc_river = tn / (tn + fp) if (tn + fp) > 0 else 0
        acc_road = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        print(f"  Accuracy: {acc:.4f} (River: {acc_river:.3f}, Road: {acc_road:.3f})")
        
        results.append({
            'delay': d,
            'accuracy': acc,
            'acc_river': acc_river,
            'acc_road': acc_road,
            'n_train': len(X_train),
            'n_test': len(X_test)
        })
        cm_data.append({'delay': d, 'cm': cm, 'acc': acc})

    if not results:
        print("No valid results.")
        return

    res_df = pd.DataFrame(results)
    res_df.to_csv(OUTPUT_DIR / "river_vs_road_by_delay.csv", index=False)
    print("\nFinal Summary:")
    print(res_df)
    
    plt.figure(figsize=(10, 6))
    plt.plot(res_df['delay'], res_df['accuracy'], 'o-', label='Overall Accuracy', linewidth=2)
    plt.plot(res_df['delay'], res_df['acc_river'], 's--', label='River Accuracy', alpha=0.7)
    plt.plot(res_df['delay'], res_df['acc_road'], '^--', label='Road Accuracy', alpha=0.7)
    plt.xlabel('Delay (h)')
    plt.ylabel('Accuracy')
    plt.title('River vs Road Classification Accuracy by Delay (All Months)')
    plt.ylim(0, 1.05)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.savefig(OUTPUT_DIR / "accuracy_trend.png")
    plt.close()
    
    if cm_data:
        cols = 4
        rows = int(np.ceil(len(cm_data) / cols))
        fig, axes = plt.subplots(rows, cols, figsize=(4*cols, 4*rows))
        axes = axes.flatten() if rows*cols > 1 else [axes]
        
        for i, item in enumerate(cm_data):
            ax = axes[i]
            sns.heatmap(item['cm'], annot=True, fmt='d', cmap='Blues', ax=ax,
                        xticklabels=['River', 'Road'], yticklabels=['River', 'Road'])
            ax.set_title(f"Delay {item['delay']}h (Acc={item['acc']:.2f})")
            
        for j in range(len(cm_data), len(axes)):
            axes[j].axis('off')
            
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "confusion_matrices_by_delay.png")
        plt.close()

if __name__ == "__main__":
    run_analysis()
