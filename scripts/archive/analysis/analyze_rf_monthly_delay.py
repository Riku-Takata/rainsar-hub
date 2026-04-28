
import os
import re
import numpy as np
import pandas as pd
import rasterio
from rasterio import features
import geopandas as gpd
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score, f1_score
import joblib
import matplotlib.pyplot as plt
import seaborn as sns

# --- Configuration ---
DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
MASK_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks")
OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
PIXEL_COUNTS_CSV = OUTPUT_DIR / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_METRICS_CSV = OUTPUT_DIR / "rf_classification_metrics.csv"
OUTPUT_PLOT_DIR = OUTPUT_DIR / "rf_plots"
OUTPUT_PLOT_DIR.mkdir(exist_ok=True)

TARGET_COUNTS = {
    4: 10000,
    9: 5000,
    10: 5000
}
MAX_PER_EVENT = 500
MONTHS = [10]
DELAYS = range(12) # 0 to 11

# Outlier threshold
LINEAR_THRESHOLD = 2.0

def load_and_sample_event(row, n_road_target, n_paddy_target, rainfall_info):
    """
    Loads TIF data for a specific event, extracts features, and samples N pixels 
    based on the provided ratios.
    
    rainfall_info: dict or Series containing 'rain_mean' and 'rain_duration'
    """
    grid_id = row['grid_id']
    event_dir_name = row['event_dir']
    
    event_path = DATA_DIR / grid_id / event_dir_name
    mask_path = MASK_DIR / grid_id
    
    # Paths
    s1_after_vv = event_path / "after_vv.tif"
    s1_before_vv = event_path / "before_vv.tif"
    s1_after_vh = event_path / "after_vh.tif"
    s1_before_vh = event_path / "before_vh.tif"
    
    mask_road_tif = mask_path / f"{grid_id}_road_mask.tif"
    mask_paddy_tif = mask_path / f"{grid_id}_paddy_mask.tif"
    mask_road_json = mask_path / f"{grid_id}_motorway.geojson"
    mask_paddy_json = mask_path / f"{grid_id}_paddy.geojson"
    
    if not (s1_after_vv.exists() and s1_before_vv.exists() and s1_after_vh.exists() and s1_before_vh.exists()):
        return None, None

    try:
        # --- Load S1 Data & Crop to Intersection ---
        with rasterio.open(s1_after_vv) as src:
            vv_after = src.read(1)
            nodata = src.nodata
            h_av, w_av = vv_after.shape
            transform = src.transform
            
        with rasterio.open(s1_before_vv) as src:
            vv_before = src.read(1)
            h_bv, w_bv = vv_before.shape
            
        with rasterio.open(s1_after_vh) as src:
            vh_after = src.read(1)
            h_ah, w_ah = vh_after.shape
            
        with rasterio.open(s1_before_vh) as src:
            vh_before = src.read(1)
            h_bh, w_bh = vh_before.shape
            
        # Determine common shape
        common_h = min(h_av, h_bv, h_ah, h_bh)
        common_w = min(w_av, w_bv, w_ah, w_bh)
        
        # Crop all
        vv_after = vv_after[:common_h, :common_w]
        vv_before = vv_before[:common_h, :common_w]
        vh_after = vh_after[:common_h, :common_w]
        vh_before = vh_before[:common_h, :common_w]
        
        height, width = common_h, common_w
        
        # --- Create Valid Mask ---
        # Pixels must be valid in ALL 4 bands
        valid_mask = (
            (~np.isnan(vv_after)) & (~np.isnan(vv_before)) &
            (~np.isnan(vh_after)) & (~np.isnan(vh_before))
        )
        if nodata is not None:
             valid_mask &= (vv_after != nodata)
        
        if not np.any(valid_mask):
            return None, None
            
        # --- Value Scaling (dB to Linear if needed) ---
        # Check Mean of VV After
        vv_vals = vv_after[valid_mask]
        if len(vv_vals) == 0: return None, None
        
        mean_val = np.mean(vv_vals)
        is_db = mean_val < -1
        
        def to_linear(arr, is_db_fmt):
            if is_db_fmt:
                return np.power(10, arr / 10.0)
            return arr
            
        vv_after_lin = to_linear(vv_after, is_db)
        vv_before_lin = to_linear(vv_before, is_db)
        # VH check (assume same format as VV basically, or check mean again)
        # Simplify: assume same
        vh_after_lin = to_linear(vh_after, is_db)
        vh_before_lin = to_linear(vh_before, is_db)
        
        vv_diff_lin = vv_after_lin - vv_before_lin
        vh_diff_lin = vh_after_lin - vh_before_lin
        
        # --- Rainfall Features ---
        # Create constant arrays for rainfall features
        rain_mean_val = rainfall_info.get('rain_mean', 0.0)
        rain_duration_val = rainfall_info.get('rain_duration', 0.0)
        
        rain_mean_arr = np.full((height, width), rain_mean_val, dtype=np.float32)
        rain_duration_arr = np.full((height, width), rain_duration_val, dtype=np.float32)

        # --- Load Masks ---
        def get_mask(json_path, tif_path):
            if json_path.exists():
                try:
                    gdf = gpd.read_file(json_path)
                    if not gdf.empty:
                        arr = features.rasterize(
                            shapes=gdf.geometry, out_shape=(height, width), transform=transform,
                            fill=0, default_value=1, dtype=rasterio.uint8
                        )
                        return arr > 0
                except:
                    pass
            if tif_path.exists():
                try:
                    with rasterio.open(tif_path) as src_m:
                        arr = src_m.read(1)
                        # Resize crop
                        arr = arr[:height, :width]
                        if arr.shape == (height, width):
                            return arr > 0
                except:
                    pass
            return np.zeros((height, width), dtype=bool)

        mask_road_bool = get_mask(mask_road_json, mask_road_tif)
        mask_paddy_bool = get_mask(mask_paddy_json, mask_paddy_tif)
        
        # Combine with Valid Mask & Outlier Filter
        # Outlier check on VV After Linear
        no_outlier = vv_after_lin <= LINEAR_THRESHOLD
        
        final_road_mask = mask_road_bool & valid_mask & no_outlier
        final_paddy_mask = mask_paddy_bool & valid_mask & no_outlier
        
        # --- Sampling ---
        X_local = []
        y_local = []
        
        # Feature columns: vv_after, vh_after, vv_diff, vh_diff, rain_mean, rain_duration
        features_stack = np.stack([
            vv_after_lin, vh_after_lin, vv_diff_lin, vh_diff_lin, rain_mean_arr, rain_duration_arr
        ], axis=-1)
        
        # Road
        road_pixels = features_stack[final_road_mask]
        n_road_avail = len(road_pixels)
        if n_road_avail > 0:
            n_target = min(n_road_avail, n_road_target)
            
            # Random sample
            indices = np.random.choice(n_road_avail, n_target, replace=False)
            X_local.append(road_pixels[indices])
            y_local.append(np.full(n_target, 0)) # 0 for Road
            
        # Paddy
        paddy_pixels = features_stack[final_paddy_mask]
        n_paddy_avail = len(paddy_pixels)
        if n_paddy_avail > 0:
            n_target = min(n_paddy_avail, n_paddy_target)
            
            indices = np.random.choice(n_paddy_avail, n_target, replace=False)
            X_local.append(paddy_pixels[indices])
            y_local.append(np.full(n_target, 1)) # 1 for Paddy
            
        if X_local:
            return np.vstack(X_local), np.concatenate(y_local)
        
    except Exception as e:
        # print(f"Error {grid_id}/{event_dir_name}: {e}")
        pass
        
    return None, None

def main():
    print("Starting Random Forest Analysis...")
    
    # Load Rainfall Data
    print("Loading Rainfall Data...")
    gsmap_df = pd.read_csv(OUTPUT_DIR / "gsmap_events.csv")
    s1_pairs_df = pd.read_csv(OUTPUT_DIR / "s1_pairs.csv")
    
    # Preprocess dates
    gsmap_df['start_ts_utc'] = pd.to_datetime(gsmap_df['start_ts_utc'])
    gsmap_df['end_ts_utc'] = pd.to_datetime(gsmap_df['end_ts_utc'])
    s1_pairs_df['event_start_ts_utc'] = pd.to_datetime(s1_pairs_df['event_start_ts_utc'])
    
    # Helper to find rainfall info for a grid/event
    # We match using S1 event_start_ts_utc which should correspond to gsmap start/end
    # Actually, s1_pairs links to gsmap_events via grid_id and time proximity/exact match
    # Since we don't have the FK in the CSV export, let's merge on grid_id and start_ts
    
    # Merge S1 pairs with GSMap events
    # Allow a small tolerance if needed, but they should be exact data points
    print("Merging Rainfall Data...")
    
    # Rename for clarity
    # s1_pairs: event_start_ts_utc  -> roughly matches gsmap start
    
    # To match 'event_dir' from pixel counts to s1_pairs:
    # event_dir looks like: delay_7.5h_20180929
    # We can extract the date part (20180929) and match with date part of s1_pairs['event_start_ts_utc']?
    # Or start_ts_utc?
    
    # Let's create a lookup dict: (grid_id, date_str) -> {rain_mean, rain_duration}
    
    rainfall_lookup = {}
    
    # Join s1_pairs and gsmap
    # Assume 1:1 match on (grid_id, start_ts)
    merged_rainfall = pd.merge(
        s1_pairs_df, 
        gsmap_df, 
        left_on=['grid_id', 'event_start_ts_utc'], 
        right_on=['grid_id', 'start_ts_utc'],
        how='inner'
    )
    
    print(f"Matched {len(merged_rainfall)} rainfall events.")
    
    for _, r in merged_rainfall.iterrows():
        # Calculate duration
        duration_h = (r['end_ts_utc'] - r['start_ts_utc']).total_seconds() / 3600.0
        if duration_h <= 0: duration_h = 1.0 # Minimum 1h or just use 1 if end=start (sometimes 1hr res)
        
        # Calculate mean rain
        rain_sum = r['sum_gauge_mm_h']
        rain_mean = rain_sum / duration_h if duration_h > 0 else 0
        
        # Key: grid_id + date string from start time (YYYYMMDD)
        # Note: 'event_dir' has date part. E.g. 'delay_7.5h_20180929' -> 20180929
        # Check if the event dir date corresponds to rain start date?
        # Usually yes.
        date_key = r['start_ts_utc'].strftime("%Y%m%d")
        full_key = (r['grid_id'], date_key)
        
        rainfall_lookup[full_key] = {
            'rain_mean': rain_mean,
            'rain_duration': duration_h
        }
        
        # Also add key for 'end_ts' date just in case?
        # Sometimes events span days. The directory usually names the START date or the S1 After date?
        # The directory name 'delay_Xh_YYYYMMDD' usually uses the date of the S1 AFTER image or rain end?
        # Let's verify event_dir naming convention.
        # Looking at previous logs, 'delay_10.4h_20240828':
        # "Delay 10.4h" means 10.4h after rain ends.
        # If rain ended on 2024-08-28 10:00, S1 was at 20:24.
        # The directory date likely refers to the S1 acquisition date or event start?
        # Re-checking 'count_events_per_delay_monthly.py' or earlier contexts.
        # Actually, looking at 's1_pairs.csv', we have 'delay_h' and 'event_start_ts_utc'.
        # We can fuzzy match based on grid and delay?
        
        # Let's refine lookup: use (grid_id, rounded_delay) to help match?
        # Or iterate s1_pairs again?
        
    # Better approach for Lookup:
    # Iterate pixel count rows, parse event_dir to find Grid + Delay + Date
    # Find matching row in s1_pairs_df
    # Then get linked rainfall data.
    
    # Let's verify s1_pairs matching content
    # s1_pairs has 'delay_h'.
    # event_dir: 'delay_10.4h_20240828' -> delay=10.4 approx.
    
    # Optimize: Pre-process s1_pairs + gsmap into a quick lookup
    # Key = (grid_id, delay_rounded_1decimal) -> info?
    # Date might be tricky if timezones differ.
    
    s1_rain_map = {}
    for _, r in merged_rainfall.iterrows():
        # Approx delay for matching
        d_val = float(r['delay_h'])
        d_key = round(d_val, 1)
        grid = r['grid_id']
        
        # Calculate rain info
        dur = (r['end_ts_utc'] - r['start_ts_utc']).total_seconds() / 3600.0
        dur = max(dur, 1.0) # avoid zero div
        mean_r = r['sum_gauge_mm_h'] / dur
        
        # Store using a combined key? Or a list for that grid?
        if grid not in s1_rain_map: s1_rain_map[grid] = []
        s1_rain_map[grid].append({
            'delay': d_val,
            'date_str': r['start_ts_utc'].strftime("%Y%m%d"),
            'rain_mean': mean_r,
            'rain_duration': dur
        })

    
    df_counts = pd.read_csv(PIXEL_COUNTS_CSV)
    
    # Load existing metrics if available to skip finished tasks
    existing_metrics = []
    if OUTPUT_METRICS_CSV.exists():
        try:
            existing_df = pd.read_csv(OUTPUT_METRICS_CSV)
            existing_metrics = list(zip(existing_df['month'], existing_df['delay_int']))
            print(f"Found {len(existing_metrics)} existing records. Skipping them.")
            # Load into list to append new results
            all_metrics = existing_df.to_dict('records')
        except:
            all_metrics = []
    else:
        all_metrics = []

    for month in MONTHS:
        target_val = TARGET_COUNTS.get(month, 5000)
        target_r = target_val
        target_p = target_val
        
        for delay in DELAYS:
            if (month, delay) in existing_metrics:
                print(f"Skipping Month {month} / Delay {delay}h (Already processed)")
                continue

            print(f"\n=== Processing Month {month} / Delay {delay}h ===")
            
            subset = df_counts[(df_counts['month'] == month) & (df_counts['delay_int'] == delay)]
            
            total_avail_road = subset['road_pixels'].sum()
            total_avail_paddy = subset['paddy_pixels'].sum()
            
            print(f"  Available: Road={total_avail_road}, Paddy={total_avail_paddy}")
            print(f"  Target:    Road={target_r}, Paddy={target_p}")
            
            if total_avail_road == 0 or total_avail_paddy == 0:
                print("  Skipping due to lack of data.")
                continue
            
            # --- Balanced Event-based Sampling (Fair Allocation) ---
            n_events = len(subset)
            if n_events == 0: continue
            
            def find_fair_limit(counts, target, max_limit):
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

            # Calculate a fair limit L such that Sum(min(available, L)) == 5000
            road_limit = find_fair_limit(subset['road_pixels'], target_r, MAX_PER_EVENT)
            paddy_limit = find_fair_limit(subset['paddy_pixels'], target_p, MAX_PER_EVENT)
            
            print(f"  Fair Sampling Limits: Road={road_limit}, Paddy={paddy_limit} (Target={target_r}, n={n_events})")

            # --- Data Collection ---
            X_list = []
            y_list = []
            
            for idx, row in subset.iterrows():
                # Find rainfall info
                g_id = row['grid_id']
                evt_dir = row['event_dir']
                delay_val = row['delay_float']
                
                rain_info = {'rain_mean': 0.0, 'rain_duration': 0.0}
                
                if g_id in s1_rain_map:
                    candidates = s1_rain_map[g_id]
                    best_c = None
                    min_diff = 0.2
                    for c in candidates:
                        diff = abs(c['delay'] - delay_val)
                        if diff < min_diff:
                            min_diff = diff
                            best_c = c
                    
                    if best_c:
                        rain_info['rain_mean'] = best_c['rain_mean']
                        rain_info['rain_duration'] = best_c['rain_duration']
                
                # Sample based on the calculated fair limits
                X_ev, y_ev = load_and_sample_event(row, road_limit, paddy_limit, rain_info)
                if X_ev is not None:
                    X_list.append(X_ev)
                    y_list.append(y_ev)
            
            if not X_list:
                print("  No feature data extracted.")
                continue
                
            X_all = np.vstack(X_list)
            y_all = np.concatenate(y_list)
            
            # --- Final Balancing ---
            # Ensure we don't exceed the total target (5000) and classes are balanced
            idx_r = np.where(y_all == 0)[0]
            idx_p = np.where(y_all == 1)[0]
            np.random.shuffle(idx_r)
            np.random.shuffle(idx_p)
            
            limit = min(target_r, target_p, len(idx_r), len(idx_p))
            idx_r = idx_r[:limit]
            idx_p = idx_p[:limit]
            
            print(f"  Final Training Set: Road={len(idx_r)}, Paddy={len(idx_p)} (Target {target_r})")
            
            final_idx = np.concatenate([idx_r, idx_p])
            np.random.shuffle(final_idx)
            
            X_final = X_all[final_idx]
            y_final = y_all[final_idx]
            
            # --- 5-Fold Cross Validation ---
            skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
            rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
            
            fold_metrics = []
            total_cm = np.zeros((2, 2), dtype=int)
            
            # To store per-class metrics
            prec_r_list, rec_r_list, f1_r_list = [], [], []
            prec_p_list, rec_p_list, f1_p_list = [], [], []
            
            for fold, (train_index, val_index) in enumerate(skf.split(X_final, y_final)):
                X_train, X_val = X_final[train_index], X_final[val_index]
                y_train, y_val = y_final[train_index], y_final[val_index]
                
                rf.fit(X_train, y_train)
                y_pred = rf.predict(X_val)
                
                # Global
                acc = accuracy_score(y_val, y_pred)
                
                # Per Class (0=Road, 1=Paddy)
                pr_r = precision_score(y_val, y_pred, pos_label=0)
                re_r = recall_score(y_val, y_pred, pos_label=0)
                f1_r = f1_score(y_val, y_pred, pos_label=0)
                
                pr_p = precision_score(y_val, y_pred, pos_label=1)
                re_p = recall_score(y_val, y_pred, pos_label=1)
                f1_p = f1_score(y_val, y_pred, pos_label=1)
                
                cm = confusion_matrix(y_val, y_pred)
                total_cm += cm
                
                fold_metrics.append(acc)
                
                prec_r_list.append(pr_r)
                rec_r_list.append(re_r)
                f1_r_list.append(f1_r)
                
                prec_p_list.append(pr_p)
                rec_p_list.append(re_p)
                f1_p_list.append(f1_p)
                
            # Average Metrics
            avg_acc = np.mean(fold_metrics)
            avg_f1_paddy = np.mean(f1_p_list)
            
            print(f"  CV Results: Acc={avg_acc:.4f}, F1(Paddy)={avg_f1_paddy:.4f}")
            
            # --- Plot Confusion Matrix (Japanese) ---
            # Set Japanese font
            plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']
            
            plt.figure(figsize=(6, 5))
            # annot keys: TN, FP / FN, TP
            # X axis: Predicted (Road, Paddy), Y axis: True (Road, Paddy)
            sns.heatmap(total_cm, annot=True, fmt='d', cmap='Blues',
                        xticklabels=['道路', '水田'],
                        yticklabels=['道路', '水田'])
            plt.title(f"{month}月 Delay {delay}h 混同行列\nAccuracy: {avg_acc:.3f}, F1(Water): {avg_f1_paddy:.3f}")
            plt.ylabel('正解クラス')
            plt.xlabel('予測クラス')
            
            cm_filename = f"cm_m{month}_d{delay}h.png"
            plt.savefig(OUTPUT_PLOT_DIR / cm_filename)
            plt.close()
            
            # Store Detailed Metrics
            # CM layout: [[TN, FP], [FN, TP]] (Row: True, Col: Pred)
            tn, fp = total_cm[0]
            fn, tp = total_cm[1]
            
            all_metrics.append({
                'month': month,
                'delay_int': delay,
                'accuracy': avg_acc,
                
                'road_precision': np.mean(prec_r_list),
                'road_recall': np.mean(rec_r_list),
                'road_f1': np.mean(f1_r_list),
                'road_support': len(idx_r), # Used in training (approx support)
                
                'paddy_precision': np.mean(prec_p_list),
                'paddy_recall': np.mean(rec_p_list),
                'paddy_f1': np.mean(f1_p_list),
                'paddy_support': len(idx_p),
                
                'cm_tn_road_road': tn,
                'cm_fp_road_paddy': fp,
                'cm_fn_paddy_road': fn,
                'cm_tp_paddy_paddy': tp
            })

            # Incremental Save (so we don't lose data if crash)
            metrics_df = pd.DataFrame(all_metrics)
            metrics_df.to_csv(OUTPUT_METRICS_CSV, index=False)
            print(f"  Saved metrics to {OUTPUT_METRICS_CSV}")

            # --- Feature Importance Analysis ---
            FEATURE_NAMES = ['vv_after', 'vh_after', 'vv_diff', 'vh_diff', 'rain_mean', 'rain_duration']
            
            # rf is already fitted on the last fold, but we should collect from all folds if we want robust stats.
            # However, for simplicity and since we re-instantiated RF inside the loop but didn't save them:
            # We can re-fit on the ENTIRE dataset for the best representative feature importance of this (Month, Delay) configuration.
            
            rf_full = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
            rf_full.fit(X_final, y_final)
            
            importances = rf_full.feature_importances_
            indices = np.argsort(importances)[::-1]
            
            # Print Feature Ranking
            print("\n  Feature Ranking:")
            for f in range(X_final.shape[1]):
                print(f"  {f+1}. {FEATURE_NAMES[indices[f]]} ({importances[indices[f]]:.4f})")
                
            # Plot Feature Importance
            plt.figure(figsize=(8, 5))
            plt.title(f"{month}月 Delay {delay}h Feature Importance")
            plt.bar(range(X_final.shape[1]), importances[indices], align="center")
            plt.xticks(range(X_final.shape[1]), [FEATURE_NAMES[i] for i in indices], rotation=45)
            plt.xlim([-1, X_final.shape[1]])
            plt.ylabel("Importance")
            plt.tight_layout()
            
            fi_filename = f"feature_importance_m{month}_d{delay}h.png"
            plt.savefig(OUTPUT_PLOT_DIR / fi_filename)
            plt.close()
            
            # Append to a feature importance tracking CSV
            fi_csv_path = OUTPUT_DIR / "rf_feature_importances.csv"
            fi_row = {
                'month': month,
                'delay_int': delay,
            }
            for i, name in enumerate(FEATURE_NAMES):
                fi_row[name] = importances[i]
            
            fi_df = pd.DataFrame([fi_row])
            if fi_csv_path.exists():
                fi_df.to_csv(fi_csv_path, mode='a', header=False, index=False)
            else:
                fi_df.to_csv(fi_csv_path, mode='w', header=True, index=False)
            print(f"  Saved feature importance to {fi_csv_path}")

    print(f"\nAnalysis Completed. Metrics saved to {OUTPUT_METRICS_CSV}")

if __name__ == "__main__":
    main()
