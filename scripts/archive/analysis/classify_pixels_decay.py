import os
import sys
import numpy as np
import pandas as pd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
# from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(HUB_DIR / "scripts"))
from common_utils import decode_grid_id, parse_summary_txt, S1_SAMPLES_DIR, DATA_DIR, setup_logger

logger = setup_logger("classify_decay")
OUT_DIR = DATA_DIR / "analysis" / "classification"

# Classification Thresholds (User Specified)
ROAD_SLOPE_THRESH = -0.05
ROAD_MEAN_THRESH = 1.75
PADDY_SLOPE_THRESH = 0.05
PADDY_MEAN_THRESH = 2.0

def load_tif_as_db(path):
    with rasterio.open(path) as src:
        data = src.read(1)
        # Assuming data is in dB as per summary. Replace nodata with Nan
        data = np.where(data == src.nodata, np.nan, data)
        # If typical values are > 0 (linear) or around -20 to 0 (dB). 
        # Sentinel-1 Sigma0 in dB is usually negative (-20 to 0).
        # Linear is 0 to 1.
        # Let's check ranges later. Assuming dB based on 'Diff' being in range ~1-3 dB.
        return data

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Identify Valid Grids (found in previous step)
    # We reload the CSV to be dynamic
    detailed_csv = DATA_DIR / "analysis" / "detailed_analysis_paired.csv"
    if not detailed_csv.exists():
        logger.error("detailed_analysis_paired.csv not found")
        return
        
    df = pd.read_csv(detailed_csv)
    # Filter 1.0 < Delay <= 12.0
    df = df[(df["Delay_Hours"] > 1.0) & (df["Delay_Hours"] <= 12.0)]
    
    # Count events per grid
    grid_counts = df["GridID"].value_counts()
    target_grids = grid_counts[grid_counts >= 2].index.tolist()
    
    logger.info(f"Target Grids (>= 2 events in 1-6h): {target_grids}")
    
    # Storage for all pixels (Grand Total)
    y_true_all = []
    y_pred_all = []
    
    # Class mapping: 0=Unknown, 1=Road, 2=Paddy, 3=Other (True mask only)
    # Predicted: 0=Unknown, 1=Road, 2=Paddy
    
    for grid_id in target_grids:
        logger.info(f"Processing {grid_id}...")
        
        # Parse events from summary txt to get filenames
        events = parse_summary_txt(grid_id)
        
        # Filter events that match the delays in our DF
        valid_delays = df[df["GridID"] == grid_id]["Delay_Hours"].values
        target_events = []
        for evt in events:
            # Match approximate delay (float precision)
            d = evt['delay']
            if any(np.isclose(d, valid_delays, atol=0.01)):
                target_events.append(evt)
        
        if len(target_events) < 2:
            logger.warning(f"  Could not match enough events for {grid_id}")
            continue
            
        # Initialize Arrays
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        # Helper to find file
        def get_file(scene_id):
            matches = list(grid_dir.glob(f"*{scene_id}*_proc.tif"))
            return matches[0] if matches else None

        # Stack Data
        # We need sum_t, sum_y, sum_ty, sum_t2 for vectorized slope
        # Slope = (N * sum_ty - sum_t * sum_y) / (N * sum_t2 - sum_t^2)
        # Also need Sum_y for Mean
        
        # We assume dimensions might vary slightly, but we need fixed accumulation arrays.
        # Strategy: Use the SHAPE of the first valid pair we encounter, OR max shape?
        # Actually, if we crop, we need to know the crop size beforehand or handle dynamic sizing.
        # Better strategy: Load all shapes first? No, too slow.
        # Just use the first file's shape as "Reference" and crop everything to it?
        # If subsequent is smaller, we have a problem (our accumulation array is too big).
        # We should iterate events to find MINIMUM shape first.
        
        # 1. Determine Min Shape
        min_H, min_W = 99999, 99999
        # Filter Duplicate Pairs
        unique_pairs = {}
        for evt in target_events:
            pair_key = (evt['after_scene'], evt['before_scene'])
            if pair_key not in unique_pairs:
                unique_pairs[pair_key] = evt
            else:
                if evt['delay'] < unique_pairs[pair_key]['delay']:
                    unique_pairs[pair_key] = evt
        
        valid_events_check = []
        if len(unique_pairs) < 2:
            logger.warning(f"  {grid_id}: Not enough distinct S1 pairs ({len(unique_pairs)}) for 1-12h slope.")
            continue
            
        for evt in unique_pairs.values():
            fa, fb = get_file(evt['after_scene']), get_file(evt['before_scene'])
            if fa and fb:
                with rasterio.open(fa) as s: h, w = s.shape
                min_H = min(min_H, h)
                min_W = min(min_W, w)
                with rasterio.open(fb) as s: h, w = s.shape
                min_H = min(min_H, h)
                min_W = min(min_W, w)
                valid_events_check.append(evt)
        
        if min_H == 99999: continue
        
        H, W = min_H, min_W
        logger.info(f"  Unified Grid Size: {H}x{W}")
        
        # Initialize Arrays
        N = len(valid_events_check)
        # --- RESET and do proper array accumulation ---
        sum_t_arr = np.zeros((H, W), dtype=np.float32)
        sum_y_arr = np.zeros((H, W), dtype=np.float32)
        sum_ty_arr = np.zeros((H, W), dtype=np.float32)
        sum_t2_arr = np.zeros((H, W), dtype=np.float32)
        count_pix = np.zeros((H, W), dtype=np.int8)
        
        masks_loaded = False
        road_mask = np.zeros((H, W), dtype=bool)
        paddy_mask = np.zeros((H, W), dtype=bool)
        
        for evt in target_events:
            f_after = get_file(evt['after_scene'])
            f_before = get_file(evt['before_scene'])
            if not f_after or not f_before: continue
            
            # Load Masks from the FIRST event we process
            if not masks_loaded:
                # Try to find mask files
                m_road = list(grid_dir.glob(f"*{evt['after_scene']}*_highway_mask.tif"))
                m_paddy = list(grid_dir.glob(f"*{evt['after_scene']}*_paddy_mask.tif"))
                
                if m_road:
                    with rasterio.open(m_road[0]) as m:
                        # Crop to matched H, W
                        vals = m.read(1)
                        if vals.shape[0] >= H and vals.shape[1] >= W:
                            road_mask = vals[:H, :W] > 0
                        else:
                            mh = min(vals.shape[0], H)
                            mw = min(vals.shape[1], W)
                            road_mask = np.zeros((H, W), dtype=bool)
                            road_mask[:mh, :mw] = vals[:mh, :mw] > 0

                if m_paddy:
                    with rasterio.open(m_paddy[0]) as m:
                        vals = m.read(1)
                        if vals.shape[0] >= H and vals.shape[1] >= W:
                            paddy_mask = vals[:H, :W] > 0
                        else:
                            mh = min(vals.shape[0], H)
                            mw = min(vals.shape[1], W)
                            paddy_mask = np.zeros((H, W), dtype=bool)
                            paddy_mask[:mh, :mw] = vals[:mh, :mw] > 0
                
                logger.info(f"  Masks Loaded: Road={np.sum(road_mask)}, Paddy={np.sum(paddy_mask)}")
                masks_loaded = True
            
            img_after = load_tif_as_db(f_after)
            img_before = load_tif_as_db(f_before)
            
            # Crop to unified size
            img_after = img_after[:H, :W]
            img_before = img_before[:H, :W]
            
            diff = img_after - img_before
            t = float(evt['delay'])
            
            valid = ~np.isnan(diff)
            
            sum_t_arr[valid] += t
            sum_y_arr[valid] += diff[valid]
            sum_ty_arr[valid] += (t * diff[valid])
            sum_t2_arr[valid] += (t * t)
            count_pix[valid] += 1
            
        # Calculate Slope (a) and Mean (b_average)
        # Filter pixels with < 2 events
        enough_data = count_pix >= 2
        
        slope_map = np.full((H, W), np.nan, dtype=np.float32)
        mean_map = np.full((H, W), np.nan, dtype=np.float32)
        
        # Calculate Slope
        numerator = (count_pix * sum_ty_arr) - (sum_t_arr * sum_y_arr)
        denominator = (count_pix * sum_t2_arr) - (sum_t_arr * sum_t_arr)
        
        # Avoid division by zero
        valid_slope = enough_data & (denominator != 0)
        
        slope_map[valid_slope] = numerator[valid_slope] / denominator[valid_slope]
        mean_map[enough_data] = sum_y_arr[enough_data] / count_pix[enough_data]
        
        # Classification
        # 0: Unknown/Bg, 1: Road, 2: Paddy
        pred_map = np.zeros((H, W), dtype=np.int8) # Default 0
        
        # Apply Rules (Slope Only)
        # Road: Slope <= -0.05
        is_road = valid_slope & (slope_map <= ROAD_SLOPE_THRESH)
        pred_map[is_road] = 1
        
        # Paddy: Slope >= 0.05
        # Overwrite if Paddy
        is_paddy = valid_slope & (slope_map >= PADDY_SLOPE_THRESH)
        pred_map[is_paddy] = 2
        
        # Ground Truth
        gt_map = np.zeros((H, W), dtype=np.int8)
        gt_map[paddy_mask] = 2 # Assign Paddy first
        gt_map[road_mask] = 1  # Overwrite with Road if overlap
        
        # "Other" are valid pixels (enough data) that are neither road nor paddy
        is_other = enough_data & (~road_mask) & (~paddy_mask)
        gt_map[is_other] = 3
        
        # Evaluation Mask
        eval_mask = enough_data
        
        y_true = gt_map[eval_mask]
        y_pred = pred_map[eval_mask]
        
        y_true_all.extend(y_true)
        y_pred_all.extend(y_pred)
        
        # Debug info for one grid
        n_road_pred = np.sum(is_road)
        n_paddy_pred = np.sum(is_paddy)
        logger.info(f"  {grid_id}: Pred_Roads={n_road_pred}, Pred_Paddies={n_paddy_pred}")
        
        # --- Per-Grid Report ---
        if len(y_true) > 0:
            generate_report(y_true, y_pred, grid_id)

    # Aggregate Analysis
    print("\n=== AGGREGATE RESULT ===")
    generate_report(np.array(y_true_all), np.array(y_pred_all), "ALL_GRIDS")

def generate_report(y_true_raw, y_pred_raw, label_id):
    # Map to strings
    def map_label_true(v):
        return {1: "Road", 2: "Paddy", 3: "Other"}.get(v, "Err")
    def map_label_pred(v):
        return {0: "Unknown", 1: "Road", 2: "Paddy"}.get(v, "Err")
        
    y_true_str = [map_label_true(v) for v in y_true_raw]
    y_pred_str = [map_label_pred(v) for v in y_pred_raw]
    
    # Generate Matrix using Pandas Crosstab
    # Rows: True, Cols: Pred
    df_cm = pd.DataFrame({'True': y_true_str, 'Pred': y_pred_str})
    
    # Ensure all columns/index exist
    cm_df = pd.crosstab(df_cm['True'], df_cm['Pred'], dropna=False)
    
    # Reindex to ensure order
    row_order = ["Road", "Paddy", "Other"]
    col_order = ["Road", "Paddy", "Unknown"]
    
    # Fill missing with 0
    cm_final = cm_df.reindex(index=row_order, columns=col_order, fill_value=0).values
    
    # Report
    print(f"--- Classification Result: {label_id} ---")
    print(f"Total Pixels: {len(y_true_raw)}")
    
    # Manual Print
    row_labs = ["True Road ", "True Paddy", "True Other"]
    col_labs = ["Pred Road", "Pred Paddy", "Pred Unk"]
    
    print(f"{'':12} | {col_labs[0]:10} | {col_labs[1]:10} | {col_labs[2]:10} |")
    print("-" * 50)
    for i in range(3):
        print(f"{row_labs[i]:12} | {cm_final[i,0]:10d} | {cm_final[i,1]:10d} | {cm_final[i,2]:10d} |")
        
    # Graphical
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm_final, annot=True, fmt="d", cmap="Blues", 
                xticklabels=col_labs, yticklabels=row_labs)
    plt.title(f"Confusion Matrix ({label_id})\nSlope Only Rules")
    plt.ylabel("Ground Truth")
    plt.xlabel("Prediction")
    plt.tight_layout()
    plt.savefig(OUT_DIR / f"confusion_matrix_{label_id}.png")
    # print(f"Saved matrix to {OUT_DIR / f'confusion_matrix_{label_id}.png'}")
    plt.close()

if __name__ == "__main__":
    main()
