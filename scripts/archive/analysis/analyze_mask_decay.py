import os
import sys
import numpy as np
import pandas as pd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(HUB_DIR / "scripts"))
from common_utils import parse_summary_txt, S1_SAMPLES_DIR, DATA_DIR, setup_logger

logger = setup_logger("mask_distribution")
OUT_DIR = DATA_DIR / "analysis" / "mask_distribution"

# Thresholds to evaluate
ROAD_THRESH = -0.05
PADDY_THRESH = 0.05

def load_tif_as_db(path):
    with rasterio.open(path) as src:
        data = src.read(1)
        data = np.where(data == src.nodata, np.nan, data)
        return data

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Identify Valid Grids
    detailed_csv = DATA_DIR / "analysis" / "detailed_analysis_paired.csv"
    if not detailed_csv.exists(): return
    df = pd.read_csv(detailed_csv)
    df = df[(df["Delay_Hours"] > 1.0) & (df["Delay_Hours"] <= 12.0)]
    grid_counts = df["GridID"].value_counts()
    target_grids = grid_counts[grid_counts >= 2].index.tolist()
    
    logger.info(f"Target Grids: {target_grids}")
    
    road_slopes = []
    paddy_slopes = []
    
    for grid_id in target_grids:
        logger.info(f"Processing {grid_id}...")
        events = parse_summary_txt(grid_id)
        valid_delays = df[df["GridID"] == grid_id]["Delay_Hours"].values
        target_events = [e for e in events if any(np.isclose(e['delay'], valid_delays, atol=0.01))]
        
        if len(target_events) < 2: continue
        
        grid_dir = S1_SAMPLES_DIR / grid_id
        def get_file(scene_id):
            matches = list(grid_dir.glob(f"*{scene_id}*_proc.tif"))
            return matches[0] if matches else None

        # Filter Duplicate Pairs
        # If multiple events start/end at different times but map to SAME S1 Pair, 
        # we have identical 'y' but different 't'. This yields Slope=0 artificially.
        # We must ensure we have at least 2 DISTINCT pairs (distinct 'y').
        
        unique_pairs = {}
        for evt in target_events:
            pair_key = (evt['after_scene'], evt['before_scene'])
            if pair_key not in unique_pairs:
                unique_pairs[pair_key] = evt
            else:
                # If duplicate, keep the one with shortest delay? 
                # Or based on logic? Usually shortest delay is the primary "cause".
                if evt['delay'] < unique_pairs[pair_key]['delay']:
                    unique_pairs[pair_key] = evt
        
        valid_events_check = []
        # Need at least 2 unique pairs to have variation in Y
        if len(unique_pairs) < 2:
            logger.warning(f"  {grid_id}: Not enough distinct S1 pairs ({len(unique_pairs)}) for 1-6h slope.")
            continue
            
        # Determine Min Shape
        min_H, min_W = 99999, 99999
        for evt in unique_pairs.values():
            fa, fb = get_file(evt['after_scene']), get_file(evt['before_scene'])
            if fa and fb:
                with rasterio.open(fa) as s: h, w = s.shape
                min_H, min_W = min(min_H, h), min(min_W, w)
                with rasterio.open(fb) as s: h, w = s.shape
                min_H, min_W = min(min_H, h), min(min_W, w)
                valid_events_check.append(evt)
        
        if min_H == 99999: continue
        H, W = min_H, min_W
        
        # Load Masks
        # We need to find ANY mask file in the folder (they are usually per scene but content is static per grid usually?)
        # Actually masks are generated per scene in process_masks.py but based on static GeoJSON.
        # So we can pick the first available mask file.
        m_roads = list(grid_dir.glob("*_highway_mask.tif"))
        m_paddies = list(grid_dir.glob("*_paddy_mask.tif"))
        
        road_mask = np.zeros((H, W), dtype=bool)
        paddy_mask = np.zeros((H, W), dtype=bool)
        
        if m_roads:
            with rasterio.open(m_roads[0]) as m:
                vals = m.read(1)
                mh, mw = min(vals.shape[0], H), min(vals.shape[1], W)
                road_mask[:mh, :mw] = vals[:mh, :mw] > 0
                
        if m_paddies:
            with rasterio.open(m_paddies[0]) as m:
                vals = m.read(1)
                mh, mw = min(vals.shape[0], H), min(vals.shape[1], W)
                paddy_mask[:mh, :mw] = vals[:mh, :mw] > 0
                
        if np.sum(road_mask) == 0 and np.sum(paddy_mask) == 0:
            logger.info("  No mask pixels found.")
            continue
        
        # Accumulate Slope Data
        sum_t_arr = np.zeros((H, W), dtype=np.float64)
        sum_y_arr = np.zeros((H, W), dtype=np.float64)
        sum_ty_arr = np.zeros((H, W), dtype=np.float64)
        sum_t2_arr = np.zeros((H, W), dtype=np.float64)
        count_pix = np.zeros((H, W), dtype=np.int8)
        
        for evt in valid_events_check:
            fa = get_file(evt['after_scene'])
            fb = get_file(evt['before_scene'])
            logger.info(f"    Event {evt['delay']:.2f}h: After={fa.name}, Before={fb.name}")
            
            img_after = load_tif_as_db(fa)[:H, :W].astype(np.float64)
            img_before = load_tif_as_db(fb)[:H, :W].astype(np.float64)
            diff = img_after - img_before
            t = float(evt['delay'])
            
            valid = ~np.isnan(diff)
            sum_t_arr[valid] += t
            sum_y_arr[valid] += diff[valid]
            sum_ty_arr[valid] += (t * diff[valid])
            sum_t2_arr[valid] += (t * t)
            count_pix[valid] += 1
            
        enough_data = count_pix >= 2
        numerator = (count_pix * sum_ty_arr) - (sum_t_arr * sum_y_arr)
        denominator = (count_pix * sum_t2_arr) - (sum_t_arr * sum_t_arr) # type: ignore
        
        valid_slope = enough_data & (denominator != 0)
        slope_map = np.full((H, W), np.nan, dtype=np.float32)
        slope_map[valid_slope] = numerator[valid_slope] / denominator[valid_slope]
        
        # Extract Slopes for Masks
        r_vals = slope_map[valid_slope & road_mask]
        p_vals = slope_map[valid_slope & paddy_mask]
        
        # Filter outliers for clean plotting (-5 to 5 dB/h is reasonable range, theoretical mostly -1 to +1)
        r_vals = r_vals[(r_vals > -5) & (r_vals < 5)]
        p_vals = p_vals[(p_vals > -5) & (p_vals < 5)]
        
        road_slopes.extend(r_vals)
        paddy_slopes.extend(p_vals)
        
        logger.info(f"  Extracted: Road={len(r_vals)}, Paddy={len(p_vals)}")

    # Plot Distribution
    logger.info(f"Total: Road={len(road_slopes)}, Paddy={len(paddy_slopes)}")
    
    plt.figure(figsize=(10, 6))
    if len(road_slopes) > 0:
        sns.kdeplot(road_slopes, fill=True, color='black', label='Road Pixels', warn_singular=False)
        plt.axvline(np.median(road_slopes), color='black', linestyle='--', label=f'Road Median: {np.median(road_slopes):.3f}')
    if len(paddy_slopes) > 0:
        sns.kdeplot(paddy_slopes, fill=True, color='green', label='Paddy Pixels', warn_singular=False)
        plt.axvline(np.median(paddy_slopes), color='green', linestyle='--', label=f'Paddy Median: {np.median(paddy_slopes):.3f}')
        
    plt.axvline(ROAD_THRESH, color='red', linestyle=':', label=f'Road Thresh ({ROAD_THRESH})')
    plt.axvline(PADDY_THRESH, color='blue', linestyle=':', label=f'Paddy Thresh ({PADDY_THRESH})')
    
    plt.title("Distribution of Decay Rates (Slopes) in Masked Areas (VV 1-6h)")
    plt.xlabel("Decay Rate [dB/hr]")
    plt.ylabel("Density")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xlim(-1.0, 1.0) # Focus on central meaningful part
    
    out_png = OUT_DIR / "slope_distribution.png"
    plt.savefig(out_png)
    print(f"Saved distribution plot to {out_png}")
    
    # Calculate Accuracy with current thresholds
    print_stats(road_slopes, "Road", ROAD_THRESH, is_leq=True)
    print_stats(paddy_slopes, "Paddy", PADDY_THRESH, is_leq=False)

def print_stats(data, label, thresh, is_leq=True):
    arr = np.array(data)
    if len(arr) == 0:
        print(f"\n--- {label} (No Data) ---")
        return

    # Raw Stats
    if is_leq:
        correct = np.sum(arr <= thresh)
    else:
        correct = np.sum(arr >= thresh)
    
    acc = correct / len(arr) * 100
    print(f"\n--- {label} Analysis ---")
    print(f"Total Pixels: {len(arr)}")
    print(f"[Raw] Mean={np.mean(arr):.3f}, Median={np.median(arr):.3f}, Std={np.std(arr):.3f}")
    print(f"[Raw] Accuracy (Slope {'<=' if is_leq else '>='} {thresh}): {correct}/{len(arr)} ({acc:.1f}%)")

    # Sigma Clipped Stats (Simple 3-sigma)
    mean = np.mean(arr)
    std = np.std(arr)
    clipped = arr[(arr >= mean - 3*std) & (arr <= mean + 3*std)]
    
    # Recalculate on clipped
    if is_leq:
        c_correct = np.sum(clipped <= thresh)
    else:
        c_correct = np.sum(clipped >= thresh)
        
    c_acc = c_correct / len(clipped) * 100
    print(f"[Clipped 3-sigma] Kept: {len(clipped)} ({len(clipped)/len(arr)*100:.1f}%)")
    print(f"[Clipped 3-sigma] Mean={np.mean(clipped):.3f}, Median={np.median(clipped):.3f}, Std={np.std(clipped):.3f}")
    print(f"[Clipped 3-sigma] Accuracy: {c_correct}/{len(clipped)} ({c_acc:.1f}%)")


if __name__ == "__main__":
    main()
