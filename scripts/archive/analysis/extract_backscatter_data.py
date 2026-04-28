import rasterio
import numpy as np
import pandas as pd
from pathlib import Path
import logging
import re
import sys

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
# Scan both Expanded and Final datasets
SAMPLES_DIRS = [
    DATA_DIR / "expanded" / "samples",
    DATA_DIR / "final" / "samples"
]
ANALYSIS_DIR = DATA_DIR / "expanded" / "analysis"
EVO_DIR = ANALYSIS_DIR / "evolution"
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("extract_backscatter")

def sigma_clip(data, sigma=2.0):
    """
    Remove items more than sigma*std from mean.
    """
    if len(data) == 0: return data
    m = np.mean(data)
    s = np.std(data)
    lower = m - sigma * s
    upper = m + sigma * s
    return data[(data >= lower) & (data <= upper)]

def get_stats(event_dir, use_sigma_clip=True, sigma=2.0):
    paths = {
        "after": event_dir / "after.tif",
        "before": event_dir / "before.tif",
        "mask_road": event_dir / "mask_road.tif",
        "mask_paddy": event_dir / "mask_paddy.tif"
    }
    raw = {}
    for key, p in paths.items():
        if not p.exists(): return None
        try:
            with rasterio.open(p) as src:
                raw[key] = src.read(1)
        except: return None
            
    if not raw: return None
    shapes = [v.shape for v in raw.values()]
    min_h = min(s[0] for s in shapes)
    min_w = min(s[1] for s in shapes)
    
    data = {k: v[:min_h, :min_w] for k, v in raw.items()}
    
    try:
        after = data["after"]
        before = data["before"]
        mask_road = data["mask_road"] == 1
        mask_paddy = data["mask_paddy"] == 1
        
        diff = after - before
        valid = ~np.isnan(diff) & (after > -50) & (before > -50) & (after < 20)
        
        # Absolute Stats Logic (Using Filtered Pixels)
        # 1. Extract valid pixels for Road and Paddy
        idx_r = mask_road & valid
        idx_p = mask_paddy & valid
        
        r_diff_raw = diff[idx_r]
        p_diff_raw = diff[idx_p]
        
        # 2. Apply Filtering (Sigma Clip) on the DIFFERENCE mainly, 
        # but for absolute values we should use the same pixel set (indices).
        # However, finding indices after sigma clip is tricky on values array.
        # So we filter values directly.
        
        if use_sigma_clip:
            # We need to filter based on Diff outliers usually.
            # Calculate bounds
            if len(r_diff_raw) > 0:
                r_m, r_s = np.mean(r_diff_raw), np.std(r_diff_raw)
                r_mask = (r_diff_raw >= r_m - sigma*r_s) & (r_diff_raw <= r_m + sigma*r_s)
            else:
                r_mask = []
                
            if len(p_diff_raw) > 0:
                p_m, p_s = np.mean(p_diff_raw), np.std(p_diff_raw)
                p_mask = (p_diff_raw >= p_m - sigma*p_s) & (p_diff_raw <= p_m + sigma*p_s)
            else:
                p_mask = []
                
            # Filter Difference
            r_diff = r_diff_raw[r_mask] if len(r_mask) > 0 else r_diff_raw
            p_diff = p_diff_raw[p_mask] if len(p_mask) > 0 else p_diff_raw
            
            # Filter Absolute (Must match the same subset)
            # Recalculate is simplest way but inefficient. 
            # Better: r_after[idx_r][r_mask]
            
            r_after = after[idx_r][r_mask] if len(r_mask) > 0 else after[idx_r]
            r_before = before[idx_r][r_mask] if len(r_mask) > 0 else before[idx_r]
            
            p_after = after[idx_p][p_mask] if len(p_mask) > 0 else after[idx_p]
            p_before = before[idx_p][p_mask] if len(p_mask) > 0 else before[idx_p]
            
        else:
            r_diff, p_diff = r_diff_raw, p_diff_raw
            r_after = after[idx_r]
            r_before = before[idx_r]
            p_after = after[idx_p]
            p_before = before[idx_p]

        stats = {
            "r_diff_mean": np.mean(r_diff) if len(r_diff)>0 else np.nan, 
            "r_diff_std": np.std(r_diff) if len(r_diff)>0 else np.nan, 
            
            "p_diff_mean": np.mean(p_diff) if len(p_diff)>0 else np.nan, 
            "p_diff_std": np.std(p_diff) if len(p_diff)>0 else np.nan,
            
            "r_abs_after": np.mean(r_after) if len(r_after)>0 else np.nan, 
            "r_abs_before": np.mean(r_before) if len(r_before)>0 else np.nan,
            
            "p_abs_after": np.mean(p_after) if len(p_after)>0 else np.nan, 
            "p_abs_before": np.mean(p_before) if len(p_before)>0 else np.nan,
            
            "r_n": len(r_diff), "p_n": len(p_diff)
        }
        return stats
        
    except Exception as e:
        return None

def parse_delay_date(dirname):
    m = re.match(r"delay_(\d+)h_(\d{8})", dirname)
    if m:
        return int(m.group(1)), m.group(2)
    return None, None

def main():
    EVO_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load Pairs for Rainfall Data
    if PAIRS_CSV.exists():
        df_pairs = pd.read_csv(PAIRS_CSV)
        df_pairs['date_key'] = df_pairs['event_start_ts_utc'].astype(str).str.slice(0, 10).str.replace('-', '')
        
        rain_map = {}
        for _, row in df_pairs.iterrows():
            key = (row['grid_id'], row['date_key'])
            rain_map[key] = {
                'max_mm': row['max_gauge_mm_h'],
                'duration': row['hit_hours']
            }
    else:
        rain_map = {}
        logger.warning("Pairs CSV not found. Rainfall data will be missing.")
    
    all_grids = []
    for s_dir in SAMPLES_DIRS:
        if s_dir.exists():
            found = [d for d in s_dir.iterdir() if d.is_dir()]
            logger.info(f"Scanning {len(found)} grids in {s_dir.name}...")
            all_grids.extend(found)
        else:
            logger.warning(f"Samples dir not found: {s_dir}")

    logger.info(f"Total grids to process: {len(all_grids)}")
    
    all_data = []
    
    for gid_path in all_grids:
        gid = gid_path.name
        events = [d for d in gid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
        
        for event_dir in events:
            delay, date_key = parse_delay_date(event_dir.name)
            if delay is None: continue
            
            # Use Sigma Clipping by Default for Clean Stats
            stats = get_stats(event_dir, use_sigma_clip=True, sigma=2.0)
            
            if stats:
                # Rain Lookup
                r_info = rain_map.get((gid, date_key), {})
                max_mm = r_info.get('max_mm', np.nan)
                dur = r_info.get('duration', np.nan)
                
                est_total = 0.5 * max_mm * dur if (pd.notna(max_mm) and pd.notna(dur)) else np.nan
                
                row = {
                    "grid_id": gid,
                    "event": event_dir.name,
                    "delay_h": delay,
                    "rain_max_mm_h": max_mm,
                    "rain_duration_h": dur,
                    "rain_total_est_mm": est_total,
                    "road_diff_mean": stats["r_diff_mean"],
                    "road_diff_std": stats["r_diff_std"],
                    "paddy_diff_mean": stats["p_diff_mean"],
                    "paddy_diff_std": stats["p_diff_std"],
                    "road_before_mean": stats["r_abs_before"],
                    "road_after_mean": stats["r_abs_after"],
                    "paddy_before_mean": stats["p_abs_before"],
                    "paddy_after_mean": stats["p_abs_after"],
                    "road_px": stats["r_n"],
                    "paddy_px": stats["p_n"]
                }
                all_data.append(row)
        
    if all_data:
        df_all = pd.DataFrame(all_data)
        out_csv = EVO_DIR / "evolution_data_final.csv" # New Consolidated Output
        df_all.to_csv(out_csv, index=False)
        logger.info(f"Saved consolidated data to {out_csv} (N={len(df_all)})")
    else:
        logger.warning("No data found.")

if __name__ == "__main__":
    main()
