import rasterio
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pathlib import Path
import logging
import re
import sys

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_EXP_DIR = BASE_DIR / "data" / "expanded"
SAMPLES_DIR = DATA_EXP_DIR / "samples"
ANALYSIS_DIR = DATA_EXP_DIR / "analysis"
EVO_DIR = ANALYSIS_DIR / "evolution"
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("analyze_evo_exp")

def get_stats(event_dir):
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
        
        # Absolute Stats
        r_after = after[mask_road & valid]
        r_before = before[mask_road & valid]
        p_after = after[mask_paddy & valid]
        p_before = before[mask_paddy & valid]
        
        # Diff Stats
        r_diff = diff[mask_road & valid]
        p_diff = diff[mask_paddy & valid]
        
        stats = {
            "r_diff_mean": np.mean(r_diff), "r_diff_std": np.std(r_diff), 
            "p_diff_mean": np.mean(p_diff), "p_diff_std": np.std(p_diff),
            "r_abs_after": np.mean(r_after), "r_abs_before": np.mean(r_before),
            "p_abs_after": np.mean(p_after), "p_abs_before": np.mean(p_before),
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
    if not SAMPLES_DIR.exists():
        logger.error("Samples dir not found")
        return

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
    
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    logger.info(f"Scanning {len(grids)} grids...")
    
    all_data = []
    
    for gid_path in grids:
        gid = gid_path.name
        events = [d for d in gid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
        
        for event_dir in events:
            delay, date_key = parse_delay_date(event_dir.name)
            if delay is None: continue
            
            stats = get_stats(event_dir)
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
        out_csv = EVO_DIR / "evolution_data.csv"
        df_all.to_csv(out_csv, index=False)
        logger.info(f"Saved consolidated data to {out_csv} (N={len(df_all)})")
    else:
        logger.warning("No data found.")

if __name__ == "__main__":
    main()
