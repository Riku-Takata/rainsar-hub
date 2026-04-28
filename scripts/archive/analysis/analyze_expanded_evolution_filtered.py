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
logger = logging.getLogger("analyze_evo_filt")

def sigma_clip(data, sigma=2.0):
    """
    Remove items more than sigma*std from mean.
    Iteratively? Just once for robustness.
    """
    if len(data) == 0: return data
    m = np.mean(data)
    s = np.std(data)
    lower = m - sigma * s
    upper = m + sigma * s
    return data[(data >= lower) & (data <= upper)]

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
        # Basic validity check
        valid = ~np.isnan(diff) & (after > -50) & (before > -50) & (after < 20)
        
        # Extract Raw pixels
        r_vals = diff[mask_road & valid]
        p_vals = diff[mask_paddy & valid]
        
        # Sigma Clipping (Noise Removal)
        r_vals_filt = sigma_clip(r_vals, sigma=2.0)
        p_vals_filt = sigma_clip(p_vals, sigma=2.0)
        
        r_mean = np.mean(r_vals_filt) if len(r_vals_filt) > 0 else np.nan
        r_std = np.std(r_vals_filt) if len(r_vals_filt) > 0 else np.nan
        
        p_mean = np.mean(p_vals_filt) if len(p_vals_filt) > 0 else np.nan
        p_std = np.std(p_vals_filt) if len(p_vals_filt) > 0 else np.nan
        
        return r_mean, r_std, p_mean, p_std, len(r_vals_filt), len(p_vals_filt)
        
    except Exception as e:
        return None

def parse_delay_date(dirname):
    m = re.match(r"delay_(\d+)h_(\d{8})", dirname)
    if m:
        return int(m.group(1)), m.group(2)
    return None, None

def plot_overall_summary(df, out_dir):
    # Melt
    road = df[['delay_h', 'road_diff_mean']].copy()
    road['type'] = 'Road'
    road = road.rename(columns={'road_diff_mean': 'diff'})
    
    paddy = df[['delay_h', 'paddy_diff_mean']].copy()
    paddy['type'] = 'Paddy'
    paddy = paddy.rename(columns={'paddy_diff_mean': 'diff'})
    
    combined = pd.concat([road, paddy], ignore_index=True)
    combined = combined.dropna(subset=['diff'])
    
    # 1. Line Plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=combined, x='delay_h', y='diff', hue='type', style='type', 
                 markers=True, dashes=False, err_style='band', errorbar=('ci', 68), palette=['red', 'green'])
    plt.axhline(0, color='gray', linestyle='--', alpha=0.5)
    plt.title(f"Filtered Evolution (Sigma=2.0) (N={df.grid_id.nunique()})")
    plt.xlabel("Delay (h)")
    plt.ylabel("Difference (dB)")
    plt.grid(True, alpha=0.3)
    plt.savefig(out_dir / "overall_evolution_filtered.png")
    plt.close()

def main():
    if not SAMPLES_DIR.exists():
        logger.error("Samples dir not found")
        return

    EVO_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load Pairs for Rainfall
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
                r_mean, r_std, p_mean, p_std, r_n, p_n = stats
                
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
                    "road_diff_mean": r_mean,
                    "road_diff_std": r_std, # Filtered std
                    "paddy_diff_mean": p_mean,
                    "paddy_diff_std": p_std,
                    "road_px": r_n,
                    "paddy_px": p_n
                }
                all_data.append(row)
        
    if all_data:
        df_all = pd.DataFrame(all_data)
        out_csv = EVO_DIR / "evolution_data_filtered.csv"
        df_all.to_csv(out_csv, index=False)
        logger.info(f"Saved filtered data to {out_csv} (N={len(df_all)})")
        
        plot_overall_summary(df_all, EVO_DIR)
        logger.info("Generated overall summary plots (Filtered).")
    else:
        logger.warning("No data found.")

if __name__ == "__main__":
    main()
