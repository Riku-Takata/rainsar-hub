import rasterio
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
import logging
import re

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_FINAL_DIR = BASE_DIR / "data" / "final"
SAMPLES_DIR = DATA_FINAL_DIR / "samples"
ANALYSIS_DIR = DATA_FINAL_DIR / "analysis"
EVO_DIR = ANALYSIS_DIR / "evolution"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("analyze_evo")

def get_stats(event_dir):
    paths = {
        "after": event_dir / "after.tif",
        "before": event_dir / "before.tif",
        "mask_road": event_dir / "mask_road.tif",
        "mask_paddy": event_dir / "mask_paddy.tif"
    }
    for p in paths.values():
        if not p.exists(): return None

    raw = {}
    for key, p in paths.items():
        if not p.exists(): return None
        with rasterio.open(p) as src:
            raw[key] = src.read(1)
            
    # Crop to min dims
    shapes = [v.shape for v in raw.values()]
    min_h = min(s[0] for s in shapes)
    min_w = min(s[1] for s in shapes)
    
    data = {k: v[:min_h, :min_w] for k, v in raw.items()}
    
    try:
        after = data["after"]
        before = data["before"]
        mask_road = data["mask_road"] == 1
        mask_paddy = data["mask_paddy"] == 1
        
        # Calculate Diff
        diff = after - before
        
        # Valid Mask
        valid = ~np.isnan(diff) & (after > -50) & (before > -50)
        
        # Road Stats
        r_vals = diff[mask_road & valid]
        r_mean = np.mean(r_vals) if len(r_vals) > 0 else np.nan
        r_std = np.std(r_vals) if len(r_vals) > 0 else np.nan
        
        # Paddy Stats
        p_vals = diff[mask_paddy & valid]
        p_mean = np.mean(p_vals) if len(p_vals) > 0 else np.nan
        p_std = np.std(p_vals) if len(p_vals) > 0 else np.nan
        
        return r_mean, r_std, p_mean, p_std, len(r_vals), len(p_vals)
        
    except Exception as e:
        logger.error(f"Error reading {event_dir}: {e}")
        return None

def parse_delay(dirname):
    # dirname e.g. delay_5h_20200101
    m = re.search(r"delay_(\d+)h_", dirname)
    if m:
        return int(m.group(1))
    return None

def main():
    if not SAMPLES_DIR.exists():
        logger.error("Samples dir not found")
        return

    EVO_DIR.mkdir(parents=True, exist_ok=True)
    
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    logger.info(f"Scanning {len(grids)} grids...")
    
    all_data = []
    
    for gid_path in grids:
        gid = gid_path.name
        events = [d for d in gid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
        
        grid_data = []
        for event_dir in events:
            delay = parse_delay(event_dir.name)
            if delay is None: continue
            
            stats = get_stats(event_dir)
            if stats:
                r_mean, r_std, p_mean, p_std, r_n, p_n = stats
                row = {
                    "grid_id": gid,
                    "event": event_dir.name,
                    "delay_h": delay,
                    "road_diff_mean": r_mean,
                    "road_diff_std": r_std,
                    "paddy_diff_mean": p_mean,
                    "paddy_diff_std": p_std,
                    "road_px": r_n,
                    "paddy_px": p_n
                }
                grid_data.append(row)
                all_data.append(row)
        
        if not grid_data: continue
        
        # Plot for this Grid
        df_grid = pd.DataFrame(grid_data).sort_values("delay_h")
        
        plt.figure(figsize=(10, 6))
        plt.errorbar(df_grid['delay_h'], df_grid['road_diff_mean'], yerr=df_grid['road_diff_std'], 
                     fmt='-o', label='Road', capsize=5, color='red')
        plt.errorbar(df_grid['delay_h'], df_grid['paddy_diff_mean'], yerr=df_grid['paddy_diff_std'], 
                     fmt='-o', label='Paddy', capsize=5, color='green')
                     
        plt.title(f"Backscatter Difference Evolution (Grid: {gid})")
        plt.xlabel("Delay (hours)")
        plt.ylabel("Difference (After - Before) [dB]")
        plt.axhline(0, color='gray', linestyle='--')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        save_path = EVO_DIR / f"{gid}_evolution.png"
        plt.savefig(save_path)
        plt.close()
        
    # Save CSV
    if all_data:
        df_all = pd.DataFrame(all_data)
        out_csv = EVO_DIR / "evolution_data.csv"
        df_all.to_csv(out_csv, index=False)
        logger.info(f"Saved consolidated data to {out_csv}")
    else:
        logger.warning("No data found to analyze.")

    logger.info("Evolution analysis complete.")

if __name__ == "__main__":
    main()
