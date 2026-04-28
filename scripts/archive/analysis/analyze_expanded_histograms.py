import rasterio
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
import logging
import sys

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_EXP_DIR = BASE_DIR / "data" / "expanded"
SAMPLES_DIR = DATA_EXP_DIR / "samples"
ANALYSIS_DIR = DATA_EXP_DIR / "analysis"
HIST_DIR = ANALYSIS_DIR / "histograms"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("analyze_hist_exp")

def crop_center(img, target_shape):
    h = min(img.shape[0], target_shape[0])
    w = min(img.shape[1], target_shape[1])
    return img[:h, :w]

def load_data(event_dir):
    paths = {
        "after": event_dir / "after.tif",
        "before": event_dir / "before.tif",
        "mask_road": event_dir / "mask_road.tif",
        "mask_paddy": event_dir / "mask_paddy.tif"
    }
    
    # Read raw first
    raw = {}
    for key, p in paths.items():
        if not p.exists(): return None
        with rasterio.open(p) as src:
            raw[key] = src.read(1)
            
    # Determine common shape (min dims for simple alignment)
    shapes = [v.shape for v in raw.values()]
    min_h = min(s[0] for s in shapes)
    min_w = min(s[1] for s in shapes)
    
    data = {}
    for key, val in raw.items():
        data[key] = val[:min_h, :min_w]
            
    return data

def plot_event_histogram(grid_id, event_name, data, out_dir):
    after = data['after']
    before = data['before']
    mask_road = data['mask_road'] == 1
    mask_paddy = data['mask_paddy'] == 1
    
    road_after = after[mask_road]
    road_before = before[mask_road]
    paddy_after = after[mask_paddy]
    paddy_before = before[mask_paddy]
    
    valid_mask = lambda x: ~np.isnan(x) & (x > -50) & (x < 20)
    
    road_after = road_after[valid_mask(road_after)]
    road_before = road_before[valid_mask(road_before)]
    paddy_after = paddy_after[valid_mask(paddy_after)]
    paddy_before = paddy_before[valid_mask(paddy_before)]
    
    if len(road_after) == 0 or len(paddy_after) == 0:
        # logger.warning(f"  Empty data for {event_name}")
        return

    # Skip plotting if file exists? Handled in main.
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    # 1. Road
    try:
        axes[0].hist(road_before, bins=50, alpha=0.5, label='Before', density=True, color='blue')
        axes[0].hist(road_after, bins=50, alpha=0.5, label='After', density=True, color='red')
        axes[0].set_title(f"Road (N={len(road_after)})")
        axes[0].legend()
    except: pass
    
    # 2. Paddy
    try:
        axes[1].hist(paddy_before, bins=50, alpha=0.5, label='Before', density=True, color='blue')
        axes[1].hist(paddy_after, bins=50, alpha=0.5, label='After', density=True, color='green')
        axes[1].set_title(f"Paddy (N={len(paddy_after)})")
        axes[1].legend()
    except: pass

    # 3. Compare Both (After)
    try:
        axes[2].hist(road_after, bins=50, alpha=0.5, label='Road', density=True, color='red')
        axes[2].hist(paddy_after, bins=50, alpha=0.5, label='Paddy', density=True, color='green')
        axes[2].set_title("After Event")
        axes[2].legend()
    except: pass
    
    plt.suptitle(f"{grid_id} | {event_name}")
    plt.tight_layout()
    
    save_path = out_dir / f"{event_name}.png"
    plt.savefig(save_path)
    plt.close()

def main():
    if not SAMPLES_DIR.exists():
        logger.error("Samples dir not found")
        return

    HIST_DIR.mkdir(parents=True, exist_ok=True)
    
    grids = [d for d in SAMPLES_DIR.iterdir() if d.is_dir()]
    logger.info(f"Scanning {len(grids)} grids for histograms...")
    
    for gid_path in grids:
        gid = gid_path.name
        out_dir = HIST_DIR / gid
        # Check if done? No, individual check.
        out_dir.mkdir(exist_ok=True)
        
        events = [d for d in gid_path.iterdir() if d.is_dir() and d.name.startswith("delay_")]
        
        # logger.info(f"Processing {gid} ({len(events)} events)...")
        for event_dir in events:
            if (out_dir / f"{event_dir.name}.png").exists(): continue
                 
            data = load_data(event_dir)
            if data:
                plot_event_histogram(gid, event_dir.name, data, out_dir)

    logger.info("Histogram generation complete.")

if __name__ == "__main__":
    main()
