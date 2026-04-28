import os
import re
import pandas as pd
import numpy as np
import rasterio
from pathlib import Path

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
MASK_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks")

def main():
    print("Diagnosing missing events...")
    
    # Target: August (8), Delay 0
    # In Event CSV: 32 events
    # In Pixel CSV: 5 events
    
    target_month = 8
    target_delay = 0
    
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    
    candidates = []
    
    for grid_dir in grid_dirs:
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            
            dir_name = event_dir.name
            match = re.match(r"delay_([\d\.]+)h_(\d{8})", dir_name)
            if not match: continue
            
            delay_float = float(match.group(1))
            date_str = match.group(2)
            
            if int(delay_float) != target_delay:
                continue
                
            try:
                dt = pd.to_datetime(date_str, format="%Y%m%d")
                if dt.month != target_month:
                    continue
            except:
                continue
                
            candidates.append((grid_dir, event_dir))
            
    print(f"Found {len(candidates)} candidate events for Month {target_month}, Delay {target_delay}.")
    
    # Debug top 10 candidates
    for i, (grid_dir, event_dir) in enumerate(candidates):
        print(f"\n--- Checking Event {i+1}: {grid_dir.name} / {event_dir.name} ---")
        
        # 1. Check Masks
        mask_road = MASK_DIR / grid_dir.name / f"{grid_dir.name}_road_mask.tif"
        mask_paddy = MASK_DIR / grid_dir.name / f"{grid_dir.name}_paddy_mask.tif"
        
        if not mask_road.exists():
            print(f"  [MISSING] Road mask not found: {mask_road}")
        else:
            print(f"  [OK] Road mask exists.")
            
        if not mask_paddy.exists():
            print(f"  [MISSING] Paddy mask not found: {mask_paddy}")
        else:
            print(f"  [OK] Paddy mask exists.")
            
        if not (mask_road.exists() and mask_paddy.exists()):
            continue
            
        # 2. Check S1
        s1_path = event_dir / "after_vv.tif"
        if not s1_path.exists():
            print(f"  [MISSING] S1 file not found: {s1_path}")
            continue
        print(f"  [OK] S1 file exists.")
        
        # 3. Check Intersection
        try:
            with rasterio.open(s1_path) as src:
                s1_data = src.read(1)
                s1_valid = (~np.isnan(s1_data)) & (s1_data != src.nodata)
                h, w = s1_valid.shape
                print(f"  S1 Valid Pixels: {np.sum(s1_valid)}")
                
            with rasterio.open(mask_road) as src:
                road = src.read(1)
                
            with rasterio.open(mask_paddy) as src:
                paddy = src.read(1)
                
            # Resize logic check
            rh, rw = road.shape
            print(f"  Mask Shape: {rh}x{rw}, S1 Shape: {h}x{w}")
            
            min_h, min_w = min(h, rh), min(w, rw)
            
            road_cut = road[:min_h, :min_w]
            s1_cut = s1_valid[:min_h, :min_w]
            
            intersection = np.sum((road_cut > 0) & s1_cut)
            print(f"  Road Intersection: {intersection}")
            
            if intersection == 0:
                print("  [WARN] Zero Road Intersection")
                
        except Exception as e:
            print(f"  [ERROR] {e}")

if __name__ == "__main__":
    main()
