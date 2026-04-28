import pandas as pd
from pathlib import Path
import os
import re

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
PIXEL_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\monthly_delay_pixel_counts_detailed.csv")
MASK_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks")

def main():
    print("Finding missing events...")
    
    # 1. Load Pixel Counts (Successful events)
    if not PIXEL_CSV.exists():
        print("Pixel CSV not found.")
        return
        
    df_pix = pd.read_csv(PIXEL_CSV)
    # Filter for Aug(8) and Oct(10)
    df_pix = df_pix[df_pix['month'].isin([8, 10])]
    
    # Create set of successful (grid_id, event_dir)
    successful_events = set(zip(df_pix['grid_id'], df_pix['event_dir']))
    print(f"Successful events in pixel CSV (Aug/Oct): {len(successful_events)}")
    
    # 2. Iterate all directories to find 'Total Events' (matching plot logic)
    missing_events = []
    
    grid_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    
    for grid_dir in grid_dirs:
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir(): continue
            
            dir_name = event_dir.name
            match = re.match(r"delay_([\d\.]+)h_(\d{8})", dir_name)
            if not match: continue
            
            delay_float = float(match.group(1))
            date_str = match.group(2)
            delay_int = int(delay_float)
            
            if not (0 <= delay_int <= 11): continue
            
            try:
                dt = pd.to_datetime(date_str, format="%Y%m%d")
                month = dt.month
            except: continue
            
            if month not in [8, 10]: continue
            
            # This is a target event
            key = (grid_dir.name, dir_name)
            
            if key not in successful_events:
                # Diagnose WHY
                reason = "Unknown"
                
                mask_dir = MASK_DIR / grid_dir.name
                has_geojson = (mask_dir / f"{grid_dir.name}_motorway.geojson").exists() or \
                              (mask_dir / f"{grid_dir.name}_paddy.geojson").exists()
                              
                has_tif = (mask_dir / f"{grid_dir.name}_road_mask.tif").exists() or \
                          (mask_dir / f"{grid_dir.name}_paddy_mask.tif").exists()
                
                if not mask_dir.exists():
                    reason = "Mask Directory Missing"
                elif not (has_geojson or has_tif):
                    reason = "No Mask Files (TIF or GeoJSON)"
                else:
                    reason = "Zero Pixel Intersection (Mask exists but no overlap?)"
                
                missing_events.append({
                    "grid_id": grid_dir.name,
                    "event_dir": dir_name,
                    "month": month,
                    "delay": delay_int,
                    "reason": reason
                })
                
    df_missing = pd.DataFrame(missing_events)
    print(f"Missing events: {len(df_missing)}")
    
    if len(df_missing) > 0:
        print("\nBreakdown by Reason:")
        print(df_missing['reason'].value_counts())
        
        print("\nSample missing events (Zero Intersection):")
        print(df_missing[df_missing['reason'].str.contains("Zero")].head())
        
        print("\nSample missing events (No Mask Files):")
        print(df_missing[df_missing['reason'].str.contains("No Mask")].head())

        # Save report
        df_missing.to_csv(r"D:\sotsuron\rainsar-hub\data\analysis\missing_events_report.csv", index=False)
        print("Saved missing events report.")

if __name__ == "__main__":
    main()
