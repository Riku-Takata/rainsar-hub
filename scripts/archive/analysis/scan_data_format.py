"""
Comprehensive scan of all samples to:
1. Classify each event as dB-format or Linear-format
2. Count events per Month/Delay for each format type
3. Identify which data set should be used for each delay
"""
import rasterio
import numpy as np
from pathlib import Path
import pandas as pd
import re
from collections import defaultdict

samples_dir = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
TARGET_MONTHS = [8, 10]

results = []

for grid_dir in samples_dir.iterdir():
    if not grid_dir.is_dir():
        continue
    for event_dir in grid_dir.iterdir():
        if not event_dir.is_dir():
            continue
        
        # Parse event name
        dir_name = event_dir.name
        match = re.match(r"delay_([\d\.]+)h_(\d{8})", dir_name)
        if not match:
            continue
            
        delay_float = float(match.group(1))
        date_str = match.group(2)
        delay_int = int(delay_float)
        
        if not (0 <= delay_int <= 11):
            continue
        
        try:
            dt = pd.to_datetime(date_str, format="%Y%m%d")
            month = dt.month
            year = dt.year
        except:
            continue
            
        if month not in TARGET_MONTHS:
            continue
        
        vv_path = event_dir / "after_vv.tif"
        if not vv_path.exists():
            continue
        
        try:
            with rasterio.open(vv_path) as src:
                vv = src.read(1)
                nodata = src.nodata
                
            if nodata is not None:
                valid = (vv != nodata) & (~np.isnan(vv))
            else:
                valid = ~np.isnan(vv)
                
            vv_valid = vv[valid]
            if len(vv_valid) == 0:
                continue
                
            mean_val = np.mean(vv_valid)
            min_val = np.min(vv_valid)
            
            # Classify format
            if mean_val < -1:
                data_format = "dB"
            elif min_val > -1 and mean_val > 0 and mean_val < 10:
                data_format = "Linear"
            else:
                data_format = "Unknown"
                
            results.append({
                "grid_id": grid_dir.name,
                "event_dir": dir_name,
                "year": year,
                "month": month,
                "delay_int": delay_int,
                "data_format": data_format,
                "mean_val": mean_val,
                "min_val": min_val,
            })
        except Exception as e:
            pass

df = pd.DataFrame(results)
print(f"Total events scanned: {len(df)}")
print()

# Summary by format
print("=== Event Count by Data Format ===")
print(df.groupby("data_format").size())
print()

# Summary by Month/Delay/Format
print("=== Event Count by Month/Delay/Format ===")
pivot = df.pivot_table(index=["month", "delay_int"], columns="data_format", aggfunc="size", fill_value=0)
print(pivot)
print()

# Save detailed results
output_path = Path(r"D:\sotsuron\rainsar-hub\data\analysis\data_format_scan.csv")
df.to_csv(output_path, index=False)
print(f"Detailed results saved to: {output_path}")
