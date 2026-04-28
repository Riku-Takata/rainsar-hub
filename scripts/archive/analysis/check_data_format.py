import rasterio
import numpy as np
from pathlib import Path

samples_dir = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")

db_count = 0
linear_count = 0
unknown_count = 0
linear_events = []

for grid_dir in samples_dir.iterdir():
    if not grid_dir.is_dir():
        continue
    for event_dir in grid_dir.iterdir():
        if not event_dir.is_dir():
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
            
            # Heuristic: dB data has negative mean (typically -20 to 0)
            # Linear data has positive mean close to 0-1
            if mean_val < -1:  # Likely dB
                db_count += 1
            elif min_val > -1 and mean_val > 0 and mean_val < 10:  # Likely linear
                linear_count += 1
                linear_events.append(f"{grid_dir.name}/{event_dir.name}")
            else:
                unknown_count += 1
        except Exception as e:
            pass

print(f"=== Data Format Analysis ===")
print(f"Events with dB format: {db_count}")
print(f"Events with Linear format: {linear_count}")
print(f"Events with Unknown format: {unknown_count}")
print()
print(f"Sample Linear-format events (first 30):")
for e in linear_events[:30]:
    print(f"  {e}")
