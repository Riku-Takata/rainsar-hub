import rasterio
import numpy as np
import sys
from pathlib import Path

# Target Dir
TARGET_DIR = Path(r"d:\sotsuron\rainsar-hub\data\masks\N03145E13095")

def check_file(tif_path):
    with rasterio.open(tif_path) as src:
        data = src.read(1)
        total = data.size
        
        nans = np.isnan(data).sum()
        zeros = (data == 0).sum()
        # Zeros might be counted in NaNs if float? No, separate.
        
        # Valid for dB (>0)
        valid = (data > 0).sum()
        
        print(f"File: {tif_path.name}")
        print(f"  Total: {total}")
        print(f"  NaNs:  {nans} ({nans/total*100:.1f}%)")
        print(f"  Zeros: {zeros} ({zeros/total*100:.1f}%)")
        print(f"  Valid (>0): {valid} ({valid/total*100:.1f}%)")
        print("-" * 30)

def main():
    if not TARGET_DIR.exists():
        print(f"Dir not found: {TARGET_DIR}")
        return

    for tif in TARGET_DIR.glob("*.tif"):
        check_file(tif)

if __name__ == "__main__":
    main()
