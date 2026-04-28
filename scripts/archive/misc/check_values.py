import rasterio
import numpy as np
from pathlib import Path

# Grid N03145E13095 (Target)
GRID_ID = "N03145E13095"
HUB_DIR = Path(r"d:\sotsuron\rainsar-hub")
SAMPLES_DIR = HUB_DIR / "backend" / "data" / "s1_samples" / GRID_ID
# Wait, S1_SAMPLES_DIR is normally rainsar-hub/data/s1_samples ?? No, verify common_utils.
# I will use common_utils to be safe.
import sys
sys.path.append(str(Path(__file__).parent.parent))
from common_utils import S1_SAMPLES_DIR, HUB_DIR

MASKED_DIR = HUB_DIR / "data" / "masks" / GRID_ID

def stats(arr, name):
    valid = arr[~np.isnan(arr)]
    if valid.size == 0:
        print(f"[{name}] No valid pixels.")
        return
    print(f"[{name}] Shape={arr.shape}, Valid={valid.size}/{arr.size} ({valid.size/arr.size*100:.2f}%)")
    print(f"    Min={valid.min():.4f}, Max={valid.max():.4f}, Mean={valid.mean():.4f}, Median={np.median(valid):.4f}")
    
def main():
    # 1. Find a source proc tif
    if not S1_SAMPLES_DIR.exists():
        # Fallback if common_utils points elsewhere or I need to adjust
        # rainsar-hub/data/s1_samples
        pass

    # list proc files
    grid_dir = S1_SAMPLES_DIR / GRID_ID
    procs = list(grid_dir.glob("*_proc.tif"))
    if not procs:
        print("No source _proc.tif found.")
        return
    
    src_tif = procs[0]
    print(f"Source: {src_tif}")
    
    with rasterio.open(src_tif) as src:
        src_data = src.read(1)
        stats(src_data, "Source (Linear?)")
        
    # 2. Find corresponding mask tif in data/masks
    # Name format: {stem}_highway_mask.tif
    mask_tif = MASKED_DIR / f"{src_tif.stem}_highway_mask.tif"
    if not mask_tif.exists():
        print(f"Mask not found: {mask_tif}")
        return
        
    print(f"Masked: {mask_tif}")
    with rasterio.open(mask_tif) as src:
        mask_data = src.read(1)
        stats(mask_data, "Masked Result")

if __name__ == "__main__":
    main()
