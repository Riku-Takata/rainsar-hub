import rasterio
import numpy as np
import pandas as pd
import sys
from pathlib import Path

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

from common_utils import setup_logger, HUB_DIR, TARGET_GRIDS

logger = setup_logger("calc_mask_stats")

MASKS_DIR = HUB_DIR / "data" / "masks"
STATS_DIR = HUB_DIR / "data" / "stats"

def calculate_grid_stats(grid_id):
    grid_mask_dir = MASKS_DIR / grid_id
    if not grid_mask_dir.exists():
        logger.warning(f"Mask dir not found: {grid_mask_dir}")
        return

    # Prepare list for CSV
    stats_list = []

    tif_files = list(grid_mask_dir.glob("*.tif"))
    logger.info(f"Processing {len(tif_files)} files for {grid_id}...")

    for tif_path in tif_files:
        try:
            with rasterio.open(tif_path) as src:
                data = src.read(1)
                # Valid logic: not NaN and > 0 (assuming dB conversion later, but physically amplitude > 0)
                # Usually masks set invalid to NaN.
                valid_mask = ~np.isnan(data)
                
                # If the data is raw amplitude/intensity, 0 might be valid (but rare for SAR).
                # However, previous script used "valid_pixels > 0" for dB log10 safety.
                # Let's count non-NaN pixels primarily.
                valid_count = np.sum(valid_mask)
                
                # Determine type
                mask_type = "Unknown"
                if "_highway_mask" in tif_path.name:
                    mask_type = "Road"
                elif "_paddy_mask" in tif_path.name:
                    mask_type = "Paddy"

                stats_list.append({
                    "filename": tif_path.name,
                    "mask_type": mask_type,
                    "valid_pixels": valid_count,
                    "total_pixels": data.size,
                    "valid_ratio": valid_count / data.size if data.size > 0 else 0
                })

        except Exception as e:
            logger.error(f"Error reading {tif_path.name}: {e}")

    # Save to CSV
    if stats_list:
        STATS_DIR.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(stats_list)
        # Sort by filename
        df = df.sort_values("filename")
        
        out_csv = STATS_DIR / f"{grid_id}_counts.csv"
        df.to_csv(out_csv, index=False)
        logger.info(f"Saved stats to {out_csv}")
    else:
        logger.warning(f"No stats collected for {grid_id}")

def main():
    logger.info("Starting Mask Stats Calculation...")
    for grid_id in TARGET_GRIDS:
        calculate_grid_stats(grid_id)
    logger.info("All done.")

if __name__ == "__main__":
    main()
