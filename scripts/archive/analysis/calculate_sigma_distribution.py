import os
import sys
import numpy as np
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

from common_utils import setup_logger, HUB_DIR, TARGET_GRIDS

logger = setup_logger("calc_sigma_dist")

# Directories
MASKS_DIR = HUB_DIR / "data" / "masks"
SIGMA_DIR = HUB_DIR / "data" / "sigma"

def calculate_grid_sigma(grid_id):
    grid_mask_dir = MASKS_DIR / grid_id
    if not grid_mask_dir.exists():
        logger.warning(f"Mask directory not found for {grid_id}: {grid_mask_dir}")
        return

    grid_sigma_dir = SIGMA_DIR / grid_id
    grid_sigma_dir.mkdir(parents=True, exist_ok=True)
    
    # Process all PROCESSED masked tifs (highway and paddy)
    tif_files = list(grid_mask_dir.glob("*.tif"))
    if not tif_files:
        logger.warning(f"No tif files found in {grid_mask_dir}")
        return

    logger.info(f"Processing {len(tif_files)} files for {grid_id}...")

    # Set font for plots
    plt.rcParams['font.family'] = 'MS Gothic'

    for tif_path in tif_files:
        try:
            with rasterio.open(tif_path) as src:
                data = src.read(1)
                nodata = src.nodata
                
                # Mask nodata
                if nodata is not None:
                    valid_mask = data != nodata
                    # Also filter NaN if float
                    valid_mask &= ~np.isnan(data)
                else:
                    valid_mask = ~np.isnan(data)
                
                valid_pixels = data[valid_mask]
                
                if valid_pixels.size == 0:
                    # logger.info(f"  No valid pixels in {tif_path.name}")
                    continue
                
                # Input data is already in dB (as confirmed by check_values.py: -35 to +7 range)
                # So we just use the valid pixels directly.
                sigma_db = valid_pixels
                
                if sigma_db.size == 0:
                     continue
                
                # Determine Type from filename
                mask_type = "Unknown"
                if "_highway_mask" in tif_path.name:
                    mask_type = "Road"
                    color = "blue"
                elif "_paddy_mask" in tif_path.name:
                    mask_type = "Paddy"
                    color = "green"
                else:
                    color = "gray"

                # Calculate Stats for User
                min_val = np.min(sigma_db)
                max_val = np.max(sigma_db)
                mean_val = np.mean(sigma_db)
                logger.info(f"  {tif_path.name}: Range=[{min_val:.2f}, {max_val:.2f}], Mean={mean_val:.2f}, Count={len(sigma_db)}")

                # Plot
                plt.figure(figsize=(8, 6))
                sns.histplot(sigma_db, color=color, stat="count", kde=True, bins=50)
                
                # Fixed X-Axis as requested (-40 to 10 dB covers most valid SAR data including outliers)
                plt.xlim(-40, 10)
                # plt.ylim(0, 0.4) # Y-axis auto-scaled
                
                plt.title(f"Backscatter Intensity Distribution (dB)\n{grid_id} - {mask_type}\n{tif_path.name}\nN={len(sigma_db)}")
                plt.xlabel("Sigma0 (dB)")
                plt.ylabel("Count (Pixels)")
                plt.grid(True)
                
                out_name = tif_path.stem + "_hist.png"
                out_path = grid_sigma_dir / out_name
                plt.savefig(out_path)
                plt.close()
                
                # logger.info(f"  Saved {out_name}")

        except Exception as e:
            logger.error(f"Error processing {tif_path.name}: {e}")

def main():
    logger.info("Starting Sigma Distribution Calculation...")
    
    # Ensure Output Dir
    SIGMA_DIR.mkdir(parents=True, exist_ok=True)
    
    for grid_id in TARGET_GRIDS:
        calculate_grid_sigma(grid_id)
        
    logger.info("All done.")

if __name__ == "__main__":
    main()
