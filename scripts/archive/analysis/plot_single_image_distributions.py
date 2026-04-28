import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import sys

# Add current directory to sys.path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from common_utils import (
    setup_logger, parse_summary_txt, read_linear_values, linear_to_db,
    S1_SAMPLES_DIR, RESULT_DIR, TARGET_GRIDS
)

# Japanese font support
plt.rcParams['font.family'] = 'MS Gothic'

OUTPUT_DIR = RESULT_DIR / "20251212" / "single"

def main():
    logger = setup_logger("single_dist_analysis")
    
    logger.info(f"Target Output Directory: {OUTPUT_DIR}")
    
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        grid_out_dir = OUTPUT_DIR / grid_id
        grid_out_dir.mkdir(parents=True, exist_ok=True)
        
        events = parse_summary_txt(grid_id)
        if not events:
            logger.warning(f"No events for {grid_id}")
            continue
            
        # Collect unique scenes
        unique_scenes = set()
        for evt in events:
            unique_scenes.add(evt['after_scene'])
            unique_scenes.add(evt['before_scene'])
            
        logger.info(f"Found {len(unique_scenes)} unique scenes in {grid_id}")
        
        for scene_id in unique_scenes:
             # Process for both Road and Paddy
             for type_name, suffix in [('Road', '_highway_mask.tif'), ('Paddy', '_paddy_mask.tif')]:
                 stem = f"{scene_id}_proc"
                 tif_path = S1_SAMPLES_DIR / grid_id / f"{stem}{suffix}"
                 
                 linear_data = read_linear_values(tif_path)
                 if linear_data is None:
                     # Some scenes might not have masks generated or failed processing
                     # logger.warning(f"Missing data for {scene_id} ({type_name})")
                     continue
                     
                 db_data = linear_to_db(linear_data)
                 
                 # Plot
                 plt.figure(figsize=(10, 6))
                 
                 # Histogram with KDE
                 # Using a fixed range might be good for comparison, but auto-range is safer (e.g. -25 to 5 dB)
                 sns.histplot(db_data, kde=True, stat="density", color='skyblue', edgecolor='black', alpha=0.6)
                 
                 # Calculate Stats
                 mean_val = np.mean(db_data)
                 median_val = np.median(db_data)
                 std_val = np.std(db_data)
                 min_val = np.min(db_data)
                 max_val = np.max(db_data)
                 
                 plt.title(f"Backscatter Distribution (dB) - {grid_id}\nID: {scene_id}\nType: {type_name} / Mean: {mean_val:.2f}, Med: {median_val:.2f}, Std: {std_val:.2f}")
                 plt.xlabel("Backscatter Intensity (dB)")
                 plt.ylabel("Density")
                 plt.grid(True, alpha=0.3, linestyle='--')
                 
                 # Add text box with stats
                 stats_text = (
                     f"Count: {len(db_data)}\n"
                     f"Mean: {mean_val:.2f}\n"
                     f"Median: {median_val:.2f}\n"
                     f"Std: {std_val:.2f}\n"
                     f"Min: {min_val:.2f}\n"
                     f"Max: {max_val:.2f}"
                 )
                 plt.text(0.95, 0.95, stats_text, transform=plt.gca().transAxes,
                          verticalalignment='top', horizontalalignment='right',
                          bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

                 out_name = f"{scene_id}_{type_name}.png"
                 plt.savefig(grid_out_dir / out_name)
                 plt.close()

    logger.info("All processing completed.")

if __name__ == "__main__":
    main()
