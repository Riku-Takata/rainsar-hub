import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
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

OUTPUT_DIR = RESULT_DIR / "20251212" / "before_check"
STATS_FILE = RESULT_DIR / "20251212" / "before_stats.csv"

def main():
    logger = setup_logger("before_dist_analysis")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output Directory: {OUTPUT_DIR}")
    
    all_stats = []
    
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        
        events = parse_summary_txt(grid_id)
        if not events:
            logger.warning(f"No events for {grid_id}")
            continue
            
        # Unique Before Scenes per Grid to avoid duplicates
        # But user might want per Event? Let's assume unique Scenes is better to avoid double counting in stats.
        # However, for the table, linking to the Event Date is helpful context.
        # I'll iterate events but keep track of processed scenes to avoid re-plotting, 
        # but I'll add rows for each event (or unique scene). 
        # If the same scene is used for multiple events, it has the same stats. 
        # I will store unique (Grid, Scene) pairs.
        
        seen_scenes = set()
        
        for evt in events:
            before_scene = evt['before_scene']
            if before_scene in seen_scenes:
                continue
            seen_scenes.add(before_scene)
            
            logger.info(f"  Analyzing Scene: {before_scene} (Date: {evt['date']})")
            
            # Data Container for Plotting
            plot_data = {}
            
            # Process Road and Paddy
            for type_name, suffix in [('Road', '_highway_mask.tif'), ('Paddy', '_paddy_mask.tif')]:
                pass  # Just for structure
            
            # Load Data
            road_path = S1_SAMPLES_DIR / grid_id / f"{before_scene}_proc_highway_mask.tif"
            paddy_path = S1_SAMPLES_DIR / grid_id / f"{before_scene}_proc_paddy_mask.tif"
            
            road_lin = read_linear_values(road_path)
            paddy_lin = read_linear_values(paddy_path)
            
            if road_lin is None or paddy_lin is None:
                logger.warning(f"    Missing data for {before_scene}")
                continue
                
            road_db = linear_to_db(road_lin)
            paddy_db = linear_to_db(paddy_lin)
            
            # Calculate Stats
            for name, data in [('Road', road_db), ('Paddy', paddy_db)]:
                mean_val = np.mean(data)
                median_val = np.median(data)
                std_val = np.std(data)
                
                all_stats.append({
                    'GridID': grid_id,
                    'SceneID': before_scene,
                    'EventDate': evt['date'], # Representative date
                    'Type': name,
                    'Mean': mean_val,
                    'Median': median_val,
                    'Std': std_val,
                    'Min': np.min(data),
                    'Max': np.max(data),
                    'Count': len(data)
                })
            
            # Plot Comparison Histogram
            plt.figure(figsize=(10, 6))
            sns.kdeplot(paddy_db, fill=True, color='green', alpha=0.3, label='Paddy')
            sns.kdeplot(road_db, fill=True, color='gray', alpha=0.3, label='Road')
            
            # Vertical lines for Medians
            plt.axvline(np.median(paddy_db), color='green', linestyle='--', alpha=0.8)
            plt.axvline(np.median(road_db), color='gray', linestyle='--', alpha=0.8)
            
            plt.title(f"Before Scene Distribution: {grid_id}\n{before_scene}")
            plt.xlabel("Backscatter Intensity (dB)")
            plt.ylabel("Density")
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            out_file = OUTPUT_DIR / f"{grid_id}_{before_scene}_dist.png"
            plt.savefig(out_file)
            plt.close()

    # Save to CSV
    if all_stats:
        df = pd.DataFrame(all_stats)
        # Reorder columns
        cols = ['GridID', 'EventDate', 'SceneID', 'Type', 'Mean', 'Median', 'Std', 'Min', 'Max', 'Count']
        df = df[cols]
        df.sort_values(by=['GridID', 'EventDate', 'Type'], inplace=True)
        
        df.to_csv(STATS_FILE, index=False, encoding='utf-8-sig')
        logger.info(f"Examples of stats:\n{df.head()}")
        logger.info(f"Saved statistics to {STATS_FILE}")
    else:
        logger.warning("No statistics generated.")

if __name__ == "__main__":
    main()
