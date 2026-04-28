import rasterio
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from pathlib import Path

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

from common_utils import setup_logger, HUB_DIR, TARGET_GRIDS, parse_summary_txt

logger = setup_logger("analyze_sigma_delay")

MASKS_DIR = HUB_DIR / "data" / "masks"
ANALYSIS_DIR = HUB_DIR / "data" / "analysis" / "temporal"
ANALYSIS_FILTERED_DIR = HUB_DIR / "data" / "analysis" / "temporal_filtered"

# Threshold for Short vs Long (Hours)
THRESHOLD_HOURS = 3.0

def analyze_grid_delay(grid_id):
    grid_mask_dir = MASKS_DIR / grid_id
    if not grid_mask_dir.exists():
        logger.warning(f"Mask dir not found: {grid_mask_dir}")
        return

    # 1. Parse Delays
    events = parse_summary_txt(grid_id)
    if not events:
        logger.warning(f"No events found in summary_delay.txt for {grid_id}")
        return
    
    # Map After Scene -> Delay
    # Note: filenames in masks are like S1A_IW_GRDH..._proc_highway_mask.tif
    # The 'after_scene' in summary_delay.txt is likely the product name prefix.
    # We'll create a mapping dict: { product_name_prefix: delay }
    delay_map = {}
    for ev in events:
        if 'after_scene' in ev and 'delay' in ev:
            # remove .zip or .SAFE if present just in case, though summary parser usually returns raw string captured
            name = ev['after_scene'].replace(".zip", "").replace(".SAFE", "")
            delay_map[name] = ev['delay']
            
    # 2. Iterate TIFs and collect data
    tif_files = list(grid_mask_dir.glob("*.tif"))
    data_records = []

    for tif_path in tif_files:
        # Match tif to delay
        # tif name: {product_name}_proc_{type}_mask.tif
        # We try to match startswith
        matched_delay = None
        matched_name = None
        
        for name_key, delay in delay_map.items():
            if tif_path.name.startswith(name_key):
                matched_delay = delay
                matched_name = name_key
                break
        
        if matched_delay is None:
            # logger.warning(f"Could not match delay for {tif_path.name}")
            continue
            
        group = "Short" if matched_delay <= THRESHOLD_HOURS else "Long"
        
        try:
            with rasterio.open(tif_path) as src:
                data = src.read(1)
                valid_mask = ~np.isnan(data)
                if not np.any(valid_mask):
                    continue
                
                # Check pixel validity: process_masks puts NaN for mask-out
                # data is already dB (verified)
                
                valid_data = data[valid_mask]
                
                # Further filtering if needed (e.g. extremely low/high outliers?)
                # For now using all valid pixels
                
                # Mask Type
                mask_type = "Unknown"
                if "_highway_mask" in tif_path.name:
                    mask_type = "Road"
                elif "_paddy_mask" in tif_path.name:
                    mask_type = "Paddy"

                # Sampling to reduce memory if too large?
                # If we have 200k pixels per file and 10 files, 2M floats is fine (8MB).
                # N03285E13005 had 223k paddy pixels * 8 files approx = 1.6M. Safe.
                
                # Append to records
                # Instead of appending 200k rows dataframe, let's keep list of arrays?
                # Or a dataframe with 'Value', 'Group', 'Type'
                
                # Creating DF row by row is slow. Create list of dataframes or dicts?
                # Better: keep arrays and concat at the end
                
                data_records.append({
                    "values": valid_data,
                    "group": group,
                    "type": mask_type,
                    "delay": matched_delay,
                    "filename": tif_path.name
                })
                
        except Exception as e:
            logger.error(f"Error reading {tif_path.name}: {e}")

    if not data_records:
        logger.warning(f"No valid data collected for {grid_id}")
        return

    # 3. Aggregate
    # We want to compare:
    # Road (Short) vs Road (Long)
    # Paddy (Short) vs Paddy (Long)
    
    # Flatten data for plotting
    plot_data = []
    
    stats_summary = []

    for rec in data_records:
        # Summarize this file
        mean_val = np.mean(rec["values"])
        median_val = np.median(rec["values"])
        
        stats_summary.append({
            "filename": rec["filename"],
            "type": rec["type"],
            "delay": rec["delay"],
            "group": rec["group"],
            "count": len(rec["values"]),
            "mean": mean_val,
            "median": median_val
        })
        
        # Add to plot data (subsample if huge?)
        # For boxplot, we need distributions.
        # Let's create a DataFrame for seaborn
        # 'Value', 'Condition' (e.g. "Road-Short")
        
        # If too many points, maybe subsample? 
        # For now, let's try full data.
        vals = rec["values"]
        labels = [f"{rec['type']}-{rec['group']}"] * len(vals)
        groups = [rec['group']] * len(vals)
        types = [rec['type']] * len(vals)
        
        df_chunk = pd.DataFrame({
            "Sigma0 (dB)": vals,
            "Condition": labels,
            "Group": groups,
            "Type": types
        })
        plot_data.append(df_chunk)

    if not plot_data:
        return

    full_df = pd.concat(plot_data, ignore_index=True)
    stats_df = pd.DataFrame(stats_summary)
    
    # 4. Save Stats
    out_dir = ANALYSIS_DIR / grid_id
    out_dir.mkdir(parents=True, exist_ok=True)
    
    stats_csv = out_dir / f"{grid_id}_temporal_stats.csv"
    stats_df.to_csv(stats_csv, index=False)
    logger.info(f"Saved stats to {stats_csv}")
    
    # 5. Plot
    # Set Japanese font
    plt.rcParams['font.family'] = 'MS Gothic'
    
    # Consistent Palette and Order
    palette_dict = {"Short": "#ff7f0e", "Long": "#1f77b4"} # Orange for Short, Blue for Long
    hue_order = ["Short", "Long"]
    
    # Boxplot
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=full_df, x="Type", y="Sigma0 (dB)", hue="Group", palette=palette_dict, hue_order=hue_order)
    plt.title(f"Backscatter Intensity by Delay Group (Short<=3h, Long>3h)\nGrid: {grid_id}")
    plt.grid(True, axis='y')
    plt.ylim(-40, 10) # Extended range as requested
    
    plot_path = out_dir / f"{grid_id}_boxplot.png"
    plt.savefig(plot_path)
    plt.close()
    
    # Violin Plot (Optional, for distribution shape)
    plt.figure(figsize=(10, 6))
    sns.violinplot(data=full_df, x="Type", y="Sigma0 (dB)", hue="Group", split=True, inner="quart", palette=palette_dict, hue_order=hue_order)
    plt.title(f"Distribution Shape by Delay Group\nGrid: {grid_id}")
    plt.grid(True, axis='y')
    plt.ylim(-40, 10)
    
    violin_path = out_dir / f"{grid_id}_violin.png"
    plt.savefig(violin_path)
    plt.close()
    
    logger.info(f"Saved plots to {out_dir}")

    # --- Filtered Analysis (IQR) ---
    logger.info("  Performing Filtered Analysis (IQR)...")
    
    filtered_dfs = []
    
    # Filter per group (Type + Group)
    for (ctype, cgroup), group_df in full_df.groupby(["Type", "Group"]):
        q1 = group_df["Sigma0 (dB)"].quantile(0.25)
        q3 = group_df["Sigma0 (dB)"].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # Filter
        df_clean = group_df[(group_df["Sigma0 (dB)"] >= lower_bound) & (group_df["Sigma0 (dB)"] <= upper_bound)]
        filtered_dfs.append(df_clean)
        
    if not filtered_dfs:
        logger.warning("All data filtered out?")
        return
        
    full_df_filtered = pd.concat(filtered_dfs, ignore_index=True)
    
    # Stats for Filtered
    stats_filtered = []
    for (ctype, cgroup), group_df in full_df_filtered.groupby(["Type", "Group"]):
         stats_filtered.append({
            "Type": ctype,
            "Group": cgroup,
            "Count": len(group_df),
            "Mean": group_df["Sigma0 (dB)"].mean(),
            "Median": group_df["Sigma0 (dB)"].median(),
            "Std": group_df["Sigma0 (dB)"].std()
        })
    stats_filtered_df = pd.DataFrame(stats_filtered)

    # Save Filtered
    out_dir_filtered = ANALYSIS_FILTERED_DIR / grid_id
    out_dir_filtered.mkdir(parents=True, exist_ok=True)
    
    stats_csv_filtered = out_dir_filtered / f"{grid_id}_temporal_stats_filtered.csv"
    stats_filtered_df.to_csv(stats_csv_filtered, index=False)
    
    # Boxplot Filtered
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=full_df_filtered, x="Type", y="Sigma0 (dB)", hue="Group", palette=palette_dict, hue_order=hue_order)
    plt.title(f"Backscatter Intensity (Filtered) by Delay Group\nGrid: {grid_id}")
    plt.grid(True, axis='y')
    plt.ylim(-40, 10)
    
    plot_path_filtered = out_dir_filtered / f"{grid_id}_boxplot_filtered.png"
    plt.savefig(plot_path_filtered)
    plt.close()
    
    # Violin Filtered
    plt.figure(figsize=(10, 6))
    sns.violinplot(data=full_df_filtered, x="Type", y="Sigma0 (dB)", hue="Group", split=True, inner="quart", palette=palette_dict, hue_order=hue_order)
    plt.title(f"Distribution Shape (Filtered) by Delay Group\nGrid: {grid_id}")
    plt.grid(True, axis='y')
    plt.ylim(-40, 10)
    
    violin_path_filtered = out_dir_filtered / f"{grid_id}_violin_filtered.png"
    plt.savefig(violin_path_filtered)
    plt.close()

    logger.info(f"Saved filtered plots to {out_dir_filtered}")

def main():
    logger.info("Starting Temporal Analysis...")
    for grid_id in TARGET_GRIDS:
        analyze_grid_delay(grid_id)
    logger.info("All done.")

if __name__ == "__main__":
    main()
