import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from pathlib import Path

# Setup sys.path
SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SCRIPTS_DIR))

from common_utils import setup_logger, HUB_DIR

logger = setup_logger("global_trend")

FILTERED_DIR = HUB_DIR / "data" / "analysis" / "temporal_filtered"
ANALYSIS_DIR = HUB_DIR / "data" / "analysis"

def main():
    logger.info("Starting Global Trend Analysis...")
    
    # 1. Collect all stats CSVs
    all_stats_files = list(FILTERED_DIR.glob("*/*_temporal_stats_filtered.csv"))
    
    if not all_stats_files:
        logger.error("No input files found!")
        return
        
    logger.info(f"Found {len(all_stats_files)} stats files.")
    
    combined_data = []
    
    for f in all_stats_files:
        # File path structure: .../{grid_id}/{grid_id}_temporal_stats_filtered.csv
        grid_id = f.parent.name
        
        try:
            df = pd.read_csv(f)
            df["GridID"] = grid_id
            combined_data.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {f}: {e}")
            
    if not combined_data:
        logger.error("No valid data loaded.")
        return

    full_df = pd.concat(combined_data, ignore_index=True)
    
    # full_df columns: Type, Group, Count, Mean, Median, Std, GridID
    
    # 2. Pivot to compare Paddy vs Road
    # We want rows: GridID, Group, Mean_Paddy, Mean_Road, Diff
    
    # Pivot
    pivot_df = full_df.pivot_table(
        index=["GridID", "Group"], 
        columns="Type", 
        values="Mean"
    ).reset_index()
    
    # Pivot columns might be: GridID, Group, Paddy, Road (if those are the types)
    if "Paddy" not in pivot_df.columns or "Road" not in pivot_df.columns:
        logger.error("Data missing 'Paddy' or 'Road' types.")
        logger.info(f"Columns found: {pivot_df.columns}")
        return
        
    # Calculate Diff
    pivot_df["Diff_Mean"] = pivot_df["Paddy"] - pivot_df["Road"]
    
    # Save Summary
    out_csv = ANALYSIS_DIR / "global_trend_stats.csv"
    pivot_df.to_csv(out_csv, index=False)
    logger.info(f"Saved global stats to {out_csv}")
    
    # 3. Visualization
    plt.rcParams['font.family'] = 'MS Gothic'
    
    # Set nice style/palette
    palette_dict = {"Short": "#ff7f0e", "Long": "#1f77b4"} # Orange, Blue
    hue_order = ["Short", "Long"]
    
    # (A) Scatter Polot: Road vs Paddy
    plt.figure(figsize=(8, 8))
    sns.scatterplot(data=pivot_df, x="Road", y="Paddy", hue="Group", palette=palette_dict, hue_order=hue_order, s=100, alpha=0.8)
    
    # Add x=y line
    min_val = min(pivot_df["Road"].min(), pivot_df["Paddy"].min()) - 1
    max_val = max(pivot_df["Road"].max(), pivot_df["Paddy"].max()) + 1
    plt.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.5, label="y=x (Equal)")
    
    plt.title("Mean Backscatter Intensity: Road vs Paddy (Global)")
    plt.xlabel("Road Mean Sigma0 (dB)")
    plt.ylabel("Paddy Mean Sigma0 (dB)")
    plt.xlim(min_val, max_val)
    plt.ylim(min_val, max_val)
    plt.legend()
    plt.grid(True)
    
    scatter_path = ANALYSIS_DIR / "global_scatter_road_vs_paddy.png"
    plt.savefig(scatter_path)
    plt.close()
    
    # (B) Histogram of Differences
    plt.figure(figsize=(10, 6))
    sns.histplot(data=pivot_df, x="Diff_Mean", hue="Group", palette=palette_dict, hue_order=hue_order, kde=True, bins=15, multiple="layer")
    
    plt.axvline(0, color='k', linestyle='--', alpha=0.5)
    plt.title("Distribution of Differences (Paddy - Road) across Grids")
    plt.xlabel("Difference (Mean Paddy - Mean Road) [dB]")
    plt.ylabel("Count of Grids")
    plt.grid(True)
    
    hist_path = ANALYSIS_DIR / "global_diff_hist.png"
    plt.savefig(hist_path)
    plt.close()

    # (C) Boxplot of Differences
    plt.figure(figsize=(6, 6))
    sns.boxplot(data=pivot_df, x="Group", y="Diff_Mean", palette=palette_dict, order=hue_order)
    plt.axhline(0, color='k', linestyle='--', alpha=0.5)
    plt.title("Difference (Paddy - Road) by Delay Group")
    plt.ylabel("Difference [dB]")
    plt.grid(True)
    
    box_path = ANALYSIS_DIR / "global_diff_boxplot.png"
    plt.savefig(box_path)
    plt.close()

    logger.info(f"Saved plots to {ANALYSIS_DIR}")

if __name__ == "__main__":
    main()
