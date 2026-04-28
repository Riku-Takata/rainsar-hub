import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Setup paths
HUB_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(HUB_DIR / "scripts"))
from common_utils import DATA_DIR

STATS_CSV_PATH = DATA_DIR / "analysis" / "before_after_stats.csv"
OUT_DETAILED_CSV = DATA_DIR / "analysis" / "detailed_analysis_paired.csv"
OUT_SUMMARY_CSV = DATA_DIR / "analysis" / "summary_trends_paired.csv"

def main():
    if not STATS_CSV_PATH.exists():
        print(f"Error: {STATS_CSV_PATH} not found.")
        return

    df = pd.read_csv(STATS_CSV_PATH)
    print(f"Loaded {len(df)} records.")

    # 1. Add Delay Group
    # Short: <= 3h, Long: > 3h
    df["Delay_Group"] = df["Delay_Hours"].apply(lambda x: "Short (<=3h)" if x <= 3 else "Long (>3h)")

    # 1.5 Filter: Keep only Grids that have BOTH Short and Long events
    # Group by GridID and check unique Delay_Groups
    valid_grids = []
    print("\n--- Filtering for Paired Grids (Both Short & Long) ---")
    for grid_id, group in df.groupby("GridID"):
        groups_present = group["Delay_Group"].unique()
        if "Short (<=3h)" in groups_present and "Long (>3h)" in groups_present:
            valid_grids.append(grid_id)
            
    original_count = len(df)
    df = df[df["GridID"].isin(valid_grids)]
    print(f"Kept {len(valid_grids)} grids with both Short/Long events.")
    print(f"Records: {original_count} -> {len(df)}")
    
    if df.empty:
        print("Error: No paired grids found! Cannot proceed.")
        return

    # 2. Outlier Detection (using Median Diff for robustness)
    # Calculate Z-score per group (Type + Delay_Group)
    df["Diff_Median_Group_Mean"] = df.groupby(["Delay_Group", "Type"])["Diff_Median"].transform("mean")
    df["Diff_Median_Group_Std"] = df.groupby(["Delay_Group", "Type"])["Diff_Median"].transform("std")
    
    # Avoid division by zero
    df["Diff_Median_Z"] = (df["Diff_Median"] - df["Diff_Median_Group_Mean"]) / df["Diff_Median_Group_Std"].replace(0, 1)
    
    # Flag Outliers (|Z| > 2)
    df["Is_Outlier"] = df["Diff_Median_Z"].abs() > 2

    # 3. Export Detailed Report
    # Select and reorder columns for clarity
    cols = [
        "GridID", "RainDate", "Delay_Hours", "Delay_Group", "Type",
        "Diff_Median", "Is_Outlier", "Diff_Median_Z",
        "Diff_Mean", "Median_Before", "Median_After", "Mean_Before", "Mean_After"
    ]
    df_detailed = df[cols].sort_values(by=["Delay_Hours", "GridID"])
    df_detailed.to_csv(OUT_DETAILED_CSV, index=False)
    print(f"Saved detailed report to {OUT_DETAILED_CSV}")

    # 4. Export Summary Trends
    summary = df.groupby(["Delay_Group", "Type"])["Diff_Median"].agg(
        Count="count",
        Mean="mean",
        Median="median",
        Std="std",
        Min="min",
        Max="max"
    ).reset_index()
    
    summary.to_csv(OUT_SUMMARY_CSV, index=False)
    print(f"Saved summary trends to {OUT_SUMMARY_CSV}")
    
    print("\n[New Summary Stats (Paired Only)]")
    print(summary)

if __name__ == "__main__":
    main()
