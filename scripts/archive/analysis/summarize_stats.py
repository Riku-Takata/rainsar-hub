import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Setup paths
HUB_DIR = Path(__file__).resolve().parent.parent.parent
STATS_CSV_PATH = HUB_DIR / "data" / "analysis" / "before_after_stats.csv"

def main():
    if not STATS_CSV_PATH.exists():
        print(f"Error: {STATS_CSV_PATH} not found.")
        return

    df = pd.read_csv(STATS_CSV_PATH)
    print(f"Loaded {len(df)} records from {STATS_CSV_PATH}")

    # Add Delay Group
    df["Delay_Group"] = df["Delay_Hours"].apply(lambda x: "Short (<=3h)" if x <= 3 else "Long (>3h)")
    
    print("\n" + "="*60)
    print(" NUMERICAL SUMMARY: Diff (After - Before) [dB]")
    print("="*60)
    
    # Analyze by Type and Delay Group
    # Metrics: Diff_Mean, Diff_Median
    
    for metric in ["Diff_Mean", "Diff_Median"]:
        print(f"\n--- Metric: {metric} ---")
        
        # Group by [Delay_Group, Type]
        grouped = df.groupby(["Delay_Group", "Type"])[metric].agg(["count", "mean", "median", "std", "min", "max"])
        print(grouped)
        
        # Check for outliers (e.g., beyond mean +/- 2*std)
        print("\n[Outlier Check (> 2*std from group mean)]")
        for name, group in df.groupby(["Delay_Group", "Type"]):
            mean_val = group[metric].mean()
            std_val = group[metric].std()
            lower = mean_val - 2 * std_val
            upper = mean_val + 2 * std_val
            
            outliers = group[(group[metric] < lower) | (group[metric] > upper)]
            
            if not outliers.empty:
                print(f"\nGroup: {name}")
                print(f"Boundaries: {lower:.2f} to {upper:.2f}")
                for _, row in outliers.iterrows():
                    print(f"  Grid: {row['GridID']}, RainDate: {row['RainDate']}, Delay: {row['Delay_Hours']}h, Val: {row[metric]:.2f}")

if __name__ == "__main__":
    main()
