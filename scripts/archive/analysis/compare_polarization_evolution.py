import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import sys

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
VV_DATA_DIR = HUB_DIR / "data_vv"
VH_DATA_DIR = HUB_DIR / "data"
OUT_DIR = VV_DATA_DIR / "analysis" / "comparison_vv_vh"

def load_data(base_dir, pol_name):
    csv_path = base_dir / "analysis" / "detailed_analysis_paired.csv"
    if not csv_path.exists():
        print(f"Warning: {csv_path} not found.")
        return None
    df = pd.read_csv(csv_path)
    df['Polarization'] = pol_name
    return df

def assign_bin(delay):
    if delay <= 1:
        return "0-1h"
    elif delay <= 3:
        return "1-3h"
    elif delay <= 6:
        return "3-6h"
    else:
        return "6h+"

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Load Data
    df_vv = load_data(VV_DATA_DIR, "VV")
    df_vh = load_data(VH_DATA_DIR, "VH")
    
    if df_vv is None or df_vh is None:
        return

    # 2. Combine and Bin
    df_all = pd.concat([df_vv, df_vh], ignore_index=True)
    df_all["Time_Bin"] = df_all["Delay_Hours"].apply(assign_bin)
    
    # Define order for plotting
    bin_order = ["0-1h", "1-3h", "3-6h", "6h+"]
    df_all["Time_Bin"] = pd.Categorical(df_all["Time_Bin"], categories=bin_order, ordered=True)
    
    # 3. Aggregate
    # Calculate Mean of the Median Differences for each bin
    agg = df_all.groupby(["Type", "Polarization", "Time_Bin"], observed=False)["Diff_Median"].agg(['mean', 'std', 'count']).reset_index()
    
    # 4. Plotting
    plt.rcParams['font.family'] = 'MS Gothic'
    sns.set_style("whitegrid")
    
    # Plot for Road
    plt.figure(figsize=(10, 6))
    road_data = agg[agg["Type"] == "Road"]
    sns.lineplot(data=road_data, x="Time_Bin", y="mean", hue="Polarization", style="Polarization", markers=True, dashes=False, linewidth=2.5, palette={"VV": "blue", "VH": "red"})
    plt.title("Evolution of Backscatter Difference on ROADS (VV vs VH)")
    plt.ylabel("Mean Difference [dB]")
    plt.xlabel("Delay Time Bin")
    plt.axhline(0, color='gray', linestyle='--')
    plt.savefig(OUT_DIR / "evolution_road_comparison.png")
    plt.close()

    # Plot for Paddy
    plt.figure(figsize=(10, 6))
    paddy_data = agg[agg["Type"] == "Paddy"]
    sns.lineplot(data=paddy_data, x="Time_Bin", y="mean", hue="Polarization", style="Polarization", markers=True, dashes=False, linewidth=2.5, palette={"VV": "blue", "VH": "red"})
    plt.title("Evolution of Backscatter Difference on PADDIES (VV vs VH)")
    plt.ylabel("Mean Difference [dB]")
    plt.xlabel("Delay Time Bin")
    plt.axhline(0, color='gray', linestyle='--')
    plt.savefig(OUT_DIR / "evolution_paddy_comparison.png")
    plt.close()
    
    # 5. Export Table
    out_csv = OUT_DIR / "evolution_binned_stats.csv"
    agg.to_csv(out_csv, index=False)
    print(f"Saved analysis to {OUT_DIR}")
    print("\n--- Binned Statistics ---")
    print(agg)

if __name__ == "__main__":
    main()
