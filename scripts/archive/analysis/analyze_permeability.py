import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
from scipy.stats import linregress
import sys

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
VV_DATA_DIR = HUB_DIR / "data_vv"
VH_DATA_DIR = HUB_DIR / "data"
# We will use VV's analysis folder to store this combined analysis
OUT_DIR = VV_DATA_DIR / "analysis" / "permeability"

def load_data(base_dir, pol_name):
    csv_path = base_dir / "analysis" / "detailed_analysis_paired.csv"
    if not csv_path.exists():
        print(f"Warning: {csv_path} not found.")
        return None
    df = pd.read_csv(csv_path)
    df['Polarization'] = pol_name
    return df

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Load Data
    df_vv = load_data(VV_DATA_DIR, "VV")
    df_vh = load_data(VH_DATA_DIR, "VH")
    
    if df_vv is None or df_vh is None:
        return

    df_all = pd.concat([df_vv, df_vh], ignore_index=True)

    # 2. Filtering: Focus on Drying Phase (1h <= Delay <= 12h)
    # We exclude very short delays where "wetting" might still be happening or immediate splashing effects dominate.
    # We want to capture the "drainage" phase.
    df_drying = df_all[(df_all["Delay_Hours"] >= 1) & (df_all["Delay_Hours"] <= 12)].copy()

    # 3. Regression Analysis
    stats_results = []
    
    plt.rcParams['font.family'] = 'MS Gothic'
    sns.set_style("whitegrid")
    plt.figure(figsize=(10, 7))
    
    colors = {"Road": "black", "Paddy": "green"}
    styles = {"VV": "solid", "VH": "dashed"}
    
    # Iterate for regression and plotting
    for pol in ["VV", "VH"]:
        for land_type in ["Road", "Paddy"]:
            subset = df_drying[(df_drying["Polarization"] == pol) & (df_drying["Type"] == land_type)]
            
            if len(subset) < 3:
                continue

            x = subset["Delay_Hours"]
            y = subset["Diff_Median"]
            
            slope, intercept, r_value, p_value, std_err = linregress(x, y)
            
            stats_results.append({
                "Polarization": pol,
                "Type": land_type,
                "Decay_Rate_dB_per_hr": slope, # Influence of 1 hour on dB
                "Intercept": intercept,
                "R_squared": r_value**2,
                "Count": len(subset)
            })

            # Plot regression line
            label = f"{pol} - {land_type} (Rate: {slope:.3f} dB/h)"
            sns.regplot(
                x=x, y=y, 
                scatter=True, fit_reg=True, ci=None,
                label=label,
                color=colors[land_type],
                line_kws={"linestyle": styles[pol], "linewidth": 2},
                scatter_kws={"alpha": 0.3, "s": 30}
            )

    plt.title("Signal Decay Rate during Drying Phase (1h - 12h)\nProxy for Permeability/Drainage")
    plt.xlabel("Delay (Hours)")
    plt.ylabel("Backscatter Difference (Median) [dB]")
    plt.axhline(0, color='gray', linestyle='--')
    plt.legend()
    
    out_plot = OUT_DIR / "decay_regression.png"
    plt.savefig(out_plot)
    plt.close()
    print(f"Saved regression plot to {out_plot}")

    # 4. Save Stats
    df_stats = pd.DataFrame(stats_results)
    out_csv = OUT_DIR / "decay_slopes.csv"
    df_stats.to_csv(out_csv, index=False)
    print(f"Saved slope stats to {out_csv}")
    
    print("\n--- Decay Rate Analysis (Slope dB/h) ---")
    print(df_stats.sort_values(by="Decay_Rate_dB_per_hr"))

    # 5. Interpretation Output
    print("\n--- Interpretation ---")
    
    # Compare Road vs Paddy for VV
    try:
        vv_road_slope = df_stats[(df_stats["Polarization"]=="VV") & (df_stats["Type"]=="Road")]["Decay_Rate_dB_per_hr"].values[0]
        vv_paddy_slope = df_stats[(df_stats["Polarization"]=="VV") & (df_stats["Type"]=="Paddy")]["Decay_Rate_dB_per_hr"].values[0]
        
        print(f"[VV Polarization]")
        print(f"  Road Decay Rate: {vv_road_slope:.4f} dB/hr")
        print(f"  Paddy Decay Rate: {vv_paddy_slope:.4f} dB/hr")
        
        if abs(vv_road_slope) > abs(vv_paddy_slope):
             print("  -> Road decays FASTER (steeper negative slope) implies faster drainage (Low Permeability).")
        else:
             print("  -> Paddy decays FASTER.")
             
    except IndexError:
        pass

if __name__ == "__main__":
    main()
