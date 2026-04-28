import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.stats import linregress
import sys

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
VV_DATA_DIR = HUB_DIR / "data_vv"
OUT_DIR = VV_DATA_DIR / "analysis" / "mid_decay"

def load_data():
    csv_path = VV_DATA_DIR / "analysis" / "detailed_analysis_paired.csv"
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    return df

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = load_data()
    if df is None: return

    # Filter for 1.0 < Delay <= 6.0
    # The user asked for "1-3h to 3-6h", so covering the 1-6h range captures this evolution.
    min_h = 1.0
    max_h = 6.0
    df_mid = df[(df["Delay_Hours"] > min_h) & (df["Delay_Hours"] <= max_h)].copy()
    
    print(f"Data points in {min_h}-{max_h}h range: {len(df_mid)}")

    stats = []
    
    plt.rcParams['font.family'] = 'MS Gothic'
    sns.set_style("whitegrid")
    plt.figure(figsize=(10, 7))

    colors = {"Road": "black", "Paddy": "green"}
    
    for land_type in ["Road", "Paddy"]:
        subset = df_mid[df_mid["Type"] == land_type]
        if len(subset) < 3:
            print(f"Not enough data for {land_type}")
            continue
            
        x = subset["Delay_Hours"]
        y = subset["Diff_Median"]
        
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        
        stats.append({
            "Type": land_type,
            "Slope_dB_hr": slope,
            "R2": r_value**2,
            "Count": len(subset)
        })
        
        label = f"{land_type} (Rate: {slope:.4f} dB/h)"
        sns.regplot(
            data=subset, x="Delay_Hours", y="Diff_Median",
            color=colors[land_type], label=label,
            scatter_kws={'s': 50, 'alpha': 0.6}, ci=None
        )

    plt.title(f"VV Polarization Decay Rate (Mid-Term: {min_h}-{max_h}h)")
    plt.xlabel("Delay (Hours)")
    plt.ylabel("Diff Median [dB]")
    plt.axhline(0, color='gray', linestyle='--')
    plt.legend()
    
    out_img = OUT_DIR / "vv_mid_term_decay.png"
    plt.savefig(out_img)
    plt.close()
    print(f"Saved plot to {out_img}")
    
    print("\n--- Mid-Term Decay Stats (VV 1-6h) ---")
    df_stats = pd.DataFrame(stats)
    print(df_stats)
    
    if len(df_stats) == 2:
        road_slope = df_stats[df_stats["Type"]=="Road"]["Slope_dB_hr"].values[0]
        paddy_slope = df_stats[df_stats["Type"]=="Paddy"]["Slope_dB_hr"].values[0]
        
        diff = abs(road_slope) - abs(paddy_slope)
        if diff > 0:
            print(f"\nResult: Road decays FASTER by {diff:.4f} dB/h.")
        else:
            print(f"\nResult: Paddy decays FASTER by {abs(diff):.4f} dB/h.")

if __name__ == "__main__":
    main()
