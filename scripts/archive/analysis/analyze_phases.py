import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.stats import linregress
import numpy as np

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
VV_DATA_DIR = HUB_DIR / "data_vv"
VH_DATA_DIR = HUB_DIR / "data"
OUT_DIR = VV_DATA_DIR / "analysis" / "phases"

def load_data(base_dir, pol_name):
    csv_path = base_dir / "analysis" / "detailed_analysis_paired.csv"
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    df['Polarization'] = pol_name
    return df

def analyze_phase(df, label, min_h, max_h):
    subset = df[(df["Delay_Hours"] > min_h) & (df["Delay_Hours"] <= max_h)].copy()
    # Special handling for 0-1h to include 0 if exists, though data starts at 0.15
    if min_h == 0:
        subset = df[(df["Delay_Hours"] >= min_h) & (df["Delay_Hours"] <= max_h)].copy()
        
    if len(subset) < 3:
        return None, None
        
    x = subset["Delay_Hours"]
    y = subset["Diff_Median"]
    slope, intercept, _, _, _ = linregress(x, y)
    
    return slope, subset

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df_vv = load_data(VV_DATA_DIR, "VV")
    df_vh = load_data(VH_DATA_DIR, "VH")
    if df_vv is None or df_vh is None: return
    
    df_all = pd.concat([df_vv, df_vh], ignore_index=True)
    
    plt.rcParams['font.family'] = 'MS Gothic'
    sns.set_style("whitegrid")
    
    stats = []

    # Plot separate graphs for Road and Paddy
    for land_type in ["Road", "Paddy"]:
        fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
        fig.suptitle(f"Phase Analysis: {land_type}", fontsize=16)
        
        for i, pol in enumerate(["VV", "VH"]):
            ax = axes[i]
            data_pol = df_all[(df_all["Polarization"] == pol) & (df_all["Type"] == land_type)]
            
            # Phase 1: 0 - 1h
            slope1, sub1 = analyze_phase(data_pol, "0-1h", 0, 1.0)
            if slope1 is not None:
                sns.regplot(data=sub1, x="Delay_Hours", y="Diff_Median", ax=ax, color="red", scatter_kws={'s': 50}, label=f"0-1h: {slope1:.3f} dB/h")
                stats.append({"Pol": pol, "Type": land_type, "Phase": "0-1h", "Slope": slope1})

            # Phase 2: 1 - 12h
            slope2, sub2 = analyze_phase(data_pol, "1-12h", 1.0, 12.0)
            if slope2 is not None:
                sns.regplot(data=sub2, x="Delay_Hours", y="Diff_Median", ax=ax, color="blue", scatter_kws={'s': 50}, label=f"1-12h: {slope2:.3f} dB/h")
                stats.append({"Pol": pol, "Type": land_type, "Phase": "1-12h", "Slope": slope2})
            
            ax.set_title(f"Polarization: {pol}")
            ax.set_xlabel("Delay (Hours)")
            ax.set_ylabel("Diff Median [dB]")
            ax.axhline(0, color='gray', linestyle='--')
            ax.legend()
            ax.set_xlim(0, 12)

        out_file = OUT_DIR / f"phase_analysis_{land_type}.png"
        plt.tight_layout()
        plt.savefig(out_file)
        plt.close()
        print(f"Saved plot to {out_file}")

    print("\n--- Phase Slope Analysis ---")
    df_stats = pd.DataFrame(stats)
    print(df_stats.pivot_table(index=["Type", "Pol"], columns="Phase", values="Slope"))

if __name__ == "__main__":
    main()
