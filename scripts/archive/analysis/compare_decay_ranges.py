import pandas as pd
from scipy.stats import linregress
from pathlib import Path
import sys

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
VV_DATA_DIR = HUB_DIR / "data_vv"
VH_DATA_DIR = HUB_DIR / "data"

def load_data(base_dir, pol_name):
    csv_path = base_dir / "analysis" / "detailed_analysis_paired.csv"
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    df['Polarization'] = pol_name
    return df

def calculate_slope(df, min_h, max_h):
    subset = df[(df["Delay_Hours"] >= min_h) & (df["Delay_Hours"] <= max_h)]
    if len(subset) < 3:
        return None
    slope, _, _, _, _ = linregress(subset["Delay_Hours"], subset["Diff_Median"])
    return slope

def main():
    df_vv = load_data(VV_DATA_DIR, "VV")
    df_vh = load_data(VH_DATA_DIR, "VH")
    
    if df_vv is None or df_vh is None:
        return

    df_all = pd.concat([df_vv, df_vh], ignore_index=True)
    
    results = []
    
    for pol in ["VV", "VH"]:
        for land_type in ["Road", "Paddy"]:
            sub = df_all[(df_all["Polarization"] == pol) & (df_all["Type"] == land_type)]
            
            # Range 1: 1h - 12h (Drying)
            slope_drying = calculate_slope(sub, 1.0, 12.0)
            
            # Range 2: 0h - 12h (Full)
            slope_full = calculate_slope(sub, 0.0, 12.0)
            
            results.append({
                "Pol": pol,
                "Type": land_type,
                "Slope_1_12h": slope_drying,
                "Slope_0_12h": slope_full,
                "Change": slope_full - slope_drying if (slope_full and slope_drying) else None
            })
            
    df_res = pd.DataFrame(results)
    print(df_res.to_string(float_format="%.4f"))
    
    # Check significance
    for _, row in df_res.iterrows():
        if row['Slope_0_12h'] is None: continue
        diff = row['Slope_0_12h'] - row['Slope_1_12h']
        direction = "STEEPER" if abs(row['Slope_0_12h']) > abs(row['Slope_1_12h']) else "FLATTER"
        print(f"\n[{row['Pol']} - {row['Type']}]")
        print(f"  1-12h: {row['Slope_1_12h']:.4f} -> 0-12h: {row['Slope_0_12h']:.4f}")
        print(f"  Result: The slope becomes {direction} by {abs(diff):.4f}")

if __name__ == "__main__":
    main()
