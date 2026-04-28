import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Setup paths
HUB_DIR = Path(r"D:\sotsuron\rainsar-hub")
VV_DATA_DIR = HUB_DIR / "data_vv"
VH_DATA_DIR = HUB_DIR / "data"

def load_data(base_dir, pol_name):
    csv_path = base_dir / "analysis" / "detailed_analysis_paired.csv"
    if not csv_path.exists():
        print(f"Warning: {csv_path} not found.")
        return None
    df = pd.read_csv(csv_path)
    df['Polarization'] = pol_name
    return df

def calculate_decay(df):
    # Filter for Paddy and Road
    results = []
    
    for land_type in ["Road", "Paddy"]:
        # Get Short (<=3h) and Long (>3h) stats (using Median Diff)
        subset = df[df["Type"] == land_type]
        
        # We rely on pre-calculated Delay_Group or recalculate it to be safe
        subset["Delay_Group"] = subset["Delay_Hours"].apply(lambda x: "Short" if x <= 3 else "Long")
        
        # Calculate mean of the Medians (Diff_Median column)
        short_val = subset[subset["Delay_Group"] == "Short"]["Diff_Median"].mean()
        long_val = subset[subset["Delay_Group"] == "Long"]["Diff_Median"].mean()
        
        decay_amount = short_val - long_val
        decay_rate = (decay_amount / short_val) * 100 if short_val != 0 else 0
        
        results.append({
            "Type": land_type,
            "Short_Mean_dB": short_val,
            "Long_Mean_dB": long_val,
            "Decay_Amount_dB": decay_amount,
            "Decay_Rate_Percent": decay_rate
        })
        
    return pd.DataFrame(results)

def main():
    print("Loading Data...")
    df_vv = load_data(VV_DATA_DIR, "VV")
    df_vh = load_data(VH_DATA_DIR, "VH")
    
    if df_vv is None or df_vh is None:
        print("Stopping due to missing data.")
        return

    print("\n--- VV Polarization Decay ---")
    res_vv = calculate_decay(df_vv)
    print(res_vv.to_string(index=False, float_format="%.4f"))
    
    print("\n--- VH Polarization Decay ---")
    res_vh = calculate_decay(df_vh)
    print(res_vh.to_string(index=False, float_format="%.4f"))

    print("\n--- Comparison Summary ---")
    # Interpretation text
    for i, row_vv in res_vv.iterrows():
        land_type = row_vv['Type']
        row_vh = res_vh[res_vh['Type'] == land_type].iloc[0]
        
        print(f"[{land_type}]")
        print(f"  VV Decay: {row_vv['Decay_Amount_dB']:.2f} dB ({row_vv['Decay_Rate_Percent']:.1f}%)")
        print(f"  VH Decay: {row_vh['Decay_Amount_dB']:.2f} dB ({row_vh['Decay_Rate_Percent']:.1f}%)")
        if row_vv['Decay_Rate_Percent'] > row_vh['Decay_Rate_Percent']:
             print(f"  -> VV decays faster (sharper signal drop).")
        else:
             print(f"  -> VH decays faster.")

if __name__ == "__main__":
    main()
