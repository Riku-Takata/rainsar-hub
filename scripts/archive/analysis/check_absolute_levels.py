import pandas as pd
import numpy as np
from pathlib import Path

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
CSV_PATH = BASE_DIR / "data" / "expanded" / "analysis" / "evolution" / "evolution_data.csv"

def main():
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found.")
        return
    
    df = pd.read_csv(CSV_PATH)
    
    print(f"Loaded {len(df)} events from {CSV_PATH.name}")
    
    # Check columns
    required = ['road_before_mean', 'road_after_mean', 'paddy_before_mean', 'paddy_after_mean']
    missing = [c for c in required if c not in df.columns]
    
    if missing:
        print(f"Error: Missing columns: {missing}")
        print("Please re-run analyze_expanded_evolution.py to populate absolute values.")
        return
        
    # Drop rows with NaN in these columns
    df_valid = df.dropna(subset=required)
    print(f"Valid events (with absolute values): {len(df_valid)}")
    
    # Calculate Overall Stats
    stats = df_valid[required].mean()
    
    print("\n=== Average Absolute Backscatter Intensity [dB] ===")
    print(f"Road  Before: {stats['road_before_mean']:.2f} dB")
    print(f"Road  After:  {stats['road_after_mean']:.2f} dB")
    print(f"      Change: {stats['road_after_mean'] - stats['road_before_mean']:.2f} dB")
    print("-" * 40)
    print(f"Paddy Before: {stats['paddy_before_mean']:.2f} dB")
    print(f"Paddy After:  {stats['paddy_after_mean']:.2f} dB")
    print(f"      Change: {stats['paddy_after_mean'] - stats['paddy_before_mean']:.2f} dB")
    print("-" * 40)
    
    # Check by Delay Bin (Short vs Long)
    bins = [0, 6, 12, 24]
    labels = ["0-6h", "6-12h", "12h+"]
    df_valid['delay_bin'] = pd.cut(df_valid['delay_h'], bins=bins, labels=labels, right=False)
    
    print("\n=== Change by Delay Bin ===")
    grouped = df_valid.groupby('delay_bin', observed=False)[required].mean()
    
    # Add Change columns
    grouped['Road_Diff'] = grouped['road_after_mean'] - grouped['road_before_mean']
    grouped['Paddy_Diff'] = grouped['paddy_after_mean'] - grouped['paddy_before_mean']
    grouped['Contrast'] = grouped['Road_Diff'] - grouped['Paddy_Diff']
    
    print(grouped[['Road_Diff', 'Paddy_Diff', 'Contrast']].round(2))
    
    # Check by Rain Intensity (Moderate vs Heavy)
    def classify(row):
        total = row.get('rain_total_est_mm', 0)
        if total < 20: return "Light"
        elif total < 50: return "Moderate"
        else: return "Heavy"
    
    if 'rain_total_est_mm' in df_valid.columns:
        df_valid['rain_cat'] = df_valid.apply(classify, axis=1)
        print("\n=== Change by Rain Category ===")
        r_grouped = df_valid.groupby('rain_cat', observed=False)[required].mean()
        r_grouped['Road_Diff'] = r_grouped['road_after_mean'] - r_grouped['road_before_mean']
        r_grouped['Paddy_Diff'] = r_grouped['paddy_after_mean'] - r_grouped['paddy_before_mean']
        r_grouped['Contrast'] = r_grouped['Road_Diff'] - r_grouped['Paddy_Diff']
        
        # Sort by predefined order
        order = ["Light", "Moderate", "Heavy"]
        r_grouped = r_grouped.reindex(order)
        print(r_grouped[['Road_Diff', 'Paddy_Diff', 'Contrast']].round(2))

if __name__ == "__main__":
    main()
