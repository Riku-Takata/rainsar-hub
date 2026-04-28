import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import re

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_EXP_DIR = BASE_DIR / "data" / "expanded"
ANALYSIS_DIR = DATA_EXP_DIR / "analysis"
EVO_DIR = ANALYSIS_DIR / "evolution"
CSV_PATH = EVO_DIR / "evolution_data.csv" # Unfiltered has absolute values
OUT_DIR = ANALYSIS_DIR / "seasonal_analysis"

def parse_month(event_str):
    # event_str example: delay_1h_20200501
    m = re.search(r"_(\d{8})", event_str)
    if m:
        date_str = m.group(1)
        month = int(date_str[4:6])
        return month
    return None

def main():
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found.")
        return
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(CSV_PATH)
    
    # Filter valid
    required = ['road_before_mean', 'road_after_mean', 'paddy_before_mean', 'paddy_after_mean']
    df = df.dropna(subset=required + ['event'])
    
    # Add Season Column
    def get_season(row):
        month = parse_month(row['event'])
        if month is None: return "Unknown"
        # Growing: May(5) - Aug(8)
        if 5 <= month <= 8:
            return "Growing Season (May-Aug)"
        else:
            return "Off-Season (Sep-Apr)"
            
    df['season'] = df.apply(get_season, axis=1)
    
    # === 1. Absolute Levels Comparison ===
    print("=== Seasonal Absolute Backscatter Levels (dB) ===")
    summary = df.groupby('season', observed=False)[required].mean().T
    print(summary.round(2))
    
    # Calculate Change & Contrast
    df['Road_Diff'] = df['road_after_mean'] - df['road_before_mean']
    df['Paddy_Diff'] = df['paddy_after_mean'] - df['paddy_before_mean']
    df['Contrast'] = df['Road_Diff'] - df['Paddy_Diff']
    
    print("\n=== Seasonal Change & Contrast (dB) ===")
    diff_summary = df.groupby('season', observed=False)[['Road_Diff', 'Paddy_Diff', 'Contrast']].mean()
    print(diff_summary.round(2))
    
    # === 3. Monthly Breakdown (Investigation for Puddling Period) ===
    print("\n=== Monthly Breakdown (dB) ===")
    df['month'] = df['event'].apply(parse_month)
    monthly = df.groupby('month', observed=False).agg({
        'paddy_before_mean': 'mean',
        'paddy_after_mean': 'mean',
        'road_before_mean': 'mean',
        'Paddy_Diff': 'mean',
        'Road_Diff': 'mean',
        'event': 'count'
    })
    
    # Calculate Contrast
    monthly['Contrast'] = monthly['Road_Diff'] - monthly['Paddy_Diff']
    
    # Format for display
    display_cols = ['Paddy_Before', 'Road_Before', 'Paddy_Diff', 'Road_Diff', 'Contrast', 'Count']
    monthly_renamed = monthly.rename(columns={
        'paddy_before_mean': 'Paddy_Before',
        'road_before_mean': 'Road_Before',
        'event': 'Count'
    })
    print(monthly_renamed[display_cols].round(2))
    
    # === 4. Plot (Off-Season Only) ===
    off_season_df = df[df['season'] == "Off-Season (Sep-Apr)"].copy()
    
    if len(off_season_df) > 0:
        print(f"\nAnalyzing Off-Season Evolution (N={len(off_season_df)})...")
        
        # Melt
        road = off_season_df[['delay_h', 'Road_Diff']].copy()
        road['Type'] = 'Road'
        road = road.rename(columns={'Road_Diff': 'Diff (dB)'})
        
        paddy = off_season_df[['delay_h', 'Paddy_Diff']].copy()
        paddy['Type'] = 'Paddy'
        paddy = paddy.rename(columns={'Paddy_Diff': 'Diff (dB)'})
        
        melted = pd.concat([road, paddy], ignore_index=True)
        
        # Bins
        bins = [0, 3, 6, 9, 12, 24]
        labels = ["0-3h", "3-6h", "6-9h", "9-12h", "12h+"]
        melted['Time Bin'] = pd.cut(melted['delay_h'], bins=bins, labels=labels, right=False)
        
        plt.figure(figsize=(10, 6))
        sns.pointplot(data=melted, x='Time Bin', y='Diff (dB)', hue='Type', 
                      palette={'Road': 'red', 'Paddy': 'green'}, 
                      markers=['o', 's'], linestyles=['-', '--'], capsize=0.1)
        
        plt.axhline(0, color='gray', linestyle=':', alpha=0.5)
        plt.title("Backscatter Evolution: Off-Season (Sep-Apr)")
        plt.ylabel("Difference (After - Before) [dB]")
        plt.xlabel("Wait Time (Delay)")
        plt.ylim(-2.0, 2.0)
        
        fname = "evolution_off_season.png"
        plt.savefig(OUT_DIR / fname)
        print(f"Saved plot: {fname}")
        
        # Stats table
        print("\nOff-Season Evolution Stats:")
        print(melted.groupby(['Time Bin', 'Type'], observed=False)['Diff (dB)'].agg(['mean', 'count']).unstack().round(3))

if __name__ == "__main__":
    main()
