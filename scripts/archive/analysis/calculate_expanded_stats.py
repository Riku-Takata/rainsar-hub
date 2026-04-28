import pandas as pd
import numpy as np
from pathlib import Path

def main():
    # Input CSV from the evolution analysis
    csv_path = r"d:\sotsuron\rainsar-hub\data\expanded\analysis\evolution\evolution_data.csv"
    
    if not Path(csv_path).exists():
        print(f"Error: {csv_path} not found.")
        print("Please run 'analyze_expanded_evolution.py' first.")
        return

    df = pd.read_csv(csv_path)
    # Filter valid rows
    df = df.dropna(subset=['road_diff_mean', 'paddy_diff_mean'])
    
    print(f"Loaded {len(df)} events from {len(df['grid_id'].unique())} grids.")

    # Calculate "Contrast" (Road Change - Paddy Change)
    # If Road brightens MORE than Paddy, this is positive.
    df['contrast_diff'] = df['road_diff_mean'] - df['paddy_diff_mean']
    
    # Binning by Delay
    bins = [0, 3, 6, 9, 12, 24]
    labels = ["0-3h", "3-6h", "6-9h", "9-12h", "12h+"]
    df['delay_bin'] = pd.cut(df['delay_h'], bins=bins, labels=labels, right=False)
    
    # Aggregation
    summary = df.groupby('delay_bin', observed=False).agg({
        'road_diff_mean': ['mean', 'std', 'count'],
        'paddy_diff_mean': ['mean', 'std'],
        'contrast_diff': ['mean', 'std']
    })
    
    # Flatten columns
    summary.columns = [
        'Road_Mean', 'Road_Std', 'Count',
        'Paddy_Mean', 'Paddy_Std',
        'Contrast_Mean', 'Contrast_Std'
    ]
    
    print("\n=== Statistical Summary by Delay (After - Before) [dB] ===")
    print(summary[['Road_Mean', 'Paddy_Mean', 'Contrast_Mean', 'Count']].round(4))
    
    # === Rainfall Analysis ===
    if 'rain_total_est_mm' in df.columns:
        print("\n=== Rainfall Analysis (Total & Duration) ===")
        # Filter for valid data
        df_rain = df.dropna(subset=['rain_total_est_mm', 'rain_duration_h', 'contrast_diff'])
        
        # 1. Correlations
        corr_tot_cnt = df_rain['rain_total_est_mm'].corr(df_rain['contrast_diff'])
        corr_dur_cnt = df_rain['rain_duration_h'].corr(df_rain['contrast_diff'])
        corr_max_cnt = df_rain['rain_max_mm_h'].corr(df_rain['contrast_diff'])
        
        print(f"Correlation (Total Rain vs Contrast): {corr_tot_cnt:.4f}")
        print(f"Correlation (Duration vs Contrast):   {corr_dur_cnt:.4f}")
        print(f"Correlation (Peak Rate vs Contrast):  {corr_max_cnt:.4f}")
        
        # 2. Binned Analysis (Total Rain)
        bins = [0, 20, 50, 100, 200]
        labels = ["<20mm", "20-50mm", "50-100mm", "100mm+"]
        df_rain['total_bin'] = pd.cut(df_rain['rain_total_est_mm'], bins=bins, labels=labels, right=False)
        
        print("\nContrast by Total Precipitation:")
        print(df_rain.groupby('total_bin', observed=False)['contrast_diff'].agg(['mean', 'std', 'count']).round(4))

        # 3. Binned Analysis (Duration)
        dbins = [0, 3, 6, 12, 24]
        dlabels = ["<3h", "3-6h", "6-12h", "12h+"]
        df_rain['dur_bin'] = pd.cut(df_rain['rain_duration_h'], bins=dbins, labels=dlabels, right=False)
        
        print("\nContrast by Rainfall Duration:")
        print(df_rain.groupby('dur_bin', observed=False)['contrast_diff'].agg(['mean', 'std', 'count']).round(4))

    # Overall Mean
    print("\n=== Overall Averages ===")
    print(f"Road Change (After-Before): {df['road_diff_mean'].mean():.4f} dB")
    print(f"Paddy Change (After-Before): {df['paddy_diff_mean'].mean():.4f} dB")
    print(f"Net Contrast (Road - Paddy): {df['contrast_diff'].mean():.4f} dB")
    
    # T-test (Simple independent t-test per bin? Or paired since it's same event?)
    # Since Road and Paddy are from the same event, it's a Paired sample.
    # We can test if 'contrast_diff' is significantly different from 0.
    
    print("\n=== Significance Test (Paired T-Test: Road vs Paddy) ===")
    from scipy import stats
    
    for label in labels:
        subset = df[df['delay_bin'] == label]
        if len(subset) > 1:
            # H0: Road_Diff - Paddy_Diff = 0
            t_stat, p_val = stats.ttest_1samp(subset['contrast_diff'], 0)
            sig = "**" if p_val < 0.01 else "*" if p_val < 0.05 else ""
            print(f"{label}: Mean Diff={subset['contrast_diff'].mean():.4f}, p={p_val:.4e} {sig}")

if __name__ == "__main__":
    main()
