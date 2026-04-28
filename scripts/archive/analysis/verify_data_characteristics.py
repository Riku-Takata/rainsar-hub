import os
import glob
import pandas as pd
import numpy as np

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"

def aggregate_stats(pol, stat_type):
    # stat_type: 'sigma' or 'diff'
    search_path = os.path.join(BASE_DIR, pol, stat_type, "**", "*stats.csv")
    files = glob.glob(search_path, recursive=True)
    
    if not files:
        print(f"No files found for {pol} {stat_type}")
        return None

    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Filter rows where count > 0 to avoid NaNs
            # Assuming columns exists
            all_data.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue
            
    if not all_data:
        return None
        
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df

def analyze_char(df, pol, stat_type):
    print(f"--- Analysis for {pol.upper()} {stat_type.upper()} ---")
    if stat_type == 'sigma':
        # Columns: paddy_before_mean, paddy_after_mean, road_before_mean, road_after_mean
        cols = ['paddy_before_mean', 'paddy_after_mean', 'road_before_mean', 'road_after_mean']
        for col in cols:
            if col in df.columns:
                mean_val = df[col].mean()
                std_val = df[col].std()
                print(f"{col}: Mean = {mean_val:.2f}, Std = {std_val:.2f}")
                
    elif stat_type == 'diff':
        # Columns: paddy_diff_mean, road_diff_mean
        cols = ['paddy_diff_mean', 'road_diff_mean', 'paddy_diff_std', 'road_diff_std']
        # Note: diff_std in csv is the std of the diff distributions for one event.
        # We want the mean of these stds (average spread) and the std of the means (variability of the center).
        
        for col in ['paddy_diff_mean', 'road_diff_mean']:
            if col in df.columns:
                 # Mean of means (Center of the distribution of shifts)
                mean_val = df[col].mean()
                # Std of means (How much the shift varies between events)
                std_of_means = df[col].std()
                print(f"Global {col}: Mean = {mean_val:.2f} (avg shift), StdDev = {std_of_means:.2f} (variability of shift)")
        
        for col in ['paddy_diff_std', 'road_diff_std']:
             if col in df.columns:
                # Mean of stds (Average width of the diff histogram per event)
                mean_std = df[col].mean()
                print(f"Average Local {col}: {mean_std:.2f} (avg spread of diff per event)")

def main():
    for pol in ['vv', 'vh']:
        # Sigma
        sigma_df = aggregate_stats(pol, 'sigma')
        if sigma_df is not None:
            analyze_char(sigma_df, pol, 'sigma')
        
        # Diff
        diff_df = aggregate_stats(pol, 'diff')
        if diff_df is not None:
            analyze_char(diff_df, pol, 'diff')

if __name__ == "__main__":
    main()
