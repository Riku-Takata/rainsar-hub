import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\visualization"

def collect_sigma_data(pol):
    # Search for sigma stats
    search_path = os.path.join(BASE_DIR, pol, "sigma", "**", "*stats.csv")
    files = glob.glob(search_path, recursive=True)
    all_rows = []
    
    print(f"[{pol}] Found {len(files)} sigma stats files.")
    
    # We need rainfall info. Typically in diff_stats.csv, OR in database.
    # But stats.csv usually doesn't have rain info columns (precip, duration).
    # However, 'diff_stats.csv' DOES have them.
    # Strategy: Read diff stats to build a lookup dict { (grid, event): {rain_params} }
    # Then read sigma stats and map.
    
    diff_path = os.path.join(BASE_DIR, pol, "diff", "**", "*diff_stats.csv")
    diff_files = glob.glob(diff_path, recursive=True)
    rain_lookup = {}
    
    print(f"[{pol}] Building rainfall lookup from {len(diff_files)} diff files...")
    for df_f in diff_files:
        try:
            d = pd.read_csv(df_f)
            if 'total_precip_mm' in d.columns and 'grid_id' in d.columns:
                # Assuming 1 row per file
                row = d.iloc[0]
                key = (row['grid_id'], row['event_name'])
                rain_lookup[key] = {
                    'total_precip_mm': row['total_precip_mm'],
                    'max_intensity_mm_h': row['max_intensity_mm_h'],
                    'duration_hours': row['duration_hours'],
                    'delay_hours': row['delay_hours']
                }
        except: continue

    for f in files:
        try:
            df = pd.read_csv(f)
            # Typically 1 row per file
            if df.empty: continue
            row = df.iloc[0]
            
            grid_id = row['grid_id']
            event_name = row['event_name']
            
            # Lookup rain
            rain_info = rain_lookup.get((grid_id, event_name))
            if not rain_info: continue
            
            new_row = {
                'polarization': pol,
                'grid_id': grid_id,
                'event_name': event_name,
                'paddy_after_mean': row.get('paddy_after_mean', np.nan),
                'road_after_mean': row.get('road_after_mean', np.nan),
                'paddy_before_mean': row.get('paddy_before_mean', np.nan),
                'road_before_mean': row.get('road_before_mean', np.nan),
                **rain_info
            }
            all_rows.append(new_row)
            
        except Exception as e:
            continue
            
    if not all_rows: return None
    return pd.DataFrame(all_rows)

def analyze_after_correlation():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_data = []
    for pol in ['vv', 'vh']:
        df = collect_sigma_data(pol)
        if df is not None:
             all_data.append(df)
    
    if not all_data:
        print("No data found.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    
    # Filter for valid values
    full_df = full_df.dropna(subset=['paddy_after_mean', 'road_after_mean', 'total_precip_mm'])
    
    # Analyze all delays
    fresh_df = full_df
    print(f"\nAnalyzing {len(fresh_df)} events with All Delays...")
    
    # Correlation Analysis
    cols = ['total_precip_mm', 'max_intensity_mm_h', 
            'paddy_after_mean', 'road_after_mean']
    
    print("\n--- Correlation Analysis (After Sigma vs Rain - All Delays) ---")
    corr_matrix = fresh_df[cols].corr()
    print(corr_matrix[['paddy_after_mean', 'road_after_mean']])
    
    # Visualization
    plt.figure(figsize=(12, 5))
    
    # 1. Road After vs Total Precip
    plt.subplot(1, 2, 1)
    sns.scatterplot(data=fresh_df, x='total_precip_mm', y='road_after_mean', hue='polarization', alpha=0.3)
    plt.title('Road After Sigma vs Total Precip (All Delays)')
    plt.xlabel('Total Precipitation (mm)')
    plt.ylabel('Road After Sigma (dB)')
    
    # 2. Paddy After vs Total Precip
    plt.subplot(1, 2, 2)
    sns.scatterplot(data=fresh_df, x='total_precip_mm', y='paddy_after_mean', hue='polarization', alpha=0.3)
    plt.title('Paddy After Sigma vs Total Precip (All Delays)')
    plt.xlabel('Total Precipitation (mm)')
    plt.ylabel('Paddy After Sigma (dB)')
    
    plt.tight_layout()
    plot_path = os.path.join(OUTPUT_DIR, 'after_sigma_vs_rain_params_alldelays.png')
    plt.savefig(plot_path)
    print(f"Saved scatter plots to {plot_path}")
    
    # Check if 'After' correlates better than 'Diff' (Comparison)
    # We don't have diff here directly but we can infer from previous steps or just trust this output.

if __name__ == "__main__":
    analyze_after_correlation()
