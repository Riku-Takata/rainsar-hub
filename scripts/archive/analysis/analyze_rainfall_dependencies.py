import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\visualization"

def collect_event_data(pol):
    search_path = os.path.join(BASE_DIR, pol, "diff", "**", "*diff_stats.csv")
    files = glob.glob(search_path, recursive=True)
    all_rows = []
    
    print(f"[{pol}] Found {len(files)} stats files.")
    
    for f in files:
        try:
            df = pd.read_csv(f)
            
            # Helper to safely get value
            def get_val(col):
                return df[col].iloc[0] if col in df.columns and not df.empty else np.nan

            if 'delay_hours' not in df.columns: continue
            
            # Only interest in files with valid land data
            p_count = get_val('paddy_diff_count')
            r_count = get_val('road_diff_count')
            
            if pd.isna(p_count) and pd.isna(r_count): continue
            
            row = {
                'polarization': pol,
                'grid_id': get_val('grid_id'),
                'event_name': get_val('event_name'),
                'delay_hours': get_val('delay_hours'),
                'total_precip_mm': get_val('total_precip_mm'),
                'max_intensity_mm_h': get_val('max_intensity_mm_h'),
                'duration_hours': get_val('duration_hours'),
                'paddy_diff_mean': get_val('paddy_diff_mean'),
                'paddy_diff_std': get_val('paddy_diff_std'),
                'road_diff_mean': get_val('road_diff_mean'),
                'road_diff_std': get_val('road_diff_std')
            }
            all_rows.append(row)
            
        except Exception as e:
            continue
            
    if not all_rows: return None
    return pd.DataFrame(all_rows)

def analyze_dependencies():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_data = []
    for pol in ['vv', 'vh']:
        df = collect_event_data(pol)
        if df is not None:
             all_data.append(df)
    
    if not all_data:
        print("No data found.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    
    # Save to CSV
    csv_path = os.path.join(OUTPUT_DIR, 'event_parameters.csv')
    full_df.to_csv(csv_path, index=False)
    print(f"Saved aggregated data to {csv_path}")
    
    # Analysis: Correlation Matrix
    # Filter for relevant columns
    cols = ['total_precip_mm', 'max_intensity_mm_h', 'duration_hours', 
            'paddy_diff_mean', 'road_diff_mean']
    
    print("\n--- Correlation Analysis (All Delays - Whole Period) ---")
    corr_matrix = full_df[cols].corr()
    print(corr_matrix)
    
    # Analysis specific to small delays (e.g. < 3h) where rain effect is strongest
    fresh_df = full_df[full_df['delay_hours'] <= 3]
    print("\n--- Correlation Analysis (Delay <= 3h) ---")
    corr_matrix_fresh = fresh_df[cols].corr()
    print(corr_matrix_fresh)
    
    # Visualization (Using All Data for scatter)
    # 1. Diff vs Total Precip (Scatter with regression)
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    sns.scatterplot(data=full_df, x='total_precip_mm', y='road_diff_mean', hue='polarization', alpha=0.3)
    plt.title('Road Diff vs Total Precip (All Delays)')
    plt.xlabel('Total Precipitation (mm)')
    plt.ylabel('Road Mean Diff (dB)')
    
    plt.subplot(1, 2, 2)
    sns.scatterplot(data=full_df, x='max_intensity_mm_h', y='road_diff_mean', hue='polarization', alpha=0.3)
    plt.title('Road Diff vs Max Intensity (All Delays)')
    plt.xlabel('Max Intensity (mm/h)')
    plt.ylabel('Road Mean Diff (dB)')
    
    plt.tight_layout()
    plot_path = os.path.join(OUTPUT_DIR, 'diff_vs_rain_params_alldelays.png')
    plt.savefig(plot_path)
    print(f"Saved scatter plots to {plot_path}")

if __name__ == "__main__":
    analyze_dependencies()
