import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import linregress

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\visualization"

def collect_diff_data(pol):
    search_path = os.path.join(BASE_DIR, pol, "diff", "**", "*diff_stats.csv")
    files = glob.glob(search_path, recursive=True)
    all_rows = []
    
    for f in files:
        try:
            df = pd.read_csv(f)
            row_data = {}
            if 'delay_hours' not in df.columns: continue
            
            row_data['delay_hours'] = df['delay_hours']
            
            # Handle minimal/missing columns
            for land in ['paddy', 'road']:
                 mean_col = f'{land}_diff_mean'
                 count_col = f'{land}_diff_count'
                 if mean_col in df.columns and count_col in df.columns:
                     row_data[mean_col] = df[mean_col]
                     row_data[count_col] = df[count_col]
                 else:
                     row_data[mean_col] = pd.Series([np.nan]*len(df))
                     row_data[count_col] = pd.Series([0]*len(df))
            
            all_rows.append(pd.DataFrame(row_data))
        except:
            continue
            
    if not all_rows: return None
    return pd.concat(all_rows, ignore_index=True)

def plot_decay(pol):
    df = collect_diff_data(pol)
    if df is None: return

    df['delay_hours'] = df['delay_hours'].round().astype(int)
    delays = sorted(df['delay_hours'].unique())
    # Limit to reasonable range for visualization (e.g., 0-12h)
    delays = [d for d in delays if d <= 12]
    
    paddy_means = []
    road_means = []
    valid_delays = []

    for d in delays:
        subset = df[df['delay_hours'] == d]
        
        # Weighted Means
        p_sub = subset.dropna(subset=['paddy_diff_mean', 'paddy_diff_count'])
        if not p_sub.empty and p_sub['paddy_diff_count'].sum() > 0:
            p_val = np.average(p_sub['paddy_diff_mean'], weights=p_sub['paddy_diff_count'])
        else: p_val = np.nan
            
        r_sub = subset.dropna(subset=['road_diff_mean', 'road_diff_count'])
        if not r_sub.empty and r_sub['road_diff_count'].sum() > 0:
            r_val = np.average(r_sub['road_diff_mean'], weights=r_sub['road_diff_count'])
        else: r_val = np.nan
        
        if not np.isnan(p_val) and not np.isnan(r_val):
            paddy_means.append(p_val)
            road_means.append(r_val)
            valid_delays.append(d)

    # Plotting
    plt.figure(figsize=(10, 6))
    
    # Scatter points
    plt.scatter(valid_delays, paddy_means, color='green', label='Paddy Mean Diff', s=80, alpha=0.7)
    plt.scatter(valid_delays, road_means, color='red', label='Road Mean Diff', s=80, alpha=0.7)
    
    # Trend lines
    if len(valid_delays) > 1:
        slope_p, intercept_p, _, _, _ = linregress(valid_delays, paddy_means)
        slope_r, intercept_r, _, _, _ = linregress(valid_delays, road_means)
        
        line_x = np.array([min(valid_delays), max(valid_delays)])
        plt.plot(line_x, slope_p*line_x + intercept_p, 'g--', label=f'Paddy Trend ({slope_p:.3f} dB/h)')
        plt.plot(line_x, slope_r*line_x + intercept_r, 'r--', label=f'Road Trend ({slope_r:.3f} dB/h)')

    plt.title(f'Backscatter Difference Decay ({pol.upper()} Pol)', fontsize=14)
    plt.xlabel('Delay after Rainfall (hours)', fontsize=12)
    plt.ylabel('Mean Difference (After - Before) [dB]', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    save_path = os.path.join(OUTPUT_DIR, f'{pol}_diff_decay_trend.png')
    plt.savefig(save_path)
    print(f"Saved plot to {save_path}")

def main():
    plot_decay('vv')
    plot_decay('vh')

if __name__ == "__main__":
    main()
