import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\visualization"

def collect_diff_data(pol):
    search_path = os.path.join(BASE_DIR, pol, "diff", "**", "*diff_stats.csv")
    files = glob.glob(search_path, recursive=True)
    all_rows = []
    
    print(f"[{pol}] Found {len(files)} stats files.")
    
    for f in files:
        try:
            df = pd.read_csv(f)
            row_data = {}
            if 'delay_hours' not in df.columns: continue
            
            row_data['delay_hours'] = df['delay_hours']
            
            for land in ['paddy', 'road']:
                 mean_col = f'{land}_diff_mean'
                 std_col = f'{land}_diff_std' # Std of pixels in this grid/event
                 count_col = f'{land}_diff_count'
                 
                 if mean_col in df.columns and std_col in df.columns and count_col in df.columns:
                     row_data[mean_col] = df[mean_col]
                     row_data[std_col] = df[std_col]
                     row_data[count_col] = df[count_col]
                 else:
                     row_data[mean_col] = pd.Series([np.nan]*len(df))
                     row_data[std_col] = pd.Series([np.nan]*len(df))
                     row_data[count_col] = pd.Series([0]*len(df))
            
            all_rows.append(pd.DataFrame(row_data))
        except:
            continue
            
    if not all_rows: return None
    return pd.concat(all_rows, ignore_index=True)

def calculate_pooled_stats(pol):
    df = collect_diff_data(pol)
    if df is None: return None

    df['delay_hours'] = df['delay_hours'].round().astype(int)
    delays = sorted(df['delay_hours'].unique())
    # Filter 0-12h
    delays = [d for d in delays if d <= 12]
    
    stats_list = []

    for d in delays:
        subset = df[df['delay_hours'] == d]
        
        row_res = {'delay_hours': d}
        
        for land in ['paddy', 'road']:
            # Drop NaNs for this land type
            sub = subset.dropna(subset=[f'{land}_diff_mean', f'{land}_diff_std', f'{land}_diff_count'])
            sub = sub[sub[f'{land}_diff_count'] > 0]
            
            if sub.empty:
                row_res[f'{land}_mean'] = np.nan
                row_res[f'{land}_std_pixel'] = np.nan
                row_res[f'{land}_count_total'] = 0
                continue

            counts = sub[f'{land}_diff_count']
            means = sub[f'{land}_diff_mean']
            stds = sub[f'{land}_diff_std']
            
            total_count = counts.sum()
            
            # 1. Global Mean (Weighted Average)
            global_mean = np.average(means, weights=counts)
            
            # 2. Global Variance (Total Sum of Squares / Total Count)
            # Total Var = Within-Group Var + Between-Group Var
            # Within-Group SS = Sum( (n_i - 1) * s_i^2 ) approx Sum( n_i * s_i^2 ) for large n
            # Between-Group SS = Sum( n_i * (mean_i - global_mean)^2 )
            
            # Weighted variance of means (Between-Group variability)
            between_var = np.average((means - global_mean)**2, weights=counts)
            
            # Average of variances (Within-Group variability - represent pixel noise)
            within_var = np.average(stds**2, weights=counts)
            
            total_var = within_var + between_var
            total_std = np.sqrt(total_var)
            
            row_res[f'{land}_mean'] = global_mean
            row_res[f'{land}_std_pixel'] = total_std
            row_res[f'{land}_count_total'] = total_count

        stats_list.append(row_res)
        
    return pd.DataFrame(stats_list)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for pol in ['vv', 'vh']:
        print(f"Processing {pol}...")
        results = calculate_pooled_stats(pol)
        if results is not None:
            csv_path = os.path.join(OUTPUT_DIR, f'{pol}_decay_stats_detailed.csv')
            results.to_csv(csv_path, index=False)
            print(f"Saved stats to {csv_path}")
            
            # Print quick analysis of variance
            print(f"--- {pol.upper()} Variance Analysis ---")
            print(results[['delay_hours', 'paddy_mean', 'paddy_std_pixel', 'road_mean', 'road_std_pixel']].to_string(index=False))
            
            # Generate plot with error bars
            plt.figure(figsize=(10, 6))
            
            # Plot Paddy
            plt.errorbar(results['delay_hours'], results['paddy_mean'], yerr=results['paddy_std_pixel'], 
                         fmt='o-', color='green', label='Paddy Mean ±1std', capsize=5, alpha=0.7)
            
            # Plot Road
            plt.errorbar(results['delay_hours'], results['road_mean'], yerr=results['road_std_pixel'], 
                         fmt='o-', color='red', label='Road Mean ±1std', capsize=5, alpha=0.7)
            
            plt.title(f'Diff Decay with Variance ({pol.upper()})')
            plt.xlabel('Delay (h)')
            plt.ylabel('Backscatter Diff (After-Before) [dB]')
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.legend()
            
            plot_path = os.path.join(OUTPUT_DIR, f'{pol}_diff_decay_with_std.png')
            plt.savefig(plot_path)
            print(f"Saved plot with error bars to {plot_path}")

if __name__ == "__main__":
    main()
