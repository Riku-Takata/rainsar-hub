import os
import glob
import pandas as pd
import numpy as np

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"

def collect_diff_data(pol):
    search_path = os.path.join(BASE_DIR, pol, "diff", "**", "*diff_stats.csv")
    files = glob.glob(search_path, recursive=True)
    
    all_rows = []
    
    print(f"Found {len(files)} files for {pol}...")
    
    for f in files:
        try:
            df = pd.read_csv(f)
            
            # Initialize a dict with common columns
            row_data = {}
            
            if 'delay_hours' in df.columns:
                row_data['delay_hours'] = df['delay_hours']
            else:
                continue # Skip if no delay info
            
            # Check for paddy columns
            if 'paddy_diff_mean' in df.columns and 'paddy_diff_count' in df.columns:
                row_data['paddy_diff_mean'] = df['paddy_diff_mean']
                row_data['paddy_diff_count'] = df['paddy_diff_count']
            else:
                row_data['paddy_diff_mean'] = pd.Series([np.nan] * len(df))
                row_data['paddy_diff_count'] = pd.Series([0] * len(df))

            # Check for road columns
            if 'road_diff_mean' in df.columns and 'road_diff_count' in df.columns:
                row_data['road_diff_mean'] = df['road_diff_mean']
                row_data['road_diff_count'] = df['road_diff_count']
            else:
                row_data['road_diff_mean'] = pd.Series([np.nan] * len(df))
                row_data['road_diff_count'] = pd.Series([0] * len(df))
            
            # Construct DataFrame from the dict or just append the extracted series
            # Since usually these CSVs differ in length (often 1 row), straight append is easiest if we standardize columns
            
            temp_df = pd.DataFrame(row_data)
            all_rows.append(temp_df)

        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue
            
    if not all_rows:
        return None
        
    combined_df = pd.concat(all_rows, ignore_index=True)
    return combined_df

def analyze_decay(pol):
    df = collect_diff_data(pol)
    if df is None:
        print(f"No data for {pol}")
        return

    # Round delay to nearest hour
    df['delay_hours'] = df['delay_hours'].round().astype(int)
    
    print(f"\n--- {pol.upper()} Decay Analysis (Diff vs Delay) ---")
    print(f"{'Delay(h)':<10} {'Paddy Diff':<12} {'Road Diff':<12} {'Diff (R-P)':<12} {'Count(P/R)'}")
    print("-" * 60)
    
    sorted_delays = sorted(df['delay_hours'].unique())
    
    results = []
    
    for d in sorted_delays:
        subset = df[df['delay_hours'] == d]
        
        # Calculate weighted means ignoring NaNs
        
        # PADDY
        paddy_sub = subset.dropna(subset=['paddy_diff_mean', 'paddy_diff_count'])
        if not paddy_sub.empty and paddy_sub['paddy_diff_count'].sum() > 0:
            paddy_w_mean = np.average(paddy_sub['paddy_diff_mean'], weights=paddy_sub['paddy_diff_count'])
            paddy_count = paddy_sub['paddy_diff_count'].sum()
        else:
            paddy_w_mean = np.nan
            paddy_count = 0
            
        # ROAD
        road_sub = subset.dropna(subset=['road_diff_mean', 'road_diff_count'])
        if not road_sub.empty and road_sub['road_diff_count'].sum() > 0:
            road_w_mean = np.average(road_sub['road_diff_mean'], weights=road_sub['road_diff_count'])
            road_count = road_sub['road_diff_count'].sum()
        else:
            road_w_mean = np.nan
            road_count = 0
        
        diff_val = np.nan
        if not np.isnan(paddy_w_mean) and not np.isnan(road_w_mean):
            diff_val = road_w_mean - paddy_w_mean
        
        paddy_str = f"{paddy_w_mean:.3f}" if not np.isnan(paddy_w_mean) else "NaN"
        road_str = f"{road_w_mean:.3f}" if not np.isnan(road_w_mean) else "NaN"
        diff_str = f"{diff_val:.3f}" if not np.isnan(diff_val) else "NaN"
        count_str = f"{int(paddy_count)}/{int(road_count)}"
        
        print(f"{d:<10} {paddy_str:<12} {road_str:<12} {diff_str:<12} {count_str}")
        
        if not np.isnan(paddy_w_mean) and not np.isnan(road_w_mean):
            results.append({
                'delay': d,
                'paddy': paddy_w_mean,
                'road': road_w_mean
            })

    # Simple trend analysis
    if len(results) >= 2:
        # Filter for delays < 12h for trend to avoid long-tail noise
        short_term = [r for r in results if r['delay'] <= 12]
        
        if len(short_term) >= 2:
            first = short_term[0]
            last = short_term[-1]
            span = last['delay'] - first['delay']
            
            if span > 0:
                paddy_slope = (last['paddy'] - first['paddy']) / span
                road_slope = (last['road'] - first['road']) / span
                
                print("\n--- Decay Rate Estimate (Linear approx 0-12h) ---")
                print(f"Paddy Slope: {paddy_slope:.4f} dB/hr")
                print(f"Road Slope:  {road_slope:.4f} dB/hr")

def main():
    analyze_decay('vv')
    analyze_decay('vh')

if __name__ == "__main__":
    main()
