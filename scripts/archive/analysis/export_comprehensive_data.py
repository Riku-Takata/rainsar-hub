import os
import glob
import pandas as pd
import numpy as np

BASE_DIR = r"D:\sotsuron\rainsar-hub\data\result"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\visualization"

def get_stats_data(pol, stat_type):
    # stat_type: 'sigma' or 'diff'
    search_path = os.path.join(BASE_DIR, pol, stat_type, "**", "*stats.csv")
    files = glob.glob(search_path, recursive=True)
    all_rows = []
    
    print(f"[{pol}] Reading {len(files)} {stat_type} stats files...")
    
    for f in files:
        try:
            df = pd.read_csv(f)
            if df.empty: continue
            
            # Standardize column names if needed or just take the first row
            row = df.iloc[0].to_dict()
            row['source_file'] = f
            all_rows.append(row)
        except: continue
        
    return pd.DataFrame(all_rows)

def export_comprehensive():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    final_dfs = []
    
    for pol in ['vv', 'vh']:
        print(f"\nProcessing {pol.upper()}...")
        
        # 1. Get Diff Stats (Contains Diff values + Rain Metadata)
        df_diff = get_stats_data(pol, 'diff')
        if df_diff.empty:
            print("No diff data found.")
            continue
            
        # 2. Get Sigma Stats (Contains Before/After Absolute values)
        df_sigma = get_stats_data(pol, 'sigma')
        
        # Merge on (grid_id, event_name)
        # We need to be careful about column overlaps.
        # Key columns: grid_id, event_name, polarization
        
        # Prepare Diff DF
        # Keep: grid_id, event_name, delay_hours, total_precip_mm, max_intensity_mm_h, duration_hours
        #       paddy_diff_mean, paddy_diff_std, road_diff_mean, road_diff_std
        
        # Rename strictly to avoid collision if needed, but usually they are distinct
        
        # Prepare Sigma DF
        # Keep: grid_id, event_name
        #       paddy_before_mean, paddy_after_mean, road_before_mean, road_after_mean
        
        if df_sigma.empty:
            print("No sigma data found, using only diff.")
            merged = df_diff
        else:
            # Drop potential duplicate cols from sigma (like polarization, month if they exist)
            cols_to_use = ['grid_id', 'event_name', 
                           'paddy_before_mean', 'paddy_after_mean', 
                           'road_before_mean', 'road_after_mean']
            
            # Filter sigma df to only have these cols
            df_sigma_clean = df_sigma[[c for c in cols_to_use if c in df_sigma.columns]]
            
            # Merge
            merged = pd.merge(df_diff, df_sigma_clean, on=['grid_id', 'event_name'], how='left')
        
        merged['polarization'] = pol
        final_dfs.append(merged)
    
    if not final_dfs:
        print("No data processed.")
        return

    full_df = pd.concat(final_dfs, ignore_index=True)
    
    # Calculate Separability (Road Diff - Paddy Diff)
    if 'road_diff_mean' in full_df.columns and 'paddy_diff_mean' in full_df.columns:
        full_df['separability_diff'] = full_df['road_diff_mean'] - full_df['paddy_diff_mean']
        
    # Organize columns
    # Move identifiers to front
    front_cols = ['grid_id', 'event_name', 'polarization', 'delay_hours', 
                  'total_precip_mm', 'max_intensity_mm_h', 'duration_hours']
    
    # Remaining columns
    other_cols = [c for c in full_df.columns if c not in front_cols]
    final_cols = front_cols + sorted(other_cols)
    
    # Filter only existing columns
    final_cols = [c for c in final_cols if c in full_df.columns]
    
    full_df = full_df[final_cols]
    
    save_path = os.path.join(OUTPUT_DIR, "comprehensive_event_analysis.csv")
    full_df.to_csv(save_path, index=False)
    print(f"\nSaved comprehensive analysis to: {save_path}")
    print(f"Total Rows: {len(full_df)}")

if __name__ == "__main__":
    export_comprehensive()
