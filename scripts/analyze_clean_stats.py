import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
import sys

# Add current directory to sys.path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from common_utils import (
    setup_logger, parse_summary_txt, read_linear_values, linear_to_db,
    S1_SAMPLES_DIR, RESULT_DIR, TARGET_GRIDS
)

# Japanese font support
plt.rcParams['font.family'] = 'MS Gothic'

OUTPUT_DIR = RESULT_DIR / "20251212" / "clean_stats"
STATS_FILE = OUTPUT_DIR / "clean_stats.csv"

def sigmaclip(a, low=3.0, high=3.0):
    """
    Standard Sigma Clipping
    Iteratively removes elements outside mean +/- sigma * std.
    """
    c = np.asarray(a)
    delta = 1
    while delta:
        if c.size == 0:
            break
        c_mean = c.mean()
        c_std = c.std()
        size = c.size
        critlower = c_mean - c_std * low
        critupper = c_mean + c_std * high
        c = c[(c >= critlower) & (c <= critupper)]
        delta = size - c.size
    return c, c_mean, c_std

def main():
    logger = setup_logger("clean_stats_analysis")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_stats = []
    
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        events = parse_summary_txt(grid_id)
        if not events: continue
        
        # Track unique scenes for Before/After individual stats?
        # Actually simplest is to process by Event to match Difference calculation
        
        for evt in events:
            before_s = evt['before_scene']
            after_s = evt['after_scene']
            
            for type_name, suffix in [('Road', '_highway_mask.tif'), ('Paddy', '_paddy_mask.tif')]:
                path_before = S1_SAMPLES_DIR / grid_id / f"{before_s}_proc{suffix}"
                path_after = S1_SAMPLES_DIR / grid_id / f"{after_s}_proc{suffix}"
                
                val_b = read_linear_values(path_before)
                val_a = read_linear_values(path_after)
                
                if val_b is None or val_a is None:
                    continue
                
                min_n = min(len(val_b), len(val_a))
                val_b = val_b[:min_n]
                val_a = val_a[:min_n]
                
                db_b = linear_to_db(val_b)
                db_a = linear_to_db(val_a)
                diff_db = db_a - db_b
                
                # --- Apply Filtering (Sigma-Clip) independently ---
                
                # 1. Before Intensity Filtered
                clean_b, mean_b, std_b = sigmaclip(db_b, 3.0, 3.0)
                median_b = np.median(clean_b) if clean_b.size > 0 else np.nan
                
                # 2. After Intensity Filtered
                clean_a, mean_a, std_a = sigmaclip(db_a, 3.0, 3.0)
                median_a = np.median(clean_a) if clean_a.size > 0 else np.nan
                
                # 3. Difference Filtered (Clip the DIFFERENCE distribution)
                clean_diff, mean_diff, std_diff = sigmaclip(diff_db, 3.0, 3.0)
                median_diff = np.median(clean_diff) if clean_diff.size > 0 else np.nan
                
                all_stats.append({
                    'GridID': grid_id,
                    'EventDate': evt['date'],
                    'BeforeID': before_s,
                    'AfterID': after_s,
                    'Type': type_name,
                    # Before Stats
                    'Mean_Before': mean_b,
                    'Median_Before': median_b,
                    'Std_Before': std_b,
                    # After Stats
                    'Mean_After': mean_a,
                    'Median_After': median_a,
                    'Std_After': std_a,
                    # Diff Stats
                    'Mean_Diff': mean_diff,
                    'Median_Diff': median_diff,
                    'Std_Diff': std_diff,
                    # Filter Info
                    'Count_Raw': len(diff_db),
                    'Count_Clean_Diff': len(clean_diff),
                    'Discard_Rate_Diff': 1 - (len(clean_diff)/len(diff_db))
                })
                
    if not all_stats:
        logger.warning("No stats.")
        return
        
    df = pd.DataFrame(all_stats)
    df.to_csv(STATS_FILE, index=False, encoding='utf-8-sig')
    logger.info(f"Saved clean stats to {STATS_FILE}")
    
    # --- Visualization & Analysis ---
    
    # 1. Overall Characteristics (Filtered Before)
    plt.figure(figsize=(10, 6))
    sns.kdeplot(df[df['Type']=='Paddy']['Median_Before'], fill=True, label='Paddy Before (Clean Median)', color='green', alpha=0.4)
    sns.kdeplot(df[df['Type']=='Road']['Median_Before'], fill=True, label='Road Before (Clean Median)', color='gray', alpha=0.4)
    plt.title("Filtered Backscatter Distribution (Before)\n3-Sigma Clipped Medians")
    plt.xlabel("Intensity (dB)")
    plt.legend()
    plt.savefig(OUTPUT_DIR / "clean_dist_before.png")
    plt.close()
    
    # 2. Seasonal Analysis (Filtered Before)
    df['EventDate'] = pd.to_datetime(df['EventDate'])
    df['Month'] = df['EventDate'].dt.month
    flooded_months = [5, 6, 7, 8]
    df['Season'] = df['Month'].apply(lambda x: 'Flooded (May-Aug)' if x in flooded_months else 'Non-Flooded (Sep-Apr)')
    
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='Season', y='Median_Before', hue='Type', palette={'Road': 'gray', 'Paddy': 'green'})
    plt.title("Seasonal Filtered Backscatter (Before)")
    plt.ylabel("Intensity (dB)")
    plt.savefig(OUTPUT_DIR / "clean_seasonal_before.png")
    plt.close()
    
    # 3. Change Analysis (Filtered Diff)
    plt.figure(figsize=(10, 6))
    sns.kdeplot(df[df['Type']=='Paddy']['Median_Diff'], fill=True, label='Paddy Change (Clean Median)', color='green', alpha=0.4)
    sns.kdeplot(df[df['Type']=='Road']['Median_Diff'], fill=True, label='Road Change (Clean Median)', color='gray', alpha=0.4)
    plt.axvline(0, color='k', linestyle='--')
    plt.title("Filtered Change Distribution (After - Before)\n3-Sigma Clipped Diff Median")
    plt.xlabel("Change (dB)")
    plt.legend()
    plt.savefig(OUTPUT_DIR / "clean_dist_change.png")
    plt.close()

    # Generate Pivot Summary for differences
    # Pivot to align Road/Paddy per event
    df_pivot = df.pivot_table(
        index=['GridID', 'EventDate', 'Season'], 
        columns='Type', 
        values=['Median_Before', 'Median_Diff']
    )
    df_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    
    # Compare Before Difference (Paddy - Road)
    df_pivot['Diff_Before_Type'] = df_pivot['Median_Before_Paddy'] - df_pivot['Median_Before_Road']
    
    # Compare Change Difference (Paddy - Road)
    # i.e. how much more did Paddy change?
    df_pivot['Relative_Change'] = df_pivot['Median_Diff_Paddy'] - df_pivot['Median_Diff_Road']

    summary_file = OUTPUT_DIR / "clean_pivot_summary.csv"
    df_pivot.to_csv(summary_file)
    
    # Print Key Stats
    print("\n=== Clean Analysis Summary (3-Sigma Clipped) ===")
    
    # 1. Before Characteristics
    print("\n[Before Intensity]")
    print(df.groupby(['Type', 'Season'])['Median_Before'].describe()[['mean', 'std', '50%']])
    
    # 2. Before Difference (Paddy - Road)
    print("\n[Before Difference (Paddy - Road)]")
    print(df_pivot.groupby('Season')['Diff_Before_Type'].describe()[['mean', 'std']])
    
    # 3. Change (After - Before)
    print("\n[Change (After - Before)]")
    print(df.groupby(['Type', 'Season'])['Median_Diff'].describe()[['mean', 'std', '50%']])

if __name__ == "__main__":
    main()
