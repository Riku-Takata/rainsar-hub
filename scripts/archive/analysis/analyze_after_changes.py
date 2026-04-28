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

OUTPUT_DIR = RESULT_DIR / "20251212" / "after_changes"
STATS_FILE = RESULT_DIR / "20251212" / "after_stats.csv"

def main():
    logger = setup_logger("after_change_analysis")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_stats = []
    
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        events = parse_summary_txt(grid_id)
        if not events: continue
        
        for evt in events:
            # We care about the CHANGE (After - Before)
            # So we must pair them up for each event.
            
            # Key: grid_id, date, type
            
            before_s = evt['before_scene']
            after_s = evt['after_scene']
            
            # Process Road and Paddy
            for type_name, suffix in [('Road', '_highway_mask.tif'), ('Paddy', '_paddy_mask.tif')]:
                path_before = S1_SAMPLES_DIR / grid_id / f"{before_s}_proc{suffix}"
                path_after = S1_SAMPLES_DIR / grid_id / f"{after_s}_proc{suffix}"
                
                val_b = read_linear_values(path_before)
                val_a = read_linear_values(path_after)
                
                if val_b is None or val_a is None:
                    continue
                    
                # Trim to min length (usually same)
                min_n = min(len(val_b), len(val_a))
                val_b = val_b[:min_n]
                val_a = val_a[:min_n]
                
                # Convert to dB
                db_b = linear_to_db(val_b)
                db_a = linear_to_db(val_a)
                
                # Calculate Change in dB
                # Change = After_dB - Before_dB
                diff_db = db_a - db_b
                
                # Stats
                all_stats.append({
                    'GridID': grid_id,
                    'EventDate': evt['date'],
                    'Delay': evt.get('delay', -1),
                    'Type': type_name,
                    'Mean_Before': np.mean(db_b),
                    'Mean_After': np.mean(db_a),
                    'Mean_Diff': np.mean(diff_db),
                    'Median_Before': np.median(db_b),
                    'Median_After': np.median(db_a),
                    'Median_Diff': np.median(diff_db),
                    'Std_Diff': np.std(diff_db),
                    'Count': min_n
                })
                
                # Plot Histogram for this event if it's significant?
                # Or just aggregate later?
                # Let's save a few examples?
                pass

    if not all_stats:
        logger.warning("No stats generated.")
        return
        
    df = pd.DataFrame(all_stats)
    df.to_csv(STATS_FILE, index=False, encoding='utf-8-sig')
    logger.info(f"Saved stats to {STATS_FILE}")
    
    # --- Visualization ---
    
    # 1. Distribution of Changes (Road vs Paddy)
    plt.figure(figsize=(10, 6))
    sns.kdeplot(df[df['Type']=='Paddy']['Median_Diff'], fill=True, label='Paddy Change (Median)', color='green', alpha=0.4)
    sns.kdeplot(df[df['Type']=='Road']['Median_Diff'], fill=True, label='Road Change (Median)', color='gray', alpha=0.4)
    plt.axvline(0, color='k', linestyle='--')
    plt.title("Distribution of Backscatter Change (After - Before)\nMedian per Event")
    plt.xlabel("Change in dB (After - Before)")
    plt.legend()
    plt.savefig(OUTPUT_DIR / "dist_change_db.png")
    plt.close()
    
    # 2. Scatter: Before vs After (Median)
    # plt.figure(figsize=(8, 8))
    # sns.scatterplot(data=df[df['Type']=='Paddy'], x='Median_Before', y='Median_After', label='Paddy', color='green', alpha=0.5)
    # sns.scatterplot(data=df[df['Type']=='Road'], x='Median_Before', y='Median_After', label='Road', color='gray', alpha=0.5)
    # plt.plot([-30, 0], [-30, 0], 'k--')
    # plt.title("Before vs After Median Intensity")
    # plt.axis('equal')
    # plt.grid(True)
    # plt.savefig(OUTPUT_DIR / "scatter_before_vs_after.png")
    # plt.close()
    
    # 3. Boxplot of Changes
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df, x='Type', y='Median_Diff', palette={'Road': 'gray', 'Paddy': 'green'})
    plt.axhline(0, color='k', linestyle='--')
    plt.title("Backscatter Change by Type (Median Diff)")
    plt.ylabel("Change (dB)")
    plt.savefig(OUTPUT_DIR / "boxplot_change.png")
    plt.close()

    # Summary
    summary = df.groupby('Type')[['Mean_Diff', 'Median_Diff', 'Std_Diff']].describe()
    summary.to_csv(OUTPUT_DIR / "change_summary.csv")
    print("\n=== Change Statistics (After - Before) ===")
    print(summary)
    
    # Interpretation
    paddy_change = df[df['Type']=='Paddy']['Median_Diff'].mean()
    road_change = df[df['Type']=='Road']['Median_Diff'].mean()
    
    print("\n=== Interpretation Hint ===")
    print(f"Avg Change (Road): {road_change:.2f} dB")
    print(f"Avg Change (Paddy): {paddy_change:.2f} dB")
    
    if paddy_change < road_change:
         print("-> Paddy decreases MORE (or increases LESS) than Road.")
    elif paddy_change > road_change:
         print("-> Paddy increases MORE (or decreases LESS) than Road.")

if __name__ == "__main__":
    main()
