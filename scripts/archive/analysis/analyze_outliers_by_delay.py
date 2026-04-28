import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
import re
import numpy as np

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
RESULT_DIR = BASE_DIR / "data" / "result" / "sigma"
OUTLIER_DIR = BASE_DIR / "data" / "result" / "outliers"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("OutlierAnalysis")

def extract_delay(row):
    name = str(row.get('event_name', ''))
    m = re.search(r'delay_(\d+)h_', name)
    if m:
        return int(m.group(1))
    return -1

def main():
    OUTLIER_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info("Scanning for stats.csv files...")
    all_files = list(RESULT_DIR.rglob("stats.csv"))
    
    if not all_files:
        logger.warning("No stats files found.")
        return

    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f)
            if 'event_name' not in df.columns:
                df['event_name'] = f.parent.name
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error reading {f}: {e}")
            
    if not dfs:
        return
        
    global_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows: {len(global_df)}")
    
    # Extract Delay
    global_df['delay'] = global_df.apply(extract_delay, axis=1)
    
    # Filter valid delays
    df_valid = global_df[global_df['delay'] >= 0].copy()
    
    # Analyze by Type, Pol, Delay
    summary_stats = []
    
    # We only care about 'after' timing for outlier detection?
    # Or should we calculate for 'before' as well? 
    # Usually we want to detect outliers in the target observation (After), 
    # but 'before' stability is also important.
    # Let's calculate for both but focus on 'after'.
    
    grouped = df_valid.groupby(['type', 'pol', 'timing', 'delay'])
    
    for (mtype, pol, timing, delay), group in grouped:
        if len(group) < 5: continue # Skip if too few samples
        
        # Calculate IQR based on the distribution of STATISTICS (Mean/Median of events)
        # However, to estimate pixel-level thresholds, we ideally need pixel distributions.
        # But we only have stats.
        # Use Median of Medians/Percentiles for robustness
        median_q25 = group['q25'].median()
        median_q75 = group['q75'].median()
        median_val = group['median'].median()
        
        iqr = median_q75 - median_q25
        
        lower_fence = median_q25 - 1.5 * iqr
        upper_fence = median_q75 + 1.5 * iqr
        
        summary_stats.append({
            'type': mtype,
            'pol': pol,
            'timing': timing,
            'delay': delay,
            'median': median_val,
            'q25': median_q25,
            'q75': median_q75,
            'iqr': iqr,
            'lower_threshold': lower_fence,
            'upper_threshold': upper_fence,
            'count': len(group)
        })
        
    summary_df = pd.DataFrame(summary_stats)
    summary_df.to_csv(OUTLIER_DIR / "delay_outlier_thresholds.csv", index=False)
    
    # Plotting
    for mtype in ['road', 'paddy']:
        for pol in ['vv', 'vh']:
            subset = summary_df[(summary_df['type'] == mtype) & (summary_df['pol'] == pol)]
            if subset.empty: continue
            
            plt.figure(figsize=(12, 6))
            
            # Plot for After
            sub_after = subset[subset['timing'] == 'after']
            if not sub_after.empty:
                plt.plot(sub_after['delay'], sub_after['median'], label='Median (After)', color='blue', marker='o')
                plt.fill_between(sub_after['delay'], sub_after['lower_threshold'], sub_after['upper_threshold'], 
                                 color='blue', alpha=0.1, label='Normal Range (1.5 IQR)')
                plt.plot(sub_after['delay'], sub_after['lower_threshold'], linestyle='--', color='blue', alpha=0.5)
                plt.plot(sub_after['delay'], sub_after['upper_threshold'], linestyle='--', color='blue', alpha=0.5)

            # Plot for Before (Reference)
            sub_before = subset[subset['timing'] == 'before']
            if not sub_before.empty:
                plt.plot(sub_before['delay'], sub_before['median'], label='Median (Before)', color='gray', linestyle='--', marker='x')
                # plt.fill_between(...) # Maybe too messy if we plot range for before too
            
            plt.title(f"Outlier Thresholds by Delay (Median) ({mtype.upper()} {pol.upper()})")
            plt.xlabel("Delay (hours)")
            plt.ylabel("Backscatter (dB)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            plt.savefig(OUTLIER_DIR / f"thresholds_{mtype}_{pol}.png")
            plt.close()
            
            # Print Table for After
            print(f"\n=== Thresholds: {mtype.upper()} {pol.upper()} (After) ===")
            print(f"{'Delay':<6} {'Lower':<10} {'Median':<10} {'Upper':<10} {'IQR':<10}")
            if not sub_after.empty:
                for _, row in sub_after.sort_values('delay').iterrows():
                    print(f"{int(row['delay']):<6} {row['lower_threshold']:.2f}      {row['median']:.2f}      {row['upper_threshold']:.2f}      {row['iqr']:.2f}")

    logger.info(f"Saved outlier analysis to {OUTLIER_DIR}")

if __name__ == "__main__":
    main()
