import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
import re
import sys

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
RESULT_DIR = BASE_DIR / "data" / "result" / "sigma"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("QuickStats")

def extract_delay(row):
    # Try from event_name column
    name = str(row.get('event_name', ''))
    m = re.search(r'delay_(\d+)h_', name)
    if m:
        return int(m.group(1))
    return -1

def main():
    logger.info("Scanning for stats.csv files...")
    all_files = list(RESULT_DIR.rglob("stats.csv"))
    logger.info(f"Found {len(all_files)} stats files.")
    
    if not all_files:
        logger.warning("No stats files found.")
        return

    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f)
            # Ensure event_name exists, if not use folder name
            if 'event_name' not in df.columns:
                df['event_name'] = f.parent.name
            
            # Ensure timing exists
            if 'timing' not in df.columns:
                # Infer from something? Hard without it.
                pass
                
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error reading {f}: {e}")
            
    if not dfs:
        return
        
    global_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows: {len(global_df)}")
    
    # Extract Delay
    global_df['delay'] = global_df.apply(extract_delay, axis=1)
    
    # Filter
    valid_df = global_df[global_df['delay'] >= 0].copy()
    logger.info(f"Valid rows (delay >= 0): {len(valid_df)}")
    
    output_dir = RESULT_DIR / "quick_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save Combined
    valid_df.to_csv(output_dir / "quick_all_stats.csv", index=False)
    
    # === Group by Delay ===
    print("\n=== Interim Global Statistics (By Delay & Timing) ===")
    
    # Group
    grouped = valid_df.groupby(['type', 'pol', 'timing', 'delay'])['mean'].agg(['mean', 'std', 'count']).reset_index()
    grouped.to_csv(output_dir / "delay_stats.csv", index=False)
    
    # Plot & Print
    for mtype in ['road', 'paddy']:
        for pol in ['vv', 'vh']:
            subset = grouped[(grouped['type'] == mtype) & (grouped['pol'] == pol)]
            if subset.empty: continue
            
            # Plot
            plt.figure(figsize=(10, 6))
            sns.lineplot(data=subset, x='delay', y='mean', hue='timing', style='timing', markers=True, dashes=False)
            plt.title(f"Backscatter Trend by Delay ({mtype.upper()} {pol.upper()})")
            plt.xlabel("Delay (hours)")
            plt.ylabel("Mean Backscatter (dB)")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(output_dir / f"trend_delay_{mtype}_{pol}.png")
            plt.close()
            
            print(f"\n--- {mtype.upper()} {pol.upper()} ---")
            print(f"{'Delay(h)':<10} {'After(dB)':<12} {'Before(dB)':<12} {'Diff(dB)':<10}")
            
            # Pivot for printing
            pivoted = subset.pivot(index='delay', columns='timing', values='mean')
            
            cols = pivoted.columns
            if 'after' in cols and 'before' in cols:
                for delay in sorted(pivoted.index):
                    after_val = pivoted.loc[delay, 'after']
                    before_val = pivoted.loc[delay, 'before']
                    # Handle NaN
                    if pd.isna(after_val) or pd.isna(before_val):
                        diff = float('nan')
                    else:
                        diff = after_val - before_val
                        
                    print(f"{delay:<10} {after_val:.2f}        {before_val:.2f}        {diff:+.2f}")
            else:
                print("(Missing timing data for comparison)")

    logger.info(f"\nSaved results to {output_dir}")

if __name__ == "__main__":
    main()
