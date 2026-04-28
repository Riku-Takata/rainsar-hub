"""
Analyze Data Count by Delay
- Inputs:
  - Nationwide Diff Stats CSV (data/result/diff/nationwide_diff_stats_thesis.csv)
  - OR Nationwide Sigma Stats (data/result/sigma/nationwide_stats_thesis.csv)
- Outputs:
  - Histogram of Event Counts by Delay (data/result/stats/delay_count_histogram.png)
  - CSV Summary (data/result/stats/delay_counts.csv)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import re
import logging

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
DIFF_CSV = BASE_DIR / "data" / "result" / "diff" / "nationwide_diff_stats_thesis.csv"
RESULT_DIR = BASE_DIR / "data" / "result" / "stats"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DelayCountAnalysis")

def main():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    
    if not DIFF_CSV.exists():
        logger.error(f"Diff CSV not found: {DIFF_CSV}")
        return

    logger.info(f"Loading {DIFF_CSV}")
    df = pd.read_csv(DIFF_CSV)
    
    # Extract Delay if not present (it should be present in diff stats now)
    if 'delay' not in df.columns:
        # Fallback to extraction from event_name
        logger.info("Extracting delay from event_name...")
        def extract_delay(name):
            m = re.search(r'delay_(\d+)h_', str(name))
            return int(m.group(1)) if m else -1
        df['delay'] = df['event_name'].apply(extract_delay)
        
    # Analyze Counts
    # We want number of EVENTS (unique grid_id + event_name) per Delay
    # Or number of samples? Usually "amount of data" means number of valid events available.
    
    # Filter valid delays
    df = df[df['delay'] >= 0]
    
    # Drop duplicates to count events (since diff stats has rows for Pol/Type)
    # We want unique events: (grid_id, event_name)
    unique_events = df[['grid_id', 'event_name', 'delay']].drop_duplicates()
    
    logger.info(f"Total Unique Events: {len(unique_events)}")

    # Plot Histogram
    plt.figure(figsize=(10, 6))
    counts, bins, patches = plt.hist(unique_events['delay'], bins=range(0, 14), color='skyblue', edgecolor='black', align='left', rwidth=0.8)
    plt.title("Number of Rain Events by Delay Hour")
    plt.xlabel("Delay (hours)")
    plt.ylabel("Count")
    plt.xticks(range(0, 13))
    plt.grid(axis='y', alpha=0.3)
    
    # Add count labels
    for i, count in enumerate(counts):
        if count > 0:
            plt.text(bins[i], count + (max(counts)*0.01), str(int(count)), ha='center', va='bottom')
            
    plt.savefig(RESULT_DIR / "delay_count_histogram.png")
    plt.close()
    
    # Save CSV
    count_df = unique_events['delay'].value_counts().sort_index().reset_index()
    count_df.columns = ['delay', 'count']
    count_df.to_csv(RESULT_DIR / "delay_counts.csv", index=False)
    
    print("\n=== Delay Counts ===")
    print(count_df)

if __name__ == "__main__":
    main()
