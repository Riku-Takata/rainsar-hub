"""
Analyze Seasonal Trends (Thesis)
- Inputs:
  - Nationwide Stats CSV (data/result/sigma/nationwide_stats_thesis.csv)
  - Nationwide Diff Stats CSV (data/result/diff/nationwide_diff_stats_thesis.csv) - Optional (if ready)
- Outputs:
  - Seasonal Analysis Plots (data/result/seasonal/plots/*.png)
  - Seasonal Summary CSV (data/result/seasonal/seasonal_stats.csv)
- Logic:
  1. Load CSVs.
  2. Extract Date from event_name (e.g., *20190721*).
  3. Define Seasons/Irrigation Periods.
     - Irrigation (Flooded): May - Aug
     - Non-Irrigation: Sep - Apr
  4. Compare Backscatter/Difference between periods.
  5. Generate Monthly Trend Plots and Boxplots.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
import re
import sys

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
SIGMA_CSV = BASE_DIR / "data" / "result" / "sigma" / "nationwide_stats_thesis.csv"
DIFF_CSV = BASE_DIR / "data" / "result" / "diff" / "nationwide_diff_stats_thesis.csv"
RESULT_DIR = BASE_DIR / "data" / "result" / "seasonal"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SeasonalAnalysis")

def extract_month(event_name):
    # event_name format: delay_6h_20190721
    m = re.search(r'(\d{8})', str(event_name))
    if m:
        date_str = m.group(1)
        return int(date_str[4:6])
    return -1

def get_period(month):
    if 5 <= month <= 8:
        return 'Irrigation (May-Aug)'
    elif month == -1:
        return 'Unknown'
    else:
        return 'Non-Irrigation (Sep-Apr)'

def analyze_dataset(df, name_prefix):
    if df.empty: return

    df['month'] = df['event_name'].apply(extract_month)
    df['period'] = df['month'].apply(get_period)
    
    # Filter valid months
    df = df[df['month'] != -1].copy()
    
    # 1. Monthly Trend
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='month', y='median', hue='type', style='pol', markers=True, dashes=False, errorbar='sd')
    plt.title(f"Monthly Trend of {name_prefix} (Median)")
    plt.xlabel("Month")
    plt.ylabel(f"{name_prefix} (dB)")
    plt.grid(True, alpha=0.3)
    plt.xticks(range(1, 13))
    plt.savefig(RESULT_DIR / f"{name_prefix}_monthly_trend.png")
    plt.close()
    
    # 2. Period Boxplot
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='period', y='median', hue='type')
    plt.title(f"Seasonal Comparison of {name_prefix} (Median)")
    plt.xlabel("Period")
    plt.ylabel(f"{name_prefix} (dB)")
    plt.grid(True, alpha=0.3)
    plt.savefig(RESULT_DIR / f"{name_prefix}_period_boxplot.png")
    plt.close()
    
    # 3. Save Stats
    summary = df.groupby(['type', 'pol', 'period'])['median'].describe()
    summary.to_csv(RESULT_DIR / f"{name_prefix}_seasonal_stats.csv")
    print(f"\n=== {name_prefix} Seasonal Stats ===")
    print(summary[['count', 'mean', 'std']])

def main():
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Analyze Absolute Backscatter (Sigma0)
    if SIGMA_CSV.exists():
        logger.info(f"Loading Sigma CSV: {SIGMA_CSV}")
        df_sigma = pd.read_csv(SIGMA_CSV)
        # We focus on 'before' stats for seasonal characteristics of land cover
        # But 'after' is also interesting. Let's process valid rows.
        # Ideally, we split by timing.
        
        if 'timing' in df_sigma.columns:
            for timing in ['before', 'after']:
                sub = df_sigma[df_sigma['timing'] == timing].copy()
                analyze_dataset(sub, f"Sigma0_{timing.upper()}")
        else:
            analyze_dataset(df_sigma, "Sigma0")
    else:
        logger.warning("Sigma CSV not found.")

    # 2. Analyze Difference (Diff)
    if DIFF_CSV.exists():
        logger.info(f"Loading Diff CSV: {DIFF_CSV}")
        df_diff = pd.read_csv(DIFF_CSV)
        analyze_dataset(df_diff, "Diff")
    else:
        logger.info("Diff CSV not found (Analysis running?). Skipping Diff analysis.")

if __name__ == "__main__":
    main()
