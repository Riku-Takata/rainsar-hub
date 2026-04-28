import os
import sys
import logging
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup paths
BASE_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = BASE_DIR / "backend"
OUTPUT_DIR = BASE_DIR / "data_vv" / "analysis" / "nationwide"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.append(str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

# DB Config
DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("export_nation")

def main():
    logger.info("Connecting to DB...")
    engine = create_engine(DATABASE_URL)

    query = """
        SELECT *
        FROM s1_pairs 
        WHERE source = 'cdse_nationwide_search'
    """
    
    logger.info("Reading data...")
    df = pd.read_sql(query, engine)
    
    if df.empty:
        logger.warning("No data found!")
        return

    logger.info(f"Loaded {len(df)} rows.")

    # 1. Export Raw Pairs
    pair_csv = OUTPUT_DIR / "nationwide_pairs.csv"
    df.to_csv(pair_csv, index=False)
    logger.info(f"Saved raw pairs to {pair_csv}")

    # 2. Grid Summary
    # Count, Min Delay, Max Delay, Avg Delay
    grid_groups = df.groupby('grid_id')
    grid_summary = grid_groups.agg(
        pair_count=('id', 'count'),
        min_delay=('delay_h', 'min'),
        max_delay=('delay_h', 'max'),
        mean_delay=('delay_h', 'mean')
    ).reset_index()
    
    # Sort by count desc
    grid_summary = grid_summary.sort_values('pair_count', ascending=False)
    
    summary_csv = OUTPUT_DIR / "nationwide_grid_summary.csv"
    grid_summary.to_csv(summary_csv, index=False)
    logger.info(f"Saved grid summary to {summary_csv}")
    
    # 3. Delay Histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df['delay_h'], bins=50, range=(0, 24), color='skyblue', edgecolor='black')
    plt.title("Distribution of Time Delays (Nationwide Search)")
    plt.xlabel("Delay (hours)")
    plt.ylabel("Count")
    plt.grid(True, alpha=0.5)
    
    hist_png = OUTPUT_DIR / "delay_histogram.png"
    plt.savefig(hist_png)
    logger.info(f"Saved histogram to {hist_png}")

    # 4. Generate Report Markdown
    report_md = OUTPUT_DIR / "nationwide_search_report.md"
    
    total_grids = df['grid_id'].nunique()
    total_pairs = len(df)
    
    # Filter for "High Usage" candidates (e.g., > 10 pairs)
    high_yield = grid_summary[grid_summary['pair_count'] >= 10]
    
    # Filter for "Short Delay" availability (e.g., min < 3h)
    quick_response = grid_summary[grid_summary['min_delay'] < 3.0]
    
    md_content = f"""# Nationwide Sentinel-1 Search Report

**Date**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Total Qualified Grids**: {total_grids}
- **Total Pairs Found**: {total_pairs}
- **Average Pairs per Grid**: {total_pairs / total_grids:.2f}

## Processing Output
- Raw Data: [`nationwide_pairs.csv`](./nationwide_pairs.csv)
- Grid Summary: [`nationwide_grid_summary.csv`](./nationwide_grid_summary.csv)
- Delay Distribution: ![Delay Histogram](./delay_histogram.png)

## Candidate Highlights

### Top 10 Grids by Volume
| Grid ID | Count | Min Delay (h) | Max Delay (h) |
| :--- | :---: | :---: | :---: |
"""
    for _, row in grid_summary.head(10).iterrows():
        md_content += f"| {row['grid_id']} | {row['pair_count']} | {row['min_delay']:.1f} | {row['max_delay']:.1f} |\n"

    md_content += "\n### Grids with Short Delays (< 3h) (Top 10 by count)\n"
    md_content += "| Grid ID | Count | Min Delay (h) | Mean Delay (h) |\n"
    md_content += "| :--- | :---: | :---: | :---: |\n"
    
    for _, row in quick_response.sort_values('pair_count', ascending=False).head(10).iterrows():
        md_content += f"| {row['grid_id']} | {row['pair_count']} | {row['min_delay']:.1f} | {row['mean_delay']:.1f} |\n"

    with open(report_md, 'w', encoding='utf-8') as f:
        f.write(md_content)
        
    logger.info(f"Saved report to {report_md}")

if __name__ == "__main__":
    main()
