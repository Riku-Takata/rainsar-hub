import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json

# Load env from backend
env_path = r"D:\sotsuron\rainsar-hub\backend\.env"
load_dotenv(env_path)

DB_USER = os.getenv("DB_USER", "rainsar")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rainsar_pw")
DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("DB_PORT_HOST", "3307")
DB_NAME = os.getenv("DB_NAME", "rainsar_hub")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Input/Output paths
INPUT_JSON = r"D:\sotsuron\rainsar-hub\data\thesis_grids_final_filtered.json"
OUTPUT_DIR = r"D:\sotsuron\rainsar-hub\data\result\event_distribution"

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    # 1. Load Target Grids
    print(f"Loading grids from {INPUT_JSON}...")
    with open(INPUT_JSON, 'r', encoding='utf-8') as f:
        grids_data = json.load(f)
    
    target_grid_ids = [item['grid_id'] for item in grids_data]
    print(f"Loaded {len(target_grid_ids)} target grids.")

    # 2. Query Database
    print("Querying database for events...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # We want all valid events for these grids
            # Changed to include 0h delays as per user feedback and DB availability
            query = text("""
                SELECT 
                    grid_id, 
                    delay_h, 
                    after_start_ts_utc,
                    event_end_ts_utc,
                    max_gauge_mm_h
                FROM s1_pairs
                WHERE source = 'cdse_nationwide_search'
                AND delay_h >= 0 AND delay_h <= 12.0
                AND max_gauge_mm_h >= 10.0
                AND grid_id IN :grids
                AND before_scene_id IS NOT NULL
                ORDER BY event_end_ts_utc
            """)
            
            df = pd.read_sql(query, conn, params={"grids": target_grid_ids})
            
    except Exception as e:
        print(f"Database error: {e}")
        return

    print(f"Retrieved {len(df)} events.")
    
    if df.empty:
        print("No events found matching criteria.")
        return

    # 3. Process Data
    # Convert timestamps
    df['event_end_ts_utc'] = pd.to_datetime(df['event_end_ts_utc'])
    df['month'] = df['event_end_ts_utc'].dt.month

    # 4. Generate Statistics & CSVs

    # Monthly Counts
    monthly_counts = df['month'].value_counts().sort_index()
    monthly_counts_df = monthly_counts.reset_index()
    monthly_counts_df.columns = ['month', 'count']
    
    # Fill missing months with 0
    all_months = pd.DataFrame({'month': range(1, 13)})
    monthly_counts_df = pd.merge(all_months, monthly_counts_df, on='month', how='left').fillna(0)
    monthly_counts_df['count'] = monthly_counts_df['count'].astype(int)
    
    monthly_counts_csv = os.path.join(OUTPUT_DIR, "monthly_event_counts.csv")
    monthly_counts_df.to_csv(monthly_counts_csv, index=False)
    print(f"Saved: {monthly_counts_csv}")

    # Delay Statistics per Month
    delay_stats = df.groupby('month')['delay_h'].agg(['mean', 'median', 'std', 'min', 'max', 'count']).reset_index()
    delay_stats = pd.merge(all_months, delay_stats, on='month', how='left') # Keep structure even if empty months
    
    delay_stats_csv = os.path.join(OUTPUT_DIR, "monthly_delay_stats.csv")
    delay_stats.to_csv(delay_stats_csv, index=False)
    print(f"Saved: {delay_stats_csv}")

    # Raw values for checking
    raw_values_csv = os.path.join(OUTPUT_DIR, "delay_values.csv")
    df[['month', 'delay_h', 'grid_id', 'event_end_ts_utc']].to_csv(raw_values_csv, index=False)
    print(f"Saved: {raw_values_csv}")


    # 5. Visualizations
    
    # Plot 1: Monthly Event Counts (Bar)
    plt.figure(figsize=(10, 6))
    sns.barplot(data=monthly_counts_df, x='month', y='count', color='royalblue')
    plt.title('Number of Rainfall Events per Month')
    plt.xlabel('Month')
    plt.ylabel('Event Count')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plot_counts_path = os.path.join(OUTPUT_DIR, "plot_monthly_counts.png")
    plt.savefig(plot_counts_path)
    plt.close()
    print(f"Saved: {plot_counts_path}")

    # Plot 2: Delay Distribution per Month (Boxplot)
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='month', y='delay_h', palette='viridis')
    plt.title('Distribution of Delay (Hours) per Month')
    plt.xlabel('Month')
    plt.ylabel('Delay (h)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.ylim(0, 13) # Focus on 1-12h range
    plt.tight_layout()
    plot_delay_box_path = os.path.join(OUTPUT_DIR, "plot_monthly_delays_boxplot.png")
    plt.savefig(plot_delay_box_path)
    plt.close()
    print(f"Saved: {plot_delay_box_path}")

    # Plot 3: Delay Histograms (FacetGrid or separate)
    # Let's simple distribution of all delays first
    plt.figure(figsize=(10, 6))
    sns.histplot(df['delay_h'], bins=24, kde=True, color='teal')
    plt.title('Overall Delay Distribution (1-12h)')
    plt.xlabel('Delay (h)')
    plt.ylabel('Count')
    plt.grid(linestyle='--', alpha=0.5)
    plt.tight_layout()
    plot_delay_hist_path = os.path.join(OUTPUT_DIR, "plot_overall_delay_hist.png")
    plt.savefig(plot_delay_hist_path)
    plt.close()
    print(f"Saved: {plot_delay_hist_path}")
    
    # Summary
    print("\n--- Summary ---")
    print(f"Total Events: {len(df)}")
    print(f"Most Frequent Month: {monthly_counts_df.loc[monthly_counts_df['count'].idxmax()]['month']} (Count: {monthly_counts_df['count'].max()})")
    print("Done.")

if __name__ == "__main__":
    main()
