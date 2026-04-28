import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Paths
DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
OUTPUT_FILE = OUTPUT_DIR / "monthly_event_counts.png"
OUTPUT_CSV = OUTPUT_DIR / "monthly_event_counts.csv"

def main():
    print("Starting analysis...")
    print("Starting analysis (validated events from pixel CSV)...")
    
    PIXEL_CSV = Path(r"D:\sotsuron\rainsar-hub\data\analysis\monthly_delay_pixel_counts_detailed.csv")
    
    if not PIXEL_CSV.exists():
        print(f"Error: {PIXEL_CSV} not found.")
        return

    df = pd.read_csv(PIXEL_CSV)
    
    print(f"Total validated events found: {len(df)}")
    
    if len(df) == 0:
        print("No events found.")
        return

    # 2. Count events per month
    monthly_counts = df['month'].value_counts().sort_index()
    print("\nMonthly Counts:")
    print(monthly_counts)
    
    # Save counts to CSV
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    monthly_counts.to_csv(OUTPUT_CSV, header=['count'])
    print(f"Counts saved to {OUTPUT_CSV}")

    # 3. Plot Histogram
    plt.figure(figsize=(10, 6))
    
    # Use seaborn for a nicer look
    sns.set_style("whitegrid")
    # Set Japanese Font
    plt.rcParams['font.family'] = 'Meiryo'
    
    # Ensure all months are represented (1-12) or at least the relevant ones
    # The user asked for specific months but let's show what we have
    
    ax = sns.barplot(x=monthly_counts.index, y=monthly_counts.values, color='skyblue')
    
    plt.title('月ごとの降雨イベント数', fontsize=16)
    plt.xlabel('月', fontsize=12)
    plt.ylabel('イベント数', fontsize=12)
    
    # Add count labels on top of bars
    for i, v in enumerate(monthly_counts.values):
        ax.text(i, v + 5, str(v), ha='center', fontsize=10)
        
    plt.tight_layout()
    plt.savefig(OUTPUT_FILE, dpi=300)
    print(f"Histogram saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
