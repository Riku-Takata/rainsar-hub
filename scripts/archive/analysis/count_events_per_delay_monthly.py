import os
import re
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# Paths
DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
OUTPUT_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
OUTPUT_CSV = OUTPUT_DIR / "monthly_delay_event_counts.csv"
OUTPUT_PLOT = OUTPUT_DIR / "monthly_delay_event_counts.png"

TARGET_MONTHS = [4, 9, 10]

def main():
    print("Starting validated event count analysis (from pixel CSV)...")
    
    # Load the pixel count detailed CSV (which contains only valid events with masks > 0)
    PIXEL_CSV = OUTPUT_DIR / "monthly_delay_pixel_counts_detailed.csv"
    
    if not PIXEL_CSV.exists():
        print(f"Error: {PIXEL_CSV} does not exist.")
        return

    df = pd.read_csv(PIXEL_CSV)
    
    # Filter target months
    df = df[df['month'].isin(TARGET_MONTHS)]
    
    print(f"Total validated events found: {len(df)}")
    
    if len(df) == 0:
        print("No valid events found for target months.")
        return

    # Count unique events per Month/Delay
    counts = df.groupby(['month', 'delay_int']).size().reset_index(name='count')
    # Rename delay_int to delay for compatibility
    counts = counts.rename(columns={'delay_int': 'delay'})
    
    # Save CSV
    counts.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved counts to {OUTPUT_CSV}")
    
    # Visualization
    # Setup plot with 1x3 subplots (one for each remaining month)
    plt.rcParams['font.family'] = 'Meiryo'
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 6), sharey=True)
    
    for i, month in enumerate(TARGET_MONTHS):
        ax = axes[i]
        monthly_data = counts[counts['month'] == month]
        
        # Ensure all delays 0-11 are present for consistent plotting
        full_delays = pd.DataFrame({'delay': range(0, 12)})
        plot_data = full_delays.merge(monthly_data, on='delay', how='left').fillna(0)
        
        sns.barplot(data=plot_data, x='delay', y='count', ax=ax, color='skyblue', edgecolor='black')
        
        ax.set_title(f"{month}月", fontsize=14)
        ax.set_xlabel("経過時間 (時間後)", fontsize=12)
        if i == 0:
            ax.set_ylabel("イベント数", fontsize=12)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add values on top
        for index, row in plot_data.iterrows():
            if row['count'] > 0:
                ax.text(index, row['count'] + 1, str(int(row['count'])), ha='center', fontsize=10)

    plt.tight_layout()
    fig.suptitle('月ごとの経過時間別イベント数', fontsize=16, y=1.05)
    plt.savefig(OUTPUT_PLOT, bbox_inches='tight', dpi=300)
    print(f"Saved plot to {OUTPUT_PLOT}")

if __name__ == "__main__":
    main()
