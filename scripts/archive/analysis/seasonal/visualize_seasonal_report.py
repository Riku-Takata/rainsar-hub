
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
STATS_CSV = BASE_DIR / "data" / "result" / "seasonal" / "seasonal_stats_all.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "report_plots"
TABLE_DIR = BASE_DIR / "data" / "result" / "seasonal" / "report_tables"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TABLE_DIR.mkdir(parents=True, exist_ok=True)

def main():
    if not STATS_CSV.exists():
        print(f"Stats CSV not found: {STATS_CSV}")
        return

    print("Loading stats...")
    df = pd.read_csv(STATS_CSV)
    
    # Delay Bin
    df['delay_bin'] = df['delay_h'].apply(lambda x: int(x))
    
    # 1. Aggregate for Tables
    # Group by Month, Delay, Type -> Sum Counts, Mean Sigma/Diff
    # Note: Mean of means is approx, but okay for report if weighted? 
    # Use sum(clean_pixel_count * clean_sigma_mean) / sum(clean_pixel_count) for true weighted mean?
    # Let's keep it simple: Counts are most important here.
    
    agg = df.groupby(['month', 'delay_bin', 'type', 'pol']).agg({
        'raw_pixel_count': 'sum',
        'clean_pixel_count': 'sum',
        'fude_count': 'sum',
        'grid_id': 'count'
    }).rename(columns={'grid_id': 'event_count'}).reset_index()
    
    agg.to_csv(TABLE_DIR / "seasonal_summary_counts.csv", index=False)
    
    # 2. Visualizations
    
    # Heatmap: Clean Pixel Counts by Month vs Delay
    for mtype in ['road', 'paddy']:
        subset = agg[agg['type'] == mtype]
        # Sum across pols? Or just take one (pixels should be same for VV/VH usually, unless masked differently? 
        # Actually our script calculates pixels for each pol iteration. 
        # The mask is the same, so pixel count should be identical per event.
        # So we should average or just take VV.
        # Let's pivot and sum (will double count if we sum VV+VH).
        # Better to filter pol='vv' for counts.
        
        subset_vv = subset[subset['pol'] == 'vv']
        
        pivot = subset_vv.pivot(index='delay_bin', columns='month', values='clean_pixel_count').fillna(0)
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(pivot, annot=True, fmt=',.0f', cmap="YlGnBu")
        plt.title(f"Clean {mtype.capitalize()} Pixels (VV) by Month & Delay")
        plt.ylabel("Delay Bin (h)")
        plt.xlabel("Month")
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / f"heatmap_clean_pixels_{mtype}.png")
        plt.close()
        
        # Fude Count Heatmap (Paddy only)
        if mtype == 'paddy':
            pivot_fude = subset_vv.pivot(index='delay_bin', columns='month', values='fude_count').fillna(0)
            plt.figure(figsize=(10, 8))
            sns.heatmap(pivot_fude, annot=True, fmt=',.0f', cmap="Greens")
            plt.title(f"Paddy Fude Count (VV) by Month & Delay")
            plt.ylabel("Delay Bin (h)")
            plt.xlabel("Month")
            plt.tight_layout()
            plt.savefig(OUTPUT_DIR / f"heatmap_fude_count.png")
            plt.close()

    # Bar Charts: Total Clean Pixels per Delay (Aggregated seasons)
    # Compare Road vs Paddy?
    # Or FacetGrid
    
    agg_delay = df.groupby(['delay_bin', 'type', 'pol'])['clean_pixel_count'].sum().reset_index()
    agg_delay = agg_delay[agg_delay['pol'] == 'vv']
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=agg_delay, x='delay_bin', y='clean_pixel_count', hue='type')
    plt.title("Total Clean Pixels by Delay (All Target Months)")
    plt.ylabel("Pixel Count")
    plt.xlabel("Delay Bin (h)")
    plt.yscale('log') # Paddy is much larger
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "bar_clean_pixels_log.png")
    plt.close()

    print(f"Visualizations saved to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
