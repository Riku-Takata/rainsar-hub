import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import sys

# Add current directory to sys.path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from common_utils import setup_logger, RESULT_DIR

# Japanese font support
plt.rcParams['font.family'] = 'MS Gothic'

CLEAN_STATS_FILE = RESULT_DIR / "20251212" / "clean_stats" / "clean_stats.csv"
OUTPUT_DIR = RESULT_DIR / "20251212" / "grid_stats"

def main():
    if not CLEAN_STATS_FILE.exists():
        print(f"Error: {CLEAN_STATS_FILE} not found.")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(CLEAN_STATS_FILE)
    
    # 1. Pivot for Difference Calculation
    # We need to compare Road vs Paddy for SAME Grid/Event
    
    df_pivot = df.pivot_table(
        index=['GridID', 'EventDate', 'BeforeID', 'AfterID'], 
        columns='Type', 
        values=['Median_Before', 'Median_Diff', 'Mean_Diff']
    )
    df_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Calculate Metrics per Event
    df_pivot['Diff_Before_Type'] = df_pivot['Median_Before_Paddy'] - df_pivot['Median_Before_Road']
    df_pivot['Change_Paddy'] = df_pivot['Median_Diff_Paddy']
    df_pivot['Change_Road'] = df_pivot['Median_Diff_Road']
    
    # 2. Aggregation per GridID
    grid_stats = df_pivot.groupby('GridID').agg(
        Count=('EventDate', 'count'),
        
        # Before Difference (Paddy - Road)
        Before_Type_Diff_Mean=('Diff_Before_Type', 'mean'),
        Before_Type_Diff_Std=('Diff_Before_Type', 'std'),
        
        # Change (After - Before)
        Change_Paddy_Mean=('Change_Paddy', 'mean'),
        Change_Paddy_Std=('Change_Paddy', 'std'),
        Change_Road_Mean=('Change_Road', 'mean'),
        Change_Road_Std=('Change_Road', 'std')
    ).reset_index()
    
    grid_stats.to_csv(OUTPUT_DIR / "grid_summary_stats.csv", index=False, encoding='utf-8-sig')
    
    # 3. Visualization per Grid
    
    # A. Before Difference (Paddy - Road) per Grid (Boxplot)
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='GridID', y='Diff_Before_Type', data=df_pivot, palette='coolwarm')
    plt.axhline(-2.7, color='r', linestyle='--', label='Global Avg (-2.7 dB)')
    plt.title("Difference in Before Intensity (Paddy - Road) per Grid")
    plt.ylabel("Paddy - Road (dB)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "boxplot_diff_before_by_grid.png")
    plt.close()
    
    # B. Change (After - Before) per Grid (Barplot with Error bars)
    # We want grouped bar plot: GridID on X, Y is Change, Hue is Type (Road/Paddy)
    # We need the original long-form DF for this, simpler.
    
    plt.figure(figsize=(14, 6))
    sns.pointplot(x='GridID', y='Median_Diff', hue='Type', data=df, 
                  dodge=0.4, join=False, capsize=0.1, palette={'Road': 'gray', 'Paddy': 'green'})
    plt.axhline(0, color='k', linestyle='-')
    plt.title("Backscatter Change (After - Before) per Grid")
    plt.ylabel("Change (dB)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "pointplot_change_by_grid.png")
    plt.close()
    
    print("\n=== Per-Grid Analysis Completed ===")
    print(f"Summary saved to {OUTPUT_DIR}")
    print("\n[Grid Summary Head]")
    print(grid_stats.head())

if __name__ == "__main__":
    main()
