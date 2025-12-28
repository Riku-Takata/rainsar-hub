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

INPUT_FILE = RESULT_DIR / "20251212" / "before_stats.csv"
OUTPUT_DIR = RESULT_DIR / "20251212" / "seasonal_characteristics"

def main():
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found.")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(INPUT_FILE)
    
    # Convert EventDate to datetime
    df['EventDate'] = pd.to_datetime(df['EventDate'])
    df['Month'] = df['EventDate'].dt.month
    
    # Define Season (Flooded: 5-8, Non-Flooded: Others)
    # User asked to exclude 5-8.
    flooded_months = [5, 6, 7, 8]
    
    df_non_flooded = df[~df['Month'].isin(flooded_months)].copy()
    df_flooded = df[df['Month'].isin(flooded_months)].copy()
    
    print(f"Total Scenes: {len(df) // 2}")
    print(f"Non-Flooded Scenes (Excluding May-Aug): {len(df_non_flooded) // 2}")
    print(f"Flooded Scenes (May-Aug): {len(df_flooded) // 2}")
    
    # --- Analysis on Non-Flooded Data ---
    
    # Pivot
    df_pivot = df_non_flooded.pivot_table(
        index=['GridID', 'EventDate', 'SceneID'], 
        columns='Type', 
        values=['Mean', 'Median', 'Std', 'Count']
    )
    df_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Calculate Differences
    df_pivot['Diff_Mean'] = df_pivot['Mean_Paddy'] - df_pivot['Mean_Road']
    df_pivot['Diff_Median'] = df_pivot['Median_Paddy'] - df_pivot['Median_Road']
    
    # 1. Scatter Plot
    plt.figure(figsize=(8, 8))
    sns.scatterplot(data=df_pivot, x='Median_Road', y='Median_Paddy', hue='GridID', alpha=0.7)
    plt.plot([-30, 0], [-30, 0], 'k--', label='y=x')
    plt.title("Road vs Paddy Median Intensity (Sep - Apr)")
    plt.xlabel("Road Median (dB)")
    plt.ylabel("Paddy Median (dB)")
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "scatter_road_vs_paddy_non_flooded.png")
    plt.close()

    # 2. Difference Histogram
    plt.figure(figsize=(10, 6))
    sns.histplot(df_pivot['Diff_Median'], kde=True, bins=15)
    plt.axvline(0, color='k', linestyle='--')
    plt.title("Difference Distribution (Paddy - Road) [Sep - Apr]")
    plt.xlabel("Difference in Median (dB)")
    plt.savefig(OUTPUT_DIR / "dist_diff_median_non_flooded.png")
    plt.close()
    
    # Summary Stats
    summary = df_pivot[['Mean_Road', 'Mean_Paddy', 'Median_Road', 'Median_Paddy', 'Diff_Mean', 'Diff_Median']].describe()
    summary.to_csv(OUTPUT_DIR / "non_flooded_summary.csv")
    
    # --- Comparison with Flooded ---
    # Quick pivot for flooded
    df_pivot_f = df_flooded.pivot_table(
        index=['GridID', 'EventDate', 'SceneID'], 
        columns='Type', 
        values=['Mean', 'Median']
    )
    df_pivot_f.columns = [f"{col[0]}_{col[1]}" for col in df_pivot_f.columns]
    df_pivot_f['Diff_Median'] = df_pivot_f['Median_Paddy'] - df_pivot_f['Median_Road']
    
    summary_f = df_pivot_f[['Median_Road', 'Median_Paddy', 'Diff_Median']].describe()
    summary_f.to_csv(OUTPUT_DIR / "flooded_summary.csv")

    print("\n=== Non-Flooded Check (Sep-Apr) ===")
    print(summary)
    print("\n=== Flooded Check (May-Aug) For Comparison ===")
    print(summary_f)
    
    mean_diff_non = df_pivot['Diff_Median'].mean()
    mean_diff_flooded = df_pivot_f['Diff_Median'].mean()
    
    print("\n=== Interpretation Hint ===")
    print(f"Non-Flooded Avg Diff: {mean_diff_non:.2f} dB")
    print(f"Flooded Avg Diff:     {mean_diff_flooded:.2f} dB")

if __name__ == "__main__":
    main()
