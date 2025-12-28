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
OUTPUT_DIR = RESULT_DIR / "20251212" / "characteristics"

def main():
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found.")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(INPUT_FILE)
    
    # Pivot to compare Road vs Paddy per scene
    # Keys: GridID, EventDate, SceneID
    df_pivot = df.pivot_table(
        index=['GridID', 'EventDate', 'SceneID'], 
        columns='Type', 
        values=['Mean', 'Median', 'Std', 'Count']
    )
    
    # Flatten columns
    df_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    
    # Calculate Differences (Paddy - Road)
    df_pivot['Diff_Mean'] = df_pivot['Mean_Paddy'] - df_pivot['Mean_Road']
    df_pivot['Diff_Median'] = df_pivot['Median_Paddy'] - df_pivot['Median_Road']
    
    # 1. Distribution of Absolute Values (Overall)
    plt.figure(figsize=(10, 6))
    sns.kdeplot(df[df['Type']=='Paddy']['Median'], fill=True, label='Paddy Median', color='green', alpha=0.4)
    sns.kdeplot(df[df['Type']=='Road']['Median'], fill=True, label='Road Median', color='gray', alpha=0.4)
    plt.title("Distribution of Median Backscatter Intensity (All Scenes)")
    plt.xlabel("Backscatter Intensity (dB)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(OUTPUT_DIR / "dist_median_overall.png")
    plt.close()

    # 2. Scatter Plot: Road vs Paddy
    plt.figure(figsize=(8, 8))
    sns.scatterplot(data=df_pivot, x='Median_Road', y='Median_Paddy', hue='GridID', alpha=0.7)
    plt.plot([-30, 0], [-30, 0], 'k--', label='y=x')
    plt.title("Road vs Paddy Median Intensity")
    plt.xlabel("Road Median (dB)")
    plt.ylabel("Paddy Median (dB)")
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "scatter_road_vs_paddy.png")
    plt.close()

    # 3. Difference Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(df_pivot['Diff_Median'], kde=True, bins=20)
    plt.axvline(0, color='k', linestyle='--')
    plt.title("Difference Distribution (Paddy - Road)\nPositive = Paddy is brighter")
    plt.xlabel("Difference in Median (dB)")
    plt.savefig(OUTPUT_DIR / "dist_diff_median.png")
    plt.close()
    
    # 4. Summary Stats
    summary = df_pivot[['Mean_Road', 'Mean_Paddy', 'Median_Road', 'Median_Paddy', 'Diff_Mean', 'Diff_Median']].describe()
    summary.to_csv(OUTPUT_DIR / "characteristics_summary.csv")
    
    print("Analysis Completed.")
    print("=== Summary Statistics ===")
    print(summary)
    
    # Physical Interpretation Text
    mean_diff = df_pivot['Diff_Median'].mean()
    road_mean = df_pivot['Median_Road'].mean()
    paddy_mean = df_pivot['Median_Paddy'].mean()
    
    print("\n=== Interpretation Hint ===")
    print(f"Average Road Median: {road_mean:.2f} dB")
    print(f"Average Paddy Median: {paddy_mean:.2f} dB")
    print(f"Average Difference (Paddy - Road): {mean_diff:.2f} dB")
    
    if mean_diff > 0:
        print("-> Paddy fields tend to have HIGHER backscatter than Roads.")
    else:
        print("-> Paddy fields tend to have LOWER backscatter than Roads.")

if __name__ == "__main__":
    main()
