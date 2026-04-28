import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

def main():
    csv_path = r"d:\sotsuron\rainsar-hub\data\final\analysis\evolution\evolution_data.csv"
    out_dir = Path(csv_path).parent
    
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        print("No CSV found.")
        return

    # Filter invalid
    df = df.dropna(subset=['road_diff_mean', 'paddy_diff_mean'])
    
    # 1. Melt for Seaborn
    # We want a unified dataframe with columns: [delay, type, diff]
    road = df[['delay_h', 'road_diff_mean']].copy()
    road['type'] = 'Road'
    road = road.rename(columns={'road_diff_mean': 'diff'})
    
    paddy = df[['delay_h', 'paddy_diff_mean']].copy()
    paddy['type'] = 'Paddy'
    paddy = paddy.rename(columns={'paddy_diff_mean': 'diff'})
    
    combined = pd.concat([road, paddy], ignore_index=True)
    
    # Plot
    plt.figure(figsize=(10, 6))
    
    # Scatter plot with regression line or just lineplot (which does mean + ci)
    # seaborn lineplot is great for this as it aggregates by x value
    sns.lineplot(data=combined, x='delay_h', y='diff', hue='type', style='type', 
                 markers=True, dashes=False, err_style='bars', ci=68, palette=['red', 'green'])
                 
    # Add a zero line
    plt.axhline(0, color='gray', linestyle='--', alpha=0.5)
    
    plt.title("Overall Backscatter Difference Evolution (All Grids)\n(Mean ± Standard Error)")
    plt.xlabel("Delay after Rainfall (hours)")
    plt.ylabel("Backscatter Difference (After - Before) [dB]")
    plt.grid(True, alpha=0.3)
    
    save_path = out_dir / "overall_evolution.png"
    plt.savefig(save_path)
    print(f"Saved plot to {save_path}")
    
    # Also save a binned version for clarity
    plt.figure(figsize=(8, 5))
    bins = [0, 3, 6, 9, 24]
    labels = ["0-3h", "3-6h", "6-9h", "9h+"]
    combined['bin'] = pd.cut(combined['delay_h'], bins=bins, labels=labels, right=False)
    
    sns.pointplot(data=combined, x='bin', y='diff', hue='type', capsize=0.1, palette=['red', 'green'], dodge=True)
    plt.axhline(0, color='gray', linestyle='--', alpha=0.5)
    plt.title("Binned Evolution Trend (All Grids)")
    plt.xlabel("Delay Bin")
    plt.ylabel("Mean Difference (dB)")
    plt.grid(True, alpha=0.3)
    
    save_path_bin = out_dir / "overall_evolution_binned.png"
    plt.savefig(save_path_bin)
    print(f"Saved binned plot to {save_path_bin}")

if __name__ == "__main__":
    main()
