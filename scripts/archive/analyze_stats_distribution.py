import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

def main():
    csv_path = Path(r"d:\sotsuron\rainsar-hub\result\distributions\aggregated_stats_difference.csv")
    if not csv_path.exists():
        print("CSV not found")
        return

    df = pd.read_csv(csv_path)
    
    # --- 1. Visualize Distribution of Stats (Mean, Std, Median) ---
    # We want to see if there are clear clusters or outliers
    
    # Calculate Median from 50% percentile
    df['Median'] = df['50%']
    
    # Plot Distribution of Mean, Median, Std for each Type
    fig, axes = plt.subplots(3, 2, figsize=(15, 15))
    
    metrics = ['Mean', 'Median', 'Std']
    for i, metric in enumerate(metrics):
        for j, t in enumerate(['道路', '田んぼ']):
            sub = df[df['Type'] == t]
            sns.histplot(sub[metric], kde=True, ax=axes[i, j], bins=30)
            axes[i, j].set_title(f"{t} - {metric} Distribution")
            
            # Calculate IQR for filtering
            Q1 = sub[metric].quantile(0.25)
            Q3 = sub[metric].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR
            
            axes[i, j].axvline(lower, color='r', linestyle='--', label='IQR Lower')
            axes[i, j].axvline(upper, color='r', linestyle='--', label='IQR Upper')
            axes[i, j].legend()
            
            print(f"[{t} - {metric}] IQR Range: {lower:.4f} ~ {upper:.4f}")

    plt.tight_layout()
    output_dir = Path(r"d:\sotsuron\rainsar-hub\result\distributions\analysis_v2")
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_dir / "stats_distribution.png")
    print(f"Saved distribution plot to {output_dir}")

    # --- 2. Define Filtering Criteria ---
    # We will use the IQR rule on 'Std' (to remove noisy grids) and 'Mean' (to remove bias outliers)
    # But we need to be careful not to remove the "Signal" (e.g. high mean due to rain)
    # So we should filter primarily on 'Std' (Noise) and maybe extreme 'Mean' outliers.
    
    print("\n=== OUTLIER DETECTION (Based on Std) ===")
    valid_grids = set(df['Grid'].unique())
    noise_grids = set()
    
    for t in ['道路', '田んぼ']:
        sub = df[df['Type'] == t]
        Q1 = sub['Std'].quantile(0.25)
        Q3 = sub['Std'].quantile(0.75)
        IQR = Q3 - Q1
        upper_limit = Q3 + 1.5 * IQR
        
        # We only care about UPPER limit for Std (too noisy)
        outliers = sub[sub['Std'] > upper_limit]
        print(f"[{t}] Std Upper Limit: {upper_limit:.4f}")
        print(f"  Found {len(outliers)} outliers.")
        if not outliers.empty:
            print(outliers[['Grid', 'Delay', 'Std']])
            noise_grids.update(outliers['Grid'].unique())

    print("\n=== OUTLIER DETECTION (Based on Mean/Median Consistency) ===")
    # If Road Mean is too far from 0, it's suspicious.
    road_sub = df[df['Type'] == '道路']
    road_mean_avg = road_sub['Mean'].mean()
    road_mean_std = road_sub['Mean'].std()
    road_limit_upper = road_mean_avg + 3 * road_mean_std
    road_limit_lower = road_mean_avg - 3 * road_mean_std
    
    print(f"[Road Mean] Limit: {road_limit_lower:.4f} ~ {road_limit_upper:.4f}")
    road_outliers = road_sub[(road_sub['Mean'] > road_limit_upper) | (road_sub['Mean'] < road_limit_lower)]
    if not road_outliers.empty:
        print(road_outliers[['Grid', 'Delay', 'Mean']])
        noise_grids.update(road_outliers['Grid'].unique())

    print(f"\nTotal Noise Grids Identified: {len(noise_grids)}")
    print(f"Noise Grids: {noise_grids}")
    
    with open(output_dir / "noise_grids.txt", "w") as f:
        for g in noise_grids:
            f.write(f"{g}\n")
    print(f"Saved noise grids list to {output_dir / 'noise_grids.txt'}")
    
    # Save Valid Data
    valid_df = df[~df['Grid'].isin(noise_grids)]
    valid_df.to_csv(output_dir / "valid_stats.csv", index=False, encoding='utf-8-sig')
    print(f"Saved valid stats to {output_dir / 'valid_stats.csv'}")

if __name__ == "__main__":
    main()
