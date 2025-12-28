import pandas as pd
from pathlib import Path
import numpy as np

def main():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    csv_path = Path(r"d:\sotsuron\rainsar-hub\result\distributions\aggregated_stats_difference.csv")
    if not csv_path.exists():
        print("CSV not found")
        return

    df = pd.read_csv(csv_path)
    
    print("=== NOISE ANALYSIS ===")
    
    # High Std Dev
    for t in df['Type'].unique():
        sub = df[df['Type'] == t]
        mean_std = sub['Std'].mean()
        std_std = sub['Std'].std()
        thresh = mean_std + 2 * std_std
        
        print(f"\n--- High Std Dev for {t} (Threshold: >{thresh:.4f}) ---")
        noisy_std = sub[sub['Std'] > thresh]
        if not noisy_std.empty:
            print(noisy_std[['Grid', 'Delay', 'Std']])
        else:
            print("None")

    # Extreme Mean
    for t in df['Type'].unique():
        sub = df[df['Type'] == t]
        mean_mean = sub['Mean'].mean()
        std_mean = sub['Mean'].std()
        upper = mean_mean + 2 * std_mean
        lower = mean_mean - 2 * std_mean
        
        print(f"\n--- Extreme Mean for {t} (Range: {lower:.4f} ~ {upper:.4f}) ---")
        noisy_mean = sub[(sub['Mean'] > upper) | (sub['Mean'] < lower)]
        if not noisy_mean.empty:
            print(noisy_mean[['Grid', 'Delay', 'Mean']])
        else:
            print("None")

    print("\n=== TREND ANALYSIS (Delay vs Std) ===")
    bins = [0, 1, 3, 6, 12]
    labels = ['0-1h', '1-3h', '3-6h', '6-12h']
    
    for t in df['Type'].unique():
        sub = df[df['Type'] == t].copy()
        sub['DelayBin'] = pd.cut(sub['Delay'], bins=bins, labels=labels)
        
        print(f"\n--- Type: {t} ---")
        # Use observed=True to silence warning
        grouped = sub.groupby('DelayBin', observed=True)['Std'].agg(['mean', 'count', 'std'])
        print(grouped)

if __name__ == "__main__":
    main()
