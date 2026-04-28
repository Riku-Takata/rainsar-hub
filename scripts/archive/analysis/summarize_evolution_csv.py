import pandas as pd
import numpy as np

def main():
    csv_path = r"d:\sotsuron\rainsar-hub\data\final\analysis\evolution\evolution_data.csv"
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        print("No CSV found.")
        return

    # Filter out missing/NaN
    df = df.dropna(subset=['road_diff_mean', 'paddy_diff_mean'])
    
    # Define Bins
    # 0-3h (Short), 3-6h (Medium), 6-9h (Long), >9h (Very Long)
    bins = [0, 3, 6, 9, 24]
    labels = ["0-3h", "3-6h", "6-9h", "9+h"]
    df['delay_bin'] = pd.cut(df['delay_h'], bins=bins, labels=labels, right=False)
    
    print("--- Summary by Delay Bin ---")
    grouped = df.groupby('delay_bin', observed=False)[['road_diff_mean', 'paddy_diff_mean']].agg(['mean', 'std', 'count'])
    print(grouped)
    
    print("\n--- Overall Trend ---")
    print(f"Road Overall Mean: {df['road_diff_mean'].mean():.4f}")
    print(f"Paddy Overall Mean: {df['paddy_diff_mean'].mean():.4f}")
    
    # Check "Recovery" speed
    # Compare 0-3h vs 6-9h
    short = df[df['delay_h'] < 3]
    long = df[(df['delay_h'] >= 6) & (df['delay_h'] < 9)]
    
    print("\n--- Recovery Check (Short vs Long) ---")
    print(f"Road Short (<3h): {short['road_diff_mean'].mean():.4f}")
    print(f"Road Long (6-9h): {long['road_diff_mean'].mean():.4f}")
    print(f"Paddy Short (<3h): {short['paddy_diff_mean'].mean():.4f}")
    print(f"Paddy Long (6-9h): {long['paddy_diff_mean'].mean():.4f}")

if __name__ == "__main__":
    main()
