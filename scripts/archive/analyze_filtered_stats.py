import pandas as pd
from pathlib import Path

def main():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 1000)

    csv_path = Path(r"d:\sotsuron\rainsar-hub\result\distributions\aggregated_stats_difference.csv")
    if not csv_path.exists():
        print("CSV not found")
        return

    df = pd.read_csv(csv_path)
    
    # --- 1. Define Blacklist (Noise) ---
    # Based on previous analysis
    blacklist_grids = ['N03355E13125'] # Consistently high road std
    
    # Specific outliers (Grid, Delay)
    # We will filter these out dynamically or hardcode if few
    # Road Extreme Mean: N03335E13095 (9.36h)
    # Paddy Extreme Mean: N03295E13185 (0.15h), N03375E13095 (10.23h)
    
    outliers = [
        ('N03335E13095', 9.36, '道路'),
        ('N03295E13185', 0.15, '田んぼ'),
        ('N03375E13095', 10.23, '田んぼ')
    ]

    print("=== REMOVED NOISY DATA ===")
    print(f"Blacklisted Grids (All Delays): {blacklist_grids}")
    print("Specific Outliers:")
    for o in outliers:
        print(f"  Grid: {o[0]}, Delay: {o[1]}, Type: {o[2]}")

    # --- 2. Filter Data ---
    # Remove blacklisted grids
    df_clean = df[~df['Grid'].isin(blacklist_grids)].copy()
    
    # Remove specific outliers
    for g, d, t in outliers:
        mask = (df_clean['Grid'] == g) & (df_clean['Delay'] == d) & (df_clean['Type'] == t)
        df_clean = df_clean[~mask]

    print(f"\nOriginal Count: {len(df)}")
    print(f"Clean Count: {len(df_clean)}")

    # --- 3. Re-Analyze Trends ---
    print("\n=== CLEAN DATA ANALYSIS (Delay vs Std) ===")
    bins = [0, 1, 3, 6, 12]
    labels = ['0-1h', '1-3h', '3-6h', '6-12h']
    
    for t in df_clean['Type'].unique():
        sub = df_clean[df_clean['Type'] == t].copy()
        sub['DelayBin'] = pd.cut(sub['Delay'], bins=bins, labels=labels)
        
        print(f"\n--- Type: {t} ---")
        # Aggregating Mean of Std (Stability) and Mean of Mean (Bias)
        grouped = sub.groupby('DelayBin', observed=True).agg({
            'Std': ['mean', 'std', 'count'],
            'Mean': ['mean', 'std']
        })
        print(grouped)

    # --- 4. Compare Road vs Paddy (Signal vs Noise) ---
    print("\n=== SIGNAL (Paddy) vs NOISE (Road) RATIO ===")
    # We want to see if Paddy variability is consistently higher than Road variability
    # Group by DelayBin and compare Mean Std
    
    paddy_stats = df_clean[df_clean['Type'] == '田んぼ'].copy()
    paddy_stats['DelayBin'] = pd.cut(paddy_stats['Delay'], bins=bins, labels=labels)
    p_grp = paddy_stats.groupby('DelayBin', observed=True)['Std'].mean()
    
    road_stats = df_clean[df_clean['Type'] == '道路'].copy()
    road_stats['DelayBin'] = pd.cut(road_stats['Delay'], bins=bins, labels=labels)
    r_grp = road_stats.groupby('DelayBin', observed=True)['Std'].mean()
    
    ratio = p_grp / r_grp
    print("\nRatio of Paddy Std / Road Std:")
    print(ratio)

if __name__ == "__main__":
    main()
