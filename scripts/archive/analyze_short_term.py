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
    
    # --- Filter Data (Short-term & Clean) ---
    # 1. Short-term: Delay <= 1.5h
    df_short = df[df['Delay'] <= 1.5].copy()
    
    # 2. Remove Blacklist (Noise)
    blacklist_grids = ['N03355E13125']
    df_short = df_short[~df_short['Grid'].isin(blacklist_grids)]
    
    # 3. Remove Specific Outliers
    # Paddy Extreme Mean: N03295E13185 (0.15h)
    df_short = df_short[~((df_short['Grid'] == 'N03295E13185') & (df_short['Delay'] == 0.15))]

    print(f"Analyzed {len(df_short)} short-term events (<= 1.5h).")

    # --- Compare Characteristics ---
    stats = []
    for t in ['道路', '田んぼ']:
        sub = df_short[df_short['Type'] == t]
        
        # Weighted Average by Count (to account for different grid sizes)
        total_count = sub['Count'].sum()
        weighted_mean = (sub['Mean'] * sub['Count']).sum() / total_count
        
        # Approximate Median/IQR by averaging (not perfect but indicative)
        avg_median = sub['50%'].mean()
        avg_iqr = (sub['75%'] - sub['25%']).mean()
        avg_std = sub['Std'].mean()
        
        stats.append({
            'Type': t,
            'Total Count': total_count,
            'Weighted Mean (Linear Diff)': weighted_mean,
            'Avg Median': avg_median,
            'Avg Std (Variability)': avg_std,
            'Avg IQR (Spread)': avg_iqr
        })
    
    res_df = pd.DataFrame(stats)
    print("\n=== Short-term Characteristics (Road vs Paddy) ===")
    print(res_df)
    
    # Calculate Difference
    road_mean = res_df[res_df['Type'] == '道路']['Weighted Mean (Linear Diff)'].values[0]
    paddy_mean = res_df[res_df['Type'] == '田んぼ']['Weighted Mean (Linear Diff)'].values[0]
    
    print(f"\nDifference (Paddy - Road): {paddy_mean - road_mean:.6f}")
    print(f"Ratio (Paddy / Road): {paddy_mean / road_mean:.2f}x")

if __name__ == "__main__":
    main()
