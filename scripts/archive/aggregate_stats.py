import os
import pandas as pd
from pathlib import Path

def main():
    base_dir = Path(r"d:\sotsuron\rainsar-hub\result\distributions")
    all_stats = []

    # Walk through all grid directories
    for grid_dir in base_dir.iterdir():
        if not grid_dir.is_dir():
            continue
        
        grid_id = grid_dir.name
        
        # Walk through all event (delay) directories
        for event_dir in grid_dir.iterdir():
            if not event_dir.is_dir():
                continue
            
            delay_str = event_dir.name.replace('h', '')
            try:
                delay = float(delay_str)
            except ValueError:
                continue
                
            stats_file = event_dir / "stats_difference.csv"
            if not stats_file.exists():
                continue
            
            try:
                df = pd.read_csv(stats_file)
                # The csv has a column 'type' (田んぼ, 道路)
                # We want to extract rows for each type
                
                for _, row in df.iterrows():
                    entry = {
                        'Grid': grid_id,
                        'Delay': delay,
                        'Type': row['type'],
                        'Count': row['count'],
                        'Mean': row['mean'],
                        'Std': row['std'],
                        'Min': row['min'],
                        'Max': row['max'],
                        '25%': row['25%'],
                        '50%': row['50%'],
                        '75%': row['75%']
                    }
                    all_stats.append(entry)
            except Exception as e:
                print(f"Error reading {stats_file}: {e}")

    if not all_stats:
        print("No stats found.")
        return

    df_all = pd.DataFrame(all_stats)
    
    # Sort by Delay
    df_all = df_all.sort_values(by=['Delay', 'Grid'])
    
    # Save to CSV
    output_path = base_dir / "aggregated_stats_difference.csv"
    df_all.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Saved aggregated stats to {output_path}")
    
    # Print summary to stdout
    print("\n=== Aggregated Stats Summary ===")
    print(df_all.to_string())
    
    # Basic Analysis
    print("\n=== Analysis by Type ===")
    for t in df_all['Type'].unique():
        print(f"\nType: {t}")
        sub = df_all[df_all['Type'] == t]
        print(sub[['Delay', 'Mean', 'Std']].describe())

if __name__ == "__main__":
    main()
