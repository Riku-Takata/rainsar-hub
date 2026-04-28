import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
BEST_50_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_50_grids_v3.csv"
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"

def main():
    if not BEST_50_CSV.exists():
        print("V3 Selection not found.")
        return
        
    df_grids = pd.read_csv(BEST_50_CSV)
    target_grids = df_grids['grid_id'].tolist()
    
    df_pairs = pd.read_csv(PAIRS_CSV)
    
    # Filter for targets
    df_subset = df_pairs[df_pairs['grid_id'].isin(target_grids)].copy()
    print(f"Total events in Top 50: {len(df_subset)}")
    
    # Filter for Rain >= 10
    df_heavy = df_subset[df_subset['max_gauge_mm_h'] >= 10.0]
    print(f"Heavy Rain (>10mm/h) events: {len(df_heavy)}")
    
    # Group by grid
    stats = df_heavy.groupby('grid_id').size().reset_index(name='heavy_count')
    stats = stats.sort_values('heavy_count', ascending=False)
    
    print("\nTop Grids by Heavy Rain Count (within current Best 50):")
    print(stats.head(10))
    print(f"\nGrids with >= 5 heavy events: {len(stats[stats['heavy_count'] >= 5])}")
    print(f"Grids with >= 3 heavy events: {len(stats[stats['heavy_count'] >= 3])}")

if __name__ == "__main__":
    main()
