import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
GRID_LIST_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_10_diverse_grids.csv"
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"

def main():
    print(f"Loading grids from {GRID_LIST_CSV}...")
    df_grids = pd.read_csv(GRID_LIST_CSV)
    target_grids = set(df_grids['grid_id'].unique())
    print(f"Targets: {len(target_grids)}")
    
    print(f"Loading pairs from {PAIRS_CSV}...")
    df_pairs = pd.read_csv(PAIRS_CSV)
    
    # 1. Filter by Grid only
    df_grid_only = df_pairs[df_pairs['grid_id'].isin(target_grids)]
    print(f"Events matching Grid ID only: {len(df_grid_only)}")
    
    # 2. Filter by Rain >= 10
    df_rain = df_grid_only[df_grid_only['max_gauge_mm_h'] >= 10.0]
    print(f"Events matching Grid + Rain>=10: {len(df_rain)}")
    
    # 3. Filter by Delay <= 12
    df_delay = df_grid_only[df_grid_only['delay_h'] <= 12.0]
    print(f"Events matching Grid + Delay<=12: {len(df_delay)}")
    
    # 4. Filter by Both (The Logic used in Download Script)
    df_final = df_pairs[
        (df_pairs['grid_id'].isin(target_grids)) &
        (df_pairs['max_gauge_mm_h'] >= 10.0) &
        (df_pairs['delay_h'] <= 12.0)
    ]
    print(f"Events matching ALL filters: {len(df_final)}")
    
    print("\n--- Breakdown by Grid (Final Set) ---")
    print(df_final.groupby('grid_id').size())

if __name__ == "__main__":
    main()
