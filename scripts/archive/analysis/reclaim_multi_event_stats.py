import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DETAILED_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"
S1_PAIRS_CSV = BASE_DIR / "data" / "analysis" / "s1_pairs.csv"

def reconcile_multi_event_grids():
    # Load data
    df_pixels = pd.read_csv(DETAILED_CSV)
    df_pairs = pd.read_csv(S1_PAIRS_CSV)
    
    # Filter October pairs to find grids with >= 2 unique Before scenes
    df_pairs['month'] = pd.to_datetime(df_pairs['event_start_ts_utc']).dt.month
    oct_pairs = df_pairs[df_pairs['month'] == 10]
    grid_counts = oct_pairs.groupby('grid_id')['before_scene_id'].nunique()
    multi_event_grid_ids = grid_counts[grid_counts >= 2].index
    
    # Filter detailed pixel counts for these grids in October
    oct_df = df_pixels[(df_pixels['month'] == 10) & (df_pixels['grid_id'].isin(multi_event_grid_ids))]
    
    # Get unique geographical stats for these grids
    unique_grid_stats = oct_df.groupby('grid_id').first()[['road_pixels', 'paddy_pixels']]
    
    n_multi = len(unique_grid_stats)
    total_road = unique_grid_stats['road_pixels'].sum()
    total_paddy = unique_grid_stats['paddy_pixels'].sum()
    
    print(f"--- 10月 複数イベント(Before画像)を持つグリッドの統計 ---")
    print(f"対象グリッド数: {n_multi}")
    print(f"道路ピクセル合計: {total_road:,}")
    print(f"水田ピクセル合計: {total_paddy:,}")

if __name__ == "__main__":
    reconcile_multi_event_grids()
