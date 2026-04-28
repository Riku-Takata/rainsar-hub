import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DETAILED_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"

def reconcile():
    df = pd.read_csv(DETAILED_CSV)
    oct_df = df[df['month'] == 10]
    
    # Events
    n_events = len(oct_df)
    
    # Grids
    n_grids = oct_df['grid_id'].nunique()
    
    # Grid-level pixel counts (Unique geographical pixels)
    # We take the unique mask size for each grid id.
    # Note: different events in the same grid MUST have the same mask.
    unique_grid_pixels = oct_df.groupby('grid_id').first()[['road_pixels', 'paddy_pixels']]
    
    total_road_geo = unique_grid_pixels['road_pixels'].sum()
    total_paddy_geo = unique_grid_pixels['paddy_pixels'].sum()
    
    # Event-level pixel counts (Total samples used in classification across all events)
    total_road_event_sum = oct_df['road_pixels'].sum()
    total_paddy_event_sum = oct_df['paddy_pixels'].sum()
    
    print(f"--- 10月データ集計結果 ---")
    print(f"降雨イベント数: {n_events}")
    print(f"ユニークなグリッド数: {n_grids}")
    print(f"\n[地理的統計 (重複なし)]")
    print(f"  道路ピクセル合計 (454グリッド分): {total_road_geo:,}")
    print(f"  水田ピクセル合計 (454グリッド分): {total_paddy_geo:,}")
    print(f"\n[全イベントの延べ統計 (分析対象サンプル数)]")
    print(f"  道路ピクセル延べ合計 (713イベント分): {total_road_event_sum:,}")
    print(f"  水田ピクセル延べ合計 (713イベント分): {total_paddy_event_sum:,}")

if __name__ == "__main__":
    reconcile()
