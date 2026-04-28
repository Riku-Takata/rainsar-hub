"""Check which grids actually have valid data files"""
import sys
sys.path.insert(0, 'd:/sotsuron/rainsar-hub/scripts/thesis')
import common
from pathlib import Path

grids_with_data = []
grids_without_data = []
total_events_with_data = 0

for grid_dir in common.SAMPLES_DIR.iterdir():
    if not grid_dir.is_dir():
        continue
    
    grid_id = grid_dir.name
    has_valid_event = False
    events_in_grid = 0
    
    for event_dir in grid_dir.iterdir():
        if not event_dir.is_dir():
            continue
        
        # Check if TIF files exist
        after_vv = event_dir / "after_vv.tif"
        before_vv = event_dir / "before_vv.tif"
        
        if after_vv.exists() and before_vv.exists():
            has_valid_event = True
            events_in_grid += 1
    
    if has_valid_event:
        grids_with_data.append(grid_id)
        total_events_with_data += events_in_grid
    else:
        grids_without_data.append(grid_id)

print("=== 実データの確認結果 ===")
print(f"TIFファイルがあるグリッド数: {len(grids_with_data)}")
print(f"TIFファイルがないグリッド数: {len(grids_without_data)}")
print(f"有効イベント総数: {total_events_with_data}")
print()

if grids_without_data:
    print("データがないグリッド:")
    for g in grids_without_data[:10]:
        print(f"  - {g}")
    if len(grids_without_data) > 10:
        print(f"  ... 他 {len(grids_without_data) - 10} グリッド")
