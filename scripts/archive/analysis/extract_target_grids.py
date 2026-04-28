"""
5-13イベントのグリッドを抽出

卒論分析用のグリッドリストを作成
"""

from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parents[2]
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
OUTPUT_DIR = BASE_DIR / "data"

def count_events(grid_dir):
    """完了イベント数をカウント"""
    if not grid_dir.is_dir():
        return 0
    
    count = 0
    for event_dir in grid_dir.iterdir():
        if not event_dir.is_dir():
            continue
        
        after_vv = event_dir / "after_vv.tif"
        before_vv = event_dir / "before_vv.tif"
        
        if after_vv.exists() and before_vv.exists():
            count += 1
    
    return count

def main():
    print("="*60)
    print("5-13イベントのグリッド抽出")
    print("="*60)
    
    grid_dirs = sorted([d for d in SAMPLES_DIR.iterdir() if d.is_dir()])
    
    grid_data = []
    for grid_dir in grid_dirs:
        events = count_events(grid_dir)
        if 5 <= events <= 13:
            grid_data.append((grid_dir.name, events))
    
    print(f"\n対象グリッド数: {len(grid_data)}")
    print(f"総イベント数: {sum(e for _, e in grid_data)}\n")
    
    # イベント数別の内訳
    from collections import Counter
    counts = Counter(e for _, e in grid_data)
    
    print("イベント数別グリッド数:")
    for count in sorted(counts.keys()):
        print(f"  {count}イベント: {counts[count]:3d}グリッド")
    
    # 保存
    output_file = OUTPUT_DIR / "analysis_target_grids.txt"
    with open(output_file, 'w') as f:
        for grid_id, events in sorted(grid_data):
            f.write(f"{grid_id}\n")
    
    print(f"\nグリッドリストを保存: {output_file}")
    print(f"\n次のステップ: 後方散乱強度分析スクリプトを実装")

if __name__ == "__main__":
    main()
