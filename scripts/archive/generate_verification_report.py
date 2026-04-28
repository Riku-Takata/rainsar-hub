"""
QGIS確認用レポート生成
前処理データの詳細情報とQGISでの確認手順を出力
"""
from pathlib import Path
import json

BASE_DIR = Path("d:/sotsuron/rainsar-hub")
EXPANDED_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
OUTPUT_FILE = BASE_DIR / "data" / "preprocess_verification_report.md"

def main():
    # Find processed files
    all_tiffs = list(EXPANDED_DIR.rglob("*.tif"))
    
    # Group by grid
    grid_data = {}
    for tiff in all_tiffs:
        grid_id = tiff.parent.parent.name
        event_name = tiff.parent.name
        
        if grid_id not in grid_data:
            grid_data[grid_id] = {}
        if event_name not in grid_data[grid_id]:
            grid_data[grid_id][event_name] = []
        
        grid_data[grid_id][event_name].append(tiff.name)
    
    # Generate report
    report = []
    report.append("# 前処理データ検証レポート\n")
    report.append(f"**生成日時**: 2026-01-12\n")
    report.append(f"**総TIFFファイル数**: {len(all_tiffs)}\n")
    report.append(f"**処理済みグリッド数**: {len(grid_data)}\n")
    report.append(f"**完了イベント数**: {len(all_tiffs) // 4}\n\n")
    
    report.append("## 処理状況\n\n")
    report.append("| グリッドID | イベント数 | ファイル数 |\n")
    report.append("|-----------|-----------|----------|\n")
    
    for grid_id in sorted(grid_data.keys())[:10]:
        events = grid_data[grid_id]
        file_count = sum(len(files) for files in events.values())
        report.append(f"| {grid_id} | {len(events)} | {file_count} |\n")
    
    if len(grid_data) > 10:
        report.append(f"| ... | ... | ... |\n")
        report.append(f"| **合計** | **{sum(len(e) for e in grid_data.values())}** | **{len(all_tiffs)}** |\n")
    
    report.append("\n## QGISでの確認手順\n\n")
    
    # Select sample for QGIS
    sample_grid = sorted(grid_data.keys())[0]
    sample_event = sorted(grid_data[sample_grid].keys())[0]
    sample_dir = EXPANDED_DIR / sample_grid / sample_event
    
    report.append(f"### サンプルデータ\n\n")
    report.append(f"**グリッド**: `{sample_grid}`\n")
    report.append(f"**イベント**: `{sample_event}`\n")
    report.append(f"**ディレクトリ**: `{sample_dir}`\n\n")
    
    report.append("### 手順\n\n")
    report.append("1. **QGISを起動**\n\n")
    report.append("2. **TIFFファイルを追加**\n")
    report.append(f"   - レイヤー → レイヤーを追加 → ラスタレイヤー\n")
    report.append(f"   - 以下のファイルを追加:\n")
    report.append(f"     - `{sample_dir / 'after_vv.tif'}`\n")
    report.append(f"     - `{sample_dir / 'after_vh.tif'}`\n")
    report.append(f"     - `{sample_dir / 'before_vv.tif'}`\n")
    report.append(f"     - `{sample_dir / 'before_vh.tif'}`\n\n")
    
    report.append("3. **マスクを追加**\n")
    mask_dir = MASKS_DIR / sample_grid
    report.append(f"   - レイヤー → レイヤーを追加 → ベクタレイヤー\n")
    report.append(f"   - 以下のGeoJSONを追加:\n")
    report.append(f"     - `{mask_dir / (sample_grid + '_motorway.geojson')}`\n")
    report.append(f"     - `{mask_dir / (sample_grid + '_paddy.geojson')}`\n\n")
    
    report.append("4. **確認項目**\n\n")
    report.append("   - [ ] TIFFファイルが正しく表示される\n")
    report.append("   - [ ] 後方散乱強度値が表示される（-30~10 dB程度）\n")
    report.append("   - [ ] VVとVHで値が異なる\n")
    report.append("   - [ ] BeforeとAfterで変化が見られる\n")
    report.append("   - [ ] マスク（道路・田んぼ）がTIFFと重なる\n")
    report.append("   - [ ] 座標系が一致している（EPSG:4326）\n\n")
    
    report.append("## 後方散乱強度の確認方法\n\n")
    report.append("1. レイヤーを右クリック → プロパティ\n")
    report.append("2. シンボロジー → レンダータイプ: 単バンド疑似カラー\n")
    report.append("3. 最小値: -30、最大値: 10 に設定\n")
    report.append("4. カラーランプ: Spectral（逆順）\n")
    report.append("5. 適用をクリック\n\n")
    
    report.append("## 期待される結果\n\n")
    report.append("- **道路**: 高い後方散乱強度（明るい色）\n")
    report.append("- **田んぼ（降雨前）**: 中程度の後方散乱強度\n")
    report.append("- **田んぼ（降雨後）**: やや低下した後方散乱強度\n")
    report.append("- **VV vs VH**: VHの方が全体的に低い値\n\n")
    
    report.append("## 次のステップ\n\n")
    report.append("前処理が完了したら:\n\n")
    report.append("1. マスク生成確認\n")
    report.append("2. 後方散乱強度分析スクリプト実装\n")
    report.append("3. 差分計算スクリプト実装\n")
    report.append("4. VV vs VH比較スクリプト実装\n")
    
    # Write report
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.writelines(report)
    
    print(f"検証レポート生成完了: {OUTPUT_FILE}")
    print(f"\nQGIS確認用サンプル:")
    print(f"  グリッド: {sample_grid}")
    print(f"  イベント: {sample_event}")
    print(f"  ディレクトリ: {sample_dir}")

if __name__ == "__main__":
    main()
