# ALOS-2 SAR 浸水判別

`D:\shuron\GT-data` の手動浸水推定データ（`wajima_*_shp`）と ALOS-2 SAR 画像から、被災前後の後方散乱強度低下に基づく浸水候補域を作成します。

## 実行

```powershell
cd D:\shuron\flood_detection_sar
.\run_detection.ps1
```

または直接:

```powershell
python .\sar_flood_detection.py --data-dir D:\shuron\GT-data --output-dir D:\shuron\flood_detection_sar\output
```

## 判定方法

1. 被災前（240811/240812）と被災後（240922/240923）の同一フレームをペア化します。
2. SAR の DN を `20*log10(DN) - 83` で dB に変換します。
3. 被災後画像を被災前画像のグリッドへ再投影し、`post_db - pre_db` を計算します。
4. 各 `inputarea.shp` 内で、手動浸水域 `floodarea.shp` と最も整合する負のしきい値を探索します。
5. `delta_db <= threshold` を浸水候補とし、100 m2 未満の小ポリゴンを除外します。

## 主な出力

- `output/summary.csv`: しきい値、面積、precision/recall/F1
- `output/report.md`: 解析レポート
- `output/*_delta_db.tif`: 後方散乱強度差分
- `output/*_detected_flood.tif`: 浸水候補マスク
- `output/*_detected_flood.geojson`: 浸水候補ポリゴン
- `output/detected_flood_merged.geojson`: 浸水候補ポリゴンの統合版
