# ALOS-2 SAR 浸水判別レポート

## 方法

- 被災前（240811/240812）と被災後（240922/240923）の同一フレームをペア化した。
- DN を `20*log10(DN) - 83` で後方散乱強度 dB に変換した。
- 被災後画像を被災前画像のグリッドへ再投影し、`delta_db = post_db - pre_db` を計算した。
- 各 `inputarea.shp` 内で `floodarea.shp` と最も整合する負のしきい値を探索し、`delta_db <= threshold` を浸水候補とした。
- 100 m2 未満の小ポリゴンは GeoJSON と最終マスクから除外した。

## 使用した SAR ペア

- frame 0760: IMG-HH-ALOS2552020760-240812-UBSL2.1GUA.tif -> IMG-HH-ALOS2558230760-240923-UBSL2.1GUA.tif
- frame 2860: IMG-HH-ALOS2551802860-240811-UBSR2.1GUD.tif -> IMG-HH-ALOS2558012860-240922-UBSR2.1GUD.tif
- frame 2870: IMG-HH-ALOS2551802870-240811-UBSR2.1GUD.tif -> IMG-HH-ALOS2558012870-240922-UBSR2.1GUD.tif

## 結果

| source | frame | threshold dB | manual m2 | detected m2 | precision | recall | f1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| wajima_city_hall | 2860 | -0.500 | 35427.1 | 17275.0 | 0.362 | 0.328 | 0.344 |
| wajima_highschool | 2860 | -0.500 | 97546.2 | 62943.8 | 0.273 | 0.306 | 0.288 |
| wajima_houshi_cho | 2860 | -0.500 | 93902.4 | 77718.8 | 0.257 | 0.343 | 0.294 |
| wajima_koise_cho | 2860 | -0.500 | 198965.8 | 47087.5 | 0.712 | 0.284 | 0.406 |
| wajima_takuda_machi | 2860 | -0.500 | 47016.6 | 35056.2 | 0.256 | 0.313 | 0.282 |

## 出力

- `*_delta_db.tif`: 後方散乱強度差分（被災後 - 被災前、dB）
- `*_detected_flood.tif`: 浸水候補マスク
- `*_detected_flood.geojson`: 浸水候補ポリゴン
- `summary.csv`: しきい値、面積、評価指標の一覧